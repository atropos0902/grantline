package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"grantline/internal/engine"
	"grantline/internal/harness"
	"grantline/internal/lsn"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
)

// stick makes commits at the source wait for a standby that never answers. They
// are in the log, and no reader can see them.
func stick(t testing.TB) {
	t.Helper()
	set(t, `alter system set synchronous_standby_names = 'ghost'`)
}

func unstick(t testing.TB) {
	t.Helper()
	set(t, `alter system reset synchronous_standby_names`)
}

func set(t testing.TB, sql string) {
	t.Helper()
	ctx := context.Background()
	for _, q := range []string{sql, `select pg_reload_conf()`} {
		if err := srv.src.Exec(ctx, "postgres", q); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(150 * time.Millisecond)
}

// stuck starts statements that commit into the log and then hang, and returns a
// function that waits for them.
func stuck(t testing.TB, p *harness.Pair, sqls ...string) (wait func()) {
	t.Helper()
	done := make(chan error, len(sqls))
	for _, sql := range sqls {
		go func() {
			done <- srv.src.Exec(context.Background(), p.Name, sql)
		}()
	}
	for {
		var n int
		if err := p.Source.QueryRow(context.Background(), `select count(*) from pg_stat_activity where wait_event = 'SyncRep' and datname = $1`, p.Name).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if n == len(sqls) {
			break
		}
		select {
		case err := <-done:
			t.Fatalf("statement ended early: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}
	return func() {
		for range sqls {
			if err := <-done; err != nil {
				t.Error(err)
			}
		}
	}
}

// A revoke whose commit is in the log but not visible yet survives the chunk
// that was read without it.
func TestRevokeSurvivesChunk(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 1000) i`)
	ctx := context.Background()
	var r *run
	var wait func()
	var held lsn.LSN
	r = start(t, p, small, func(c *engine.Config, _ *pgtarget.Config) {
		c.Hooks.BeforeCopy = func(context.Context) error {
			stick(t)
			wait = stuck(t, p,
				`delete from grants where id % 10 = 0`,
				`update grants set state = 'revoked' where id % 10 = 1`,
				`begin; delete from grants where id % 10 = 2; insert into grants select i, 'u', 'again', 'revoked', null from generate_series(2, 1000, 10) i; commit`)
			var s string
			if err := p.Source.QueryRow(ctx, `select pg_current_wal_lsn()::text`).Scan(&s); err != nil {
				return err
			}
			held, _ = lsn.Parse(s)
			return nil
		}
		c.Hooks.ChunkStaged = func(ctx context.Context, _ *source.Table, _ source.ChunkRead) error {
			for r.e.Status().Applied < held { // the revokes are in the target before the chunk is
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(time.Millisecond):
				}
			}
			return nil
		}
	})
	r.ready()
	refused := r.e.Status().Refused
	unstick(t)
	wait()
	r.caughtUp()
	r.same("grants")
	if refused != 300 {
		t.Errorf("%d rows refused, want 300: the race wasn't set up", refused)
	}
}

// A commit that reached the log before the slot's start isn't in the stream.
// Copying has to wait until it is visible.
func TestCommitOlderThanSlot(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 100) i`)
	ctx := context.Background()
	open := func() func() {
		c, err := p.Source.Acquire(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, q := range []string{`set synchronous_commit = local`, `begin`, `select pg_current_xact_id()`} {
			if _, err := c.Exec(ctx, q); err != nil {
				t.Fatal(err)
			}
		}
		return func() {
			_, _ = c.Exec(ctx, `commit`)
			c.Release()
		}
	}
	phase := func(slot string) string {
		var w string
		_ = p.Source.QueryRow(ctx, `select coalesce(string_agg(wait_event, ','), '') from pg_stat_activity where application_name = $1`, "grantline "+slot).Scan(&w)
		return w
	}
	// The slot becomes usable in steps, each waiting for the transactions that
	// were open at the one before.
	endA := open()
	released := make(chan struct{})
	var wait func()
	go func() {
		defer close(released)
		for phase("grantline_"+p.Name) == "" || !strings.Contains(phase("grantline_"+p.Name), "transactionid") {
			time.Sleep(10 * time.Millisecond)
		}
		endB := open()
		endA()
		time.Sleep(700 * time.Millisecond)
		stick(t)
		wait = stuck(t, p, `delete from grants where id <= 50`)
		endB()
		time.Sleep(4 * time.Second)
		unstick(t)
		wait()
	}()
	r := start(t, p, small)
	r.ready()
	<-released
	r.caughtUp()
	r.same("grants")
}

// An update can arrive before its row, without the large values it didn't touch.
func TestUpdateBeforeItsRow(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i, repeat(i::text, 3000) from generate_series(1, 200) i`)
	var r *run
	ctx := context.Background()
	var after lsn.LSN
	r = start(t, p, small, func(c *engine.Config, _ *pgtarget.Config) {
		c.Hooks.AfterReading = func(ctx context.Context, _ *source.Table, _ string) error {
			if after != 0 {
				return nil
			}
			srcExec(t, p, `update grants set state = 'revoked' where id <= 40`)
			srcExec(t, p, `update grants set id = id + 5000 where id between 21 and 30`)
			srcExec(t, p, `delete from grants where id between 31 and 35`)
			var s string
			if err := p.Source.QueryRow(ctx, `select pg_current_wal_lsn()::text`).Scan(&s); err != nil {
				return err
			}
			after, _ = lsn.Parse(s)
			return nil
		}
		c.Hooks.ChunkStaged = func(ctx context.Context, _ *source.Table, _ source.ChunkRead) error {
			for r.e.Status().Applied < after {
				time.Sleep(time.Millisecond)
			}
			return nil
		}
	})
	r.ready()
	_ = ctx
	r.caughtUp()
	r.same("grants")
	if n := r.e.Status().Reread; n < 35 {
		t.Errorf("%d keys read again, want at least 35: the case wasn't set up", n)
	}
}

// A chunk that lands between a delete's check and the chunk's own check must
// not put the row back. The delete's transaction is held open in the target.
func TestChunkTakesItsPlaceInLine(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 400) i`)
	var once atomic.Bool
	prepared, release := make(chan struct{}), make(chan struct{})
	r := start(t, p, small, func(c *engine.Config, tc *pgtarget.Config) {
		c.ChunkRows, c.Copiers = 100000, 1
		c.Hooks.AfterReading = func(ctx context.Context, _ *source.Table, _ string) error {
			if once.CompareAndSwap(false, true) {
				srcExec(t, p, `delete from grants where id <= 100`)
			}
			return nil
		}
		c.Hooks.ChunkStaged = func(context.Context, *source.Table, source.ChunkRead) error {
			<-prepared
			return nil
		}
		tc.BeforeCommit = func(g *target.Group) {
			for _, x := range g.Txns {
				if x.Changes == 100 { // the delete, prepared and not committed
					close(prepared)
					select {
					case <-release:
					case <-time.After(1500 * time.Millisecond):
					}
				}
			}
		}
	})
	r.ready()
	close(release)
	r.caughtUp()
	r.same("grants")
}

// Work prepared by a worker that has been replaced doesn't commit.
func TestReplacedWorkerCantCommit(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	var hold atomic.Bool
	held, free := make(chan struct{}), make(chan struct{})
	old := start(t, p, small, func(_ *engine.Config, tc *pgtarget.Config) {
		tc.KeepOld = true
		tc.BeforeCommit = func(g *target.Group) {
			if hold.CompareAndSwap(true, false) {
				close(held)
				<-free
			}
		}
	})
	old.ready()
	hold.Store(true)
	srcExec(t, p, `insert into grants values (1, 'old', 'r', 'active', 1, null)`)
	<-held
	ctx := t.Context()
	nt, err := pgtarget.New(ctx, pgtarget.Config{DSN: p.TgtDS, Replication: p.Name, Lanes: 1, KeepOld: true})
	if err != nil {
		t.Fatal(err)
	}
	defer nt.Close()
	if _, err := nt.Open(ctx); err != nil {
		t.Fatal(err)
	}
	close(free)
	err = <-old.done
	old.done <- err
	if !errors.Is(err, target.ErrFenced) {
		t.Errorf("old worker ended with %v, want fenced", err)
	}
	var n int
	if err := p.Target.QueryRow(ctx, `select count(*) from grants`).Scan(&n); err != nil || n != 0 {
		t.Errorf("%d rows from the old worker, err %v", n, err)
	}
}

// A worker that dies after receiving a transaction and before committing it
// loses nothing.
func TestRestartLosesNothing(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	var fail atomic.Bool
	r := start(t, p, small, func(_ *engine.Config, tc *pgtarget.Config) {
		tc.BeforeCommit = func(*target.Group) {
			for fail.Load() {
				time.Sleep(10 * time.Millisecond)
			}
		}
	})
	r.ready()
	fail.Store(true)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 50) i`)
	time.Sleep(1500 * time.Millisecond) // long enough for a status report
	r.stop()
	fail.Store(false)
	<-r.done
	r.done <- nil
	r2 := start(t, p, small)
	r2.caughtUp()
	r2.same("grants")
}

// One transaction takes and releases a badge number, the next takes it on
// another row. The overlap test can't see the released value.
func TestFreedUniqueValue(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	r := start(t, p, small, func(c *engine.Config, tc *pgtarget.Config) { c.GroupMax, tc.Lanes = 1, 4 })
	r.ready()
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 64) i`)
	r.caughtUp()
	for round := range 30 {
		for i := 1; i <= 8; i++ {
			srcExec(t, p, fmt.Sprintf(`update grants set badge = null where id = %d`, i))
			srcExec(t, p, fmt.Sprintf(`update grants set badge = %d where id = %d`, 100000+i+1000*round, 8+i+(round%6)*8))
			srcExec(t, p, fmt.Sprintf(`begin; insert into grants values (%[1]d, 'x', 'r', 's', %[2]d, null); delete from grants where id = %[1]d; commit`, 500+i, 7000+i))
			srcExec(t, p, fmt.Sprintf(`update grants set badge = %d where id = %d`, 7000+i, i))
		}
	}
	r.caughtUp()
	r.same("grants")
	t.Logf("scheduler: %+v", r.e.Status().Sched)
}

// Commits reach the target in source order: a reader never sees a later
// transaction without an earlier one.
func TestReadersSeeSourceOrder(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	r := start(t, p, small, func(c *engine.Config, _ *pgtarget.Config) { c.GroupMax = 1 })
	r.ready()
	ctx := t.Context()
	var bad atomic.Int64
	done := make(chan struct{})
	go func() {
		defer close(done)
		for r.e.Status().Changes < 600 && ctx.Err() == nil {
			var n, top int
			if err := p.Target.QueryRow(ctx, `select count(*), coalesce(max(id), 0) from grants`).Scan(&n, &top); err == nil && n != top {
				bad.Add(1)
			}
		}
	}()
	for i := 1; i <= 600; i++ {
		filler := ""
		if i%5 == 0 { // slow ones, so a later one would overtake
			filler = strings.Repeat("x", 200_000)
		}
		srcExec(t, p, `insert into grants values ($1, 'u', 'r', 'active', null, $2)`, i, filler)
	}
	r.caughtUp()
	<-done
	r.same("grants")
	if n := bad.Load(); n > 0 {
		t.Errorf("a reader saw a gap %d times", n)
	}
}

func TestRolledBackSavepointInLargeTransaction(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	r := start(t, p, small, func(c *engine.Config, _ *pgtarget.Config) { c.SpillAt = 64 << 10 })
	r.ready()
	srcExec(t, p, `begin; insert into grants select i, 'keep', 'r', 'a', null, repeat('x', 200) from generate_series(1, 4000) i;
		savepoint s; insert into grants select i, 'drop', 'r', 'a', null, repeat('x', 200) from generate_series(10001, 14000) i; rollback to s;
		insert into grants select i, 'keep', 'r', 'a', null, repeat('x', 200) from generate_series(20001, 24000) i; commit`)
	srcExec(t, p, `begin; insert into grants select i, 'gone', 'r', 'a', null, repeat('x', 200) from generate_series(30001, 34000) i; rollback`)
	r.caughtUp()
	r.same("grants")
	if r.e.Status().Spilled == 0 {
		t.Error("nothing was spilled: the transaction was too small for the test")
	}
}

func TestSchemaChanges(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants values (1, 'a', 'b', 'active', 1, null)`)
	r := start(t, p, small)
	r.ready()
	srcExec(t, p, `alter table grants add column note text`)
	srcExec(t, p, `insert into grants values (2, 'a', 'b', 'active', 2, null, 'hello')`)
	r.caughtUp()
	r.same("grants")
	if w := r.e.Status().Waiting; w != "" {
		t.Fatalf("a nullable column waited for approval: %s", w)
	}
	srcExec(t, p, `alter table grants alter column badge type bigint`)
	srcExec(t, p, `insert into grants values (3, 'a', 'b', 'active', 30000000000, null, 'big')`)
	r.waitFor("approval", 10*time.Second, func() bool { return r.e.Status().Waiting != "" })
	sql := r.e.Status().Waiting
	if err := r.e.Approve("drop table grants"); err == nil {
		t.Error("another statement was accepted")
	}
	if err := r.e.Approve(sql); err != nil {
		t.Fatal(err)
	}
	r.caughtUp()
	r.same("grants")
}

// A slot the source gave up on is reported, never replaced.
func TestLostSlot(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 500) i`)
	r := start(t, p, small)
	r.ready()
	r.close()
	s, err := source.Open(t.Context(), p.SourceDSN, "grantline_"+p.Name, "grants_pub")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.DropSlot(t.Context()); err != nil {
		t.Fatal(err)
	}
	s.Close()
	srcExec(t, p, `delete from grants where id <= 10`)
	srcExec(t, p, `insert into grants values (600, 'u', 'r', 'active', 600, null)`)
	r2 := start(t, p, small)
	err = <-r2.done
	r2.done <- err
	if !errors.Is(err, source.ErrSlotLost) {
		t.Fatalf("got %v, want a lost slot", err)
	}
	nt, err := pgtarget.New(t.Context(), pgtarget.Config{DSN: p.TgtDS, Replication: p.Name, Lanes: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := nt.Recopy(t.Context()); err != nil {
		t.Fatal(err)
	}
	nt.Close()
	r3 := start(t, p, small)
	r3.ready()
	r3.caughtUp()
	r3.same("grants")
}

// A key moves without its large value. One chunk was read before and has the
// old key, the other was read after and has the new one, and both are in the
// target when the move arrives.
func TestKeyMovesBetweenChunks(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i, repeat(i::text, 3000) from generate_series(1, 150) i`)
	var r *run
	var once atomic.Bool
	r = start(t, p, func(c *engine.Config, _ *pgtarget.Config) {
		c.ChunkRows, c.Copiers = 100, 1
		c.Hooks.BeforeCopy = func(context.Context) error { r.e.Pause(); return nil }
		c.Hooks.ChunkStaged = func(context.Context, *source.Table, source.ChunkRead) error {
			if once.CompareAndSwap(false, true) { // the first chunk has been read
				srcExec(t, p, `update grants set id = id + 1000 where id <= 5`)
				srcExec(t, p, `update grants set state = 'revoked' where id in (1001, 1002)`)
				srcExec(t, p, `delete from grants where id = 1003`)
			}
			return nil
		}
	})
	r.waitFor("both chunks", 20*time.Second, func() bool { return r.e.Status().ChunksDone == 2 })
	r.e.Resume()
	r.ready()
	r.caughtUp()
	r.same("grants")
}

// A chunk can be read after a row has moved and gone, and land before the
// stream gets to the move. The move finds no row, and nothing is wrong.
func TestChunkNewerThanTheStream(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i, repeat(i::text, 3000) from generate_series(1, 50) i`)
	var r *run
	r = start(t, p, small, func(c *engine.Config, _ *pgtarget.Config) {
		c.Hooks.BeforeCopy = func(context.Context) error {
			r.e.Pause()
			srcExec(t, p, `update grants set id = id + 1000 where id <= 5`)
			srcExec(t, p, `update grants set state = 'x' where id in (1001, 6, 7)`)
			srcExec(t, p, `update grants set id = 2001 where id = 1001`)
			srcExec(t, p, `delete from grants where id in (2001, 1002, 6)`)
			return nil
		}
	})
	r.waitFor("the chunk", 20*time.Second, func() bool { return r.e.Status().ChunksDone == 1 })
	time.Sleep(300 * time.Millisecond) // long enough to finish the table too early
	r.e.Resume()
	r.ready()
	r.caughtUp()
	r.same("grants")
}
