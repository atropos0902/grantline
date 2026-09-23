package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/engine"
	"grantline/internal/load"
	"grantline/internal/lsn"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
)

// races makes revokes whose commits are in the log and not visible, reads chunks
// meanwhile, and lands them after the revokes. n counts rows refused: each is a
// revoke that a chunk read without it, with the chunk landing second.
func races(ctx context.Context, n int) (map[string]any, error) {
	s, stop, err := start(ctx, []string{"fsync=off"}, []string{"fsync=off"})
	if err != nil {
		return nil, err
	}
	defer stop()
	for _, p := range []*pgxpool.Pool{s.S, s.D} {
		if _, err := p.Exec(ctx, load.Schema); err != nil {
			return nil, err
		}
	}
	forced, returned, rounds := 0, 0, 0
	for ; forced < n; rounds++ {
		f, back, err := raceRound(ctx, s, rounds)
		if err != nil {
			return nil, fmt.Errorf("round %d: %w", rounds, err)
		}
		forced += f
		returned += back
		if rounds%10 == 0 {
			fmt.Fprintf(os.Stderr, "round %d: %d forced, %d returned\n", rounds, forced, returned)
		}
		if f == 0 {
			return nil, errors.New("a round forced no race")
		}
		if returned > 0 && n <= 1 {
			break
		}
	}
	return map[string]any{"forced": forced, "rounds": rounds, "revoked_grants_that_returned": returned}, nil
}

func raceRound(ctx context.Context, s *servers, round int) (forced, returned int, err error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	for _, q := range []string{`truncate grants, members`, `insert into grants select i, 'u' || i, 'r', 'active', i, null from generate_series(1, 1200) i`} {
		if _, err := s.S.Exec(ctx, q); err != nil {
			return 0, 0, err
		}
	}
	if _, err := s.D.Exec(ctx, `truncate grants, members`); err != nil {
		return 0, 0, err
	}
	set := func(q string) error {
		for _, q := range []string{q, `select pg_reload_conf()`} {
			if err := s.src.Exec(ctx, "postgres", q); err != nil {
				return err
			}
		}
		time.Sleep(100 * time.Millisecond)
		return nil
	}
	perm := rand.Perm(1200)[:120]
	var wg sync.WaitGroup
	var held lsn.LSN
	var e *engine.Engine
	name := fmt.Sprintf("race%d", round)
	tgt, err := pgtarget.New(ctx, pgtarget.Config{DSN: s.dst.DSN("postgres"), Replication: name, Lanes: 4, Journal: true})
	if err != nil {
		return 0, 0, err
	}
	defer tgt.Close()
	e = engine.New(engine.Config{Name: name, SourceDSN: s.src.DSN("postgres"), Publication: "grants_pub", ChunkRows: 300, Copiers: 3, Heartbeat: 50 * time.Millisecond,
		Hooks: engine.Hooks{
			BeforeCopy: func(ctx context.Context) error {
				if err := set(`alter system set synchronous_standby_names = 'ghost'`); err != nil {
					return err
				}
				for t := range 6 {
					ids := make([]string, 20)
					for i := range ids {
						ids[i] = fmt.Sprint(1 + perm[t*20+i])
					}
					in := strings.Join(ids, ",")
					sql := []string{
						`delete from grants where id in (` + in + `)`,
						`update grants set state = 'revoked' where id in (` + in + `)`,
						`begin; delete from grants where id in (` + in + `); insert into grants select unnest(array[` + in + `]), 'again', 'r', 'revoked', null; commit`,
					}[t%3]
					wg.Go(func() { _ = s.src.Exec(context.WithoutCancel(ctx), "postgres", sql) })
				}
				for {
					var k int
					if err := s.S.QueryRow(ctx, `select count(*) from pg_stat_activity where wait_event = 'SyncRep'`).Scan(&k); err != nil {
						return err
					}
					if k == 6 {
						break
					}
					time.Sleep(5 * time.Millisecond)
				}
				var pos string
				if err := s.S.QueryRow(ctx, `select pg_current_wal_lsn()::text`).Scan(&pos); err != nil {
					return err
				}
				held, _ = lsn.Parse(pos)
				return nil
			},
			ChunkStaged: func(ctx context.Context, _ *source.Table, _ source.ChunkRead) error {
				for e.Status().Applied < held {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(time.Millisecond):
					}
				}
				return nil
			},
		}}, tgt)
	rctx, rstop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- e.Run(rctx) }()
	defer func() {
		rstop()
		<-done
		_ = set(`alter system reset synchronous_standby_names`)
		wg.Wait()
		if src, err := source.Open(ctx, s.src.DSN("postgres"), "grantline_"+name, "grants_pub"); err == nil {
			_ = src.DropSlot(ctx)
			src.Close()
		}
	}()
	for e.Status().Phase != target.Ready {
		select {
		case <-ctx.Done():
			return 0, 0, fmt.Errorf("never ready: %w", ctx.Err())
		case err := <-done:
			done <- err
			return 0, 0, err
		case <-time.After(5 * time.Millisecond):
		}
	}
	forced = int(e.Status().Refused)
	if err := set(`alter system reset synchronous_standby_names`); err != nil {
		return 0, 0, err
	}
	wg.Wait()
	var pos string
	if err := s.S.QueryRow(ctx, `select pg_current_wal_lsn()::text`).Scan(&pos); err != nil {
		return 0, 0, err
	}
	end, _ := lsn.Parse(pos)
	for e.Status().Applied < end {
		select {
		case <-ctx.Done():
			return forced, 0, fmt.Errorf("target at %s, want %s: %w", e.Status().Applied, end, ctx.Err())
		case err := <-done:
			done <- err
			return forced, 0, err
		case <-time.After(5 * time.Millisecond):
		}
	}
	rows, err := s.S.Query(ctx, `select id from grants where state = 'active'`)
	if err != nil {
		return 0, 0, err
	}
	active := map[int64]bool{}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		active[id] = true
	}
	rows, err = s.D.Query(ctx, `select id from grants where state = 'active'`)
	if err != nil {
		return 0, 0, err
	}
	for rows.Next() {
		var id int64
		_ = rows.Scan(&id)
		if !active[id] {
			returned++
		}
	}
	if returned == 0 {
		if err := same(ctx, s.S, s.D); err != nil {
			return forced, 0, err
		}
	}
	if forced == 0 { // the log position guard refuses nothing, so count what it let back in
		forced = returned
	}
	return forced, returned, nil
}

// kills ends workers with SIGKILL: half of them while tables are being copied,
// half while changes stream. After each one a new worker finishes and both sides
// are compared.
func kills(ctx context.Context, n int) (map[string]any, error) {
	s, stop, err := start(ctx, []string{"fsync=off"}, []string{"fsync=off"})
	if err != nil {
		return nil, err
	}
	defer stop()
	dir, err := os.MkdirTemp("", "kills")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	if _, err := s.S.Exec(ctx, load.Schema); err != nil {
		return nil, err
	}
	if err := load.Fill(ctx, s.S, 150000); err != nil {
		return nil, err
	}
	m := &load.Mix{Pool: s.S, Rows: 150000, Workers: 3, Pause: 2 * time.Millisecond}
	copying, streaming, inCopy := 0, 0, 0
	prev := ""
	res := func() map[string]any {
		return map[string]any{"kills": copying + streaming, "kills_in_copying": copying, "before_copy_was_ready": inCopy, "kills_in_streaming": streaming, "changes": m.Changes.Load()}
	}
	for i := range n {
		fresh := i < n/2
		name := "streaming"
		if fresh {
			name = fmt.Sprintf("copy%d", i)
			if prev != "" {
				src, err := source.Open(ctx, s.src.DSN("postgres"), "grantline_"+prev, "grants_pub")
				if err != nil {
					return res(), err
				}
				if err := src.DropSlot(ctx); err != nil {
					return res(), err
				}
				src.Close()
			}
			prev = name
			// Half the copies start over rows that are already there.
			if i%2 == 0 {
				_, _ = s.D.Exec(ctx, `truncate grants, members`)
			}
		}
		cfg, err := writeConfig(dir, name, s.src.DSN("postgres"), s.dst.DSN("postgres"), "")
		if err != nil {
			return res(), err
		}
		p, err := spawn(dir, cfg)
		if err != nil {
			return res(), err
		}
		m.Start(ctx)
		wait := 120 + rand.IntN(1500)
		if fresh { // the copy takes about two seconds
			wait = 150 + rand.IntN(1100)
		}
		time.Sleep(time.Duration(wait) * time.Millisecond)
		p.signal(syscall.SIGKILL)
		<-p.done
		if fresh {
			copying++
			var phase string
			if err := s.D.QueryRow(ctx, `select phase from grantline.progress where replication = $1`, name).Scan(&phase); err != nil || phase != "ready" {
				inCopy++
			}
		} else {
			streaming++
		}
		p2, err := spawn(dir, cfg)
		if err != nil {
			return res(), err
		}
		time.Sleep(time.Duration(rand.IntN(300)) * time.Millisecond)
		m.Stop()
		if err := settled(ctx, s.S, s.D, name, p2, 60*time.Second); err != nil {
			return res(), fmt.Errorf("after kill %d: %w", i, err)
		}
		if err := same(ctx, s.S, s.D); err != nil {
			return res(), fmt.Errorf("after kill %d: %w", i, err)
		}
		p2.signal(syscall.SIGTERM)
		<-p2.done
		if i%10 == 9 {
			fmt.Fprintf(os.Stderr, "%v\n", res())
		}
	}
	out := res()
	out["differences"] = 0
	return out, nil
}

// pauses stops a worker's process, lets another take over, and lets the first
// one go on. A trigger in the target logs every move of the position with the
// session that made it.
func pauses(ctx context.Context, n int) (map[string]any, error) {
	s, stop, err := start(ctx, []string{"fsync=off"}, []string{"fsync=off"})
	if err != nil {
		return nil, err
	}
	defer stop()
	dir, err := os.MkdirTemp("", "pauses")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	if _, err := s.S.Exec(ctx, load.Schema); err != nil {
		return nil, err
	}
	if err := load.Fill(ctx, s.S, 20000); err != nil {
		return nil, err
	}
	keep, _ := writeConfig(dir, "keep", s.src.DSN("postgres"), s.dst.DSN("postgres"), ", keepOldSessions: true")
	if b, err := os.ReadFile(keep); err != nil {
		return nil, err
	} else if err := os.WriteFile(keep, []byte(strings.Replace(string(b), "name: keep", "name: paused", 1)), 0o600); err != nil {
		return nil, err
	}
	kick, _ := writeConfig(dir, "paused", s.src.DSN("postgres"), s.dst.DSN("postgres"), "")
	m := &load.Mix{Pool: s.S, Rows: 20000, Workers: 3, Pause: time.Millisecond}
	a, err := spawn(dir, kick)
	if err != nil {
		return nil, err
	}
	if err := settled(ctx, s.S, s.D, "paused", a, 60*time.Second); err != nil {
		return nil, err
	}
	if _, err := s.D.Exec(ctx, `create table moves (n bigserial, epoch bigint, app text);
		create function log_move() returns trigger language plpgsql as $$ begin insert into moves (epoch, app) values (new.epoch, current_setting('application_name')); return new; end $$;
		create trigger log_move after update on grantline.progress for each row execute function log_move();
		alter table grantline.progress enable always trigger log_move`); err != nil {
		return nil, err
	}
	fenced, cut, late := 0, 0, 0
	res := func() map[string]any {
		return map[string]any{"tries": n, "old_worker_refused_by_epoch": fenced, "old_worker_lost_its_sessions": cut, "commits_from_an_old_worker": late}
	}
	for i := range n {
		m.Start(ctx)
		time.Sleep(time.Duration(200+rand.IntN(400)) * time.Millisecond)
		a.signal(syscall.SIGSTOP)
		cfg := kick
		if i%2 == 1 {
			cfg = keep
		}
		b, err := spawn(dir, cfg)
		if err != nil {
			return res(), err
		}
		var before int64
		_ = s.D.QueryRow(ctx, `select epoch from grantline.progress where replication = 'paused'`).Scan(&before)
		for end := time.Now().Add(40 * time.Second); ; time.Sleep(20 * time.Millisecond) {
			var k int
			if err := s.D.QueryRow(ctx, `select count(*) from moves where epoch = (select max(epoch) from moves) and epoch > (select epoch from moves where n = (select min(n) from moves))`).Scan(&k); err != nil {
				return res(), err
			}
			if k > 5 {
				break
			}
			select {
			case <-b.done:
				return res(), fmt.Errorf("try %d: new worker ended:\n%s", i, b.tail())
			default:
			}
			if time.Now().After(end) {
				return res(), fmt.Errorf("try %d: the new worker isn't moving:\n%s", i, b.tail())
			}
		}
		a.signal(syscall.SIGCONT)
		select {
		case <-a.done:
		case <-time.After(15 * time.Second):
			return res(), fmt.Errorf("try %d: the old worker is still running:\n%s", i, a.tail())
		}
		if strings.Contains(a.tail(), "newer worker") {
			fenced++
		} else {
			cut++
		}
		m.Stop()
		if err := settled(ctx, s.S, s.D, "paused", b, 60*time.Second); err != nil {
			return res(), fmt.Errorf("try %d: %w", i, err)
		}
		if err := same(ctx, s.S, s.D); err != nil {
			return res(), fmt.Errorf("try %d: %w", i, err)
		}
		// A session may not move the position once another session has moved it
		// under a higher epoch.
		var bad int
		if err := s.D.QueryRow(ctx, `select count(*) from moves x where exists (select 1 from moves y where y.n < x.n and y.epoch > x.epoch)`).Scan(&bad); err != nil {
			return res(), err
		}
		late += bad
		var apps int
		if err := s.D.QueryRow(ctx, `select coalesce(max(c), 0) from (select count(distinct app) c from moves group by epoch) x`).Scan(&apps); err != nil {
			return res(), err
		}
		if apps > 1 {
			return res(), fmt.Errorf("try %d: two workers moved the position under one epoch", i)
		}
		if _, err := s.D.Exec(ctx, `delete from moves where epoch < (select max(epoch) from moves)`); err != nil {
			return res(), err
		}
		a = b
		if i%10 == 9 {
			fmt.Fprintf(os.Stderr, "%v\n", res())
		}
	}
	a.signal(syscall.SIGTERM)
	<-a.done
	return res(), nil
}

// failover loses the primary under load and promotes the standby. In the last
// try the stream doesn't wait for the standby, and the engine has to notice that
// the copy is ahead of the new primary.
func failover(ctx context.Context, n int) (map[string]any, error) {
	out := map[string]any{"tries": 0, "differences": 0}
	for i := range n {
		if err := failoverOnce(ctx, true); err != nil {
			return out, fmt.Errorf("try %d: %w", i, err)
		}
		out["tries"] = i + 1
		fmt.Fprintf(os.Stderr, "failover %d: same on both sides\n", i+1)
	}
	err := failoverOnce(ctx, false)
	if !errors.Is(err, target.ErrDiverged) {
		return out, fmt.Errorf("without the wait the engine said %v, want diverged", err)
	}
	out["without_the_wait"] = "stopped: " + err.Error()
	return out, nil
}

func sh(ctx context.Context, name string, args ...string) (string, error) {
	b, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return string(b), fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, b)
	}
	return strings.TrimSpace(string(b)), nil
}
