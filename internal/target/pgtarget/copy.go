package pgtarget

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"grantline/internal/bugs"
	"grantline/internal/lsn"
	"grantline/internal/sched"
	"grantline/internal/source"
)

// Chunk is one planned range of a table.
type Chunk struct {
	No     int
	Bounds source.Bounds
	Landed bool
}

// Plan is what is already known about copying a table.
type Plan struct {
	Chunks   []Chunk
	Planned  bool // the list is complete
	Finished bool
	HadRows  bool
}

// CopyPlan reads a table's plan, starting one if there is none.
func (t *Target) CopyPlan(ctx context.Context, st *source.Table) (*Plan, error) {
	var p Plan
	err := t.Pool.QueryRow(ctx, `select planned, finished, had_rows from grantline.copied where replication = $1 and tbl = $2`,
		t.cfg.Replication, st.FullName()).Scan(&p.Planned, &p.Finished, &p.HadRows)
	if errors.Is(err, pgx.ErrNoRows) {
		err = t.Pool.QueryRow(ctx, fmt.Sprintf(`insert into grantline.copied (replication, tbl, had_rows)
			select $1, $2, exists (select 1 from %s) returning had_rows`, st.Ident()), t.cfg.Replication, st.FullName()).Scan(&p.HadRows)
	}
	if err != nil {
		return nil, err
	}
	rows, err := t.Pool.Query(ctx, `select no, lo, hi, landed from grantline.chunk
		where replication = $1 and tbl = $2 order by no`, t.cfg.Replication, st.FullName())
	if err != nil {
		return nil, err
	}
	p.Chunks, err = pgx.CollectRows(rows, func(r pgx.CollectableRow) (Chunk, error) {
		var c Chunk
		return c, r.Scan(&c.No, &c.Bounds.Lo, &c.Bounds.Hi, &c.Landed)
	})
	return &p, err
}

// AddChunk saves a planned range. last says the table's list is complete.
func (t *Target) AddChunk(ctx context.Context, st *source.Table, c Chunk, last bool) error {
	return pgx.BeginFunc(ctx, t.Pool, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `select grantline.advance($1, $2, null, null, false)`, t.cfg.Replication, t.epoch); err != nil {
			return classify(err)
		}
		if _, err := tx.Exec(ctx, `insert into grantline.chunk (replication, tbl, no, lo, hi) values ($1, $2, $3, $4, $5)`,
			t.cfg.Replication, st.FullName(), c.No, c.Bounds.Lo, c.Bounds.Hi); err != nil || !last {
			return err
		}
		_, err := tx.Exec(ctx, `update grantline.copied set planned = true where replication = $1 and tbl = $2`,
			t.cfg.Replication, st.FullName())
		return err
	})
}

// Copier stages chunks on a connection of its own.
type Copier struct {
	t *Target
	s *session
}

// NewCopier opens a connection for landing chunks.
func (t *Target) NewCopier(ctx context.Context) (*Copier, error) {
	s, err := t.dial(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := s.c.Exec(ctx, `set temp_buffers = '32MB'`); err != nil {
		s.close()
		return nil, err
	}
	return &Copier{t: t, s: s}, nil
}

// Close drops the connection.
func (c *Copier) Close() { c.s.close() }

// Stage loads rows into a private staging table and returns how many there were
// and their checksum.
func (c *Copier) Stage(ctx context.Context, st *source.Table, cols []string, binary bool, r io.Reader) (int64, uint32, error) {
	stage := stageName(st)
	if _, err := c.s.c.Exec(ctx, fmt.Sprintf(`create temp table if not exists %s (like %s); truncate %s`, stage, st.Ident(), stage)); err != nil {
		return 0, 0, err
	}
	q := make([]string, len(cols))
	for i, n := range cols {
		q[i] = pgx.Identifier{n}.Sanitize()
	}
	format := "text"
	if binary {
		format = "binary"
	}
	sum := crc32.New(source.Castagnoli)
	tag, err := c.s.c.PgConn().CopyFrom(ctx, io.TeeReader(r, sum), fmt.Sprintf(`copy %s (%s) from stdin (format %s)`,
		stage, strings.Join(q, ", "), format))
	return tag.RowsAffected(), sum.Sum32(), err
}

func stageName(st *source.Table) string {
	return pgx.Identifier{"stage " + st.FullName()}.Sanitize()
}

// Landing is a staged chunk, or a set of keys read again, ready to go into the
// table.
type Landing struct {
	Table  *source.Table
	Cols   []string
	Read   source.ChunkRead
	Chunk  *Chunk   // nil for keys read again
	Keys   []string // keys read again
	Delete bool     // also remove rows the source no longer has
	Done   func(refused int64)
}

// Item makes the scheduler item that lands it. A chunk is a writer like any
// other: without a place in the commit order, a delete could come between its
// check of the ledger and its insert.
func (c *Copier) Item(l *Landing) (*sched.Item, error) {
	m, err := c.t.meta(l.Table.FullName())
	if err != nil {
		return nil, err
	}
	w := &landWork{c: c, l: l}
	it := &sched.Item{Work: w, Detached: true, Range: &sched.Range{Table: m.id}}
	if l.Chunk != nil {
		ord := m.ord()
		end := func(v []string) *sched.Ord {
			if v == nil {
				return nil
			}
			o := ord.of(v[0])
			return &o
		}
		it.Range.Lo, it.Range.Hi = end(l.Chunk.Bounds.Lo), end(l.Chunk.Bounds.Hi)
	}
	if l.Done != nil {
		it.Done = func() { l.Done(w.refused) }
	}
	return it, nil
}

// LandNow lands without a place in the commit order. Only the known bug and the
// test that looks for it use it.
func (c *Copier) LandNow(ctx context.Context, l *Landing) error {
	w := &landWork{c: c, l: l}
	for {
		err := w.Prepare(ctx, -1)
		if err == nil {
			if err = w.Commit(ctx); err == nil {
				if l.Done != nil {
					l.Done(w.refused)
				}
				return nil
			}
		}
		_ = w.Rollback(ctx)
		if !errors.Is(err, sched.ErrConflict) {
			return err
		}
	}
}

type landWork struct {
	c       *Copier
	l       *Landing
	refused int64
}

func (w *landWork) Rollback(ctx context.Context) error { return w.c.s.rollback(ctx) }

// Prepare runs the guard. A staged row goes in only if the last transaction the
// stream applied to its key was visible in the snapshot the row was read under.
//
// Everything in front of this landing that touches the table has committed, and
// everything behind it that touches the range waits. So the ledger can be looked
// at first. A key that isn't in it has no row in a target that began empty, and
// such rows go in with a plain insert, which costs a third of an upsert.
func (w *landWork) Prepare(ctx context.Context, _ int) error {
	t, l := w.c.t, w.l
	conn := w.c.s.c.PgConn()
	stage := stageName(l.Table)
	repl, tbl := source.Literal(t.cfg.Replication), source.Literal(l.Table.FullName())
	snap := source.Literal(l.Read.Snapshot)
	newer := "not pg_visible_in_snapshot(l.xid, " + snap + "::pg_snapshot)"
	if bugs.LogPositionGuard {
		newer = "l.lsn > " + source.Literal(l.Read.Marker.String()) + "::pg_lsn"
	}
	ledger := fmt.Sprintf("l.replication = %s and l.tbl = %s", repl, tbl)

	look := &pgconn.Batch{}
	look.ExecParams("begin", nil, nil, nil, nil)
	look.ExecParams(fmt.Sprintf(`select exists (select 1 from grantline.ledger l where %s and l.key <> '*'),
		exists (select 1 from grantline.ledger l where %s and l.key = '*' and %s)`, ledger, ledger, newer), nil, nil, nil, nil)
	rs, err := conn.ExecBatch(ctx, look).ReadAll()
	if err != nil {
		return classify(err)
	}
	row := rs[1].Rows[0]
	entries, truncated := string(row[0]) == "t", string(row[1]) == "t"

	// same joins the ledger to rows by key, refuse is true for rows to leave out.
	same := func(alias string) string { return fmt.Sprintf("%s and l.key = %s", ledger, keySQL(l.Table.Key, alias)) }
	refuse := func(alias string) string {
		if truncated {
			return "true"
		}
		return fmt.Sprintf("exists (select 1 from grantline.ledger l where %s and %s)", same(alias), newer)
	}
	var names, sets, key []string
	for _, n := range l.Cols {
		q := pgx.Identifier{n}.Sanitize()
		names = append(names, q)
		if !slices.Contains(l.Table.Key, n) {
			sets = append(sets, q+" = excluded."+q)
		}
	}
	for _, k := range l.Table.Key {
		key = append(key, pgx.Identifier{k}.Sanitize())
	}
	action := "nothing"
	if sets != nil {
		action = "update set " + strings.Join(sets, ", ")
	}
	cols := strings.Join(names, ", ")
	insert := fmt.Sprintf("insert into %s (%s) select %s from %s s where ", l.Table.Ident(), cols, cols, stage)
	upsert := fmt.Sprintf(" on conflict (%s) do %s", strings.Join(key, ", "), action)

	b := &pgconn.Batch{}
	refusedAt := -1
	n := 0
	do := func(sql string) { b.ExecParams(sql, nil, nil, nil, nil); n++ }
	if entries || truncated {
		chunkNo := "null"
		if l.Chunk != nil {
			chunkNo = strconv.Itoa(l.Chunk.No)
		}
		refusedAt = n
		do(fmt.Sprintf(`insert into grantline.journal (replication, tbl, key, op, xid, lsn, snapshot, chunk, epoch)
			select %s, %s, %s, 'R', l.xid, l.lsn, %s::pg_snapshot, %s::int, %d from %s s left join grantline.ledger l on %s where %s`,
			repl, tbl, keySQL(l.Table.Key, "s"), snap, chunkNo, t.epoch, stage, same("s"), refuse("s")))
	}
	fresh := l.Chunk != nil && !l.Delete
	switch {
	case truncated: // every row is older than the truncate
	case fresh && !entries:
		do(insert + "true")
	case fresh:
		do(insert + fmt.Sprintf("not exists (select 1 from grantline.ledger l where %s)", same("s")))
		do(insert + fmt.Sprintf("exists (select 1 from grantline.ledger l where %s) and not %s", same("s"), refuse("s")) + upsert)
	case entries:
		do(insert + "not " + refuse("s") + upsert)
	default:
		do(insert + "true" + upsert)
	}
	if l.Delete && l.Chunk != nil {
		eq := make([]string, len(key))
		for i, k := range key {
			eq[i] = "s." + k + " = d." + k
		}
		do(fmt.Sprintf(`delete from %s d where %s and not exists (select 1 from %s s where %s) and not %s`, l.Table.Ident(),
			l.Table.Where(l.Chunk.Bounds), stage, strings.Join(eq, " and "), refuse("d")))
	}
	if l.Chunk != nil {
		do(fmt.Sprintf(`update grantline.chunk set landed = true, snapshot = %s, marker = %s, rows = %d, crc32c = %d, epoch = %d, at = clock_timestamp()
			where replication = %s and tbl = %s and no = %d`, source.Literal(l.Read.Snapshot), source.Literal(l.Read.Marker.String()),
			l.Read.Rows, l.Read.CRC, t.epoch, repl, tbl, l.Chunk.No))
	} else {
		keys := make([]string, len(l.Keys))
		for i, k := range l.Keys {
			keys[i] = source.Literal(k)
		}
		// A key whose last change the snapshot can't see stays on the list.
		do(fmt.Sprintf(`update grantline.ledger l set reread = false where l.replication = %s and l.tbl = %s and l.key in (%s) and not (%s)`,
			repl, tbl, strings.Join(keys, ", "), newer))
	}
	rs, err = conn.ExecBatch(ctx, b).ReadAll()
	if err != nil {
		return classify(err)
	}
	w.refused = 0
	if refusedAt >= 0 {
		w.refused = rs[refusedAt].CommandTag.RowsAffected()
	}
	return nil
}

func (w *landWork) Commit(ctx context.Context) error {
	err := w.c.s.commit(ctx, `select grantline.advance($1, $2, null, null, false)`,
		[][]byte{[]byte(w.c.t.cfg.Replication), []byte(strconv.FormatInt(w.c.t.epoch, 10))})
	if err != nil {
		return err
	}
	w.c.t.Refused.Add(w.refused)
	w.c.t.Landed.Add(w.l.Read.Rows)
	return nil
}

// RereadKeys lists keys whose rows have to be read again.
func (t *Target) RereadKeys(ctx context.Context, st *source.Table, limit int) ([]string, error) {
	rows, err := t.Pool.Query(ctx, `select key from grantline.ledger where replication = $1 and tbl = $2 and reread limit $3`,
		t.cfg.Replication, st.FullName(), limit)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowTo[string])
}

// KeyIn renders a condition that picks rows by their key text.
func KeyIn(st *source.Table, keys []string) string {
	lits := make([]string, len(keys))
	for i, k := range keys {
		lits[i] = source.Literal(k)
	}
	if len(st.Key) == 1 {
		return fmt.Sprintf("%s in (%s)", pgx.Identifier{st.Key[0]}.Sanitize(), strings.Join(lits, ", "))
	}
	return fmt.Sprintf("%s in (%s)", keySQL(st.Key, pgx.Identifier{st.Name}.Sanitize()), strings.Join(lits, ", "))
}

// FinishItem ends a table's copying if nothing is left to read again. It runs
// alone, so transactions behind it see the result and stop keeping the ledger.
func (t *Target) FinishItem(st *source.Table, done func(finished bool)) *sched.Item {
	w := &finishWork{t: t, st: st}
	return &sched.Item{Work: w, Barrier: true, Done: func() { done(w.ok) }}
}

type finishWork struct {
	t  *Target
	st *source.Table
	s  *session
	ok bool
}

func (w *finishWork) Prepare(ctx context.Context, lane int) error {
	w.s = w.t.lanes[lane]
	if _, err := w.s.c.Exec(ctx, "begin"); err != nil {
		return err
	}
	var left int
	err := w.s.c.QueryRow(ctx, `select (select count(*) from grantline.ledger where replication = $1 and tbl = $2 and reread)
		+ (select count(*) from grantline.chunk where replication = $1 and tbl = $2 and not landed)`,
		w.t.cfg.Replication, w.st.FullName()).Scan(&left)
	if err != nil {
		return classify(err)
	}
	if w.ok = left == 0; !w.ok {
		return nil
	}
	if _, err := w.s.c.Exec(ctx, `delete from grantline.ledger where replication = $1 and tbl = $2`, w.t.cfg.Replication, w.st.FullName()); err != nil {
		return classify(err)
	}
	_, err = w.s.c.Exec(ctx, `update grantline.copied set finished = true where replication = $1 and tbl = $2`, w.t.cfg.Replication, w.st.FullName())
	return classify(err)
}

func (w *finishWork) Commit(ctx context.Context) error {
	if err := w.s.commit(ctx, `select grantline.advance($1, $2, null, null, false)`,
		[][]byte{[]byte(w.t.cfg.Replication), []byte(strconv.FormatInt(w.t.epoch, 10))}); err != nil {
		return err
	}
	if w.ok {
		w.t.done(w.st.FullName()).Store(true)
	}
	return nil
}

func (w *finishWork) Rollback(ctx context.Context) error {
	if w.s == nil {
		return nil
	}
	return w.s.rollback(ctx)
}

// DDLItem runs one statement alone in the commit order.
func (t *Target) DDLItem(sql string) *sched.Item {
	return &sched.Item{Work: &ddlWork{t: t, sql: sql}, Barrier: true}
}

type ddlWork struct {
	t   *Target
	sql string
	s   *session
}

func (w *ddlWork) Prepare(ctx context.Context, lane int) error {
	w.s = w.t.lanes[lane]
	if _, err := w.s.c.Exec(ctx, "begin"); err != nil {
		return err
	}
	_, err := w.s.c.Exec(ctx, w.sql)
	return classify(err)
}

func (w *ddlWork) Commit(ctx context.Context) error {
	return w.s.commit(ctx, `select grantline.advance($1, $2, null, null, false)`,
		[][]byte{[]byte(w.t.cfg.Replication), []byte(strconv.FormatInt(w.t.epoch, 10))})
}

func (w *ddlWork) Rollback(ctx context.Context) error {
	if w.s == nil {
		return nil
	}
	return w.s.rollback(ctx)
}

// Recopy forgets how far copying got, so every table is copied again over the
// rows that are there. The stream position starts over with a new slot.
func (t *Target) Recopy(ctx context.Context) error {
	return pgx.BeginFunc(ctx, t.Pool, func(tx pgx.Tx) error {
		for _, q := range []string{
			`delete from grantline.chunk where replication = $1`,
			`delete from grantline.copied where replication = $1`,
			`delete from grantline.ledger where replication = $1`,
			`update grantline.progress set phase = 'new', ready_at = null, epoch = epoch + 1, owner = '' where replication = $1`,
		} {
			if _, err := tx.Exec(ctx, q, t.cfg.Replication); err != nil {
				return err
			}
		}
		return nil
	})
}

// Position is the committed position, the phase and when its last transaction
// committed at the source.
func (t *Target) Position(ctx context.Context) (lsn.LSN, string, error) {
	var pos, phase string
	if err := t.Pool.QueryRow(ctx, `select lsn::text, phase from grantline.progress where replication = $1`, t.cfg.Replication).Scan(&pos, &phase); err != nil {
		return 0, "", err
	}
	p, err := lsn.Parse(pos)
	return p, phase, err
}

// TrimJournal removes entries older than the given number of hours.
func (t *Target) TrimJournal(ctx context.Context, hours int) error {
	_, err := t.Pool.Exec(ctx, `delete from grantline.journal where replication = $1 and at < now() - make_interval(hours => $2)`, t.cfg.Replication, hours)
	return err
}
