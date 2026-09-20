package pgtarget

import (
	"context"
	"fmt"
	"hash/maphash"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"grantline/internal/bugs"
	"grantline/internal/pgoutput"
	"grantline/internal/sched"
	"grantline/internal/target"
	"grantline/internal/txn"
	"grantline/internal/xid"
)

// plan is how changes in one version of a table are applied.
type plan struct {
	meta   *tableMeta
	name   string
	ident  string
	rel    *pgoutput.Relation
	key    []int // positions of the primary key in the relation
	all    []int
	ord    ordKind
	upsert string
	delete string
	many   string // upserts a run of rows sent as one array per column, empty when a column is itself an array
	gone   string // deletes a run of rows, only for a key of one column
	uniq   [][]int
	fks    [][]int
}

type ordKind byte

const (
	ordNone ordKind = iota
	ordInt
	ordUUID
)

func (m *tableMeta) ord() ordKind {
	switch m.Column(m.Key[0]).Type {
	case "smallint", "integer", "bigint":
		return ordInt
	case "uuid":
		return ordUUID
	}
	return ordNone
}

func (k ordKind) of(text string) sched.Ord {
	switch k {
	case ordInt:
		if n, err := strconv.ParseInt(text, 10, 64); err == nil {
			return sched.Ord{Known: true, Int: n}
		}
	case ordUUID:
		// The text form of a uuid sorts the way Postgres sorts the value.
		return sched.Ord{Known: true, Str: text}
	}
	return sched.Ord{}
}

func (t *Target) plan(rel *pgoutput.Relation) (*plan, error) {
	if p, ok := t.plans.Load(rel); ok {
		return p.(*plan), nil
	}
	m, err := t.meta(rel.FullName())
	if err != nil {
		return nil, err
	}
	p := &plan{meta: m, name: m.FullName(), ident: m.Ident(), rel: rel, ord: m.ord()}
	pos := func(names []string) ([]int, bool) {
		var out []int
		for _, n := range names {
			i := slices.IndexFunc(rel.Columns, func(c pgoutput.Column) bool { return c.Name == n })
			if i < 0 {
				return nil, false
			}
			out = append(out, i)
		}
		return out, true
	}
	var ok bool
	if p.key, ok = pos(m.Key); !ok {
		return nil, fmt.Errorf("%s: the stream doesn't carry the primary key", rel.FullName())
	}
	for _, u := range m.uniques {
		if ix, ok := pos(u); ok {
			p.uniq = append(p.uniq, ix)
		}
	}
	for _, f := range m.fks {
		ix, _ := pos(f.Cols)
		p.fks = append(p.fks, ix)
	}
	all := make([]int, len(rel.Columns))
	for i := range all {
		all[i] = i
	}
	p.all = all
	p.upsert = p.upsertSQL(all)
	p.manySQL()
	p.delete = fmt.Sprintf("delete from %s where %s", p.ident, p.keyWhere(1))
	t.plans.Store(rel, p)
	return p, nil
}

func (p *plan) col(i int) string { return pgx.Identifier{p.rel.Columns[i].Name}.Sanitize() }

func (p *plan) keyWhere(first int) string {
	w := make([]string, len(p.key))
	for i, k := range p.key {
		w[i] = fmt.Sprintf("%s = $%d", p.col(k), first+i)
	}
	return strings.Join(w, " and ")
}

func (p *plan) upsertSQL(cols []int) string {
	var names, vals, sets, key []string
	for n, i := range cols {
		names = append(names, p.col(i))
		vals = append(vals, fmt.Sprintf("$%d", n+1))
		if !slices.Contains(p.key, i) {
			sets = append(sets, fmt.Sprintf("%s = excluded.%s", p.col(i), p.col(i)))
		}
	}
	for _, k := range p.key {
		key = append(key, p.col(k))
	}
	do := "nothing"
	if sets != nil {
		do = "update set " + strings.Join(sets, ", ")
	}
	return fmt.Sprintf("insert into %s (%s) values (%s) on conflict (%s) do %s",
		p.ident, strings.Join(names, ", "), strings.Join(vals, ", "), strings.Join(key, ", "), do)
}

// manySQL makes the statements for runs of rows. Rows go through them one at a
// time and in order, so a unique value that moves between two rows of a run
// behaves as it would in two statements.
func (p *plan) manySQL() {
	var arrays, names, sets, key []string
	for i := range p.rel.Columns {
		c := p.meta.Column(p.rel.Columns[i].Name)
		if c == nil || strings.HasSuffix(c.Type, "]") {
			return
		}
		arrays = append(arrays, fmt.Sprintf("$%d::%s[]", i+1, c.Type))
		names = append(names, p.col(i))
		if !slices.Contains(p.key, i) {
			sets = append(sets, fmt.Sprintf("%s = excluded.%s", p.col(i), p.col(i)))
		}
	}
	for _, k := range p.key {
		key = append(key, p.col(k))
	}
	do := "nothing"
	if sets != nil {
		do = "update set " + strings.Join(sets, ", ")
	}
	p.many = fmt.Sprintf("insert into %s (%s) select * from unnest(%s) on conflict (%s) do %s",
		p.ident, strings.Join(names, ", "), strings.Join(arrays, ", "), strings.Join(key, ", "), do)
	if len(p.key) == 1 {
		k := p.rel.Columns[p.key[0]].Name
		p.gone = fmt.Sprintf("delete from %s where %s = any($1::%s[])", p.ident, p.col(p.key[0]), p.meta.Column(k).Type)
	}
}

// KeyText is how a key is written in the ledger and the journal: the value
// itself for one column, length-prefixed values for several.
func KeyText(vals []string) string {
	if len(vals) == 1 {
		return vals[0]
	}
	var b strings.Builder
	for _, v := range vals {
		b.WriteString(strconv.Itoa(len(v)))
		b.WriteByte(':')
		b.WriteString(v)
	}
	return b.String()
}

// keySQL is KeyText in SQL, over columns of the given alias.
func keySQL(key []string, alias string) string {
	if len(key) == 1 {
		return alias + "." + pgx.Identifier{key[0]}.Sanitize() + "::text"
	}
	parts := make([]string, len(key))
	for i, k := range key {
		c := alias + "." + pgx.Identifier{k}.Sanitize() + "::text"
		parts[i] = fmt.Sprintf("octet_length(%s) || ':' || %s", c, c)
	}
	return strings.Join(parts, " || ")
}

func (p *plan) keyText(tuple []pgoutput.Value) string {
	if len(p.key) == 1 {
		return string(tuple[p.key[0]].Data)
	}
	vals := make([]string, len(p.key))
	for i, k := range p.key {
		vals[i] = string(tuple[k].Data)
	}
	return KeyText(vals)
}

var seed = maphash.MakeSeed()

func hashKey(table, what string, vals ...[]byte) uint64 {
	var h maphash.Hash
	h.SetSeed(seed)
	h.WriteString(table)
	h.WriteByte(0)
	h.WriteString(what)
	var n [4]byte
	for _, v := range vals {
		l := len(v)
		n = [4]byte{byte(l), byte(l >> 8), byte(l >> 16), byte(l >> 24)}
		h.Write(n[:])
		h.Write(v)
	}
	return h.Sum64()
}

const (
	groupBig  = 20000 // more changes than this run alone instead of listing keys
	pieceSize = 1000  // changes sent to the target at a time
)

// Item lists what the group touches.
func (t *Target) Item(g *target.Group) (*sched.Item, error) {
	it := &sched.Item{Work: &groupWork{t: t, g: g}}
	if g.Changes() > groupBig {
		it.Barrier = true
		return it, nil
	}
	var row pgoutput.Row
	for _, x := range g.Txns {
		if x.Truncates {
			it.Barrier = true
			return it, nil
		}
		err := x.Each(func(c *txn.Change) error {
			if c.Rel == nil {
				return nil
			}
			p, err := t.plan(c.Rel)
			if err != nil {
				return err
			}
			if err := pgoutput.DecodeRow(c.Op, c.Body, &row); err != nil {
				return err
			}
			p.touch(it, row.Old, row.OldFull, true)
			p.touch(it, row.New, true, false)
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return it, nil
}

// touch adds a tuple's key, unique values and parent rows. An old tuple that
// holds only the key says nothing about the unique values it frees, and then
// the scheduler's way out of a conflict is what keeps the order right.
func (p *plan) touch(it *sched.Item, tuple []pgoutput.Value, full, old bool) {
	if tuple == nil {
		return
	}
	var buf [4][]byte
	vals := buf[:0]
	for _, k := range p.key {
		vals = append(vals, tuple[k].Data)
	}
	name := p.name
	it.Keys = append(it.Keys, sched.Key{Hash: hashKey(name, "", vals...)})
	it.Points = append(it.Points, sched.Point{Table: p.meta.id, Ord: p.ord.of(string(vals[0]))})
	if !full {
		return
	}
	add := func(ix []int, table, what string, shared bool) {
		vals := make([][]byte, 0, len(ix))
		for _, i := range ix {
			if tuple[i].Kind == pgoutput.Null || tuple[i].Kind == pgoutput.Unchanged {
				return
			}
			vals = append(vals, tuple[i].Data)
		}
		it.Keys = append(it.Keys, sched.Key{Hash: hashKey(table, what, vals...), Shared: shared})
	}
	for i, ix := range p.uniq {
		add(ix, name, strings.Join(p.meta.uniques[i], ","), false)
	}
	if old {
		return
	}
	for i, ix := range p.fks {
		f := p.meta.fks[i]
		if ix != nil {
			add(ix, f.Ref, f.what, true)
		}
	}
}

type groupWork struct {
	t *Target
	g *target.Group
	s *session
}

func (w *groupWork) Rollback(ctx context.Context) error {
	if w.s == nil {
		return nil
	}
	return w.s.rollback(ctx)
}

func (w *groupWork) Prepare(ctx context.Context, lane int) error {
	w.s = w.t.lanes[lane]
	p := &piece{t: w.t, s: w.s}
	p.begin()
	for _, x := range w.g.Txns {
		p.xid = strconv.FormatUint(xid.Widen(x.XID, w.g.RefXID), 10)
		p.lsn = x.CommitLSN.String()
		if err := x.Each(func(c *txn.Change) error { return p.add(ctx, c) }); err != nil {
			return classify(err)
		}
	}
	return classify(p.flush(ctx))
}

func (w *groupWork) Commit(ctx context.Context) error {
	if bugs.PositionFirst {
		if _, err := w.t.Pool.Exec(ctx, `select grantline.advance($1, null, $2, null, false)`, w.t.cfg.Replication, w.g.EndLSN.String()); err != nil {
			return err
		}
	}
	if h := w.t.cfg.BeforeCommit; h != nil {
		h(w.g)
	}
	epoch := []byte(strconv.FormatInt(w.t.epoch, 10))
	if bugs.NoEpochCheck {
		epoch = nil
	}
	ready := "f"
	if w.g.Ready {
		ready = "t"
	}
	err := w.s.commit(ctx, `select grantline.advance($1, $2, $3, $4, $5)`, [][]byte{
		[]byte(w.t.cfg.Replication), epoch, []byte(w.g.EndLSN.String()),
		[]byte(w.g.Time.UTC().Format("2006-01-02 15:04:05.999999+00")), []byte(ready),
	})
	if err != nil {
		return err
	}
	for _, x := range w.g.Txns {
		x.Close()
	}
	return nil
}

// piece is a run of changes on its way to the target in one round trip.
type piece struct {
	t   *Target
	s   *session
	xid string // of the transaction in hand
	lsn string

	batch   *pgconn.Batch
	n       int
	row     pgoutput.Row
	partial map[int]note // statement number -> the update that needs its row to exist
	stmt    int
	ledger  notes
	journal notes
	run     run
}

// run is a stretch of upserts, or of deletes, of one table. No key is in it
// twice: an upsert can't touch a row a second time.
type run struct {
	pl   *plan
	del  bool
	n    int
	cols [][]byte // array literals under construction
	keys map[string]struct{}
}

type note struct {
	tbl, key string
	op       byte
	xid, lsn string
	reread   bool
}

type notes struct{ tbl, key, op, xid, lsn, reread []string }

var opText = [256]string{'I': "I", 'U': "U", 'D': "D", 'T': "T"}

func (n *notes) add(x note) {
	n.tbl = append(n.tbl, x.tbl)
	n.key = append(n.key, x.key)
	n.op = append(n.op, opText[x.op])
	n.xid = append(n.xid, x.xid)
	n.lsn = append(n.lsn, x.lsn)
	r := "f"
	if x.reread {
		r = "t"
	}
	n.reread = append(n.reread, r)
}

func (p *piece) begin() {
	p.batch = &pgconn.Batch{}
	p.batch.ExecParams("begin", nil, nil, nil, nil)
	p.stmt = 1
}

// exec adds a statement, after the run that came before it.
func (p *piece) exec(ctx context.Context, sql string, args [][]byte) error {
	if err := p.endRun(ctx); err != nil {
		return err
	}
	return p.put(ctx, sql, args)
}

// inRun adds a row to the run, starting a new one when it doesn't fit.
func (p *piece) inRun(ctx context.Context, pl *plan, del bool, key string, vals []pgoutput.Value, cols []int) error {
	_, again := p.run.keys[key]
	if p.run.pl != pl || p.run.del != del || again {
		if err := p.endRun(ctx); err != nil {
			return err
		}
		if p.run.keys == nil {
			p.run.keys = map[string]struct{}{}
		}
		p.run.pl, p.run.del = pl, del
		for len(p.run.cols) < len(cols) {
			p.run.cols = append(p.run.cols, nil)
		}
	}
	for i, c := range cols {
		p.run.cols[i] = appendElem(p.run.cols[i], vals[c])
	}
	p.run.keys[key] = struct{}{}
	p.run.n++
	return nil
}

func appendElem(b []byte, v pgoutput.Value) []byte {
	sep := byte(',')
	if len(b) == 0 {
		sep = '{'
	}
	b = append(b, sep)
	if v.Kind == pgoutput.Null || v.Kind == pgoutput.Unchanged {
		return append(b, "NULL"...)
	}
	return quote(b, v.Data)
}

func quote[T string | []byte](b []byte, v T) []byte {
	b = append(b, '"')
	for j := range len(v) {
		if v[j] == '"' || v[j] == '\\' {
			b = append(b, '\\')
		}
		b = append(b, v[j])
	}
	return append(b, '"')
}

func (p *piece) endRun(ctx context.Context) error {
	r := &p.run
	if r.n == 0 {
		return nil
	}
	sql, n := r.pl.many, len(r.pl.rel.Columns)
	if r.del {
		sql, n = r.pl.gone, 1
	}
	args := make([][]byte, n)
	for i := range args {
		args[i] = append(r.cols[i], '}')
	}
	err := p.put(ctx, sql, args)
	for i := range r.cols {
		r.cols[i] = r.cols[i][:0]
	}
	clear(r.keys)
	r.pl, r.n = nil, 0
	return err
}

func (p *piece) put(ctx context.Context, sql string, args [][]byte) error {
	name, err := p.s.prepare(ctx, sql)
	if err != nil {
		return err
	}
	p.batch.ExecPrepared(name, args, nil, nil)
	p.stmt++
	return nil
}

func (p *piece) add(ctx context.Context, c *txn.Change) error {
	if c.Op == pgoutput.Message {
		return nil
	}
	pl, err := p.t.plan(c.Rel)
	if err != nil {
		return err
	}
	if c.Op == pgoutput.Truncate {
		if err := p.exec(ctx, "truncate "+pl.ident, nil); err != nil {
			return err
		}
		p.note(pl, "*", 'T', false)
		p.n++
		return nil
	}
	if err := pgoutput.DecodeRow(c.Op, c.Body, &p.row); err != nil {
		return err
	}
	switch c.Op {
	case pgoutput.Delete:
		err = p.delete(ctx, pl, p.row.Old)
		p.note(pl, pl.keyText(p.row.Old), 'D', false)
	case pgoutput.Insert:
		err = p.upsert(ctx, pl, p.row.New)
		p.note(pl, pl.keyText(p.row.New), 'I', false)
	case pgoutput.Update:
		err = p.update(ctx, pl)
	}
	if err != nil {
		return err
	}
	if p.n++; p.n >= pieceSize {
		return p.flush(ctx)
	}
	return nil
}

func (p *piece) delete(ctx context.Context, pl *plan, old []pgoutput.Value) error {
	if pl.gone != "" {
		return p.inRun(ctx, pl, true, string(old[pl.key[0]].Data), old, pl.key)
	}
	args := make([][]byte, len(pl.key))
	for i, k := range pl.key {
		args[i] = old[k].Data
	}
	return p.exec(ctx, pl.delete, args)
}

func (p *piece) upsert(ctx context.Context, pl *plan, row []pgoutput.Value) error {
	if pl.many != "" {
		return p.inRun(ctx, pl, false, pl.keyText(row), row, pl.all)
	}
	args := make([][]byte, len(row))
	for i, v := range row {
		args[i] = data(v)
	}
	return p.exec(ctx, pl.upsert, args)
}

func data(v pgoutput.Value) []byte {
	if v.Kind == pgoutput.Null || v.Kind == pgoutput.Unchanged {
		return nil
	}
	return v.Data
}

func (p *piece) update(ctx context.Context, pl *plan) error {
	row, old := p.row.New, p.row.Old
	oldKey := ""
	if old != nil {
		if k := pl.keyText(old); k != pl.keyText(row) {
			oldKey = k
		}
		if p.row.OldFull {
			for i := range row {
				if row[i].Kind == pgoutput.Unchanged {
					row[i] = old[i]
				}
			}
		}
	}
	whole := !slices.ContainsFunc(row, func(v pgoutput.Value) bool { return v.Kind == pgoutput.Unchanged })
	if bugs.ToastAsNull {
		whole = true
	}
	if whole {
		if oldKey != "" {
			if err := p.delete(ctx, pl, old); err != nil {
				return err
			}
			p.note(pl, oldKey, 'D', false)
		}
		p.note(pl, pl.keyText(row), 'U', false)
		return p.upsert(ctx, pl, row)
	}
	// Some large values weren't sent. They can only come from the row that is
	// already here, under its old key if the key changed.
	var sql string
	var args [][]byte
	var err error
	if oldKey != "" {
		p.note(pl, oldKey, 'D', false)
		sql, args, err = pl.move(row, old)
	} else {
		sql, args = pl.patch(row)
	}
	if err != nil {
		return err
	}
	if err := p.exec(ctx, sql, args); err != nil {
		return err
	}
	if p.partial == nil {
		p.partial = map[int]note{}
	}
	n := note{tbl: pl.name, key: pl.keyText(row), op: 'U', xid: p.xid, lsn: p.lsn}
	p.partial[p.stmt-1] = n
	p.noted(n)
	return nil
}

// patch updates the columns that were sent.
func (p *plan) patch(row []pgoutput.Value) (string, [][]byte) {
	var sets []string
	var args [][]byte
	for i, v := range row {
		if v.Kind == pgoutput.Unchanged || slices.Contains(p.key, i) {
			continue
		}
		args = append(args, data(v))
		sets = append(sets, fmt.Sprintf("%s = $%d", p.col(i), len(args)))
	}
	if sets == nil {
		args = append(args, row[p.key[0]].Data)
		sets = append(sets, fmt.Sprintf("%s = $1", p.col(p.key[0])))
	}
	sql := fmt.Sprintf("update %s set %s where %s", p.ident, strings.Join(sets, ", "), p.keyWhere(len(args)+1))
	for _, k := range p.key {
		args = append(args, row[k].Data)
	}
	return sql, args
}

// move gives a row a new key and keeps the values that weren't sent. An update
// of the key won't do: while the table is being copied, a chunk read after the
// move may already have put a row under the new key.
func (p *plan) move(row, old []pgoutput.Value) (string, [][]byte, error) {
	var names, vals, sets, key []string
	var args [][]byte
	for i, v := range row {
		c := p.meta.Column(p.rel.Columns[i].Name)
		if c == nil {
			return "", nil, fmt.Errorf("%s: no column %s", p.name, p.rel.Columns[i].Name)
		}
		names = append(names, p.col(i))
		if v.Kind == pgoutput.Unchanged {
			vals = append(vals, "o."+p.col(i))
		} else {
			args = append(args, data(v))
			vals = append(vals, fmt.Sprintf("$%d::%s", len(args), c.Type))
		}
		if !slices.Contains(p.key, i) {
			sets = append(sets, fmt.Sprintf("%s = excluded.%s", p.col(i), p.col(i)))
		}
	}
	for _, k := range p.key {
		key = append(key, p.col(k))
	}
	sql := fmt.Sprintf("with o as (delete from %s where %s returning *) insert into %s (%s) select %s from o on conflict (%s) do update set %s",
		p.ident, p.keyWhere(len(args)+1), p.ident, strings.Join(names, ", "), strings.Join(vals, ", "), strings.Join(key, ", "), strings.Join(sets, ", "))
	for _, k := range p.key {
		args = append(args, old[k].Data)
	}
	return sql, args, nil
}

func (p *piece) note(pl *plan, key string, op byte, reread bool) {
	p.noted(note{tbl: pl.name, key: key, op: op, xid: p.xid, lsn: p.lsn, reread: reread})
}

func (p *piece) noted(n note) {
	if !p.t.done(n.tbl).Load() {
		p.ledger.add(n)
	}
	if p.t.cfg.Journal {
		p.journal.add(n)
	}
}

const ledgerSQL = `insert into grantline.ledger (replication, tbl, key, xid, lsn, op, reread)
	select distinct on (tbl, key) $1::text, tbl, key, xid, lsn, op, reread
	from unnest($2::text[], $3::text[], $4::xid8[], $5::pg_lsn[], $6::"char"[], $7::bool[]) with ordinality u(tbl, key, xid, lsn, op, reread, o)
	order by tbl, key, o desc
	on conflict (replication, tbl, key) do update set xid = excluded.xid, lsn = excluded.lsn, op = excluded.op, reread = excluded.reread`

const journalSQL = `insert into grantline.journal (replication, tbl, key, op, xid, lsn, epoch)
	select $1::text, tbl, key, op, xid, lsn, $6::bigint
	from unnest($2::text[], $3::text[], $4::xid8[], $5::pg_lsn[], $7::"char"[]) u(tbl, key, xid, lsn, op)`

// flush sends the piece. Updates that needed a row and found none are put on
// the list of keys to read again.
func (p *piece) flush(ctx context.Context) error {
	if err := p.endRun(ctx); err != nil {
		return err
	}
	repl := []byte(p.t.cfg.Replication)
	if l := &p.ledger; l.tbl != nil {
		if err := p.exec(ctx, ledgerSQL, [][]byte{repl, array(l.tbl), array(l.key), array(l.xid), array(l.lsn), array(l.op), array(l.reread)}); err != nil {
			return err
		}
	}
	if j := &p.journal; j.tbl != nil {
		if err := p.exec(ctx, journalSQL, [][]byte{repl, array(j.tbl), array(j.key), array(j.xid), array(j.lsn),
			[]byte(strconv.FormatInt(p.t.epoch, 10)), array(j.op)}); err != nil {
			return err
		}
	}
	if p.stmt == 0 {
		return nil
	}
	mr := p.s.c.PgConn().ExecBatch(ctx, p.batch)
	var missing notes
	for i := 0; mr.NextResult(); i++ {
		tag, err := mr.ResultReader().Close()
		if err != nil {
			mr.Close()
			return err
		}
		n, ok := p.partial[i]
		if !ok || tag.RowsAffected() > 0 {
			continue
		}
		if bugs.NoReread {
			continue
		}
		if p.t.done(n.tbl).Load() {
			mr.Close()
			return fmt.Errorf("%w: %s %s has no row to update", target.ErrDiverged, n.tbl, n.key)
		}
		n.reread = true
		missing.add(n)
		p.t.Reread.Add(1)
	}
	if err := mr.Close(); err != nil {
		return err
	}
	p.batch, p.n, p.stmt, p.partial = &pgconn.Batch{}, 0, 0, nil
	p.ledger, p.journal = notes{}, notes{}
	if missing.tbl == nil {
		return nil
	}
	// A later change in the same piece may have settled the key, so only the
	// entries this update wrote are marked.
	res := p.s.c.PgConn().ExecParams(ctx, `update grantline.ledger l set reread = true
		from unnest($2::text[], $3::text[], $4::xid8[]) u(tbl, key, xid)
		where l.replication = $1 and l.tbl = u.tbl and l.key = u.key and l.xid = u.xid and l.op = 'U'`,
		[][]byte{repl, array(missing.tbl), array(missing.key), array(missing.xid)}, nil, nil, nil).Read()
	return res.Err
}

// array writes a text[] literal. Values that could need quoting get it.
func array(vs []string) []byte {
	n := 2
	for _, v := range vs {
		n += len(v) + 3
	}
	b := append(make([]byte, 0, n), '{')
	for i, v := range vs {
		if i > 0 {
			b = append(b, ',')
		}
		b = quote(b, v)
	}
	return append(b, '}')
}
