// Package pgtarget applies changes to Postgres.
package pgtarget

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/lsn"
	"grantline/internal/sched"
	"grantline/internal/source"
	"grantline/internal/target"
)

//go:embed schema.sql
var schema string

// Config for a Postgres target.
type Config struct {
	DSN         string
	Replication string
	Lanes       int
	Journal     bool // keep each change for explain
	KeepOld     bool // don't end an older worker's sessions on takeover

	// Owner names this process. Again says that it had the replication before
	// and is starting the engine again: if another process has taken over since,
	// Open fails with ErrFenced and doesn't take it back. An epoch wouldn't do
	// for this. The answer that carries a new one can get lost.
	Owner string
	Again bool

	// BeforeCommit runs when a group is prepared and it is its turn. Tests use
	// it to hold a commit; it may be nil.
	BeforeCommit func(g *target.Group)
}

// Target is a Postgres database.
type Target struct {
	cfg   Config
	group string // what every worker of this replication puts first in application_name
	app   string // this worker's application_name
	Pool  *pgxpool.Pool
	epoch int64
	lanes []*session

	mu       sync.Mutex
	tables   map[string]*tableMeta
	finished map[string]*atomic.Bool
	plans    sync.Map // *pgoutput.Relation -> *plan
	replica  bool     // triggers and foreign keys are off in our sessions

	// Counters, read by the metrics.
	Refused, Reread, Landed atomic.Int64
}

// tableMeta is what the overlap test needs to know about a table.
type tableMeta struct {
	*source.Table
	id      uint32
	uniques [][]string
	fks     []foreignKey
}

type foreignKey struct {
	Cols    []string
	Ref     string
	RefCols []string
	what    string // how the parent's own rows name this key, see touch
}

var settings = map[string]string{
	"datestyle": "ISO", "intervalstyle": "postgres", "timezone": "UTC",
	"extra_float_digits": "3", "bytea_output": "hex", "lock_timeout": "200ms",
	// A paused worker's locks go away without anyone's help.
	"idle_in_transaction_session_timeout": "10s",
	"synchronous_commit":                  "on",
}

// New connects and installs the grantline schema.
func New(ctx context.Context, cfg Config) (*Target, error) {
	pc, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, err
	}
	// Postgres cuts application_name at 63 bytes, so the parts that are compared
	// come before the replication's name.
	group := fmt.Sprintf("grantline %08x ", crc32.ChecksumIEEE([]byte(cfg.Replication)))
	app := fmt.Sprintf("%s%06x ", group, rand.IntN(1<<24))
	for k, v := range settings {
		pc.ConnConfig.RuntimeParams[k] = v
	}
	pc.ConnConfig.RuntimeParams["lock_timeout"] = "5s"
	pc.ConnConfig.RuntimeParams["application_name"] = app + cfg.Replication
	pc.MaxConns = int32(cfg.Lanes) + 12
	t := &Target{cfg: cfg, group: group, app: app, tables: map[string]*tableMeta{}, finished: map[string]*atomic.Bool{}, replica: true}
	pc.AfterConnect = func(ctx context.Context, c *pgx.Conn) error {
		if _, err := c.Exec(ctx, `set session_replication_role = replica`); err != nil {
			var pe *pgconn.PgError
			if errors.As(err, &pe) && pe.Code == "42501" {
				t.replica = false
				return nil
			}
			return err
		}
		return nil
	}
	if t.Pool, err = pgxpool.NewWithConfig(ctx, pc); err != nil {
		return nil, err
	}
	if err := t.install(ctx); err != nil {
		t.Pool.Close()
		return nil, err
	}
	return t, nil
}

const schemaVersion = "grantline schema 2"

// install creates the grantline schema. It does nothing when the schema is
// there: the statements take locks, and an older worker may be holding a
// transaction open on the same tables.
func (t *Target) install(ctx context.Context) error {
	var have string
	err := t.Pool.QueryRow(ctx, `select coalesce(obj_description(to_regnamespace('grantline'), 'pg_namespace'), '')`).Scan(&have)
	if err != nil || have == schemaVersion {
		return err
	}
	// Workers may start together, and "if not exists" isn't safe against itself.
	_, err = t.Pool.Exec(ctx, "select pg_advisory_xact_lock(7291);\n"+schema+"\ncomment on schema grantline is '"+schemaVersion+"';")
	return err
}

// Open takes over: older sessions end, the epoch moves.
func (t *Target) Open(ctx context.Context) (target.State, error) {
	var st target.State
	var pos, phase string
	var err error
	if t.cfg.Again {
		err = t.Pool.QueryRow(ctx, `update grantline.progress set epoch = epoch + 1 where replication = $1 and owner = $2
			returning epoch, phase, lsn::text, system_id, timeline`, t.cfg.Replication, t.cfg.Owner).
			Scan(&st.Epoch, &phase, &pos, &st.SystemID, &st.Timeline)
		if errors.Is(err, pgx.ErrNoRows) {
			return st, target.ErrFenced
		}
	} else {
		if err = t.kick(ctx); err != nil {
			return st, err
		}
		err = t.Pool.QueryRow(ctx, `insert into grantline.progress (replication, epoch, owner) values ($1, 1, $2)
			on conflict (replication) do update set epoch = grantline.progress.epoch + 1, owner = excluded.owner
			returning epoch, phase, lsn::text, system_id, timeline`, t.cfg.Replication, t.cfg.Owner).
			Scan(&st.Epoch, &phase, &pos, &st.SystemID, &st.Timeline)
	}
	if err != nil {
		return st, fmt.Errorf("take over: %w", err)
	}
	t.epoch, st.Phase = st.Epoch, target.Phase(phase)
	if st.LSN, err = lsn.Parse(pos); err != nil {
		return st, err
	}
	if err := t.kick(ctx); err != nil {
		return st, err
	}
	for range t.cfg.Lanes {
		s, err := t.dial(ctx)
		if err != nil {
			return st, err
		}
		t.lanes = append(t.lanes, s)
	}
	return st, t.Reload(ctx)
}

// Reload reads which tables have finished copying.
func (t *Target) Reload(ctx context.Context) error {
	rows, err := t.Pool.Query(ctx, `select tbl, finished from grantline.copied where replication = $1`, t.cfg.Replication)
	if err != nil {
		return err
	}
	var name string
	var fin bool
	_, err = pgx.ForEachRow(rows, []any{&name, &fin}, func() error { t.done(name).Store(fin); return nil })
	return err
}

// kick ends the sessions an older worker left, which may be holding row locks.
func (t *Target) kick(ctx context.Context) error {
	if t.cfg.KeepOld {
		return nil
	}
	_, err := t.Pool.Exec(ctx, `select pg_terminate_backend(pid) from pg_stat_activity
		where starts_with(application_name, $1) and not starts_with(application_name, $2) and datname = current_database()`,
		t.group, t.app)
	return err
}

func (t *Target) done(tbl string) *atomic.Bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	b := t.finished[tbl]
	if b == nil {
		b = new(atomic.Bool)
		t.finished[tbl] = b
	}
	return b
}

// Epoch this worker holds.
func (t *Target) Epoch() int64 { return t.epoch }

// Begin records the start of a new replication.
func (t *Target) Begin(ctx context.Context, at lsn.LSN, systemID string, timeline int32) error {
	return t.owned(ctx, `update grantline.progress set phase = 'copying', lsn = $3, system_id = $4, timeline = $5
		where replication = $1 and epoch = $2`, at.String(), systemID, timeline)
}

// Moved records the server's identity after a failover.
func (t *Target) Moved(ctx context.Context, systemID string, timeline int32) error {
	return t.owned(ctx, `update grantline.progress set system_id = $3, timeline = $4
		where replication = $1 and epoch = $2`, systemID, timeline)
}

func (t *Target) owned(ctx context.Context, sql string, args ...any) error {
	tag, err := t.Pool.Exec(ctx, sql, append([]any{t.cfg.Replication, t.epoch}, args...)...)
	if err == nil && tag.RowsAffected() == 0 {
		return target.ErrFenced
	}
	return err
}

// Lanes is how many groups may be prepared at once.
func (t *Target) Lanes() int { return t.cfg.Lanes }

// Close drops every connection.
func (t *Target) Close() {
	for _, l := range t.lanes {
		l.close()
	}
	t.Pool.Close()
}

// SetTables loads what the overlap test needs, creating any table the target
// doesn't have.
func (t *Target) SetTables(ctx context.Context, tables []*source.Table) error {
	for i, st := range tables {
		if err := t.create(ctx, st); err != nil {
			return err
		}
		if err := t.common(ctx, st); err != nil {
			return err
		}
		m := &tableMeta{Table: st, id: uint32(i + 1)}
		rows, err := t.Pool.Query(ctx, `select array(select a.attname::text from unnest(i.indkey::int2[]) with ordinality k(n, o)
			  join pg_attribute a on a.attrelid = i.indrelid and a.attnum = k.n order by k.o)
			from pg_index i where i.indrelid = $1::regclass and i.indisunique and not i.indisprimary
			  and i.indpred is null and i.indexprs is null`, st.Ident())
		if err != nil {
			return err
		}
		if m.uniques, err = pgx.CollectRows(rows, pgx.RowTo[[]string]); err != nil {
			return err
		}
		if !t.replica {
			if m.fks, err = t.foreignKeys(ctx, st); err != nil {
				return err
			}
		}
		t.mu.Lock()
		t.tables[st.FullName()] = m
		t.mu.Unlock()
	}
	for _, m := range t.tables {
		for i := range m.fks {
			f := &m.fks[i]
			f.what = strings.Join(f.RefCols, ",")
			parent := t.tables[f.Ref]
			if parent == nil || len(parent.Key) != len(f.RefCols) {
				continue
			}
			cols := make([]string, len(f.Cols))
			for j, r := range f.RefCols {
				k := slices.Index(parent.Key, r)
				if k < 0 {
					cols = nil
					break
				}
				cols[k] = f.Cols[j]
			}
			if cols != nil {
				f.Cols, f.what = cols, ""
			}
		}
	}
	return nil
}

func (t *Target) foreignKeys(ctx context.Context, st *source.Table) ([]foreignKey, error) {
	rows, err := t.Pool.Query(ctx, `select
		  array(select a.attname::text from unnest(c.conkey) with ordinality k(n, o) join pg_attribute a on a.attrelid = c.conrelid and a.attnum = k.n order by k.o),
		  n.nspname || '.' || r.relname,
		  array(select a.attname::text from unnest(c.confkey) with ordinality k(n, o) join pg_attribute a on a.attrelid = c.confrelid and a.attnum = k.n order by k.o)
		from pg_constraint c join pg_class r on r.oid = c.confrelid join pg_namespace n on n.oid = r.relnamespace
		where c.contype = 'f' and c.conrelid = $1::regclass`, st.Ident())
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, pgx.RowToStructByPos[foreignKey])
}

func (t *Target) create(ctx context.Context, st *source.Table) error {
	var defs, key []string
	for _, c := range st.Columns {
		d := pgx.Identifier{c.Name}.Sanitize() + " " + c.Type
		if c.NotNull {
			d += " not null"
		}
		defs = append(defs, d)
	}
	for _, k := range st.Key {
		key = append(key, pgx.Identifier{k}.Sanitize())
	}
	if len(key) == 0 {
		return fmt.Errorf("%s has no primary key", st.FullName())
	}
	_, err := t.Pool.Exec(ctx, fmt.Sprintf(`create schema if not exists %s`, pgx.Identifier{st.Schema}.Sanitize()))
	if err == nil {
		_, err = t.Pool.Exec(ctx, fmt.Sprintf(`create table if not exists %s (%s, primary key (%s))`,
			st.Ident(), strings.Join(defs, ", "), strings.Join(key, ", ")))
	}
	return err
}

// common keeps the columns both ends have, with the target's types. A column
// the source has and the target doesn't will arrive with the stream.
func (t *Target) common(ctx context.Context, st *source.Table) error {
	rows, err := t.Pool.Query(ctx, `select attname::text, format_type(atttypid, atttypmod), atttypid::int8 from pg_attribute
		where attrelid = $1::regclass and attnum > 0 and not attisdropped`, st.Ident())
	if err != nil {
		return err
	}
	type col struct {
		typ string
		oid int64
	}
	have := map[string]col{}
	var name string
	var c col
	if _, err := pgx.ForEachRow(rows, []any{&name, &c.typ, &c.oid}, func() error { have[name] = c; return nil }); err != nil {
		return err
	}
	st.Binary = true
	st.Columns = slices.DeleteFunc(st.Columns, func(c source.Column) bool { _, ok := have[c.Name]; return !ok })
	for i := range st.Columns {
		c := &st.Columns[i]
		if h := have[c.Name]; c.OID >= 16384 || int64(c.OID) != h.oid {
			st.Binary = false
		}
		c.Type = have[c.Name].typ
	}
	return nil
}

func (t *Target) meta(name string) (*tableMeta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.tables[name]
	if m == nil {
		return nil, fmt.Errorf("table %s isn't part of this replication", name)
	}
	return m, nil
}

// session is one connection with its prepared statements.
type session struct {
	c     *pgx.Conn
	stmts map[string]string
}

func (t *Target) dial(ctx context.Context) (*session, error) {
	cfg, err := pgx.ParseConfig(t.cfg.DSN)
	if err != nil {
		return nil, err
	}
	for k, v := range settings {
		cfg.RuntimeParams[k] = v
	}
	cfg.RuntimeParams["application_name"] = t.app + t.cfg.Replication
	c, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if t.replica {
		if _, err := c.Exec(ctx, `set session_replication_role = replica`); err != nil {
			c.Close(ctx)
			return nil, err
		}
	}
	return &session{c: c, stmts: map[string]string{}}, nil
}

func (s *session) close() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	s.c.Close(ctx)
}

// prepare returns the statement's name, preparing it on first use.
func (s *session) prepare(ctx context.Context, sql string) (string, error) {
	if n, ok := s.stmts[sql]; ok {
		return n, nil
	}
	n := fmt.Sprintf("g%d", len(s.stmts))
	if _, err := s.c.PgConn().Prepare(ctx, n, sql, nil); err != nil {
		return "", fmt.Errorf("prepare %q: %w", sql, err)
	}
	s.stmts[sql] = n
	return n, nil
}

func (s *session) rollback(ctx context.Context) error {
	if s.c.PgConn().TxStatus() == 'I' {
		return nil
	}
	_, err := s.c.PgConn().Exec(ctx, "rollback").ReadAll()
	return err
}

// commit moves the position and commits, in one round trip.
func (s *session) commit(ctx context.Context, sql string, args [][]byte) error {
	name, err := s.prepare(ctx, sql)
	if err != nil {
		return err
	}
	b := &pgconn.Batch{}
	b.ExecPrepared(name, args, nil, nil)
	b.ExecParams("commit", nil, nil, nil, nil)
	rs, err := s.c.PgConn().ExecBatch(ctx, b).ReadAll()
	if err != nil {
		return classify(err)
	}
	if tag := rs[len(rs)-1].CommandTag.String(); tag != "COMMIT" {
		return fmt.Errorf("commit answered %s", tag)
	}
	return nil
}

// classify marks errors another lane may have caused, and the fence.
func classify(err error) error {
	var pe *pgconn.PgError
	if !errors.As(err, &pe) {
		return err
	}
	switch pe.Code {
	case "GL001":
		return fmt.Errorf("%w: %s", target.ErrFenced, pe.Message)
	case "55P03", "40P01", "40001", "23505", "23503", "23P01":
		return fmt.Errorf("%w: %w", sched.ErrConflict, err)
	}
	return err
}

// Reshape replaces what is known about a table. Plans made for older versions
// keep working, because each change carries the version it was written under.
func (t *Target) Reshape(st *source.Table) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if old := t.tables[st.FullName()]; old != nil {
		m := *old
		m.Table = st
		t.tables[st.FullName()] = &m
	}
}
