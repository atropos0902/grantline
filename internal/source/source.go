// Package source reads a Postgres primary: catalog, change stream and chunks.
package source

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/bugs"
	"grantline/internal/lsn"
)

// MarkerPrefix tags the logical messages this engine writes.
const MarkerPrefix = "grantline"

// Source is a connection to the database that owns the data.
type Source struct {
	DSN         string
	Slot        string
	Publication string
	Pool        *pgxpool.Pool
}

// Settings that make a value's text the same in a query and in the stream.
var textSettings = map[string]string{
	"datestyle": "ISO", "intervalstyle": "postgres", "timezone": "UTC",
	"extra_float_digits": "3", "bytea_output": "hex",
}

// Open connects to the source.
func Open(ctx context.Context, dsn, slot, publication string) (*Source, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	for k, v := range textSettings {
		cfg.ConnConfig.RuntimeParams[k] = v
	}
	cfg.ConnConfig.RuntimeParams["application_name"] = "grantline"
	// A marker mustn't wait for a standby: nothing that matters depends on it
	// surviving a failover.
	cfg.ConnConfig.RuntimeParams["synchronous_commit"] = "local"
	cfg.MaxConns = 16
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &Source{DSN: dsn, Slot: slot, Publication: publication, Pool: pool}, nil
}

// Close drops the connections.
func (s *Source) Close() { s.Pool.Close() }

// Column of a source table.
type Column struct {
	Name    string
	Type    string // as format_type prints it
	OID     uint32
	Mod     int32
	NotNull bool
	Default bool
}

// Table is a published table.
type Table struct {
	Schema, Name string
	Columns      []Column
	Key          []string // primary key columns, in index order
	Identity     byte
	Binary       bool // built-in types, the same on both ends: rows can be copied undecoded
}

// FullName is schema.table, unquoted.
func (t *Table) FullName() string { return t.Schema + "." + t.Name }

// Ident is schema.table, quoted for SQL.
func (t *Table) Ident() string { return pgx.Identifier{t.Schema, t.Name}.Sanitize() }

// Column returns the named column or nil.
func (t *Table) Column(name string) *Column {
	for i := range t.Columns {
		if t.Columns[i].Name == name {
			return &t.Columns[i]
		}
	}
	return nil
}

// Tables lists the publication's tables.
func (s *Source) Tables(ctx context.Context) ([]*Table, error) {
	rows, err := s.Pool.Query(ctx, `
		select n.nspname, c.relname, c.relreplident::text,
		  array(select a.attname::text from pg_index i, unnest(i.indkey) with ordinality k(n, o), pg_attribute a
		        where i.indrelid = c.oid and i.indisprimary and a.attrelid = c.oid and a.attnum = k.n order by k.o),
		  (select json_agg(json_build_object('Name', a.attname, 'Type', format_type(a.atttypid, a.atttypmod),
		     'OID', a.atttypid::int8, 'Mod', a.atttypmod, 'NotNull', a.attnotnull, 'Default', a.atthasdef) order by a.attnum)
		   from pg_attribute a where a.attrelid = c.oid and a.attnum > 0 and not a.attisdropped and a.attgenerated = '')
		from pg_publication_tables p
		join pg_namespace n on n.nspname = p.schemaname
		join pg_class c on c.relnamespace = n.oid and c.relname = p.tablename
		where p.pubname = $1 order by 1, 2`, s.Publication)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (*Table, error) {
		t := &Table{}
		var ident string
		if err := r.Scan(&t.Schema, &t.Name, &ident, &t.Key, &t.Columns); err != nil {
			return nil, err
		}
		t.Identity = ident[0]
		return t, nil
	})
}

// System identifies the server and how far it has got.
type System struct {
	ID       string
	Timeline int32
	WAL      lsn.LSN
}

// Identify reads the system ID, timeline and write position.
func (s *Source) Identify(ctx context.Context) (System, error) {
	var sys System
	var wal string
	err := s.Pool.QueryRow(ctx, `select s.system_identifier::text, c.timeline_id, pg_current_wal_lsn()::text
		from pg_control_system() s, pg_control_checkpoint() c`).Scan(&sys.ID, &sys.Timeline, &wal)
	if err != nil {
		return sys, err
	}
	// The checkpoint's timeline lags right after a promotion, the write
	// position's file name doesn't.
	var tli string
	if err := s.Pool.QueryRow(ctx, `select substr(pg_walfile_name(pg_current_wal_lsn()), 1, 8)`).Scan(&tli); err != nil {
		return sys, err
	}
	var n int32
	if _, err := fmt.Sscanf(tli, "%08X", &n); err == nil {
		sys.Timeline = n
	}
	sys.WAL, err = lsn.Parse(wal)
	return sys, err
}

// SwitchPoint returns where the server left the given timeline, or zero when it
// never did.
func (s *Source) SwitchPoint(ctx context.Context, from, to int32) (lsn.LSN, error) {
	conn, err := s.replConn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	h, err := pglogrepl.TimelineHistory(ctx, conn, to)
	if err != nil {
		return 0, err
	}
	for line := range strings.Lines(string(h.Content)) {
		f := strings.Fields(line)
		if len(f) >= 2 && f[0] == fmt.Sprint(from) {
			return lsn.Parse(f[1])
		}
	}
	return 0, nil
}

func (s *Source) replConn(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(s.DSN)
	if err != nil {
		return nil, err
	}
	for k, v := range textSettings {
		cfg.RuntimeParams[k] = v
	}
	cfg.RuntimeParams["replication"] = "database"
	cfg.RuntimeParams["application_name"] = "grantline " + s.Slot
	cfg.RuntimeParams["wal_sender_timeout"] = "20s"
	return pgconn.ConnectConfig(ctx, cfg)
}

// ErrSlotLost means the slot can't supply the changes since the last commit.
var ErrSlotLost = errors.New("replication slot lost")

// SlotState is what the source says about the slot.
type SlotState struct {
	Exists    bool
	ActivePID int32
	Lost      string // why it was invalidated, empty when it is fine
	Retained  int64  // bytes of log it keeps
	Failover  bool
}

// SlotState reads the slot's row.
func (s *Source) SlotState(ctx context.Context) (SlotState, error) {
	var st SlotState
	err := s.Pool.QueryRow(ctx, `select coalesce(active_pid, 0), coalesce(invalidation_reason, ''), failover,
		coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::int8
		from pg_replication_slots where slot_name = $1`, s.Slot).Scan(&st.ActivePID, &st.Lost, &st.Failover, &st.Retained)
	if errors.Is(err, pgx.ErrNoRows) {
		return st, nil
	}
	st.Exists = err == nil
	return st, err
}

// CreateSlot makes the slot, replacing one that nothing was copied from yet, and
// returns the position the stream starts at.
//
// Before it returns it waits for every transaction that was open when the slot
// became usable. A commit is in the log before readers see it. One that reached
// the log ahead of the slot's start isn't in the stream, and a chunk read before
// it turns visible wouldn't have it either.
func (s *Source) CreateSlot(ctx context.Context) (lsn.LSN, error) {
	if st, err := s.SlotState(ctx); err != nil {
		return 0, err
	} else if st.Exists {
		if err := s.DropSlot(ctx); err != nil {
			return 0, err
		}
	}
	conn, err := s.replConn(ctx)
	if err != nil {
		return 0, err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	res, err := pglogrepl.ParseCreateReplicationSlot(conn.Exec(ctx, fmt.Sprintf(
		`CREATE_REPLICATION_SLOT %s LOGICAL pgoutput (SNAPSHOT 'nothing', FAILOVER true)`,
		pgx.Identifier{s.Slot}.Sanitize())))
	if err != nil {
		return 0, fmt.Errorf("create slot: %w", err)
	}
	start, err := lsn.Parse(res.ConsistentPoint)
	if err != nil {
		return 0, err
	}
	if bugs.NoStartBarrier {
		return start, nil
	}
	// A snapshot's xmax won't do as the edge. It is one past the last transaction
	// that finished, so one that is still running can sit right on it. Taking an
	// ID puts the edge behind everything that has started.
	var edge string
	if err := s.Pool.QueryRow(ctx, `select pg_current_xact_id()::text`).Scan(&edge); err != nil {
		return 0, err
	}
	for {
		var ok bool
		if err := s.Pool.QueryRow(ctx, `select pg_snapshot_xmin(pg_current_snapshot()) > $1::text::xid8`, edge).Scan(&ok); err != nil {
			return 0, err
		}
		if ok {
			return start, nil
		}
		select {
		case <-ctx.Done():
			return 0, fmt.Errorf("waiting for transactions older than the slot: %w", ctx.Err())
		case <-time.After(50 * time.Millisecond):
		}
	}
}

// DropSlot removes the slot if it exists.
func (s *Source) DropSlot(ctx context.Context) error {
	if err := s.TakeSlot(ctx); err != nil && !errors.Is(err, ErrSlotLost) {
		return err
	}
	_, err := s.Pool.Exec(ctx, `select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = $1`, s.Slot)
	return err
}

// TakeSlot ends another worker's use of the slot. It is called after the epoch
// in the target has moved, so that worker can't commit anything any more.
func (s *Source) TakeSlot(ctx context.Context) error {
	for range 100 {
		st, err := s.SlotState(ctx)
		if err != nil {
			return err
		}
		switch {
		case !st.Exists:
			return fmt.Errorf("%w: %s doesn't exist", ErrSlotLost, s.Slot)
		case st.Lost != "":
			return fmt.Errorf("%w: %s was invalidated (%s)", ErrSlotLost, s.Slot, st.Lost)
		case st.ActivePID == 0:
			return nil
		}
		if _, err := s.Pool.Exec(ctx, `select pg_terminate_backend($1)`, st.ActivePID); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	return fmt.Errorf("slot %s is still in use", s.Slot)
}

// Mark writes a message into the log and returns its position. The stream
// delivers it in commit order with everything else.
func (s *Source) Mark(ctx context.Context, body string) (lsn.LSN, error) {
	var pos string
	if err := s.Pool.QueryRow(ctx, `select pg_logical_emit_message(true, $1, $2::text)::text`, MarkerPrefix, body).Scan(&pos); err != nil {
		return 0, err
	}
	return lsn.Parse(pos)
}

// NextXID is a recent transaction ID at full width, the reference for Widen.
func (s *Source) NextXID(ctx context.Context) (uint64, error) {
	var x string
	if err := s.Pool.QueryRow(ctx, `select pg_snapshot_xmax(pg_current_snapshot())::text`).Scan(&x); err != nil {
		return 0, err
	}
	var v uint64
	_, err := fmt.Sscan(x, &v)
	return v, err
}
