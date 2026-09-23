// Package preflight looks at a customer's databases before anything is
// installed, and says what would go wrong.
package preflight

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/source"
)

// Level of a finding.
type Level int

// Levels, worst last.
const (
	OK Level = iota
	Note
	Warn
	Stop
)

func (l Level) String() string { return [...]string{"ok", "note", "warn", "STOP"}[l] }

// Finding is one result.
type Finding struct {
	Level Level
	What  string
	Fix   string
}

// Run checks the source, and the target when it is Postgres.
func Run(ctx context.Context, src *source.Source, target *pgxpool.Pool) ([]Finding, error) {
	var out []Finding
	add := func(l Level, fix, format string, a ...any) {
		out = append(out, Finding{l, fmt.Sprintf(format, a...), fix})
	}
	q := func(sql string, args ...any) (string, error) {
		var s string
		err := src.Pool.QueryRow(ctx, sql, args...).Scan(&s)
		return s, err
	}

	if v, err := q(`show wal_level`); err != nil {
		return nil, err
	} else if v != "logical" {
		add(Stop, "alter system set wal_level = logical, then restart", "wal_level is %s", v)
	}
	var version int
	if err := src.Pool.QueryRow(ctx, `select current_setting('server_version_num')::int`).Scan(&version); err != nil {
		return nil, err
	}
	if version < 170000 {
		add(Stop, "upgrade to 17 or newer", "server version is %d: slots can't follow a failover", version/10000)
		return out, nil
	}
	var slots, senders int
	if err := src.Pool.QueryRow(ctx, `select
		current_setting('max_replication_slots')::int - (select count(*) from pg_replication_slots),
		current_setting('max_wal_senders')::int - (select count(*) from pg_stat_replication)`).Scan(&slots, &senders); err != nil {
		return nil, err
	}
	if st, err := src.SlotState(ctx); err != nil {
		return nil, err
	} else if slots < 1 && !st.Exists {
		add(Stop, "raise max_replication_slots, or drop slots nobody reads", "no replication slot is free")
	}
	if senders < 2 {
		add(Stop, "raise max_wal_senders", "%d connections left for replication, 2 are needed", senders)
	}
	if v, _ := q(`select current_setting('max_slot_wal_keep_size')`); v == "-1" {
		add(Warn, "set max_slot_wal_keep_size to what the disk can spare", "nothing caps the log a stopped replication keeps, so it can fill the disk")
	}
	var old, prepared int
	if err := src.Pool.QueryRow(ctx, `select (select count(*) from pg_stat_activity where backend_xid is not null and xact_start < now() - interval '1 minute'),
		(select count(*) from pg_prepared_xacts)`).Scan(&old, &prepared); err != nil {
		return nil, err
	}
	if old > 0 {
		add(Warn, "wait for them, or start at a quieter time", "%d write transactions are older than a minute; making the slot waits for all of them", old)
	}
	if prepared > 0 {
		add(Stop, "commit or roll back what pg_prepared_xacts shows", "%d prepared transactions would block making the slot", prepared)
	}
	var repl, super, monitor, signal bool
	if err := src.Pool.QueryRow(ctx, `select rolreplication, rolsuper, pg_has_role('pg_monitor', 'member'), pg_has_role('pg_signal_backend', 'member')
		from pg_roles where rolname = current_user`).Scan(&repl, &super, &monitor, &signal); err != nil {
		return nil, err
	}
	if !repl && !super {
		add(Stop, "alter role ... replication", "the role can't replicate")
	}
	if !monitor && !super {
		add(Stop, "grant pg_monitor to the role", "the role can't see other sessions, which taking over a slot needs")
	}
	if !signal && !super {
		add(Warn, "grant pg_signal_backend to the role", "the role can't end another worker's session, so a takeover waits for the server's timeout")
	}
	standbys, err := q(`select count(*)::text from pg_stat_replication where state = 'streaming' and application_name not like 'grantline%'`)
	if err != nil {
		return nil, err
	}
	if sync, _ := q(`select current_setting('synchronized_standby_slots')`); sync == "" {
		if standbys != "0" {
			add(Warn, "name the standby's slot in synchronized_standby_slots", "the stream doesn't wait for the standby, so after a failover the copy may hold changes the new primary never had")
		} else {
			add(Note, "", "no standby is connected, so there is nothing to fail over to")
		}
	}

	out, err = tables(ctx, src, out)
	if err != nil || target == nil {
		return out, err
	}
	if _, err := target.Exec(ctx, `set session_replication_role = replica`); err != nil {
		var pe *pgconn.PgError
		if !errors.As(err, &pe) || pe.Code != "42501" {
			return nil, err
		}
		add(Note, "grant set on parameter session_replication_role to the role", "the target role can't turn triggers off, so foreign keys join the overlap test and triggers fire")
	}
	if v, err := targetVersion(ctx, target); err != nil {
		return nil, err
	} else if v < 130000 {
		add(Stop, "use 13 or newer", "target version is %d: no pg_visible_in_snapshot", v/10000)
	}
	return out, nil
}

func targetVersion(ctx context.Context, p *pgxpool.Pool) (v int, err error) {
	err = p.QueryRow(ctx, `select current_setting('server_version_num')::int`).Scan(&v)
	return
}

func tables(ctx context.Context, src *source.Source, out []Finding) ([]Finding, error) {
	add := func(l Level, fix, format string, a ...any) {
		out = append(out, Finding{l, fmt.Sprintf(format, a...), fix})
	}
	var all bool
	err := src.Pool.QueryRow(ctx, `select pubinsert and pubupdate and pubdelete and pubtruncate from pg_publication where pubname = $1`, src.Publication).Scan(&all)
	if errors.Is(err, pgx.ErrNoRows) {
		add(Stop, "create publication "+src.Publication+" for table ...", "publication %s doesn't exist", src.Publication)
		return out, nil
	} else if err != nil {
		return out, err
	}
	if !all {
		add(Stop, "alter publication ... set (publish = 'insert, update, delete, truncate')", "publication %s leaves out some kinds of change", src.Publication)
	}
	list, err := src.Tables(ctx)
	if err != nil {
		return out, err
	}
	if len(list) == 0 {
		add(Stop, "alter publication ... add table ...", "publication %s has no tables", src.Publication)
	}
	for _, t := range list {
		name := t.FullName()
		if len(t.Key) == 0 {
			add(Stop, "add a primary key", "%s has no primary key", name)
			continue
		}
		switch t.Identity {
		case 'n':
			add(Stop, "alter table ... replica identity default", "%s sends no key with updates and deletes, so its revokes wouldn't arrive", name)
		case 'i':
			var same bool
			if err := src.Pool.QueryRow(ctx, `select bool_or(indisprimary) from pg_index where indrelid = $1::regclass and indisreplident`, t.Ident()).Scan(&same); err != nil {
				return out, err
			}
			if !same {
				add(Stop, "alter table ... replica identity default", "%s identifies rows by an index that isn't the primary key", name)
			}
		}
		var uniques int
		var filtered, partial, canRead, rls bool
		if err := src.Pool.QueryRow(ctx, `select
			(select count(*) from pg_index where indrelid = c.oid and indisunique and not indisprimary),
			exists (select 1 from pg_publication_rel r join pg_publication p on p.oid = r.prpubid where p.pubname = $2 and r.prrelid = c.oid and r.prqual is not null),
			exists (select 1 from pg_publication_rel r join pg_publication p on p.oid = r.prpubid where p.pubname = $2 and r.prrelid = c.oid and r.prattrs is not null),
			has_table_privilege(c.oid, 'select'), c.relrowsecurity and not (select rolbypassrls or rolsuper from pg_roles where rolname = current_user)
			from pg_class c where c.oid = $1::regclass`, t.Ident(), src.Publication).Scan(&uniques, &filtered, &partial, &canRead, &rls); err != nil {
			return out, err
		}
		if filtered {
			add(Stop, "publish the whole table", "%s is published with a row filter, which chunks wouldn't apply", name)
		}
		if partial {
			add(Stop, "publish every column", "%s is published with a column list, which chunks wouldn't apply", name)
		}
		if !canRead {
			add(Stop, "grant select on the table", "%s can't be read by this role", name)
		}
		if rls {
			add(Stop, "alter role ... bypassrls", "%s has row security that would hide rows from chunks", name)
		}
		if uniques > 0 && t.Identity != 'f' {
			add(Note, "replica identity full, if the table changes those values often", "%s has %d unique indexes besides the key, and the stream doesn't carry the values an update frees. Lanes will sometimes wait and try again", name, uniques)
		}
		var big []string
		for _, c := range t.Columns {
			switch c.Type {
			case "text", "bytea", "jsonb", "json", "xml":
				big = append(big, c.Name)
			}
		}
		if len(big) > 0 && t.Identity != 'f' {
			add(Note, "", "%s: large values in %s aren't sent by updates that leave them alone. Rows updated while their chunk is on its way are read again", name, strings.Join(big, ", "))
		}
	}
	if len(out) == 0 {
		add(OK, "", "nothing in the way")
	}
	return out, nil
}

// Worst level found.
func Worst(fs []Finding) Level {
	w := OK
	for _, f := range fs {
		w = max(w, f.Level)
	}
	return w
}
