package engine

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"

	"grantline/internal/pgoutput"
	"grantline/internal/sched"
	"grantline/internal/source"
	"grantline/internal/txn"
)

func sortedStrings(s []string) []string { slices.Sort(s); return s }

// shape compares the tables a transaction was written under with what the
// target has. The target only changes shape when the stream gets there, so it is
// the right side to compare with: the source's catalog may be ahead.
//
// A new nullable column without a default is added. Anything else waits for
// someone to approve the statement.
func (e *Engine) shape(ctx context.Context, s *sched.Scheduler, x *txn.Txn) error {
	for _, rel := range x.Relations() {
		if e.seen[rel] {
			continue
		}
		e.mu.Lock()
		t := e.tables[rel.FullName()]
		e.mu.Unlock()
		if t == nil {
			return fmt.Errorf("table %s was added to the publication; run recopy to take it in", rel.FullName())
		}
		if e.pg == nil {
			e.seen[rel] = true
			continue
		}
		nt, stmts, safe, err := e.diff(ctx, t, rel)
		if err != nil {
			return err
		}
		if len(stmts) > 0 {
			sql := strings.Join(stmts, "; ")
			if !safe {
				if err := e.ask(ctx, rel.FullName(), sql); err != nil {
					return err
				}
			}
			if err := s.Submit(e.pg.DDLItem(sql)); err != nil {
				return err
			}
			e.cfg.Log.Info("schema change", "table", rel.FullName(), "sql", sql)
			e.event("schema_changed", "schema-"+x.EndLSN.String()+"-"+rel.FullName(), map[string]any{"table": rel.FullName(), "sql": sql})
			e.mu.Lock()
			e.tables[t.FullName()] = nt
			e.mu.Unlock()
			e.pg.Reshape(nt)
		}
		e.seen[rel] = true
	}
	return nil
}

// diff returns the shape the target should have and the statements that get it
// there.
func (e *Engine) diff(ctx context.Context, t *source.Table, rel *pgoutput.Relation) (nt *source.Table, stmts []string, safe bool, err error) {
	safe = true
	ident := t.Ident()
	nt = &source.Table{Schema: t.Schema, Name: t.Name, Key: t.Key, Identity: rel.Identity}
	for _, c := range rel.Columns {
		name := pgx.Identifier{c.Name}.Sanitize()
		var typ, def string
		var notNull bool
		err := e.sources.Pool.QueryRow(ctx, `select format_type($1::oid, $2),
			coalesce((select attnotnull from pg_attribute where attrelid = to_regclass($3) and attname = $4 and not attisdropped), true),
			coalesce((select pg_get_expr(d.adbin, d.adrelid) from pg_attrdef d join pg_attribute a on a.attrelid = d.adrelid and a.attnum = d.adnum
			          where d.adrelid = to_regclass($3) and a.attname = $4), '')`, c.OID, c.Mod, ident, c.Name).Scan(&typ, &notNull, &def)
		if err != nil {
			return nil, nil, false, err
		}
		nt.Columns = append(nt.Columns, source.Column{Name: c.Name, Type: typ, OID: c.OID, Mod: c.Mod})
		have := t.Column(c.Name)
		switch {
		case have == nil:
			sql := fmt.Sprintf("alter table %s add column if not exists %s %s", ident, name, typ)
			if def != "" { // rows that were there already get it without being sent
				sql += " default " + def
			}
			stmts = append(stmts, sql)
			safe = safe && !notNull && def == ""
		case have.Type != typ:
			stmts = append(stmts, fmt.Sprintf("alter table %s alter column %s type %s using %s::%s", ident, name, typ, name, typ))
			safe = false
		}
	}
	for _, c := range t.Columns {
		if !slices.ContainsFunc(rel.Columns, func(r pgoutput.Column) bool { return r.Name == c.Name }) {
			stmts = append(stmts, fmt.Sprintf("alter table %s drop column if exists %s", ident, pgx.Identifier{c.Name}.Sanitize()))
			safe = false
		}
	}
	return nt, stmts, safe, nil
}

// ask pauses the replication until the statement is approved.
func (e *Engine) ask(ctx context.Context, tbl, sql string) error {
	e.mu.Lock()
	e.st.Waiting = sql
	e.mu.Unlock()
	e.cfg.Log.Warn("paused for a schema change", "table", tbl, "sql", sql)
	e.event("schema_waiting", "waiting-"+tbl+"-"+sql, map[string]any{"table": tbl, "sql": sql})
	defer func() {
		e.mu.Lock()
		e.st.Waiting = ""
		e.mu.Unlock()
	}()
	for {
		select {
		case got := <-e.approve:
			if got == sql {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
