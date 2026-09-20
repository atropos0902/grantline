package pgtarget

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/source"
)

// Step is one thing that happened to a key.
type Step struct {
	At       time.Time
	What     string // insert, update, delete, truncate, chunk, refused
	XID      uint64
	LSN      string
	Snapshot string
	Chunk    int32
	Epoch    int64
	Reason   string
}

var opNames = map[string]string{"I": "insert", "U": "update", "D": "delete", "T": "truncate", "R": "refused"}

// Explain lists what happened to a key, oldest first: every change the stream
// applied, the chunk whose range holds it, and whether the chunk's row was
// refused. It only reads.
func Explain(ctx context.Context, pool *pgxpool.Pool, replication, table string, st *source.Table, key []string) ([]Step, error) {
	text := KeyText(key)
	rows, err := pool.Query(ctx, `select at, op::text, coalesce(xid::text, '0'), coalesce(lsn::text, ''), coalesce(snapshot::text, ''), coalesce(chunk, -1), coalesce(epoch, 0)
		from grantline.journal where replication = $1 and tbl = $2 and (key = $3 or key = '*') order by at, ctid`, replication, table, text)
	if err != nil {
		return nil, err
	}
	var steps []Step
	var s Step
	var op, xid string
	if _, err := pgx.ForEachRow(rows, []any{&s.At, &op, &xid, &s.LSN, &s.Snapshot, &s.Chunk, &s.Epoch}, func() error {
		n := s
		n.What = opNames[op]
		_, _ = fmt.Sscan(xid, &n.XID)
		if op == "R" {
			n.Reason = fmt.Sprintf("chunk %d had a row for this key, read under snapshot %s. Transaction %d changed the key and that snapshot can't see it, so the chunk's row is older and was left out.", n.Chunk, n.Snapshot, n.XID)
		}
		steps = append(steps, n)
		return nil
	}); err != nil {
		return nil, err
	}
	chunks, err := chunksHolding(ctx, pool, replication, table, st, key)
	if err != nil {
		return nil, err
	}
	for _, c := range chunks {
		refused := false
		for i := range steps {
			if steps[i].What == "refused" && steps[i].Chunk == c.Chunk {
				refused = true
			}
		}
		switch {
		case c.At.IsZero():
			c.Reason = fmt.Sprintf("chunk %d covers this key and hasn't landed yet.", c.Chunk)
		case refused:
			c.Reason = fmt.Sprintf("chunk %d covers this key and landed without it.", c.Chunk)
		default:
			// The guard's question again, over what is on record.
			var missed uint64
			for _, st := range steps {
				var seen bool
				if st.XID == 0 || st.At.After(c.At) {
					continue
				}
				if err := pool.QueryRow(ctx, `select pg_visible_in_snapshot($1::text::xid8, $2::text::pg_snapshot)`, fmt.Sprint(st.XID), c.Snapshot).Scan(&seen); err != nil {
					return nil, err
				}
				if !seen {
					missed = st.XID
				}
			}
			if missed != 0 {
				c.Reason = fmt.Sprintf("FAULT: chunk %d was read under snapshot %s, which can't see transaction %d. The change was already here when the chunk landed, so its older row should have been left out and wasn't.", c.Chunk, c.Snapshot, missed)
				break
			}
			c.Reason = fmt.Sprintf("chunk %d covers this key and landed. Its row for the key, if the source had one under snapshot %s, was written.", c.Chunk, c.Snapshot)
		}
		at := len(steps)
		for i := range steps {
			if !c.At.IsZero() && steps[i].At.After(c.At) {
				at = i
				break
			}
		}
		steps = append(steps[:at], append([]Step{c}, steps[at:]...)...)
	}
	return steps, nil
}

func chunksHolding(ctx context.Context, pool *pgxpool.Pool, replication, table string, st *source.Table, key []string) ([]Step, error) {
	row := func(expr func(i int) string) string {
		p := make([]string, len(st.Key))
		for i, k := range st.Key {
			p[i] = expr(i) + "::" + st.Column(k).Type
		}
		return "(" + strings.Join(p, ", ") + ")"
	}
	val := row(func(i int) string { return source.Literal(key[i]) })
	lo := row(func(i int) string { return fmt.Sprintf("lo[%d]", i+1) })
	hi := row(func(i int) string { return fmt.Sprintf("hi[%d]", i+1) })
	rows, err := pool.Query(ctx, fmt.Sprintf(`select no, coalesce(at, 'epoch'), coalesce(snapshot::text, ''), coalesce(marker::text, ''), coalesce(epoch, 0)
		from grantline.chunk where replication = $1 and tbl = $2 and (lo is null or %s > %s) and (hi is null or %s <= %s)`, val, lo, val, hi), replication, table)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Step, error) {
		s := Step{What: "chunk"}
		err := r.Scan(&s.Chunk, &s.At, &s.Snapshot, &s.LSN, &s.Epoch)
		if s.At.Unix() == 0 {
			s.At = time.Time{}
		}
		return s, err
	})
}

// RowJSON reads a row by key, empty when there is none.
func RowJSON(ctx context.Context, pool *pgxpool.Pool, st *source.Table, key []string) (string, error) {
	conds := make([]string, len(st.Key))
	for i, k := range st.Key {
		conds[i] = fmt.Sprintf("%s = %s", pgx.Identifier{k}.Sanitize(), source.Literal(key[i]))
	}
	var s string
	err := pool.QueryRow(ctx, fmt.Sprintf(`select to_jsonb(t)::text from %s t where %s`, st.Ident(), strings.Join(conds, " and "))).Scan(&s)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	return s, err
}

// ChunkRow is a chunk as the console shows it.
type ChunkRow struct {
	Table    string
	No       int32
	Lo, Hi   []string
	Landed   bool
	Snapshot string
	Rows     int64
	Refused  int64
	CRC      uint32
}

// Chunks lists a table's chunks, or every table's when the name is empty.
func Chunks(ctx context.Context, pool *pgxpool.Pool, replication, table string) ([]ChunkRow, error) {
	rows, err := pool.Query(ctx, `select c.tbl, c.no, c.lo, c.hi, c.landed, coalesce(c.snapshot::text, ''), coalesce(c.rows, 0),
		  (select count(*) from grantline.journal j where j.replication = c.replication and j.tbl = c.tbl and j.op = 'R' and j.chunk = c.no),
		  coalesce(c.crc32c, 0)::bigint
		from grantline.chunk c where c.replication = $1 and ($2 = '' or c.tbl = $2) order by 1, 2 limit 5000`, replication, table)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (ChunkRow, error) {
		var c ChunkRow
		var crc int64
		err := r.Scan(&c.Table, &c.No, &c.Lo, &c.Hi, &c.Landed, &c.Snapshot, &c.Rows, &c.Refused, &crc)
		c.CRC = uint32(crc)
		return c, err
	})
}
