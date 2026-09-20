package source

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"grantline/internal/lsn"
)

// Castagnoli is the CRC32-C table chunks are summed with.
var Castagnoli = crc32.MakeTable(crc32.Castagnoli)

// Bounds is a chunk's key range: greater than Lo, up to and including Hi. A nil
// end is open.
type Bounds struct {
	Lo, Hi []string
}

func (t *Table) keyList() string {
	q := make([]string, len(t.Key))
	for i, k := range t.Key {
		q[i] = pgx.Identifier{k}.Sanitize()
	}
	return strings.Join(q, ", ")
}

// where renders the range as SQL, with each value written by lit.
func (t *Table) where(b Bounds, lit func(string) string) string {
	var conds []string
	side := func(op string, vals []string) {
		if vals == nil {
			return
		}
		ps := make([]string, len(vals))
		for i, v := range vals {
			ps[i] = lit(v) + "::" + t.Column(t.Key[i]).Type
		}
		conds = append(conds, fmt.Sprintf("(%s) %s (%s)", t.keyList(), op, strings.Join(ps, ", ")))
	}
	side(">", b.Lo)
	side("<=", b.Hi)
	if conds == nil {
		return "true"
	}
	return strings.Join(conds, " and ")
}

// Where renders the range with its values inlined, because COPY takes no
// parameters.
func (t *Table) Where(b Bounds) string { return t.where(b, Literal) }

// Literal quotes a string for SQL. Both ends run with
// standard_conforming_strings on, so a backslash needs nothing.
func Literal(v string) string { return "'" + strings.ReplaceAll(v, "'", "''") + "'" }

// NextBound returns the key n rows past lo, or nil when the table ends first.
func (s *Source) NextBound(ctx context.Context, t *Table, lo []string, n int) ([]string, error) {
	sel := make([]string, len(t.Key))
	for i, k := range t.Key {
		sel[i] = pgx.Identifier{k}.Sanitize() + "::text"
	}
	row := s.Pool.QueryRow(ctx, fmt.Sprintf(`select array[%s] from %s where %s order by %s offset %d limit 1`,
		strings.Join(sel, ", "), t.Ident(), t.Where(Bounds{Lo: lo}), t.keyList(), n-1))
	var hi []string
	if err := row.Scan(&hi); errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return hi, nil
}

// ChunkRead describes a finished read.
type ChunkRead struct {
	Snapshot string // pg_snapshot the rows were read under
	Marker   lsn.LSN
	Rows     int64
	Bytes    int64
	CRC      uint32
}

// ReadChunk copies the range to w and says which snapshot it saw. afterSnapshot
// runs once the snapshot is fixed and before any row is read; it may be nil.
func (s *Source) ReadChunk(ctx context.Context, t *Table, where string, cols []string, binary bool, w io.Writer, afterSnapshot func(snap string) error) (ChunkRead, error) {
	var res ChunkRead
	// The marker comes first, as a method that compares positions would do it.
	// This engine uses it for progress only.
	m, err := s.Mark(ctx, "chunk")
	if err != nil {
		return res, err
	}
	res.Marker = m
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return res, err
	}
	defer conn.Release()
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return res, err
	}
	defer tx.Rollback(context.WithoutCancel(ctx))
	if err := tx.QueryRow(ctx, `select pg_current_snapshot()::text`).Scan(&res.Snapshot); err != nil {
		return res, err
	}
	if afterSnapshot != nil {
		if err := afterSnapshot(res.Snapshot); err != nil {
			return res, err
		}
	}
	q := make([]string, len(cols))
	for i, c := range cols {
		q[i] = pgx.Identifier{c}.Sanitize()
	}
	format := "text"
	if binary {
		format = "binary"
	}
	sum := crc32.New(Castagnoli)
	cw := &countWriter{w: io.MultiWriter(sum, w)}
	tag, err := tx.Conn().PgConn().CopyTo(ctx, cw, fmt.Sprintf(`copy (select %s from %s where %s) to stdout (format %s)`,
		strings.Join(q, ", "), t.Ident(), where, format))
	if err != nil {
		return res, fmt.Errorf("read chunk of %s: %w", t.FullName(), err)
	}
	res.Rows, res.Bytes, res.CRC = tag.RowsAffected(), cw.n, sum.Sum32()
	return res, nil
}

type countWriter struct {
	w io.Writer
	n int64
}

func (c *countWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}
