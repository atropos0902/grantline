package integration

import (
	"encoding/binary"
	"fmt"
	"io"
	"testing"
	"time"

	"grantline/internal/harness"
	"grantline/internal/lsn"
	"grantline/internal/pgoutput"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
	"grantline/internal/txn"
)

var benchTable = &source.Table{Schema: "public", Name: "grants", Key: []string{"id"}, Columns: []source.Column{
	{Name: "id", Type: "bigint", OID: 20, NotNull: true}, {Name: "principal", Type: "text", OID: 25}, {Name: "resource", Type: "text", OID: 25},
	{Name: "state", Type: "text", OID: 25}, {Name: "badge", Type: "integer", OID: 23}, {Name: "policy", Type: "text", OID: 25}}}

var benchRel = &pgoutput.Relation{ID: 1, Schema: "public", Name: "grants", Identity: 'd', Columns: []pgoutput.Column{
	{Key: true, Name: "id", OID: 20}, {Name: "principal", OID: 25}, {Name: "resource", OID: 25}, {Name: "state", OID: 25}, {Name: "badge", OID: 23}, {Name: "policy", OID: 25}}}

func body(vals ...string) []byte {
	b := binary.BigEndian.AppendUint16([]byte{'N'}, uint16(len(vals)))
	for _, v := range vals {
		if v == "" {
			b = append(b, 'n')
			continue
		}
		b = append(b, 't')
		b = binary.BigEndian.AppendUint32(b, uint32(len(v)))
		b = append(b, v...)
	}
	return b
}

// One group of 250 updates, prepared and committed on one lane.
func BenchmarkApplyGroup(b *testing.B) {
	p := harness.NewPair(b, srv.src, srv.dst)
	ctx := b.Context()
	t, err := pgtarget.New(ctx, pgtarget.Config{DSN: p.TgtDS, Replication: "bench", Lanes: 1, Journal: true})
	if err != nil {
		b.Fatal(err)
	}
	defer t.Close()
	if _, err := t.Open(ctx); err != nil {
		b.Fatal(err)
	}
	if err := t.SetTables(ctx, []*source.Table{benchTable}); err != nil {
		b.Fatal(err)
	}
	// The table has finished copying, so no ledger is kept.
	if _, err := p.Target.Exec(ctx, `insert into grantline.copied values ('bench', 'public.grants', true, true, false)`); err != nil {
		b.Fatal(err)
	}
	if err := t.Reload(ctx); err != nil {
		b.Fatal(err)
	}
	lim := &txn.Limits{Dir: b.TempDir(), SpillAt: 1 << 20}
	pos := lsn.LSN(1 << 20)
	n := 0
	b.ReportAllocs()
	for b.Loop() {
		g := &target.Group{Time: time.Now(), RefXID: 1000}
		for range 25 {
			x := txn.New(900, lim)
			for range 10 {
				n++
				if err := x.Add('U', 900, benchRel, "", body(fmt.Sprint(n*7919%50000), "user-17", "dash-3", "revoked", "", "")); err != nil {
					b.Fatal(err)
				}
			}
			pos += 100
			x.CommitLSN, x.EndLSN = pos-10, pos
			g.Txns = append(g.Txns, x)
		}
		g.EndLSN = pos
		it, err := t.Item(g)
		if err != nil {
			b.Fatal(err)
		}
		if err := it.Work.Prepare(ctx, 0); err != nil {
			b.Fatal(err)
		}
		if err := it.Work.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(250*b.N)/b.Elapsed().Seconds(), "changes/s")
}

// One chunk of 20,000 rows, read and staged, as the engine does it.
func BenchmarkChunk(b *testing.B) {
	p := harness.NewPair(b, srv.src, srv.dst)
	ctx := b.Context()
	if _, err := p.Source.Exec(ctx, grantsSQL+`insert into grants select i, 'user-' || i, 'dash-' || i % 400, 'active', i, null from generate_series(1, 20000) i`); err != nil {
		b.Fatal(err)
	}
	src, err := source.Open(ctx, p.SourceDSN, "x", "grants_pub")
	if err != nil {
		b.Fatal(err)
	}
	defer src.Close()
	t, err := pgtarget.New(ctx, pgtarget.Config{DSN: p.TgtDS, Replication: "bench", Lanes: 1})
	if err != nil {
		b.Fatal(err)
	}
	defer t.Close()
	tables, err := src.Tables(ctx)
	if err != nil {
		b.Fatal(err)
	}
	if err := t.SetTables(ctx, tables); err != nil {
		b.Fatal(err)
	}
	tb := tables[0]
	cols := []string{"id", "principal", "resource", "state", "badge", "policy"}
	for _, f := range []string{"text", "binary"} {
		b.Run("format="+f, func(b *testing.B) {
			cp, err := t.NewCopier(ctx)
			if err != nil {
				b.Fatal(err)
			}
			defer cp.Close()
			for b.Loop() {
				pr, pw := io.Pipe()
				done := make(chan error, 1)
				go func() {
					_, _, err := cp.Stage(ctx, tb, cols, f == "binary", pr)
					pr.CloseWithError(err)
					done <- err
				}()
				_, err := src.ReadChunk(ctx, tb, "true", cols, f == "binary", pw, nil)
				pw.CloseWithError(err)
				if serr := <-done; err != nil || serr != nil {
					b.Fatal(err, serr)
				}
			}
			b.ReportMetric(float64(20000*b.N)/b.Elapsed().Seconds(), "rows/s")
		})
	}
}
