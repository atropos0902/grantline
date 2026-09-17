package pgoutput

import (
	"testing"

	"github.com/jackc/pglogrepl"
	"pgregory.net/rapid"
)

// The decoder agrees with pglogrepl's on rows it makes up.
func TestAgreesWithPglogrepl(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		tuple := func(b []byte) []byte {
			// Not zero: pglogrepl refuses an insert into a table without columns,
			// see docs/upstream.md.
			n := rapid.IntRange(1, 6).Draw(t, "cols")
			b = append(b, byte(n>>8), byte(n))
			for range n {
				k := rapid.SampledFrom([]byte{'n', 'u', 't', 'b'}).Draw(t, "kind")
				b = append(b, k)
				if k == 't' || k == 'b' {
					v := rapid.SliceOfN(rapid.Byte(), 0, 20).Draw(t, "v")
					b = append(b, byte(len(v)>>24), byte(len(v)>>16), byte(len(v)>>8), byte(len(v)))
					b = append(b, v...)
				}
			}
			return b
		}
		op := rapid.SampledFrom([]byte{Insert, Update, Delete}).Draw(t, "op")
		stream := rapid.Bool().Draw(t, "stream")
		m := []byte{op}
		if stream {
			m = append(m, 0, 0, 0, 77)
		}
		m = append(m, 0, 0, 0, 5)
		if op == Delete || op == Update && rapid.Bool().Draw(t, "old") {
			m = tuple(append(m, rapid.SampledFrom([]byte{'K', 'O'}).Draw(t, "oldKind")))
		}
		if op != Delete {
			m = tuple(append(m, 'N'))
		}
		got, err := Split(m, stream)
		if err != nil {
			t.Fatal(err)
		}
		var r Row
		if err := DecodeRow(got.Op, got.Body, &r); err != nil {
			t.Fatal(err)
		}
		ref, err := pglogrepl.ParseV2(m, stream)
		if err != nil {
			t.Fatal(err)
		}
		same := func(a []Value, b *pglogrepl.TupleData) {
			if b == nil {
				if a != nil {
					t.Fatalf("tuple %v, pglogrepl has none", a)
				}
				return
			}
			if len(a) != len(b.Columns) {
				t.Fatalf("%d columns, pglogrepl has %d", len(a), len(b.Columns))
			}
			for i, c := range b.Columns {
				if a[i].Kind != c.DataType || string(a[i].Data) != string(c.Data) {
					t.Fatalf("column %d: %v and %v", i, a[i], c)
				}
			}
		}
		switch x := ref.(type) {
		case *pglogrepl.InsertMessageV2:
			same(r.New, x.Tuple)
		case *pglogrepl.UpdateMessageV2:
			same(r.New, x.NewTuple)
			same(r.Old, x.OldTuple)
		case *pglogrepl.DeleteMessageV2:
			same(r.Old, x.OldTuple)
		default:
			t.Fatalf("%T", ref)
		}
	})
}
