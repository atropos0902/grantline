package txn

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"golang.org/x/sync/semaphore"
	"pgregory.net/rapid"

	"grantline/internal/pgoutput"
)

type msg []byte

func (m msg) u32(v uint32) msg { return binary.BigEndian.AppendUint32(m, v) }
func (m msg) u64(v uint64) msg { return binary.BigEndian.AppendUint64(m, v) }
func (m msg) u16(v uint16) msg { return binary.BigEndian.AppendUint16(m, v) }
func (m msg) str(s string) msg { return append(append(m, s...), 0) }

func relation(stream bool, xid, id uint32, cols ...string) []byte {
	m := msg{'R'}
	if stream {
		m = m.u32(xid)
	}
	m = append(m.u32(id).str("public").str("grants"), 'd').u16(uint16(len(cols)))
	for i, c := range cols {
		k := byte(0)
		if i == 0 {
			k = 1
		}
		m = append(m, k).str(c).u32(20).u32(^uint32(0))
	}
	return m
}

func insert(stream bool, xid, rel uint32, vals ...string) []byte {
	m := msg{'I'}
	if stream {
		m = m.u32(xid)
	}
	m = append(m.u32(rel), 'N').u16(uint16(len(vals)))
	for _, v := range vals {
		m = append(append(m, 't').u32(uint32(len(v))), v...)
	}
	return m
}

func begin(xid uint32) []byte  { return msg{'B'}.u64(1).u64(0).u32(xid) }
func commit() []byte           { return append(msg{'C'}, 0).u64(1).u64(2).u64(0) }
func sstart(xid uint32) []byte { return append(msg{'S'}.u32(xid), 1) }
func sstop() []byte            { return []byte{'E'} }
func scommit(xid uint32) []byte {
	return append(msg{'c'}.u32(xid), 0).u64(1).u64(2).u64(0)
}
func sabort(xid, sub uint32) []byte { return msg{'A'}.u32(xid).u32(sub) }

func feed(t testing.TB, a *Assembler, ms ...[]byte) *Txn {
	t.Helper()
	var out *Txn
	for i, m := range ms {
		x, err := a.Feed(m)
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if x != nil {
			out = x
		}
	}
	return out
}

func firstValues(t testing.TB, x *Txn) []string {
	t.Helper()
	var got []string
	var row pgoutput.Row
	if err := x.Each(func(c *Change) error {
		if err := pgoutput.DecodeRow(c.Op, c.Body, &row); err != nil {
			return err
		}
		got = append(got, fmt.Sprintf("%s/%d", row.New[0].Data, len(c.Rel.Columns)))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func lim(t testing.TB, at int64) *Limits { return &Limits{Dir: t.TempDir(), SpillAt: at} }

// A savepoint that was rolled back in a streamed transaction leaves no rows.
func TestRolledBackSavepointIsSkipped(t *testing.T) {
	for _, at := range []int64{1 << 20, 10} {
		a := NewAssembler(lim(t, at))
		x := feed(t, a, sstart(9), relation(true, 9, 1, "id"), insert(true, 9, 1, "1"), insert(true, 10, 1, "2"), sstop(),
			sabort(9, 10), sstart(9), insert(true, 9, 1, "3"), sstop(), scommit(9))
		if got := fmt.Sprint(firstValues(t, x)); got != "[1/1 3/1]" {
			t.Errorf("spill at %d: got %s", at, got)
		}
		if x.OnDisk() != (at == 10) {
			t.Errorf("spill at %d: on disk = %v", at, x.OnDisk())
		}
	}
}

// A shape that only a streamed transaction can see must not leak into others.
func TestStreamedShapeStaysPrivateUntilCommit(t *testing.T) {
	a := NewAssembler(lim(t, 1<<20))
	feed(t, a, begin(5), relation(false, 0, 1, "id"), insert(false, 0, 1, "1"), commit())
	feed(t, a, sstart(9), relation(true, 9, 1, "id", "note"), insert(true, 9, 1, "2", "x"), sstop())
	if got := fmt.Sprint(firstValues(t, feed(t, a, begin(6), insert(false, 0, 1, "3"), commit()))); got != "[3/1]" {
		t.Errorf("other transaction saw the private shape: %s", got)
	}
	if got := fmt.Sprint(firstValues(t, feed(t, a, scommit(9)))); got != "[2/2]" {
		t.Errorf("streamed transaction: %s", got)
	}
	if got := fmt.Sprint(firstValues(t, feed(t, a, begin(7), insert(false, 0, 1, "4", "y"), commit()))); got != "[4/2]" {
		t.Errorf("after commit: %s", got)
	}
}

func TestSpillKeepsOrderAndReleasesBudget(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		b := semaphore.NewWeighted(4 * grain)
		l := &Limits{Dir: t.TempDir(), SpillAt: rapid.Int64Range(1, 6*grain).Draw(rt, "spillAt"), Budget: b}
		x := New(1, l)
		rel := &pgoutput.Relation{ID: 1}
		var want [][]byte
		for _, n := range rapid.SliceOfN(rapid.IntRange(0, grain), 0, 12).Draw(rt, "sizes") {
			body := bytes.Repeat([]byte{byte(n)}, n)
			want = append(want, body)
			if err := x.Add('I', 1, rel, "", body); err != nil {
				rt.Fatal(err)
			}
		}
		i := 0
		if err := x.Each(func(c *Change) error {
			if !bytes.Equal(c.Body, want[i]) || c.Rel != rel {
				rt.Fatalf("change %d differs", i)
			}
			i++
			return nil
		}); err != nil || i != len(want) {
			rt.Fatal(i, err)
		}
		x.Close()
		if !b.TryAcquire(4 * grain) {
			rt.Fatal("budget not returned")
		}
	})
}

func FuzzSplit(f *testing.F) {
	f.Add(begin(1), false)
	f.Add(relation(true, 9, 1, "id", "note"), true)
	f.Add(insert(false, 0, 1, "abc"), false)
	f.Fuzz(func(t *testing.T, b []byte, stream bool) {
		m, err := pgoutput.Split(b, stream)
		if err == nil && m.Kind == pgoutput.KChange {
			var row pgoutput.Row
			_ = pgoutput.DecodeRow(m.Op, m.Body, &row)
		}
	})
}
