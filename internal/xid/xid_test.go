package xid

import (
	"testing"

	"pgregory.net/rapid"
)

func TestWidenAcrossWrap(t *testing.T) {
	for _, c := range []struct {
		low  uint32
		ref  uint64
		want uint64
	}{
		{756, 758, 756},
		{4294967290, 1<<32 + 5, 4294967290}, // before the wrap, seen after it
		{3, 1<<32 - 2, 1<<32 + 3},           // after the wrap, reference still before it
		{756, 5<<32 + 758, 5<<32 + 756},     // epoch 5
		{4294967295, 7<<32 + 1, 7<<32 - 1},  // epoch edge
		{4294967295, 10, 4294967295},        // nothing below epoch zero
	} {
		if got := Widen(c.low, c.ref); got != c.want {
			t.Errorf("Widen(%d, %d) = %d, want %d", c.low, c.ref, got, c.want)
		}
	}
}

func TestWidenProperty(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		full := rapid.Uint64Range(3, 1<<45).Draw(t, "full")
		drift := rapid.Int64Range(-(1<<31)+1, 1<<31-1).Draw(t, "drift")
		ref := int64(full) + drift
		if ref < 3 {
			t.Skip()
		}
		if got := Widen(uint32(full), uint64(ref)); got != full {
			t.Fatalf("Widen(%d, %d) = %d, want %d", uint32(full), ref, got, full)
		}
	})
}
