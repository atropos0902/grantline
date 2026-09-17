package pgoutput

import (
	"encoding/binary"
	"testing"
)

func row(cols int) []byte {
	b := binary.BigEndian.AppendUint32([]byte{'U'}, 1)
	b = append(b, 'N')
	b = binary.BigEndian.AppendUint16(b, uint16(cols))
	for range cols {
		b = append(b, 't')
		b = binary.BigEndian.AppendUint32(b, 12)
		b = append(b, "some-value-x"...)
	}
	return b
}

func BenchmarkSplitAndDecode(b *testing.B) {
	m := row(6)
	var r Row
	b.ReportAllocs()
	b.SetBytes(int64(len(m)))
	for b.Loop() {
		s, err := Split(m, false)
		if err != nil {
			b.Fatal(err)
		}
		if err := DecodeRow(s.Op, s.Body, &r); err != nil {
			b.Fatal(err)
		}
	}
}
