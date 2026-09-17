// Package pgoutput decodes the pgoutput logical replication protocol,
// version 2 and up, with in-progress transaction streaming.
//
// Decoding happens in two steps. The reader only looks at what it needs to
// assemble transactions (Split). Row values are decoded later, in the lane that
// applies them (DecodeRow), and they alias the message bytes.
package pgoutput

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"grantline/internal/lsn"
)

// Column is one column of a relation as the stream describes it.
type Column struct {
	Key  bool
	Name string
	OID  uint32
	Mod  int32
}

// Relation is one version of a table's shape. A change of shape gets a new
// Relation, so a pointer identifies the version a row was written under.
type Relation struct {
	ID       uint32
	Schema   string
	Name     string
	Identity byte // d, n, f or i
	Columns  []Column
}

// FullName is schema.table, unquoted.
func (r *Relation) FullName() string { return r.Schema + "." + r.Name }

// Value kinds, as sent.
const (
	Null      byte = 'n'
	Unchanged byte = 'u' // a large value the update didn't touch, not sent
	Text      byte = 't'
	Binary    byte = 'b'
)

// Value is one column value. Data aliases the message.
type Value struct {
	Kind byte
	Data []byte
}

// Row change kinds.
const (
	Insert   byte = 'I'
	Update   byte = 'U'
	Delete   byte = 'D'
	Truncate byte = 'T'
	Message  byte = 'M'
)

// Row is a decoded insert, update or delete.
type Row struct {
	Old     []Value // nil when the change doesn't carry an old tuple
	OldFull bool    // Old is the whole row, not only the key
	New     []Value // nil for a delete
}

// Kind of a protocol message after Split.
type Kind byte

// Message kinds the reader acts on.
const (
	KBegin Kind = iota + 1
	KCommit
	KStreamStart
	KStreamStop
	KStreamCommit
	KStreamAbort
	KRelation
	KChange
	KTruncate
	KMessage
	KIgnore
)

// Msg is one protocol message with its envelope decoded. Body aliases the input.
type Msg struct {
	Kind      Kind
	XID       uint32 // top transaction for begin and stream control, otherwise the (sub)transaction of a streamed change
	SubXID    uint32 // stream abort only
	CommitLSN lsn.LSN
	EndLSN    lsn.LSN
	Time      time.Time
	First     bool // first segment of a streamed transaction
	Op        byte
	RelID     uint32
	RelIDs    []uint32 // truncate
	Relation  *Relation
	Transact  bool   // logical message written inside a transaction
	Prefix    string // logical message
	Body      []byte // row payload after the relation id, or message content
}

var errShort = errors.New("pgoutput: message ends early")

type rd struct {
	b   []byte
	err error
}

func (r *rd) take(n int) []byte {
	if r.err != nil || n < 0 || len(r.b) < n {
		r.err = errShort
		return nil
	}
	p := r.b[:n:n]
	r.b = r.b[n:]
	return p
}

func (r *rd) u8() byte {
	if p := r.take(1); p != nil {
		return p[0]
	}
	return 0
}

func (r *rd) u16() uint16 {
	if p := r.take(2); p != nil {
		return binary.BigEndian.Uint16(p)
	}
	return 0
}

func (r *rd) u32() uint32 {
	if p := r.take(4); p != nil {
		return binary.BigEndian.Uint32(p)
	}
	return 0
}

func (r *rd) u64() uint64 {
	if p := r.take(8); p != nil {
		return binary.BigEndian.Uint64(p)
	}
	return 0
}

func (r *rd) str() string {
	for i, c := range r.b {
		if c == 0 {
			s := string(r.b[:i])
			r.b = r.b[i+1:]
			return s
		}
	}
	r.err = errShort
	return ""
}

var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

func (r *rd) ts() time.Time {
	return pgEpoch.Add(time.Duration(int64(r.u64())) * time.Microsecond)
}

// Split decodes a message's envelope. inStream says whether the message sits
// between a stream start and a stream stop, where every message carries the ID
// of the transaction that wrote it.
func Split(b []byte, inStream bool) (Msg, error) {
	if len(b) == 0 {
		return Msg{}, errShort
	}
	r := &rd{b: b[1:]}
	var m Msg
	switch b[0] {
	case 'B':
		m.Kind = KBegin
		m.CommitLSN = lsn.LSN(r.u64())
		m.Time = r.ts()
		m.XID = r.u32()
	case 'C':
		m.Kind = KCommit
		r.u8()
		m.CommitLSN = lsn.LSN(r.u64())
		m.EndLSN = lsn.LSN(r.u64())
		m.Time = r.ts()
	case 'S':
		m.Kind = KStreamStart
		m.XID = r.u32()
		m.First = r.u8() == 1
	case 'E':
		m.Kind = KStreamStop
	case 'c':
		m.Kind = KStreamCommit
		m.XID = r.u32()
		r.u8()
		m.CommitLSN = lsn.LSN(r.u64())
		m.EndLSN = lsn.LSN(r.u64())
		m.Time = r.ts()
	case 'A':
		m.Kind = KStreamAbort
		m.XID = r.u32()
		m.SubXID = r.u32()
	case 'R':
		m.Kind = KRelation
		if inStream {
			m.XID = r.u32()
		}
		m.Relation = decodeRelation(r)
	case 'I', 'U', 'D':
		m.Kind = KChange
		m.Op = b[0]
		if inStream {
			m.XID = r.u32()
		}
		m.RelID = r.u32()
		m.Body = r.b
	case 'T':
		m.Kind = KTruncate
		m.Op = 'T'
		if inStream {
			m.XID = r.u32()
		}
		n := int(r.u32())
		r.u8()
		if n > len(r.b)/4 {
			return m, errShort
		}
		for range n {
			m.RelIDs = append(m.RelIDs, r.u32())
		}
	case 'M':
		m.Kind = KMessage
		m.Op = 'M'
		if inStream {
			m.XID = r.u32()
		}
		m.Transact = r.u8()&1 == 1
		r.u64()
		m.Prefix = r.str()
		m.Body = r.take(int(r.u32()))
	case 'O', 'Y':
		m.Kind = KIgnore
	default:
		return m, fmt.Errorf("pgoutput: unknown message %q", b[0])
	}
	return m, r.err
}

func decodeRelation(r *rd) *Relation {
	rel := &Relation{ID: r.u32(), Schema: r.str(), Name: r.str(), Identity: r.u8()}
	n := int(r.u16())
	for range n {
		if r.err != nil {
			break
		}
		rel.Columns = append(rel.Columns, Column{
			Key: r.u8()&1 == 1, Name: r.str(), OID: r.u32(), Mod: int32(r.u32()),
		})
	}
	return rel
}

// DecodeRow decodes the payload of an insert, update or delete. The row's
// slices are reused when it already has room, and its values alias body.
func DecodeRow(op byte, body []byte, row *Row) error {
	r := &rd{b: body}
	old, nw := row.Old[:0], row.New[:0]
	*row = Row{}
	if op != Insert {
		switch {
		case len(r.b) == 0:
			return errShort
		case r.b[0] == 'K' || r.b[0] == 'O':
			row.OldFull = r.u8() == 'O'
			row.Old = tuple(r, old)
		case op == Delete:
			return fmt.Errorf("pgoutput: delete without an old tuple (tag %q)", r.b[0])
		}
	}
	if op != Delete {
		if t := r.u8(); r.err == nil && t != 'N' {
			return fmt.Errorf("pgoutput: expected a new tuple, got %q", t)
		}
		row.New = tuple(r, nw)
	}
	if r.err == nil && len(r.b) != 0 {
		return fmt.Errorf("pgoutput: %d bytes left after a row", len(r.b))
	}
	return r.err
}

func tuple(r *rd, vs []Value) []Value {
	n := int(r.u16())
	for range n {
		k := r.u8()
		switch k {
		case Null, Unchanged:
			vs = append(vs, Value{Kind: k})
		case Text, Binary:
			vs = append(vs, Value{Kind: k, Data: r.take(int(r.u32()))})
		default:
			if r.err == nil {
				r.err = fmt.Errorf("pgoutput: unknown value kind %q", k)
			}
		}
		if r.err != nil {
			return nil
		}
	}
	return vs
}
