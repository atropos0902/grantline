// Package txn assembles source transactions out of the change stream.
//
// A transaction's changes are kept as frames of undecoded bytes. They stay in
// memory while they fit and move to a file when the transaction is large or the
// shared byte budget has run out. Assembly never waits for budget: the
// transaction that would have to wait may be the one everything else is
// waiting for.
package txn

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	"golang.org/x/sync/semaphore"

	"grantline/internal/bugs"
	"grantline/internal/lsn"
	"grantline/internal/pgoutput"
)

// Change is one frame of a transaction.
type Change struct {
	Op     byte
	SubXID uint32
	Rel    *pgoutput.Relation // nil for a logical message
	Body   []byte             // row payload, or message content
	Prefix string             // logical message
}

// Txn is one source transaction.
type Txn struct {
	XID       uint32
	CommitLSN lsn.LSN
	EndLSN    lsn.LSN
	Time      time.Time
	Changes   int
	Bytes     int64
	Truncates bool

	lim     *Limits
	mem     []byte
	held    int64
	rels    []*pgoutput.Relation
	relIdx  map[*pgoutput.Relation]uint32
	file    *os.File
	w       *bufio.Writer
	aborted map[uint32]struct{}
}

// Limits decide when a transaction moves to disk.
type Limits struct {
	Dir     string
	SpillAt int64               // bytes one transaction may hold in memory
	Budget  *semaphore.Weighted // bytes all transactions may hold together
	Spilled func(bytes int64)   // told once per transaction that moves to disk, may be nil
}

const grain = 64 << 10

// New starts a transaction.
func New(xid uint32, lim *Limits) *Txn {
	return &Txn{XID: xid, lim: lim, relIdx: map[*pgoutput.Relation]uint32{}}
}

const header = 4 + 4 + 4 + 1 // length, relation, subtransaction, op

// Add appends one change. body is copied.
func (t *Txn) Add(op byte, sub uint32, rel *pgoutput.Relation, prefix string, body []byte) error {
	idx := ^uint32(0)
	if rel != nil {
		var ok bool
		if idx, ok = t.relIdx[rel]; !ok {
			idx = uint32(len(t.rels))
			t.relIdx[rel] = idx
			t.rels = append(t.rels, rel)
		}
	}
	if op == pgoutput.Message {
		p := make([]byte, 0, 2+len(prefix)+len(body))
		p = binary.BigEndian.AppendUint16(p, uint16(len(prefix)))
		p = append(p, prefix...)
		body = append(p, body...)
	}
	n := int64(header + len(body))
	if t.file == nil && !t.reserve(n) {
		if err := t.spill(); err != nil {
			return err
		}
	}
	var h [header]byte
	binary.BigEndian.PutUint32(h[0:], uint32(len(body)))
	binary.BigEndian.PutUint32(h[4:], idx)
	binary.BigEndian.PutUint32(h[8:], sub)
	h[12] = op
	if t.file != nil {
		if _, err := t.w.Write(h[:]); err != nil {
			return err
		}
		if _, err := t.w.Write(body); err != nil {
			return err
		}
	} else {
		t.mem = append(append(t.mem, h[:]...), body...)
	}
	t.Changes++
	t.Bytes += n
	t.Truncates = t.Truncates || op == pgoutput.Truncate
	return nil
}

func (t *Txn) reserve(n int64) bool {
	need := t.Bytes + n
	if need > t.lim.SpillAt {
		return false
	}
	for t.held < need {
		if t.lim.Budget != nil && !t.lim.Budget.TryAcquire(grain) {
			return false
		}
		t.held += grain
	}
	return true
}

func (t *Txn) spill() error {
	f, err := os.CreateTemp(t.lim.Dir, fmt.Sprintf("txn-%d-*", t.XID))
	if err != nil {
		return err
	}
	// The name goes away now, so a killed process leaves nothing behind.
	if err := os.Remove(f.Name()); err != nil {
		f.Close()
		return err
	}
	t.file = f
	t.w = bufio.NewWriterSize(f, 256<<10)
	_, err = t.w.Write(t.mem)
	if t.lim.Spilled != nil {
		t.lim.Spilled(t.Bytes)
	}
	t.mem = nil
	t.giveBack()
	return err
}

func (t *Txn) giveBack() {
	if t.lim.Budget != nil && t.held > 0 {
		t.lim.Budget.Release(t.held)
	}
	t.held = 0
}

// AbortSub marks a rolled back subtransaction. Its changes are skipped by Each.
func (t *Txn) AbortSub(sub uint32) {
	if bugs.SubxactAbortIgnored {
		return
	}
	if t.aborted == nil {
		t.aborted = map[uint32]struct{}{}
	}
	t.aborted[sub] = struct{}{}
}

// OnDisk says whether the changes were moved to a file.
func (t *Txn) OnDisk() bool { return t.file != nil }

// Each calls fn for every change that wasn't rolled back, in order. The Change
// is reused between calls, and its body is only valid during the call when the
// transaction is on disk.
func (t *Txn) Each(fn func(*Change) error) error {
	var c Change
	if t.file == nil {
		for b := t.mem; len(b) > 0; {
			n := int(binary.BigEndian.Uint32(b))
			if err := t.one(&c, b[:header], b[header:header+n], fn); err != nil {
				return err
			}
			b = b[header+n:]
		}
		return nil
	}
	if err := t.w.Flush(); err != nil {
		return err
	}
	br := bufio.NewReaderSize(io.NewSectionReader(t.file, 0, 1<<62), 256<<10)
	var h [header]byte
	var body []byte
	for {
		if _, err := io.ReadFull(br, h[:]); err == io.EOF {
			return nil
		} else if err != nil {
			return err
		}
		n := int(binary.BigEndian.Uint32(h[:]))
		if cap(body) < n {
			body = make([]byte, n)
		}
		body = body[:n]
		if _, err := io.ReadFull(br, body); err != nil {
			return err
		}
		if err := t.one(&c, h[:], body, fn); err != nil {
			return err
		}
	}
}

func (t *Txn) one(c *Change, h, body []byte, fn func(*Change) error) error {
	*c = Change{Op: h[12], SubXID: binary.BigEndian.Uint32(h[8:]), Body: body}
	if _, gone := t.aborted[c.SubXID]; gone {
		return nil
	}
	if i := binary.BigEndian.Uint32(h[4:]); i != ^uint32(0) {
		c.Rel = t.rels[i]
	}
	if c.Op == pgoutput.Message {
		n := int(binary.BigEndian.Uint16(body))
		c.Prefix, c.Body = string(body[2:2+n]), body[2+n:]
	}
	return fn(c)
}

// Relations lists the table versions the transaction touches.
func (t *Txn) Relations() []*pgoutput.Relation { return t.rels }

// Close releases the memory or the file.
func (t *Txn) Close() {
	t.giveBack()
	t.mem = nil
	if t.file != nil {
		t.file.Close()
		t.file = nil
	}
}
