package txn

import (
	"fmt"

	"grantline/internal/pgoutput"
)

// Assembler turns stream messages into whole transactions.
type Assembler struct {
	lim       *Limits
	rels      map[uint32]*pgoutput.Relation
	streamed  map[uint32]*open
	cur       *Txn
	curStream *open
}

// open is a transaction that is being streamed before it commits. Relation
// messages inside it describe shapes only this transaction can see yet, so they
// stay with it until it commits.
type open struct {
	t    *Txn
	rels map[uint32]*pgoutput.Relation
}

// NewAssembler makes an empty assembler.
func NewAssembler(lim *Limits) *Assembler {
	return &Assembler{lim: lim, rels: map[uint32]*pgoutput.Relation{}, streamed: map[uint32]*open{}}
}

// Feed takes one message. It returns a transaction when the message completes
// one. A logical message outside a transaction is dropped.
func (a *Assembler) Feed(b []byte) (*Txn, error) {
	m, err := pgoutput.Split(b, a.curStream != nil)
	if err != nil {
		return nil, err
	}
	switch m.Kind {
	case pgoutput.KBegin:
		a.cur = New(m.XID, a.lim)
	case pgoutput.KCommit:
		t := a.cur
		if t == nil {
			return nil, fmt.Errorf("commit at %s without a begin", m.CommitLSN)
		}
		a.cur = nil
		t.CommitLSN, t.EndLSN, t.Time = m.CommitLSN, m.EndLSN, m.Time
		return t, nil
	case pgoutput.KStreamStart:
		o := a.streamed[m.XID]
		if o == nil {
			o = &open{t: New(m.XID, a.lim), rels: map[uint32]*pgoutput.Relation{}}
			a.streamed[m.XID] = o
		}
		a.curStream = o
	case pgoutput.KStreamStop:
		a.curStream = nil
	case pgoutput.KStreamCommit:
		o := a.streamed[m.XID]
		if o == nil {
			return nil, fmt.Errorf("stream commit for unknown transaction %d", m.XID)
		}
		delete(a.streamed, m.XID)
		for id, r := range o.rels {
			a.rels[id] = r
		}
		o.t.CommitLSN, o.t.EndLSN, o.t.Time = m.CommitLSN, m.EndLSN, m.Time
		return o.t, nil
	case pgoutput.KStreamAbort:
		o := a.streamed[m.XID]
		if o == nil {
			return nil, nil
		}
		if m.SubXID == m.XID {
			delete(a.streamed, m.XID)
			o.t.Close()
			return nil, nil
		}
		o.t.AbortSub(m.SubXID)
	case pgoutput.KRelation:
		if a.curStream != nil {
			a.curStream.rels[m.Relation.ID] = m.Relation
		} else {
			a.rels[m.Relation.ID] = m.Relation
		}
	case pgoutput.KChange:
		return nil, a.add(m.Op, m.XID, m.RelID, m.Body)
	case pgoutput.KTruncate:
		for _, id := range m.RelIDs {
			if err := a.add(m.Op, m.XID, id, nil); err != nil {
				return nil, err
			}
		}
	case pgoutput.KMessage:
		if !m.Transact {
			return nil, nil
		}
		t, sub := a.target(m.XID)
		if t == nil {
			return nil, fmt.Errorf("logical message outside a transaction")
		}
		return nil, t.Add(m.Op, sub, nil, m.Prefix, m.Body)
	}
	return nil, nil
}

func (a *Assembler) target(xid uint32) (*Txn, uint32) {
	if a.curStream != nil {
		return a.curStream.t, xid
	}
	if a.cur == nil {
		return nil, 0
	}
	return a.cur, a.cur.XID
}

func (a *Assembler) add(op byte, xid, relID uint32, body []byte) error {
	t, sub := a.target(xid)
	if t == nil {
		return fmt.Errorf("change outside a transaction")
	}
	var rel *pgoutput.Relation
	if a.curStream != nil {
		rel = a.curStream.rels[relID]
	}
	if rel == nil {
		rel = a.rels[relID]
	}
	if rel == nil {
		return fmt.Errorf("change for relation %d before its description", relID)
	}
	return t.Add(op, sub, rel, "", body)
}

// Idle reports whether no transaction is open, so everything received so far
// has been handed out.
func (a *Assembler) Idle() bool { return a.cur == nil && len(a.streamed) == 0 }

// Close drops unfinished transactions.
func (a *Assembler) Close() {
	if a.cur != nil {
		a.cur.Close()
	}
	for _, o := range a.streamed {
		o.t.Close()
	}
	a.cur, a.curStream, a.streamed = nil, nil, map[uint32]*open{}
}
