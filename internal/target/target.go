// Package target holds what the engine needs from a place changes are written
// to. A new target implements Target and nothing else; see
// docs/new-target.md.
package target

import (
	"context"
	"errors"
	"time"

	"grantline/internal/lsn"
	"grantline/internal/sched"
	"grantline/internal/txn"
)

// ErrFenced means a newer worker owns this replication. The worker that gets it
// must stop.
var ErrFenced = errors.New("a newer worker has taken over")

// ErrDiverged means the target doesn't hold a row it has to hold.
var ErrDiverged = errors.New("target and source have diverged")

// Phase of a replication.
type Phase string

// Phases.
const (
	New     Phase = "new"
	Copying Phase = "copying"
	Ready   Phase = "ready"
)

// State is what the target remembers between workers.
type State struct {
	Epoch    int64
	Phase    Phase
	LSN      lsn.LSN // everything up to here is committed in the target
	SystemID string
	Timeline int32
}

// Group is one or more whole transactions that commit together.
type Group struct {
	Txns   []*txn.Txn
	EndLSN lsn.LSN
	Time   time.Time // commit time of the last transaction
	Ready  bool      // the group contains the marker that ends copying
	RefXID uint64    // reference for widening transaction IDs
}

// Changes counts the group's changes.
func (g *Group) Changes() int {
	n := 0
	for _, t := range g.Txns {
		n += t.Changes
	}
	return n
}

// Target applies transactions and remembers the position in the same commit.
type Target interface {
	// Open takes the replication over: after it returns, an older worker's
	// commits fail with ErrFenced.
	Open(ctx context.Context) (State, error)
	// Begin records where the stream starts. It is called once, for a new
	// replication, after the slot exists.
	Begin(ctx context.Context, at lsn.LSN, systemID string, timeline int32) error
	// Moved records a new server identity after a failover.
	Moved(ctx context.Context, systemID string, timeline int32) error
	// Lanes is how many groups may be prepared at once.
	Lanes() int
	// Item builds the scheduler item for a group.
	Item(g *Group) (*sched.Item, error)
	Close()
}
