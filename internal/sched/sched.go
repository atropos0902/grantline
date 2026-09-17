// Package sched runs work in parallel lanes and commits it in one order.
//
// Every piece of work gets a sequence number when it is submitted, and work
// commits in that order with no gaps. Two pieces run at the same time only when
// the keys they touch don't overlap.
//
// The overlap test can miss. A transaction that frees a unique value doesn't
// always say which value it freed, and a later transaction that takes the value
// may then run first and hold a lock the earlier one needs. The earlier one
// waits for the lock, the later one waits for its turn to commit, and Postgres
// sees only the first wait. So when work hits a conflict, everything behind it
// that hasn't committed gives up its locks and runs again afterwards.
package sched

import (
	"cmp"
	"context"
	"errors"
	"sync"

	"grantline/internal/bugs"
)

// Key is something work reads or writes, as a hash of table, columns and values.
type Key struct {
	Hash   uint64
	Shared bool // read only, for example a parent row a new child points to
}

// Point places a row in its table's chunk order. Known is false when the order
// isn't one this process can compare, and then the row counts as inside every
// chunk of the table.
type Point struct {
	Table uint32
	Ord   Ord
}

// Ord is an ordered key value. Integers compare as Int, everything else as Str.
type Ord struct {
	Known bool
	Int   int64
	Str   string
}

// Less orders two values of the same column.
func (o Ord) Less(p Ord) bool {
	if o.Int != p.Int {
		return o.Int < p.Int
	}
	return o.Str < p.Str
}

// Range is a chunk of a table, both ends included. A nil end is open.
type Range struct {
	Table  uint32
	Lo, Hi *Ord
}

func (r Range) contains(p Point) bool {
	if r.Table != p.Table {
		return false
	}
	if !p.Ord.Known {
		return true
	}
	if r.Lo != nil && (!r.Lo.Known || p.Ord.Less(*r.Lo)) {
		return !r.Lo.Known
	}
	if r.Hi != nil && (!r.Hi.Known || r.Hi.Less(p.Ord)) {
		return !r.Hi.Known
	}
	return true
}

// Work is one transaction against the target.
type Work interface {
	// Prepare does everything except the commit. After an error or a Rollback
	// it may be called again.
	Prepare(ctx context.Context, lane int) error
	// Commit makes it durable. It is called once, when every earlier piece of
	// work has committed.
	Commit(ctx context.Context) error
	// Rollback drops what Prepare did and releases its locks.
	Rollback(ctx context.Context) error
}

// Item is work plus what it touches.
type Item struct {
	Work     Work
	Keys     []Key
	Points   []Point
	Range    *Range // set for a chunk landing in the target
	Barrier  bool   // runs alone: nothing overlaps it
	Detached bool   // runs on its own connection, not in a lane
	Done     func() // called after the commit, in commit order, with no lock held; may be nil

	seq, dep uint64
	yield    chan struct{}
}

// ErrConflict marks an error another lane may have caused. Work that fails with
// it is tried again once everything before it has committed.
var ErrConflict = errors.New("conflict with another lane")

// Stats count what the scheduler had to do.
type Stats struct {
	Committed uint64
	Yields    uint64
	Retries   uint64
}

// Scheduler commits work in submission order.
type Scheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	queue  chan *Item
	wg     sync.WaitGroup
	submit sync.Mutex

	mu        sync.Mutex
	cond      *sync.Cond
	err       error
	closed    bool
	next      uint64 // last sequence number handed out
	committed uint64
	holdAfter uint64 // work behind this waits until it has committed
	lastX     map[uint64]uint64
	lastS     map[uint64]uint64
	lastTable map[uint32]uint64
	barrier   uint64
	ranges    []*Item
	live      map[uint64]*Item
	stats     Stats
	early     map[uint64]struct{} // commits that ran ahead of their turn, known bug only
}

// New starts a scheduler with this many lanes. depth is how much submitted work
// may wait; Submit blocks past it.
func New(ctx context.Context, lanes, depth int) *Scheduler {
	ctx, cancel := context.WithCancel(ctx)
	s := &Scheduler{
		ctx: ctx, cancel: cancel, queue: make(chan *Item, depth),
		lastX: map[uint64]uint64{}, lastS: map[uint64]uint64{}, lastTable: map[uint32]uint64{},
		live: map[uint64]*Item{}, early: map[uint64]struct{}{},
	}
	s.cond = sync.NewCond(&s.mu)
	context.AfterFunc(ctx, func() { s.fail(context.Cause(ctx)) })
	for lane := range lanes {
		s.wg.Go(func() {
			for it := range s.queue {
				s.run(it, lane)
			}
		})
	}
	return s
}

// Submit gives the item its place in the commit order. It is safe to call from
// several goroutines, and their items are ordered by arrival.
func (s *Scheduler) Submit(it *Item) error {
	// Held across the queue send, so the queue is in sequence order. Otherwise
	// every lane could be waiting for an item that hasn't been queued yet.
	s.submit.Lock()
	defer s.submit.Unlock()
	s.mu.Lock()
	if s.err != nil || s.closed {
		defer s.mu.Unlock()
		return cmp.Or(s.err, error(context.Canceled))
	}
	s.next++
	it.seq = s.next
	it.yield = make(chan struct{}, 1)
	it.dep = s.depend(it)
	s.live[it.seq] = it
	s.mu.Unlock()
	if it.Detached {
		s.wg.Go(func() { s.run(it, -1) })
		return nil
	}
	select {
	case s.queue <- it:
		return nil
	case <-s.ctx.Done():
		return s.Err()
	}
}

// depend returns the last sequence number the item has to wait for, and records
// what the item touches. Called with the lock held.
func (s *Scheduler) depend(it *Item) uint64 {
	dep := s.barrier
	if it.Barrier {
		dep = it.seq - 1
		s.barrier = it.seq
	}
	for _, k := range it.Keys {
		dep = max(dep, s.lastX[k.Hash])
		if k.Shared {
			s.lastS[k.Hash] = it.seq
			continue
		}
		dep = max(dep, s.lastS[k.Hash])
		s.lastX[k.Hash] = it.seq
	}
	for _, p := range it.Points {
		s.lastTable[p.Table] = it.seq
		for _, c := range s.ranges {
			if c.seq > dep && c.Range.contains(p) {
				dep = c.seq
			}
		}
	}
	if r := it.Range; r != nil {
		dep = max(dep, s.lastTable[r.Table])
		s.ranges = append(s.ranges, it)
	}
	if dep >= it.seq { // the item's own keys repeat
		dep = it.seq - 1
	}
	return dep
}

func (s *Scheduler) run(it *Item, lane int) {
	if err := s.exec(it, lane); err != nil {
		s.fail(err)
	}
}

func (s *Scheduler) exec(it *Item, lane int) error {
	serial := false
	for try := 0; ; try++ {
		if err := s.waitStart(it, serial); err != nil {
			return err
		}
		select { // an old request to give way has been obeyed by now
		case <-it.yield:
		default:
		}
		err := it.Work.Prepare(s.ctx, lane)
		if err == nil {
			ok, werr := s.waitTurn(it)
			if werr != nil {
				_ = it.Work.Rollback(context.WithoutCancel(s.ctx))
				return werr
			}
			if ok {
				break
			}
		}
		if rerr := it.Work.Rollback(context.WithoutCancel(s.ctx)); rerr != nil {
			return errors.Join(err, rerr)
		}
		if err != nil {
			if !errors.Is(err, ErrConflict) || try >= 40 {
				return err
			}
			s.giveWay(it)
		}
		serial = true
		s.mu.Lock()
		s.stats.Retries++
		s.mu.Unlock()
	}
	if err := it.Work.Commit(s.ctx); err != nil {
		return err
	}
	s.finish(it)
	return nil
}

// waitStart blocks until the work may run: its dependencies have committed, no
// earlier work has asked for quiet, and on a retry every earlier item has
// committed.
func (s *Scheduler) waitStart(it *Item, serial bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.err == nil {
		quiet := s.holdAfter == 0 || it.seq <= s.holdAfter
		if s.committed >= it.dep && quiet && (!serial || s.committed == it.seq-1) {
			return nil
		}
		s.cond.Wait()
	}
	return s.err
}

// waitTurn blocks until every earlier item has committed. It returns false when
// an earlier item needs this one's locks.
func (s *Scheduler) waitTurn(it *Item) (bool, error) {
	if bugs.UnorderedCommit {
		return true, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for s.err == nil {
		if s.committed == it.seq-1 {
			return true, nil
		}
		select {
		case <-it.yield:
			s.stats.Yields++
			return false, nil
		default:
		}
		s.cond.Wait()
	}
	return false, s.err
}

// giveWay makes everything behind the item drop its locks and wait.
func (s *Scheduler) giveWay(it *Item) {
	if bugs.NoYield {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.holdAfter == 0 || it.seq < s.holdAfter {
		s.holdAfter = it.seq
	}
	for seq, l := range s.live {
		if seq > it.seq {
			select {
			case l.yield <- struct{}{}:
			default:
			}
		}
	}
	s.cond.Broadcast()
}

func (s *Scheduler) finish(it *Item) {
	// It is still this item's turn, so Done runs in commit order without the
	// lock. Holding it here once deadlocked with a caller that held a lock of
	// its own, which Done wanted, and was asking for Stats.
	if it.Done != nil {
		it.Done()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if bugs.UnorderedCommit {
		s.early[it.seq] = struct{}{}
		for {
			if _, ok := s.early[s.committed+1]; !ok {
				break
			}
			s.committed++
			delete(s.early, s.committed)
		}
	} else {
		s.committed = it.seq
	}
	s.stats.Committed++
	delete(s.live, it.seq)
	if s.holdAfter != 0 && s.committed >= s.holdAfter {
		s.holdAfter = 0
	}
	for _, k := range it.Keys {
		if s.lastX[k.Hash] == it.seq {
			delete(s.lastX, k.Hash)
		}
		if s.lastS[k.Hash] == it.seq {
			delete(s.lastS, k.Hash)
		}
	}
	if it.Range != nil {
		for i, c := range s.ranges {
			if c == it {
				s.ranges = append(s.ranges[:i], s.ranges[i+1:]...)
				break
			}
		}
	}
	s.cond.Broadcast()
}

func (s *Scheduler) fail(err error) {
	if err == nil {
		err = context.Canceled
	}
	s.mu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	s.cancel()
}

// Failed is closed when the scheduler stops.
func (s *Scheduler) Failed() <-chan struct{} { return s.ctx.Done() }

// Err is the error that stopped the scheduler, or nil.
func (s *Scheduler) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Stats returns the counts so far.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stats
}

// Busy is how much submitted work hasn't committed.
func (s *Scheduler) Busy() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.live)
}

// Drain waits until everything submitted so far has committed.
func (s *Scheduler) Drain() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	to := s.next
	for s.err == nil && s.committed < to {
		s.cond.Wait()
	}
	return s.err
}

// Close stops the lanes and rolls back work that hasn't committed. It returns
// the error that stopped the scheduler, or nil if Close did.
func (s *Scheduler) Close() error {
	err := s.Err()
	s.fail(context.Canceled)
	s.submit.Lock()
	s.mu.Lock()
	again := s.closed
	s.closed = true
	s.mu.Unlock()
	s.submit.Unlock()
	if !again {
		close(s.queue)
	}
	s.wg.Wait()
	return err
}
