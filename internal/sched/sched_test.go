package sched

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"pgregory.net/rapid"
)

// table is a target with row locks: preparing work blocks on keys another
// uncommitted work holds, the way Postgres does, and gives up after a timeout.
type table struct {
	mu     sync.Mutex
	locks  map[string]*job
	rows   map[string]int
	order  []int
	active map[*job]struct{}
}

type job struct {
	t        *table
	id       int
	locks    []string // in the order they are taken
	declared map[string]bool
	took     time.Duration
	held     []string
	onErr    func(error)
}

func (j *job) Prepare(ctx context.Context, _ int) error {
	for _, k := range j.locks {
		deadline := time.Now().Add(200 * time.Millisecond)
		for {
			j.t.mu.Lock()
			if h := j.t.locks[k]; h == nil || h == j {
				j.t.locks[k] = j
				j.held = append(j.held, k)
				j.t.mu.Unlock()
				break
			}
			j.t.mu.Unlock()
			if time.Now().After(deadline) {
				return fmt.Errorf("lock timeout on %s: %w", k, ErrConflict)
			}
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		time.Sleep(j.took)
	}
	return nil
}

func (j *job) release() {
	for _, k := range j.held {
		if j.t.locks[k] == j {
			delete(j.t.locks, k)
		}
	}
	j.held = nil
}

func (j *job) Rollback(context.Context) error {
	j.t.mu.Lock()
	defer j.t.mu.Unlock()
	j.release()
	return nil
}

func (j *job) Commit(context.Context) error {
	time.Sleep(time.Duration(rand.IntN(3)) * time.Millisecond)
	j.t.mu.Lock()
	defer j.t.mu.Unlock()
	for _, k := range j.held {
		j.t.rows[k] = j.id
	}
	j.t.order = append(j.t.order, j.id)
	j.release()
	return nil
}

func hash(s string) uint64 {
	h := uint64(14695981039346656037)
	for i := range len(s) {
		h = (h ^ uint64(s[i])) * 1099511628211
	}
	return h
}

func newTable() *table { return &table{locks: map[string]*job{}, rows: map[string]int{}} }

// Work commits in submission order and leaves what one lane would have left,
// with the overlap test blind to some of the keys.
func TestSameResultAsOneLane(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		lanes := rapid.IntRange(1, 6).Draw(rt, "lanes")
		n := rapid.IntRange(1, 30).Draw(rt, "n")
		type spec struct {
			locks []string
			blind []bool
			took  int
			late  bool
		}
		specs := make([]spec, n)
		for i := range specs {
			perm := rapid.Permutation([]string{"a", "b", "c", "d", "e", "f"}).Draw(rt, "keys")
			k := rapid.IntRange(1, 3).Draw(rt, "count")
			specs[i] = spec{locks: perm[:k], took: rapid.IntRange(0, 9).Draw(rt, "ms"), late: rapid.IntRange(0, 5).Draw(rt, "detached") == 0}
			for range k {
				specs[i].blind = append(specs[i].blind, rapid.IntRange(0, 3).Draw(rt, "blind") == 0)
			}
		}
		synctest.Test(t, func(t *testing.T) {
			tb := newTable()
			s := New(t.Context(), lanes, 4)
			want := map[string]int{}
			done := 0
			for i, sp := range specs {
				it := &Item{Work: &job{t: tb, id: i, locks: sp.locks, took: time.Duration(sp.took) * time.Millisecond}, Detached: sp.late}
				it.Done = func() {
					if done != i {
						t.Errorf("item %d finished in place %d", i, done)
					}
					done++
				}
				for j, k := range sp.locks {
					want[k] = i
					if !sp.blind[j] {
						it.Keys = append(it.Keys, Key{Hash: hash(k)})
					}
				}
				if err := s.Submit(it); err != nil {
					t.Fatal(err)
				}
			}
			if err := s.Drain(); err != nil {
				t.Fatalf("drain: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatalf("close: %v", err)
			}
			for i, id := range tb.order {
				if id != i {
					t.Fatalf("commit order %v", tb.order)
				}
			}
			if fmt.Sprint(tb.rows) != fmt.Sprint(want) {
				t.Fatalf("rows %v, want %v", tb.rows, want)
			}
		})
	})
}

// The wait Postgres can't see: the second piece of work holds a unique value
// and waits for its turn, the first one needs the value. Neither key is in the
// overlap test.
func TestLaterWorkGivesUpItsLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		tb := newTable()
		s := New(t.Context(), 2, 4)
		start := time.Now()
		_ = s.Submit(&Item{Work: &job{t: tb, id: 0, locks: []string{"row A", "badge 12"}, took: 50 * time.Millisecond}})
		_ = s.Submit(&Item{Work: &job{t: tb, id: 1, locks: []string{"badge 12", "row B"}}})
		if err := s.Drain(); err != nil {
			t.Fatal(err)
		}
		st := s.Stats()
		_ = s.Close()
		if fmt.Sprint(tb.order) != "[0 1]" || tb.rows["badge 12"] != 1 {
			t.Fatalf("order %v rows %v", tb.order, tb.rows)
		}
		if st.Yields != 1 || time.Since(start) > 2*time.Second {
			t.Fatalf("yields %d after %v", st.Yields, time.Since(start))
		}
	})
}

// Overlapping work never runs at the same time.
func TestOverlapWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(t.Context(), 4, 8)
		var mu sync.Mutex
		running := 0
		peak := 0
		for range 6 {
			_ = s.Submit(&Item{Keys: []Key{{Hash: 1}}, Work: fn(func() error {
				mu.Lock()
				running++
				peak = max(peak, running)
				mu.Unlock()
				time.Sleep(time.Millisecond)
				mu.Lock()
				running--
				mu.Unlock()
				return nil
			})})
		}
		if err := s.Drain(); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
		if peak != 1 {
			t.Fatalf("%d overlapping pieces ran together", peak)
		}
	})
}

// Readers of a parent row run together, and a writer waits for all of them.
func TestSharedKeys(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(t.Context(), 4, 8)
		start := time.Now()
		sleep := fn(func() error { time.Sleep(10 * time.Millisecond); return nil })
		for range 3 {
			_ = s.Submit(&Item{Keys: []Key{{Hash: 1, Shared: true}}, Work: sleep})
		}
		_ = s.Submit(&Item{Keys: []Key{{Hash: 1}}, Work: sleep})
		_ = s.Drain()
		_ = s.Close()
		if d := time.Since(start); d != 20*time.Millisecond {
			t.Fatalf("took %v, want 20ms", d)
		}
	})
}

// A row inside a chunk that is landing waits for the chunk, a row outside doesn't.
func TestChunkRange(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lo, hi := Ord{Known: true, Int: 100}, Ord{Known: true, Int: 200}
		s := New(t.Context(), 4, 8)
		start := time.Now()
		began := map[int64]time.Duration{}
		var mu sync.Mutex
		_ = s.Submit(&Item{Detached: true, Range: &Range{Table: 1, Lo: &lo, Hi: &hi}, Work: fn(func() error { time.Sleep(time.Second); return nil })})
		row := func(id int64, o Ord) {
			_ = s.Submit(&Item{Points: []Point{{Table: 1, Ord: o}}, Work: fn(func() error {
				mu.Lock()
				began[id] = time.Since(start)
				mu.Unlock()
				return nil
			})})
		}
		row(99, Ord{Known: true, Int: 99})
		row(150, Ord{Known: true, Int: 150})
		row(-1, Ord{})
		_ = s.Drain()
		_ = s.Close()
		if began[99] != 0 || began[150] != time.Second || began[-1] != time.Second {
			t.Fatalf("started at %v", began)
		}
	})
}

func TestErrorStopsEverything(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(t.Context(), 2, 4)
		boom := errors.New("boom")
		ran := false
		_ = s.Submit(&Item{Work: fn(func() error { return boom })})
		_ = s.Submit(&Item{Work: fn(func() error { return nil }), Done: func() { ran = true }})
		if err := s.Drain(); !errors.Is(err, boom) {
			t.Fatalf("drain: %v", err)
		}
		if err := s.Close(); !errors.Is(err, boom) {
			t.Fatalf("close: %v", err)
		}
		if ran {
			t.Fatal("work behind a failure committed")
		}
	})
}

// Nothing in the scheduler is locked while Done runs: here it waits for someone
// who asks for Stats first.
func TestDoneHoldsNoLock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := New(t.Context(), 2, 4)
		hold := make(chan struct{})
		_ = s.Submit(&Item{Work: fn(func() error { return nil }), Done: func() { <-hold }})
		synctest.Wait()
		_ = s.Stats()
		_ = s.Busy()
		close(hold)
		if err := s.Drain(); err != nil {
			t.Fatal(err)
		}
		_ = s.Close()
	})
}

type fn func() error

func (f fn) Prepare(context.Context, int) error { return f() }
func (f fn) Commit(context.Context) error       { return nil }
func (f fn) Rollback(context.Context) error     { return nil }

func BenchmarkSubmit(b *testing.B) {
	s := New(b.Context(), 4, 256)
	keys := make([]Key, 64)
	for b.Loop() {
		for i := range keys {
			keys[i].Hash = rand.Uint64()
		}
		_ = s.Submit(&Item{Keys: append([]Key(nil), keys...), Work: fn(func() error { return nil })})
	}
	_ = s.Drain()
}
