// Package engine runs one replication: it copies the tables, follows the change
// stream and keeps the two from undoing each other.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"grantline/internal/bugs"
	"grantline/internal/events"
	"grantline/internal/lsn"
	"grantline/internal/pgoutput"
	"grantline/internal/sched"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
	"grantline/internal/txn"
)

var tracer = otel.Tracer("grantline/engine")

// Config for one replication.
type Config struct {
	Name        string
	SourceDSN   string
	Publication string
	Slot        string

	ChunkRows  int           // rows in a copied chunk
	Copiers    int           // chunks read at once
	GroupMax   int           // changes put into one target commit
	Budget     int64         // bytes buffered transactions may hold together
	SpillAt    int64         // bytes one transaction may hold before it goes to disk
	SpillDir   string        // where spilled transactions go
	Heartbeat  time.Duration // how often the source is marked, to measure lag
	JournalFor int           // hours of history kept for explain

	Events *events.Publisher // may be nil
	Log    *slog.Logger
	Hooks  Hooks
}

// Hooks let tests hold the engine at a point that matters. All may be nil.
type Hooks struct {
	SlotMade     func(ctx context.Context) error                                         // before the stream starts
	BeforeCopy   func(ctx context.Context) error                                         // stream running, nothing copied yet
	ChunkStaged  func(ctx context.Context, t *source.Table, read source.ChunkRead) error // read, not landed
	AfterReading func(ctx context.Context, t *source.Table, snapshot string) error       // snapshot fixed, rows not read
}

func (c *Config) defaults() {
	def := func(p *int, v int) {
		if *p == 0 {
			*p = v
		}
	}
	def(&c.ChunkRows, 50_000)
	def(&c.Copiers, 4)
	def(&c.GroupMax, 250)
	def(&c.JournalFor, 72)
	if c.Budget == 0 {
		c.Budget = 64 << 20
	}
	if c.SpillAt == 0 {
		c.SpillAt = 8 << 20
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = time.Second
	}
	if c.Slot == "" {
		c.Slot = "grantline_" + c.Name
	}
	if c.Log == nil {
		c.Log = slog.New(slog.DiscardHandler)
	}
}

// Status is a running engine's numbers.
type Status struct {
	Name       string
	Phase      target.Phase
	Epoch      int64
	Applied    lsn.LSN // committed in the target
	SourceEnd  lsn.LSN // newest position the source has reported
	Beat       time.Time
	Lag        time.Duration // how old the newest applied heartbeat was when it committed
	LastBeatAt time.Time
	Chunks     int
	ChunksDone int
	Refused    int64
	Reread     int64
	Changes    int64
	Rows       int64
	Spilled    int64
	Sched      sched.Stats
	Paused     bool
	Waiting    string // the statement a schema change needs, empty when none
}

// Engine runs a replication.
type Engine struct {
	cfg Config
	tgt target.Target
	pg  *pgtarget.Target // nil when the target isn't Postgres

	mu      sync.Mutex
	st      Status
	tables  map[string]*source.Table
	seen    map[*pgoutput.Relation]bool // shapes the target already has
	sources *source.Source
	resume  chan struct{} // open while paused
	approve chan string
	lags    []time.Duration

	durable atomic.Uint64
	ref     atomic.Uint64
	spilled atomic.Int64
	changes atomic.Int64
	opened  atomic.Bool
}

// Opened says whether this engine took the replication over.
func (e *Engine) Opened() bool { return e.opened.Load() }

// New makes an engine. The target decides whether tables are copied.
func New(cfg Config, tgt target.Target) *Engine {
	cfg.defaults()
	e := &Engine{cfg: cfg, tgt: tgt, tables: map[string]*source.Table{}, seen: map[*pgoutput.Relation]bool{}, approve: make(chan string, 1)}
	e.pg, _ = tgt.(*pgtarget.Target)
	e.st.Name = cfg.Name
	return e
}

// Status returns the current numbers.
func (e *Engine) Status() Status {
	e.mu.Lock()
	defer e.mu.Unlock()
	st := e.st
	st.Applied = lsn.LSN(e.durable.Load())
	st.Spilled, st.Changes = e.spilled.Load(), e.changes.Load()
	if e.pg != nil {
		st.Refused, st.Reread = e.pg.Refused.Load(), e.pg.Reread.Load()
		st.Rows = e.pg.Landed.Load()
	}
	return st
}

// Lags returns and clears the heartbeat delays seen since the last call.
func (e *Engine) Lags() []time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	l := e.lags
	e.lags = nil
	return l
}

// Pause stops applying after the group in hand. The stream stays connected.
func (e *Engine) Pause() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resume == nil {
		e.resume = make(chan struct{})
	}
	e.st.Paused = true
}

// Resume undoes Pause.
func (e *Engine) Resume() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.resume != nil {
		close(e.resume)
		e.resume = nil
	}
	e.st.Paused = false
}

// Approve runs a waiting schema change. The statement has to be the one shown.
func (e *Engine) Approve(sql string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.st.Waiting == "" || e.st.Waiting != sql {
		return fmt.Errorf("no schema change is waiting with that statement")
	}
	select {
	case e.approve <- sql:
	default:
	}
	return nil
}

func (e *Engine) event(kind, id string, fields map[string]any) {
	if e.cfg.Events != nil {
		e.cfg.Events.Publish(e.cfg.Name, kind, id, fields)
	}
}

// Run takes the replication over and runs it until ctx ends, another worker
// takes over or something fails. The caller decides whether to run it again.
func (e *Engine) Run(ctx context.Context) error {
	log := e.cfg.Log.With("replication", e.cfg.Name)
	st, err := e.tgt.Open(ctx)
	if err != nil {
		return err
	}
	e.opened.Store(true)
	src, err := source.Open(ctx, e.cfg.SourceDSN, e.cfg.Slot, e.cfg.Publication)
	if err != nil {
		return fmt.Errorf("source: %w", err)
	}
	defer src.Close()
	log = log.With("epoch", st.Epoch)
	e.mu.Lock()
	e.sources = src
	e.st.Phase, e.st.Epoch = st.Phase, st.Epoch
	e.mu.Unlock()

	sys, err := src.Identify(ctx)
	if err != nil {
		return err
	}
	tables, err := src.Tables(ctx)
	if err != nil {
		return err
	}
	if len(tables) == 0 {
		return fmt.Errorf("publication %s has no tables", e.cfg.Publication)
	}
	if e.pg != nil {
		if err := e.pg.SetTables(ctx, tables); err != nil {
			return err
		}
	}
	for _, t := range tables {
		e.tables[t.FullName()] = t
	}
	if st.Phase == target.New {
		if st.LSN, err = src.CreateSlot(ctx); err != nil {
			return err
		}
		if err := e.tgt.Begin(ctx, st.LSN, sys.ID, sys.Timeline); err != nil {
			return err
		}
		st.Phase = target.Copying
		if e.pg == nil { // changes only, there is nothing to copy
			st.Phase = target.Ready
		}
		log.Info("slot made", "start", st.LSN)
		e.event("started", fmt.Sprintf("started-%d", st.Epoch), map[string]any{"lsn": st.LSN.String()})
	} else {
		if err := e.sameHistory(ctx, src, st, sys); err != nil {
			return err
		}
		if err := src.TakeSlot(ctx); err != nil {
			e.event("slot_lost", fmt.Sprintf("slot-lost-%d", st.Epoch), map[string]any{"error": err.Error()})
			return err
		}
		log.Info("resuming", "from", st.LSN, "phase", st.Phase)
		e.event("resumed", fmt.Sprintf("resumed-%d", st.Epoch), map[string]any{"lsn": st.LSN.String()})
	}
	e.durable.Store(uint64(st.LSN))
	e.mu.Lock()
	e.st.Phase = st.Phase
	e.mu.Unlock()
	if e.pg != nil {
		// Whether a target table began empty has to be known before the stream
		// writes to it.
		for _, t := range tables {
			if _, err := e.pg.CopyPlan(ctx, t); err != nil {
				return err
			}
		}
	}
	if h := e.cfg.Hooks.SlotMade; h != nil {
		if err := h(ctx); err != nil {
			return err
		}
	}

	if next, err := src.NextXID(ctx); err != nil {
		return err
	} else {
		e.ref.Store(next)
	}
	s := sched.New(ctx, e.tgt.Lanes(), e.tgt.Lanes())
	g, gctx := errgroup.WithContext(ctx)
	batches := make(chan source.Batch, 256)
	lim := &txn.Limits{Dir: e.cfg.SpillDir, SpillAt: e.cfg.SpillAt, Budget: semaphore.NewWeighted(e.cfg.Budget),
		Spilled: func(int64) { e.spilled.Add(1) }}

	var received atomic.Uint64
	g.Go(func() error {
		return src.Stream(gctx, source.StreamConfig{
			Start: st.LSN, Limits: lim,
			Durable: func() lsn.LSN {
				if bugs.AckBeforeCommit {
					return lsn.LSN(max(received.Load(), e.durable.Load()))
				}
				return lsn.LSN(e.durable.Load())
			},
			OnWAL: func(end lsn.LSN, _ time.Time) {
				received.Store(uint64(end))
				e.mu.Lock()
				e.st.SourceEnd = max(e.st.SourceEnd, end)
				e.mu.Unlock()
			},
		}, batches)
	})
	g.Go(func() error { return e.apply(gctx, s, st.LSN, batches) })
	g.Go(func() error { return e.beat(gctx, src) })
	if e.pg != nil {
		g.Go(func() error {
			if h := e.cfg.Hooks.BeforeCopy; h != nil {
				if err := h(gctx); err != nil {
					return err
				}
			}
			return e.copyAll(gctx, src, s, st.Phase == target.Ready)
		})
		g.Go(func() error { return e.trim(gctx) })
	}
	g.Go(func() error { // the scheduler's own failure stops the rest
		select {
		case <-gctx.Done():
			return nil
		case <-s.Failed():
			return s.Err()
		}
	})
	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(100 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				stats := s.Stats()
				e.mu.Lock()
				e.st.Sched = stats
				e.mu.Unlock()
			}
		}
	}()
	err = g.Wait()
	close(stop)
	if serr := s.Close(); serr != nil && !errors.Is(serr, context.Canceled) {
		err = serr
	}
	stats := s.Stats()
	e.mu.Lock()
	e.st.Sched = stats
	e.mu.Unlock()
	if ctx.Err() != nil {
		return ctx.Err()
	}
	return err
}

// sameHistory checks, after the server's timeline moved, that the target holds
// nothing the new primary never had.
func (e *Engine) sameHistory(ctx context.Context, src *source.Source, st target.State, sys source.System) error {
	if st.SystemID != "" && st.SystemID != sys.ID {
		return fmt.Errorf("the source is a different cluster: %s, was %s", sys.ID, st.SystemID)
	}
	if st.Timeline != 0 && sys.Timeline != st.Timeline {
		at, err := src.SwitchPoint(ctx, st.Timeline, sys.Timeline)
		if err != nil {
			return err
		}
		if at == 0 || st.LSN > at {
			return fmt.Errorf("%w: the target is at %s and the new primary left timeline %d at %s", target.ErrDiverged, st.LSN, st.Timeline, at)
		}
		e.event("failover", fmt.Sprintf("failover-%d", sys.Timeline), map[string]any{"timeline": sys.Timeline, "switch": at.String()})
		return e.tgt.Moved(ctx, sys.ID, sys.Timeline)
	}
	return nil
}

// beat marks the source so lag can be measured when nothing else is happening.
func (e *Engine) beat(ctx context.Context, src *source.Source) error {
	t := time.NewTicker(e.cfg.Heartbeat)
	defer t.Stop()
	for {
		if _, err := src.Mark(ctx, fmt.Sprintf("beat %s %d", e.cfg.Name, time.Now().UnixNano())); err != nil {
			return fmt.Errorf("heartbeat: %w", err)
		}
		if next, err := src.NextXID(ctx); err == nil {
			e.ref.Store(next)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
		}
	}
}

func (e *Engine) trim(ctx context.Context) error {
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := e.pg.TrimJournal(ctx, e.cfg.JournalFor); err != nil {
				e.cfg.Log.Warn("trim journal", "error", err)
			}
		}
	}
}

// apply turns transactions into groups and submits them. A group is whole
// transactions in order, as many as are waiting, up to a limit.
func (e *Engine) apply(ctx context.Context, s *sched.Scheduler, from lsn.LSN, in <-chan source.Batch) error {
	var g *target.Group
	var held *source.Batch
	for {
		var b source.Batch
		if held != nil {
			b, held = *held, nil
		} else {
			select {
			case b = <-in:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err := e.hold(ctx); err != nil {
			return err
		}
		if b.Txn == nil {
			e.quiet(s, b.QuietTo)
			continue
		}
		g = &target.Group{RefXID: e.ref.Load()}
		n := 0
		for {
			x := b.Txn
			if x != nil && x.EndLSN > from {
				if err := e.shape(ctx, s, x); err != nil {
					return err
				}
				g.Txns = append(g.Txns, x)
				g.EndLSN, g.Time = x.EndLSN, x.Time
				n += x.Changes
			} else if x != nil {
				x.Close()
			}
			if n >= e.cfg.GroupMax {
				break
			}
			var ok bool
			select {
			case b, ok = <-in:
				if b.Txn != nil && n > 0 && n+b.Txn.Changes > e.cfg.GroupMax*2 {
					held, ok = &b, false
				}
			default:
			}
			if !ok {
				break
			}
		}
		if len(g.Txns) == 0 {
			continue
		}
		if err := e.submit(s, g); err != nil {
			return err
		}
	}
}

// quiet moves the position past log that holds nothing for us. It needs every
// earlier group to have committed, and it doesn't touch the target: after a
// restart the source simply sends that log again.
func (e *Engine) quiet(s *sched.Scheduler, to lsn.LSN) {
	if s.Busy() == 0 && uint64(to) > e.durable.Load() {
		e.durable.Store(uint64(to))
	}
}

func (e *Engine) submit(s *sched.Scheduler, g *target.Group) error {
	var beats []time.Time
	for _, x := range g.Txns {
		if x.Changes > 8 { // markers come in transactions of their own
			continue
		}
		if err := x.Each(func(c *txn.Change) error {
			if c.Op != pgoutput.Message || c.Prefix != source.MarkerPrefix {
				return nil
			}
			f := strings.Fields(string(c.Body))
			switch {
			case len(f) == 3 && f[0] == "beat" && f[1] == e.cfg.Name:
				if ns, err := strconv.ParseInt(f[2], 10, 64); err == nil {
					beats = append(beats, time.Unix(0, ns))
				}
			case len(f) == 2 && f[0] == "ready" && f[1] == e.cfg.Name:
				g.Ready = true
			}
			return nil
		}); err != nil {
			return err
		}
	}
	it, err := e.tgt.Item(g)
	if err != nil {
		return err
	}
	_, span := tracer.Start(context.Background(), "group", trace.WithAttributes(
		attribute.Int("changes", g.Changes()), attribute.Int("transactions", len(g.Txns)), attribute.String("lsn", g.EndLSN.String())))
	n := int64(g.Changes())
	it.Done = func() {
		span.End()
		e.changes.Add(n)
		e.durable.Store(uint64(g.EndLSN))
		now := time.Now()
		e.mu.Lock()
		for _, b := range beats {
			e.st.Beat, e.st.LastBeatAt, e.st.Lag = b, now, now.Sub(b)
			if len(e.lags) < 1<<16 {
				e.lags = append(e.lags, now.Sub(b))
			}
		}
		if g.Ready {
			e.st.Phase = target.Ready
		}
		e.mu.Unlock()
		if g.Ready {
			e.cfg.Log.Info("ready", "replication", e.cfg.Name, "at", g.EndLSN)
			e.event("ready", "ready-"+g.EndLSN.String(), map[string]any{"lsn": g.EndLSN.String()})
		}
	}
	return s.Submit(it)
}

func (e *Engine) hold(ctx context.Context) error {
	e.mu.Lock()
	ch := e.resume
	e.mu.Unlock()
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
