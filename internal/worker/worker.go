// Package worker keeps one replication running: it builds the target, runs the
// engine, and runs it again after a failure that can pass.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"sync"
	"time"

	"grantline/internal/config"
	"grantline/internal/engine"
	"grantline/internal/events"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/kafkatarget"
	"grantline/internal/target/pgtarget"
)

// Worker runs a replication.
type Worker struct {
	Cfg    *config.Config
	Log    *slog.Logger
	Events *events.Publisher
	Hooks  engine.Hooks

	mu      sync.Mutex
	e       *engine.Engine
	failure error
	paused  bool
	owner   string // this process, in the target
	again   bool   // it has had the replication; only used by Run's goroutine
}

// Engine returns the running engine, or nil.
func (w *Worker) Engine() *engine.Engine {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.e
}

// Failure is why the replication isn't running.
func (w *Worker) Failure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.failure
}

// Pause and Resume survive a restart of the engine, not of the process. In
// Kubernetes the Replication's spec.paused does that.
func (w *Worker) Pause(on bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.paused = on
	if w.e == nil {
		return
	}
	if on {
		w.e.Pause()
	} else {
		w.e.Resume()
	}
}

// Terminal says whether trying again can't help.
func Terminal(err error) bool {
	return errors.Is(err, target.ErrFenced) || errors.Is(err, target.ErrDiverged) || errors.Is(err, source.ErrSlotLost)
}

// Run runs until ctx ends or the error is terminal.
func (w *Worker) Run(ctx context.Context) error {
	if w.Cfg.Paused {
		w.Pause(true)
	}
	host, _ := os.Hostname()
	w.owner = fmt.Sprintf("%s %08x", host, rand.Uint32())
	wait := 200 * time.Millisecond
	for {
		began := time.Now()
		err := w.once(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		w.mu.Lock()
		w.failure, w.e = err, nil
		w.mu.Unlock()
		if Terminal(err) {
			w.Log.Error("stopped", "error", err)
			if w.Events != nil {
				w.Events.Publish(w.Cfg.Name, "stopped", "stopped-"+began.Format(time.RFC3339Nano), map[string]any{"error": err.Error()})
			}
			return err
		}
		if time.Since(began) > 30*time.Second {
			wait = 200 * time.Millisecond
		}
		w.Log.Warn("engine stopped, trying again", "error", err, "in", wait)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		wait = min(wait*2, 5*time.Second)
	}
}

// Target builds the target the configuration names. again says that this owner
// has had the replication before.
func Target(ctx context.Context, c *config.Config, owner string, again bool) (target.Target, error) {
	if c.Target.Kind == "kafka" {
		src, err := source.Open(ctx, c.Source.DSN, c.Source.Slot, c.Source.Publication)
		if err != nil {
			return nil, err
		}
		defer src.Close()
		tables, err := src.Tables(ctx)
		if err != nil {
			return nil, err
		}
		keys := map[string][]string{}
		for _, t := range tables {
			keys[t.FullName()] = t.Key
		}
		return kafkatarget.New(kafkatarget.Config{Brokers: c.Target.Brokers, Replication: c.Name, Prefix: c.Target.Prefix,
			Partitions: c.Target.Parts, Replicas: c.Target.Replicas, Keys: keys, Owner: owner, Again: again})
	}
	return pgtarget.New(ctx, pgtarget.Config{DSN: c.Target.DSN, Replication: c.Name, Lanes: c.Target.Lanes,
		Journal: c.Target.Journal == nil || *c.Target.Journal, KeepOld: c.Target.KeepOld, Owner: owner, Again: again})
}

func (w *Worker) once(ctx context.Context) error {
	tgt, err := Target(ctx, w.Cfg, w.owner, w.again)
	if err != nil {
		return err
	}
	defer tgt.Close()
	e := engine.New(engine.Config{
		Name: w.Cfg.Name, SourceDSN: w.Cfg.Source.DSN, Publication: w.Cfg.Source.Publication, Slot: w.Cfg.Source.Slot,
		ChunkRows: w.Cfg.ChunkRows, Copiers: w.Cfg.Copiers, GroupMax: w.Cfg.GroupMax, Budget: w.Cfg.BudgetMiB << 20,
		SpillDir: w.Cfg.SpillDir, Heartbeat: w.Cfg.Beat(), Events: w.Events, Log: w.Log, Hooks: w.Hooks,
	}, tgt)
	w.mu.Lock()
	w.e, w.failure = e, nil
	if w.paused {
		e.Pause()
	}
	w.mu.Unlock()
	err = e.Run(ctx)
	// Only a worker that got as far as trying to take over may have done it.
	w.again = w.again || e.Opened()
	return err
}
