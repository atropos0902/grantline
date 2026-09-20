package engine

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"

	"grantline/internal/bugs"
	"grantline/internal/lsn"
	"grantline/internal/sched"
	"grantline/internal/source"
	"grantline/internal/target/pgtarget"
)

// copyAll copies every table that isn't finished, then marks the log. When the
// stream reaches the mark, everything any chunk saw has been applied, and the
// copy can be read.
func (e *Engine) copyAll(ctx context.Context, src *source.Source, s *sched.Scheduler, ready bool) error {
	for _, t := range e.sorted() {
		if err := e.copyTable(ctx, src, s, t); err != nil {
			return fmt.Errorf("copy %s: %w", t.FullName(), err)
		}
	}
	if ready {
		return nil
	}
	_, err := src.Mark(ctx, "ready "+e.cfg.Name)
	return err
}

func (e *Engine) sorted() []*source.Table {
	e.mu.Lock()
	defer e.mu.Unlock()
	var names []string
	for n := range e.tables {
		names = append(names, n)
	}
	out := make([]*source.Table, 0, len(names))
	for _, n := range sortedStrings(names) {
		out = append(out, e.tables[n])
	}
	return out
}

func (e *Engine) copyTable(ctx context.Context, src *source.Source, s *sched.Scheduler, t *source.Table) error {
	plan, err := e.pg.CopyPlan(ctx, t)
	if err != nil {
		return err
	}
	if plan.Finished {
		return nil
	}
	e.cfg.Log.Info("copying", "table", t.FullName(), "chunks planned", len(plan.Chunks), "binary", t.Binary, "target had rows", plan.HadRows)

	work := make(chan pgtarget.Chunk, e.cfg.Copiers)
	g, gctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		defer close(work)
		send := func(c pgtarget.Chunk) error {
			select {
			case work <- c:
				return nil
			case <-gctx.Done():
				return gctx.Err()
			}
		}
		var lo []string
		for _, c := range plan.Chunks {
			lo = c.Bounds.Hi
			e.mu.Lock()
			e.st.Chunks++
			if c.Landed {
				e.st.ChunksDone++
			}
			e.mu.Unlock()
			if !c.Landed {
				if err := send(c); err != nil {
					return err
				}
			}
		}
		for no := len(plan.Chunks); !plan.Planned; no++ {
			hi, err := src.NextBound(gctx, t, lo, e.cfg.ChunkRows)
			if err != nil {
				return err
			}
			c := pgtarget.Chunk{No: no, Bounds: source.Bounds{Lo: lo, Hi: hi}}
			if err := e.pg.AddChunk(gctx, t, c, hi == nil); err != nil {
				return err
			}
			e.mu.Lock()
			e.st.Chunks++
			e.mu.Unlock()
			if err := send(c); err != nil {
				return err
			}
			if hi == nil {
				return nil
			}
			lo = hi
		}
		return nil
	})
	for range e.cfg.Copiers {
		g.Go(func() error {
			cp, err := e.pg.NewCopier(gctx)
			if err != nil {
				return err
			}
			defer cp.Close()
			for c := range work {
				l := &pgtarget.Landing{Table: t, Chunk: &c, Delete: plan.HadRows}
				if err := e.land(gctx, src, s, cp, l, t.Where(c.Bounds), t.Binary); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}
	return e.finish(ctx, src, s, t)
}

// land reads rows under a snapshot, stages them and puts them in line.
func (e *Engine) land(ctx context.Context, src *source.Source, s *sched.Scheduler, cp *pgtarget.Copier, l *pgtarget.Landing, where string, binary bool) error {
	cols := make([]string, len(l.Table.Columns))
	for i, c := range l.Table.Columns {
		cols[i] = c.Name
	}
	l.Cols = cols
	ctx, span := tracer.Start(ctx, "chunk", trace.WithAttributes(attribute.String("table", l.Table.FullName())))
	defer span.End()
	began := time.Now()
	pr, pw := io.Pipe()
	type staged struct {
		rows int64
		crc  uint32
		err  error
	}
	done := make(chan staged, 1)
	go func() {
		rows, crc, err := cp.Stage(ctx, l.Table, cols, binary, pr)
		pr.CloseWithError(err)
		done <- staged{rows, crc, err}
	}()
	bw := bufio.NewWriterSize(pw, 256<<10)
	var after func(string) error
	if h := e.cfg.Hooks.AfterReading; h != nil {
		after = func(snap string) error { return h(ctx, l.Table, snap) }
	}
	read, err := src.ReadChunk(ctx, l.Table, where, cols, binary, bw, after)
	if err == nil {
		err = bw.Flush()
	}
	pw.CloseWithError(err)
	st := <-done
	if err != nil {
		return err
	}
	if st.err != nil {
		return fmt.Errorf("stage: %w", st.err)
	}
	if st.rows != read.Rows || st.crc != read.CRC {
		return fmt.Errorf("chunk damaged on the way: read %d rows with checksum %08x, staged %d with %08x", read.Rows, read.CRC, st.rows, st.crc)
	}
	l.Read = read
	if h := e.cfg.Hooks.ChunkStaged; h != nil {
		if err := h(ctx, l.Table, read); err != nil {
			return err
		}
	}
	finished := make(chan struct{})
	l.Done = func(refused int64) {
		if l.Chunk != nil {
			e.mu.Lock()
			e.st.ChunksDone++
			e.mu.Unlock()
			e.event("chunk_landed", fmt.Sprintf("chunk-%s-%d-%s", l.Table.FullName(), l.Chunk.No, read.Snapshot), map[string]any{
				"table": l.Table.FullName(), "chunk": l.Chunk.No, "rows": read.Rows, "refused": refused,
				"ms": time.Since(began).Milliseconds(), "bytes": read.Bytes,
			})
		}
		close(finished)
	}
	if bugs.MergeOutsideOrder {
		return cp.LandNow(ctx, l)
	}
	it, err := cp.Item(l)
	if err != nil {
		return err
	}
	if err := s.Submit(it); err != nil {
		return err
	}
	// The staging table belongs to this landing until it has committed.
	select {
	case <-finished:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// finish reads again the keys whose updates came without their large values,
// and closes the table.
//
// A chunk, or a key read again, can be newer than the stream: rows the stream
// is about to mention may be gone by then. So the table isn't finished before
// the stream has passed a mark written after the last read. From there on, what
// the target holds is what the source held at the stream's position, and a row
// that should be there and isn't means that the two have diverged.
func (e *Engine) finish(ctx context.Context, src *source.Source, s *sched.Scheduler, t *source.Table) error {
	cp, err := e.pg.NewCopier(ctx)
	if err != nil {
		return err
	}
	defer cp.Close()
	for {
		mark, err := src.Mark(ctx, "read "+e.cfg.Name)
		if err != nil {
			return err
		}
		for lsn.LSN(e.durable.Load()) < mark {
			select {
			case <-time.After(5 * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		keys, err := e.pg.RereadKeys(ctx, t, 1000)
		if err != nil {
			return err
		}
		if len(keys) > 0 {
			l := &pgtarget.Landing{Table: t, Keys: keys}
			if err := e.land(ctx, src, s, cp, l, pgtarget.KeyIn(t, keys), t.Binary); err != nil {
				return err
			}
			continue
		}
		// A key can join the list after that query. FinishItem looks again, in
		// commit order, and says no if one has.
		result := make(chan bool, 1)
		if err := s.Submit(e.pg.FinishItem(t, func(ok bool) { result <- ok })); err != nil {
			return err
		}
		select {
		case ok := <-result:
			if ok {
				e.event("table_finished", "finished-"+t.FullName(), map[string]any{"table": t.FullName()})
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
