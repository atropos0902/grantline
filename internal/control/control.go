// Package control serves the API of one worker.
package control

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	v1 "grantline/gen/grantline/v1"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
	"grantline/internal/worker"
)

// Server implements ControlService.
type Server struct {
	W *worker.Worker
	// Target is nil when the target isn't Postgres: chunks and key history are
	// kept there.
	Target *pgxpool.Pool
	Source *source.Source
}

var phases = map[target.Phase]v1.Phase{target.New: v1.Phase_PHASE_NEW, target.Copying: v1.Phase_PHASE_COPYING, target.Ready: v1.Phase_PHASE_READY}

// Status collects the worker's numbers.
func (s *Server) Status(ctx context.Context) *v1.Status {
	out := &v1.Status{Replication: s.W.Cfg.Name}
	if err := s.W.Failure(); err != nil {
		out.Failure = err.Error()
	}
	if s.Source != nil {
		c, cancel := context.WithTimeout(ctx, time.Second)
		if st, err := s.Source.SlotState(c); err == nil {
			out.RetainedLogBytes = st.Retained
		}
		cancel()
	}
	e := s.W.Engine()
	if e == nil {
		return out
	}
	st := e.Status()
	out.Phase, out.Epoch = phases[st.Phase], st.Epoch
	out.AppliedLsn, out.SourceLsn = st.Applied.String(), st.SourceEnd.String()
	out.Lag = durationpb.New(st.Lag)
	if !st.LastBeatAt.IsZero() {
		out.LastHeartbeat = timestamppb.New(st.LastBeatAt)
		// Nothing heard for a while is lag too.
		if quiet := time.Since(st.Beat); quiet > st.Lag+2*s.W.Cfg.Beat() {
			out.Lag = durationpb.New(quiet)
		}
	}
	out.Chunks, out.ChunksLanded = int32(st.Chunks), int32(st.ChunksDone)
	out.RowsCopied, out.RowsRefused, out.KeysReread = st.Rows, st.Refused, st.Reread
	out.Changes, out.SpilledTransactions = st.Changes, st.Spilled
	out.Commits, out.Yields = st.Sched.Committed, st.Sched.Yields
	out.Paused, out.WaitingStatement = st.Paused, st.Waiting
	return out
}

// GetStatus returns the numbers once.
func (s *Server) GetStatus(ctx context.Context, _ *connect.Request[v1.GetStatusRequest]) (*connect.Response[v1.GetStatusResponse], error) {
	return connect.NewResponse(&v1.GetStatusResponse{Status: s.Status(ctx)}), nil
}

// WatchStatus sends them every second.
func (s *Server) WatchStatus(ctx context.Context, _ *connect.Request[v1.WatchStatusRequest], out *connect.ServerStream[v1.WatchStatusResponse]) error {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		if err := out.Send(&v1.WatchStatusResponse{Status: s.Status(ctx)}); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

var errNoHistory = connect.NewError(connect.CodeFailedPrecondition, errors.New("this replication's target keeps no history"))

// ListChunks returns the chunks of a table, or of all of them.
func (s *Server) ListChunks(ctx context.Context, req *connect.Request[v1.ListChunksRequest]) (*connect.Response[v1.ListChunksResponse], error) {
	if s.Target == nil {
		return nil, errNoHistory
	}
	name := req.Msg.Table
	if name != "" {
		name = qualify(name)
	}
	cs, err := pgtarget.Chunks(ctx, s.Target, s.W.Cfg.Name, name)
	if err != nil {
		return nil, err
	}
	out := &v1.ListChunksResponse{}
	for _, c := range cs {
		out.Chunks = append(out.Chunks, &v1.Chunk{Table: c.Table, Number: c.No, Lo: c.Lo, Hi: c.Hi, Landed: c.Landed,
			Snapshot: c.Snapshot, Rows: c.Rows, Refused: c.Refused, Crc32C: c.CRC})
	}
	return connect.NewResponse(out), nil
}

func qualify(table string) string {
	if !strings.Contains(table, ".") {
		return "public." + table
	}
	return table
}

// Explained is everything known about a key.
type Explained struct {
	Steps                []pgtarget.Step
	TargetRow, SourceRow string
}

// Explain gathers a key's history and its row on both sides.
func Explain(ctx context.Context, src *source.Source, tgt *pgxpool.Pool, replication, table string, key []string) (*Explained, error) {
	table = qualify(table)
	tables, err := src.Tables(ctx)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		if t.FullName() != table {
			continue
		}
		if len(key) != len(t.Key) {
			return nil, fmt.Errorf("%s has the key (%s), got %d values", table, strings.Join(t.Key, ", "), len(key))
		}
		var out Explained
		if out.Steps, err = pgtarget.Explain(ctx, tgt, replication, table, t, key); err != nil {
			return nil, err
		}
		if out.TargetRow, err = pgtarget.RowJSON(ctx, tgt, t, key); err != nil {
			return nil, err
		}
		if out.SourceRow, err = pgtarget.RowJSON(ctx, src.Pool, t, key); err != nil {
			return nil, err
		}
		return &out, nil
	}
	return nil, fmt.Errorf("%s isn't in the publication", table)
}

// ExplainKey returns a key's history.
func (s *Server) ExplainKey(ctx context.Context, req *connect.Request[v1.ExplainKeyRequest]) (*connect.Response[v1.ExplainKeyResponse], error) {
	if s.Target == nil {
		return nil, errNoHistory
	}
	x, err := Explain(ctx, s.Source, s.Target, s.W.Cfg.Name, req.Msg.Table, req.Msg.Key)
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	out := &v1.ExplainKeyResponse{TargetRow: x.TargetRow, SourceRow: x.SourceRow}
	for _, st := range x.Steps {
		p := &v1.Step{What: st.What, Xid: st.XID, Lsn: st.LSN, Snapshot: st.Snapshot, Chunk: st.Chunk, Epoch: st.Epoch, Reason: st.Reason}
		if !st.At.IsZero() {
			p.At = timestamppb.New(st.At)
		}
		out.Steps = append(out.Steps, p)
	}
	return connect.NewResponse(out), nil
}

// Pause stops applying.
func (s *Server) Pause(context.Context, *connect.Request[v1.PauseRequest]) (*connect.Response[v1.PauseResponse], error) {
	s.W.Pause(true)
	return connect.NewResponse(&v1.PauseResponse{}), nil
}

// Resume starts applying again.
func (s *Server) Resume(context.Context, *connect.Request[v1.ResumeRequest]) (*connect.Response[v1.ResumeResponse], error) {
	s.W.Pause(false)
	return connect.NewResponse(&v1.ResumeResponse{}), nil
}

// ApproveSchemaChange runs the statement that is waiting.
func (s *Server) ApproveSchemaChange(_ context.Context, req *connect.Request[v1.ApproveSchemaChangeRequest]) (*connect.Response[v1.ApproveSchemaChangeResponse], error) {
	e := s.W.Engine()
	if e == nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the replication isn't running"))
	}
	if err := e.Approve(req.Msg.Statement); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewResponse(&v1.ApproveSchemaChangeResponse{}), nil
}
