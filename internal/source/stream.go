package source

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"

	"grantline/internal/lsn"
	"grantline/internal/txn"
)

// Batch is what the stream hands over: a transaction, or only a position when
// the source has been quiet.
type Batch struct {
	Txn *txn.Txn
	// QuietTo, when there is no transaction, is a position with nothing to
	// apply before it.
	QuietTo lsn.LSN
}

// StreamConfig says where to start and who to tell.
type StreamConfig struct {
	Start   lsn.LSN
	Limits  *txn.Limits
	Durable func() lsn.LSN                 // everything before it is safe in the target
	OnWAL   func(end lsn.LSN, t time.Time) // newest position the server has reported
}

const (
	statusEvery = 500 * time.Millisecond
	silentFor   = 30 * time.Second
)

// Stream sends transactions to out, in commit order, until ctx ends or the
// connection fails. The same goroutine reads and reports progress, so a full out
// slows the reading down without letting the server time the connection out.
func (s *Source) Stream(ctx context.Context, cfg StreamConfig, out chan<- Batch) error {
	conn, err := s.replConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	err = pglogrepl.StartReplication(ctx, conn, s.Slot, pglogrepl.LSN(cfg.Start), pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '2'", "streaming 'on'", "messages 'true'",
			fmt.Sprintf("publication_names '%s'", s.Publication)},
	})
	if err != nil {
		return fmt.Errorf("start replication at %s: %w", cfg.Start, err)
	}
	asm := txn.NewAssembler(cfg.Limits)
	defer asm.Close()

	lastStatus, lastHeard, sent := time.Now(), time.Now(), lsn.LSN(0)
	report := func(force bool) error {
		pos := cfg.Durable()
		if !force && pos == sent && time.Since(lastStatus) < 5*time.Second {
			return nil
		}
		lastStatus, sent = time.Now(), pos
		return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{
			WALWritePosition: pglogrepl.LSN(pos), WALFlushPosition: pglogrepl.LSN(pos), WALApplyPosition: pglogrepl.LSN(pos),
		})
	}
	hand := func(b Batch) error {
		for {
			tick := time.NewTimer(statusEvery)
			select {
			case out <- b:
				tick.Stop()
				return nil
			case <-ctx.Done():
				tick.Stop()
				return ctx.Err()
			case <-tick.C:
				if err := report(true); err != nil {
					return err
				}
			}
		}
	}
	for {
		rctx, cancel := context.WithDeadline(ctx, time.Now().Add(statusEvery))
		raw, err := conn.ReceiveMessage(rctx)
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			if !pgconn.Timeout(err) {
				return fmt.Errorf("replication connection: %w", err)
			}
			if time.Since(lastHeard) > silentFor {
				return errors.New("replication connection went silent")
			}
			if err := report(false); err != nil {
				return err
			}
			continue
		}
		lastHeard = time.Now()
		var data []byte
		switch m := raw.(type) {
		case *pgproto3.CopyData:
			data = m.Data
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("replication: %w", pgconn.ErrorResponseToPgError(m))
		default:
			continue
		}
		switch data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
			if err != nil {
				return err
			}
			cfg.OnWAL(lsn.LSN(ka.ServerWALEnd), ka.ServerTime)
			if asm.Idle() {
				select {
				case out <- Batch{QuietTo: lsn.LSN(ka.ServerWALEnd)}:
				default: // busy, so there is a real position coming
				}
			}
			if ka.ReplyRequested {
				if err := report(true); err != nil {
					return err
				}
			}
		case pglogrepl.XLogDataByteID:
			x, err := pglogrepl.ParseXLogData(data[1:])
			if err != nil {
				return err
			}
			cfg.OnWAL(lsn.LSN(x.ServerWALEnd), x.ServerTime)
			t, err := asm.Feed(x.WALData)
			if err != nil {
				return err
			}
			if t != nil {
				if err := hand(Batch{Txn: t}); err != nil {
					t.Close()
					return err
				}
			}
		}
		if time.Since(lastStatus) >= statusEvery {
			if err := report(false); err != nil {
				return err
			}
		}
	}
}
