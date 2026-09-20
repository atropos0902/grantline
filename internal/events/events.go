// Package events publishes what a run does to NATS JetStream. Nothing in a
// replication depends on it: when NATS is away, events are counted and dropped.
package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// StreamName is the JetStream stream events go to.
const StreamName = "GRANTLINE"

// Event is one message.
type Event struct {
	Replication string         `json:"replication"`
	Kind        string         `json:"kind"`
	At          time.Time      `json:"at"`
	Fields      map[string]any `json:"fields,omitempty"`
}

// Publisher sends events in the background.
type Publisher struct {
	nc      *nats.Conn
	js      jetstream.JetStream
	ch      chan *nats.Msg
	dropped atomic.Int64
	sent    atomic.Int64
	done    chan struct{}
}

// Connect starts a publisher. It doesn't need NATS to be up.
func Connect(ctx context.Context, url string, log *slog.Logger) (*Publisher, error) {
	nc, err := nats.Connect(url, nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1), nats.ReconnectWait(time.Second), nats.Name("grantline"))
	if err != nil {
		return nil, err
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, err
	}
	p := &Publisher{nc: nc, js: js, ch: make(chan *nats.Msg, 1024), done: make(chan struct{})}
	go p.run(ctx, log)
	return p, nil
}

// Ensure makes the stream.
func Ensure(ctx context.Context, js jetstream.JetStream) error {
	_, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name: StreamName, Subjects: []string{"grantline.events.>"},
		MaxAge: 72 * time.Hour, MaxBytes: 256 << 20, Duplicates: 10 * time.Minute,
	})
	return err
}

func (p *Publisher) run(ctx context.Context, log *slog.Logger) {
	defer close(p.done)
	made := false
	for {
		var m *nats.Msg
		select {
		case m = <-p.ch:
			if m == nil {
				return
			}
		case <-ctx.Done():
			return
		}
		c, cancel := context.WithTimeout(ctx, 2*time.Second)
		if !made {
			if err := Ensure(c, p.js); err != nil {
				log.Debug("events are off", "error", err)
			} else {
				made = true
			}
		}
		if _, err := p.js.PublishMsg(c, m); err != nil {
			p.dropped.Add(1)
		} else {
			p.sent.Add(1)
		}
		cancel()
	}
}

// Publish queues an event. id makes repeats of it, after a restart, one message.
func (p *Publisher) Publish(replication, kind, id string, fields map[string]any) {
	body, err := json.Marshal(Event{Replication: replication, Kind: kind, At: time.Now().UTC(), Fields: fields})
	if err != nil {
		p.dropped.Add(1)
		return
	}
	m := nats.NewMsg(fmt.Sprintf("grantline.events.%s.%s", strings.ReplaceAll(replication, ".", "_"), kind))
	m.Data = body
	m.Header.Set(jetstream.MsgIDHeader, replication+":"+id)
	select {
	case p.ch <- m:
	default:
		p.dropped.Add(1)
	}
}

// Counts returns how many events were sent and dropped.
func (p *Publisher) Counts() (sent, dropped int64) { return p.sent.Load(), p.dropped.Load() }

// Close sends what is queued and disconnects.
func (p *Publisher) Close() {
	close(p.ch)
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
	}
	p.nc.Close()
}
