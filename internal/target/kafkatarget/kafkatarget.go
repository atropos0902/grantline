// Package kafkatarget writes every change to Kafka, in order, once.
//
// The changes of a group and the position after it are one Kafka transaction.
// The producer's transactional ID is the replication's name, so a new worker's
// first act makes the broker refuse the old one.
package kafkatarget

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"grantline/internal/lsn"
	"grantline/internal/pgoutput"
	"grantline/internal/sched"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
	"grantline/internal/txn"
)

// Config for a Kafka target.
type Config struct {
	Brokers     []string
	Replication string
	Prefix      string // topics are prefix + schema.table
	Partitions  int32  // one keeps every change of a table in order, more keeps order per key
	Replicas    int16
	Keys        map[string][]string // primary key columns by schema.table

	Owner        string // see pgtarget.Config
	Again        bool
	BeforeCommit func(g *target.Group) // may be nil
}

// Change is the value of a record.
type Change struct {
	Op        string             `json:"op"`
	XID       uint32             `json:"xid"`
	LSN       string             `json:"lsn"`
	N         int                `json:"n"` // place in its transaction
	At        time.Time          `json:"at"`
	OldKey    string             `json:"old_key,omitempty"`
	Row       map[string]*string `json:"row,omitempty"`
	Unchanged []string           `json:"unchanged,omitempty"` // large values Postgres didn't send
}

type progress struct {
	LSN      string `json:"lsn"`
	SystemID string `json:"system_id"`
	Timeline int32  `json:"timeline"`
	Epoch    int64  `json:"epoch"`
	Owner    string `json:"owner"`
}

// Target is a set of Kafka topics.
type Target struct {
	cfg  Config
	c    *kgo.Client
	last progress
}

func (c Config) progressTopic() string { return c.Prefix + "grantline.progress" }

// New connects.
func New(cfg Config) (*Target, error) {
	t := &Target{cfg: cfg}
	return t, t.connect()
}

func (t *Target) connect() error {
	c, err := kgo.NewClient(
		kgo.SeedBrokers(t.cfg.Brokers...),
		kgo.TransactionalID("grantline-"+t.cfg.Replication),
		kgo.TransactionTimeout(2*time.Minute),
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),
		kgo.ProducerBatchCompression(kgo.Lz4Compression(), kgo.NoCompression()),
		kgo.MaxBufferedBytes(32<<20),
		kgo.ProducerLinger(0),
	)
	if err == nil {
		t.c = c
	}
	return err
}

// Open fences older workers and reads the last committed position.
func (t *Target) Open(ctx context.Context) (target.State, error) {
	var st target.State
	if err := t.topics(ctx); err != nil {
		return st, err
	}
	if t.cfg.Again {
		p, ok, err := t.read(ctx)
		if err != nil {
			return st, err
		}
		if ok && p.Owner != t.cfg.Owner {
			return st, target.ErrFenced
		}
	}
	// Asking for a producer ID aborts what an older worker left open and makes
	// the broker refuse that worker from here on. It has to come before the read.
	// The broker answers "concurrent transactions" while it is still aborting the
	// old one, and a client that has seen that answer is no use afterwards.
	for try := 0; ; try++ {
		_, _, err := t.c.ProducerID(ctx)
		if err == nil {
			break
		}
		if !errors.Is(err, kerr.ConcurrentTransactions) || try == 60 {
			return st, fmt.Errorf("take over: %w", err)
		}
		t.c.Close()
		if err := t.connect(); err != nil {
			return st, err
		}
		select {
		case <-ctx.Done():
			return st, ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}
	p, ok, err := t.read(ctx)
	if err != nil {
		return st, err
	}
	st.Phase = target.New
	if ok {
		st.Phase, st.SystemID, st.Timeline, st.Epoch = target.Ready, p.SystemID, p.Timeline, p.Epoch
		if st.LSN, err = lsn.Parse(p.LSN); err != nil {
			return st, err
		}
	}
	st.Epoch++
	p.Epoch, p.Owner = st.Epoch, t.cfg.Owner
	t.last = p
	if ok { // so that the epoch is on record before anything else is
		return st, t.save(ctx)
	}
	return st, nil
}

func (t *Target) topics(ctx context.Context) error {
	adm := kadm.NewClient(t.c)
	compact := "compact"
	create := func(n int32, conf map[string]*string, name string) error {
		r, err := adm.CreateTopic(ctx, n, t.cfg.Replicas, conf, name)
		if err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) && !errors.Is(err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", name, err)
		}
		return nil
	}
	if err := create(1, map[string]*string{"cleanup.policy": &compact}, t.cfg.progressTopic()); err != nil {
		return err
	}
	for name := range t.cfg.Keys {
		if err := create(t.cfg.Partitions, nil, t.cfg.Prefix+name); err != nil {
			return err
		}
	}
	return nil
}

// read finds the newest committed position for this replication.
func (t *Target) read(ctx context.Context) (progress, bool, error) {
	var p progress
	topic := t.cfg.progressTopic()
	ends, err := kadm.NewClient(t.c).ListCommittedOffsets(ctx, topic)
	if err != nil {
		return p, false, err
	}
	end, ok := ends.Lookup(topic, 0)
	if !ok || end.Err != nil {
		return p, false, errors.Join(end.Err, fmt.Errorf("no end offset for %s", topic))
	}
	if end.Offset == 0 {
		return p, false, nil
	}
	c, err := kgo.NewClient(kgo.SeedBrokers(t.cfg.Brokers...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords())
	if err != nil {
		return p, false, err
	}
	defer c.Close()
	found := false
	for {
		fs := c.PollFetches(ctx)
		if err := fs.Err(); err != nil {
			return p, false, err
		}
		for _, r := range fs.Records() {
			if !r.Attrs.IsControl() && string(r.Key) == t.cfg.Replication {
				if err := json.Unmarshal(r.Value, &p); err != nil {
					return p, false, err
				}
				found = true
			}
			if r.Offset >= end.Offset-1 {
				return p, found, nil
			}
		}
	}
}

// Begin records where the stream starts.
func (t *Target) Begin(ctx context.Context, at lsn.LSN, systemID string, timeline int32) error {
	t.last = progress{LSN: at.String(), SystemID: systemID, Timeline: timeline, Epoch: t.last.Epoch, Owner: t.cfg.Owner}
	return t.save(ctx)
}

// Moved records the server's identity after a failover.
func (t *Target) Moved(ctx context.Context, systemID string, timeline int32) error {
	t.last.SystemID, t.last.Timeline = systemID, timeline
	return t.save(ctx)
}

func (t *Target) save(ctx context.Context) error {
	if err := t.c.BeginTransaction(); err != nil {
		return classify(err)
	}
	if err := t.c.ProduceSync(ctx, t.position()).FirstErr(); err != nil {
		_ = t.c.EndTransaction(ctx, kgo.TryAbort)
		return classify(err)
	}
	return classify(t.c.EndTransaction(ctx, kgo.TryCommit))
}

func (t *Target) position() *kgo.Record {
	b, _ := json.Marshal(t.last)
	return &kgo.Record{Topic: t.cfg.progressTopic(), Partition: 0, Key: []byte(t.cfg.Replication), Value: b}
}

// Lanes is one: a producer has one transaction at a time.
func (t *Target) Lanes() int { return 1 }

// Close disconnects.
func (t *Target) Close() { t.c.Close() }

// Item makes a group's work. With one lane there is nothing to overlap.
func (t *Target) Item(g *target.Group) (*sched.Item, error) {
	return &sched.Item{Work: &work{t: t, g: g}, Barrier: true}, nil
}

type work struct {
	t    *Target
	g    *target.Group
	open bool
}

func (w *work) Prepare(ctx context.Context, _ int) error {
	if err := w.t.c.BeginTransaction(); err != nil {
		return classify(err)
	}
	w.open = true
	var fail error
	done := make(chan struct{}, 1)
	promise := func(_ *kgo.Record, err error) {
		if err != nil {
			select {
			case done <- struct{}{}:
				fail = err
			default:
			}
		}
	}
	for _, x := range w.g.Txns {
		n := 0
		err := x.Each(func(c *txn.Change) error {
			if c.Rel == nil {
				return nil
			}
			r, err := w.t.record(x, c, n)
			if err != nil {
				return err
			}
			n++
			w.t.c.Produce(ctx, r, promise)
			return ctx.Err()
		})
		if err != nil {
			return err
		}
	}
	w.t.last.LSN = w.g.EndLSN.String()
	w.t.c.Produce(ctx, w.t.position(), promise)
	if err := w.t.c.Flush(ctx); err != nil {
		return err
	}
	return classify(fail)
}

func (w *work) Commit(ctx context.Context) error {
	if h := w.t.cfg.BeforeCommit; h != nil {
		h(w.g)
	}
	if err := w.t.c.EndTransaction(ctx, kgo.TryCommit); err != nil {
		return classify(err)
	}
	w.open = false
	for _, x := range w.g.Txns {
		x.Close()
	}
	return nil
}

func (w *work) Rollback(ctx context.Context) error {
	if !w.open {
		return nil
	}
	w.open = false
	if err := w.t.c.AbortBufferedRecords(ctx); err != nil {
		return err
	}
	return w.t.c.EndTransaction(ctx, kgo.TryAbort)
}

func (t *Target) record(x *txn.Txn, c *txn.Change, n int) (*kgo.Record, error) {
	name := c.Rel.FullName()
	ch := Change{XID: x.XID, LSN: x.CommitLSN.String(), N: n, At: x.Time}
	topic := t.cfg.Prefix + name
	if c.Op == pgoutput.Truncate {
		ch.Op = "truncate"
		b, err := json.Marshal(ch)
		return &kgo.Record{Topic: topic, Value: b}, err
	}
	var row pgoutput.Row
	if err := pgoutput.DecodeRow(c.Op, c.Body, &row); err != nil {
		return nil, err
	}
	key := func(vs []pgoutput.Value) string {
		var out []string
		for _, k := range t.cfg.Keys[name] {
			for i, col := range c.Rel.Columns {
				if col.Name == k {
					out = append(out, string(vs[i].Data))
				}
			}
		}
		return pgtarget.KeyText(out)
	}
	var k string
	switch c.Op {
	case pgoutput.Insert:
		ch.Op, k = "insert", key(row.New)
	case pgoutput.Update:
		ch.Op, k = "update", key(row.New)
		if row.Old != nil {
			if o := key(row.Old); o != k {
				ch.OldKey = o
			}
		}
	case pgoutput.Delete:
		ch.Op, k = "delete", key(row.Old)
	}
	if row.New != nil {
		ch.Row = make(map[string]*string, len(row.New))
		for i, v := range row.New {
			col := c.Rel.Columns[i].Name
			switch v.Kind {
			case pgoutput.Null:
				ch.Row[col] = nil
			case pgoutput.Unchanged:
				ch.Unchanged = append(ch.Unchanged, col)
			default:
				s := string(v.Data)
				ch.Row[col] = &s
			}
		}
	}
	b, err := json.Marshal(ch)
	return &kgo.Record{Topic: topic, Key: []byte(k), Value: b}, err
}

func classify(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, kerr.ProducerFenced), errors.Is(err, kerr.InvalidProducerEpoch), errors.Is(err, kerr.InvalidTxnState),
		strings.Contains(err.Error(), "fenced"):
		return fmt.Errorf("%w: %w", target.ErrFenced, err)
	}
	return err
}
