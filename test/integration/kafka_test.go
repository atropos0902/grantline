package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"grantline/internal/engine"
	"grantline/internal/harness"
	"grantline/internal/lsn"
	"grantline/internal/target"
	"grantline/internal/target/kafkatarget"
)

// consume reads a topic to its committed end.
func consume(t *testing.T, addr, topic string) []kafkatarget.Change {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	c, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.FetchIsolationLevel(kgo.ReadCommitted()), kgo.KeepControlRecords(),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ends, err := kadm.NewClient(c).ListCommittedOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	end, _ := ends.Lookup(topic, 0)
	var out []kafkatarget.Change
	for last := int64(-1); last < end.Offset-1; {
		fs := c.PollFetches(ctx)
		if err := fs.Err(); err != nil {
			t.Fatal(err)
		}
		for _, r := range fs.Records() {
			last = r.Offset
			if r.Attrs.IsControl() {
				continue
			}
			var ch kafkatarget.Change
			if err := json.Unmarshal(r.Value, &ch); err != nil {
				t.Fatal(err)
			}
			out = append(out, ch)
		}
	}
	return out
}

// Every change reaches Kafka once and in order, across restarts that happen
// with a transaction in hand, and an old worker can't add to it.
func TestKafkaOnceAndInOrder(t *testing.T) {
	kc, addr, err := harness.StartKafka(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = kc.Terminate(context.Background()) })
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	run := func(ctx context.Context, hook func(*target.Group)) (*engine.Engine, chan error) {
		tgt, err := kafkatarget.New(kafkatarget.Config{Brokers: []string{addr}, Replication: p.Name, Prefix: "t.", Partitions: 1, Replicas: 1,
			Keys: map[string][]string{"public.grants": {"id"}}, BeforeCommit: hook})
		if err != nil {
			t.Fatal(err)
		}
		e := engine.New(engine.Config{Name: p.Name, SourceDSN: p.SourceDSN, Publication: "grants_pub", Heartbeat: 50 * time.Millisecond, SpillDir: t.TempDir()}, tgt)
		done := make(chan error, 1)
		go func() { done <- e.Run(ctx); tgt.Close() }()
		return e, done
	}
	wait := func(e *engine.Engine, done chan error) {
		t.Helper()
		var s string
		if err := p.Source.QueryRow(t.Context(), `select pg_current_wal_lsn()::text`).Scan(&s); err != nil {
			t.Fatal(err)
		}
		at, _ := lsn.Parse(s)
		for end := time.Now().Add(20 * time.Second); e.Status().Applied < at; time.Sleep(5 * time.Millisecond) {
			select {
			case err := <-done:
				t.Fatalf("stopped: %v", err)
			default:
			}
			if time.Now().After(end) {
				t.Fatalf("not caught up: %+v", e.Status())
			}
		}
	}
	n := 0
	load := func(k int) {
		for range k {
			n++
			srcExec(t, p, `insert into grants values ($1::bigint, 'u', 'r', 'active', $1::int, null)`, n)
			srcExec(t, p, `update grants set state = 'revoked' where id = $1`, n)
		}
	}
	ctx1, stop1 := context.WithCancel(t.Context())
	defer stop1()
	var hold atomic.Bool
	held, free := make(chan struct{}), make(chan struct{})
	e1, d1 := run(ctx1, func(*target.Group) {
		if hold.CompareAndSwap(true, false) {
			close(held)
			<-free
		}
	})
	for e1.Status().Phase != target.Ready {
		time.Sleep(5 * time.Millisecond)
	}
	load(20)
	wait(e1, d1)
	hold.Store(true)
	load(5)
	<-held // prepared, not committed

	ctx2, stop2 := context.WithCancel(t.Context())
	e2, d2 := run(ctx2, nil)
	load(20)
	for e2.Status().Phase != target.Ready {
		select {
		case err := <-d2:
			t.Fatalf("second worker: %v", err)
		case <-time.After(5 * time.Millisecond):
		}
	}
	wait(e2, d2)
	close(free)
	if err := <-d1; !errors.Is(err, target.ErrFenced) {
		t.Errorf("first worker ended with %v, want fenced", err)
	}
	stop2()
	<-d2
	ctx3, stop3 := context.WithCancel(t.Context())
	e3, d3 := run(ctx3, nil)
	load(10)
	for e3.Status().Phase != target.Ready {
		time.Sleep(5 * time.Millisecond)
	}
	wait(e3, d3)
	stop3()
	<-d3

	got := consume(t, addr, "t.public.grants")
	if len(got) != 2*n {
		t.Errorf("%d records, want %d", len(got), 2*n)
	}
	for i, c := range got {
		want, op := fmt.Sprint(i/2+1), []string{"insert", "update"}[i%2]
		if c.Op != op || c.Row["id"] == nil || *c.Row["id"] != want {
			t.Fatalf("record %d is %s of %v, want %s of %s", i, c.Op, c.Row["id"], op, want)
		}
	}
}
