package integration

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	v1 "grantline/gen/grantline/v1"
	"grantline/gen/grantline/v1/grantlinev1connect"
	"grantline/internal/config"
	"grantline/internal/control"
	"grantline/internal/engine"
	"grantline/internal/events"
	"grantline/internal/harness"
	"grantline/internal/obs"
	"grantline/internal/preflight"
	"grantline/internal/source"
	"grantline/internal/worker"
)

func conf(t *testing.T, p *harness.Pair) *config.Config {
	t.Helper()
	path := filepath.Join(t.TempDir(), "c.yaml")
	body := "name: " + p.Name + "\nsource: {dsn: \"" + p.SourceDSN + "\", publication: grants_pub}\n" +
		"target: {kind: postgres, dsn: \"" + p.TgtDS + "\"}\nheartbeat: 100ms\nchunkRows: 100\nspillDir: " + t.TempDir() + "\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

// The API answers Connect and gRPC on one port, and tells a key's story.
func TestControlAPI(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 300) i`)
	w := &worker.Worker{Cfg: conf(t, p), Log: slog.New(slog.DiscardHandler)}
	w.Hooks = engine.Hooks{AfterReading: func(_ context.Context, _ *source.Table, _ string) error { return nil }}
	ctx, stop := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	t.Cleanup(func() { stop(); <-done })

	src, err := source.Open(ctx, p.SourceDSN, "grantline_"+p.Name, "grants_pub")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(src.Close)
	tgt, err := pgxpool.New(ctx, p.TgtDS)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(tgt.Close)
	s := &control.Server{W: w, Source: src, Target: tgt}
	mux := http.NewServeMux()
	mux.Handle(grantlinev1connect.NewControlServiceHandler(s))
	reg := prometheus.NewRegistry()
	reg.MustRegister(obs.Collector{Status: s.Status})
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	hs := httptest.NewUnstartedServer(mux)
	hs.EnableHTTP2 = true
	hs.StartTLS()
	t.Cleanup(hs.Close)

	for _, c := range []struct {
		name string
		opts []connect.ClientOption
	}{{"connect", nil}, {"grpc", []connect.ClientOption{connect.WithGRPC()}}} {
		cl := grantlinev1connect.NewControlServiceClient(hs.Client(), hs.URL, c.opts...)
		st, err := cl.WatchStatus(ctx, connect.NewRequest(&v1.WatchStatusRequest{}))
		if err != nil {
			t.Fatal(c.name, err)
		}
		for st.Receive() && st.Msg().Status.Phase != v1.Phase_PHASE_READY {
		}
		if st.Err() != nil {
			t.Fatal(c.name, st.Err())
		}
		_ = st.Close()
	}
	cl := grantlinev1connect.NewControlServiceClient(hs.Client(), hs.URL, connect.WithGRPC())
	if _, err := cl.Pause(ctx, connect.NewRequest(&v1.PauseRequest{})); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	srcExec(t, p, `delete from grants where id = 88`)
	time.Sleep(400 * time.Millisecond)
	if x := explainKey(t, cl, "88"); x.SourceRow != "" || x.TargetRow == "" {
		t.Errorf("paused: source %q target %q", x.SourceRow, x.TargetRow)
	}
	if _, err := cl.Resume(ctx, connect.NewRequest(&v1.ResumeRequest{})); err != nil {
		t.Fatal(err)
	}
	var x *v1.ExplainKeyResponse
	for end := time.Now().Add(10 * time.Second); time.Now().Before(end); time.Sleep(20 * time.Millisecond) {
		if x = explainKey(t, cl, "88"); x.TargetRow == "" {
			break
		}
	}
	var kinds []string
	for _, s := range x.Steps {
		kinds = append(kinds, s.What)
	}
	if got := strings.Join(kinds, " "); got != "chunk delete" {
		t.Errorf("history of 88 is %q", got)
	}
	if ch, err := cl.ListChunks(ctx, connect.NewRequest(&v1.ListChunksRequest{})); err != nil || len(ch.Msg.Chunks) != 4 {
		t.Errorf("chunks %v, %v", ch, err)
	}
	if _, err := cl.ApproveSchemaChange(ctx, connect.NewRequest(&v1.ApproveSchemaChangeRequest{Statement: "drop table grants"})); connect.CodeOf(err) != connect.CodeFailedPrecondition {
		t.Errorf("approving nothing: %v", err)
	}
	r, err := hs.Client().Get(hs.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(r.Body)
	if !strings.Contains(string(b), `grantline_ready{replication="`+p.Name+`"} 1`) {
		t.Errorf("metrics:\n%s", b)
	}
}

func explainKey(t *testing.T, cl grantlinev1connect.ControlServiceClient, key string) *v1.ExplainKeyResponse {
	t.Helper()
	x, err := cl.ExplainKey(t.Context(), connect.NewRequest(&v1.ExplainKeyRequest{Table: "grants", Key: []string{key}}))
	if err != nil {
		t.Fatal(err)
	}
	return x.Msg
}

func TestPreflight(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	src, err := source.Open(t.Context(), p.SourceDSN, "grantline_x", "grants_pub")
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	find := func() string {
		fs, err := preflight.Run(t.Context(), src, p.Target)
		if err != nil {
			t.Fatal(err)
		}
		var b strings.Builder
		for _, f := range fs {
			b.WriteString(f.Level.String() + " " + f.What + "\n")
		}
		return b.String()
	}
	want := func(text string) {
		t.Helper()
		if got := find(); !strings.Contains(got, text) {
			t.Errorf("want %q in\n%s", text, got)
		}
	}
	want("STOP publication grants_pub doesn't exist")
	srcExec(t, p, grantsSQL)
	want("note public.grants has 1 unique indexes")
	want("note public.grants: large values in")
	want("warn nothing caps the log")
	if got := find(); strings.Contains(got, "STOP") {
		t.Errorf("a good table was stopped:\n%s", got)
	}
	srcExec(t, p, `create table nokey (a int); alter publication grants_pub add table nokey`)
	want("STOP public.nokey has no primary key")
	srcExec(t, p, `alter table grants replica identity nothing`)
	want("its revokes wouldn't arrive")
	srcExec(t, p, `alter table grants replica identity default; alter publication grants_pub set table grants where (id > 5)`)
	want("published with a row filter")
	srcExec(t, p, `alter publication grants_pub set table grants (id, principal, resource, state)`)
	want("published with a column list")
	srcExec(t, p, `alter publication grants_pub set (publish = 'insert, update')`)
	want("leaves out some kinds of change")
}

// Events reach JetStream once, and their absence changes nothing.
func TestEvents(t *testing.T) {
	ctx := t.Context()
	nc, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{Started: true, ContainerRequest: testcontainers.ContainerRequest{
		Image: harness.NATSImage, Cmd: []string{"-js"}, ExposedPorts: []string{"4222/tcp"}, WaitingFor: wait.ForLog("Server is ready")}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = nc.Terminate(context.Background()) })
	port, err := nc.MappedPort(ctx, "4222/tcp")
	if err != nil {
		t.Fatal(err)
	}
	url := "nats://localhost:" + port.Port()
	pub, err := events.Connect(ctx, url, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'u', 'r', 'active', i from generate_series(1, 250) i`)
	w := &worker.Worker{Cfg: conf(t, p), Log: slog.New(slog.DiscardHandler), Events: pub}
	wctx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	for w.Engine() == nil || w.Engine().Status().Phase != "ready" {
		time.Sleep(10 * time.Millisecond)
	}
	stop()
	<-done
	pub.Publish(p.Name, "table_finished", "finished-public.grants", nil) // the repeat a restart would send
	pub.Close()

	c, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	js, _ := jetstream.New(c)
	cons, err := js.OrderedConsumer(ctx, events.StreamName, jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatal(err)
	}
	count := map[string]int{}
	ms, err := cons.FetchNoWait(100)
	if err != nil {
		t.Fatal(err)
	}
	for m := range ms.Messages() {
		var e events.Event
		if err := json.Unmarshal(m.Data(), &e); err != nil {
			t.Fatal(err)
		}
		count[e.Kind]++
	}
	if count["started"] != 1 || count["chunk_landed"] != 3 || count["table_finished"] != 1 || count["ready"] != 1 {
		t.Errorf("events %v", count)
	}

	off, err := events.Connect(ctx, "nats://127.0.0.1:1", slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	p2 := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p2, grantsSQL)
	r := start(t, p2, func(c *engine.Config, _ *pgtargetConfig) { c.Events = off })
	r.ready()
	srcExec(t, p2, `insert into grants values (1, 'u', 'r', 'active', 1, null)`)
	r.caughtUp()
	r.same("grants")
}
