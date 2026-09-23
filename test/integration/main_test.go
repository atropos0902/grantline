// Package integration runs the engine against real servers.
package integration

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"runtime/pprof"
	"strings"
	"testing"
	"time"

	"grantline/internal/engine"
	"grantline/internal/harness"
	"grantline/internal/lsn"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
)

var srv struct{ src, dst *harness.Postgres }

func TestMain(m *testing.M) {
	ctx := context.Background()
	var err error
	if srv.src, err = harness.StartPostgres(ctx, "fsync=off", "logical_decoding_work_mem=64kB"); err == nil {
		srv.dst, err = harness.StartPostgres(ctx, "fsync=off")
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	// Built with GOEXPERIMENT=goroutineleakprofile, the runtime finds goroutines
	// that nothing can ever wake.
	if p := pprof.Lookup("goroutineleak"); p != nil {
		var b bytes.Buffer
		_ = p.WriteTo(&b, 1)
		fmt.Println("leak profile:", strings.SplitN(b.String(), "\n", 2)[0])
		if strings.Contains(b.String(), "grantline/internal/") {
			fmt.Println(b.String())
			code = 1
		}
	}
	_ = srv.src.C.Terminate(ctx)
	_ = srv.dst.C.Terminate(ctx)
	os.Exit(code)
}

const grantsSQL = `create table grants (id bigint primary key, principal text not null, resource text not null,
  state text not null, badge int unique, policy text);
alter table grants alter column policy set storage external;
create publication grants_pub for table grants;`

type run struct {
	t    *testing.T
	p    *harness.Pair
	e    *engine.Engine
	tgt  *pgtarget.Target
	stop context.CancelFunc
	done chan error
}

type option func(*engine.Config, *pgtarget.Config)

// start runs an engine until the test ends or stop is called.
func start(t *testing.T, p *harness.Pair, opts ...option) *run {
	t.Helper()
	ctx, stop := context.WithCancel(t.Context())
	cfg := engine.Config{Name: p.Name, SourceDSN: p.SourceDSN, Publication: "grants_pub", Heartbeat: 100 * time.Millisecond, SpillDir: t.TempDir()}
	if testing.Verbose() && os.Getenv("ENGINE_LOG") != "" {
		cfg.Log = slog.New(slog.NewTextHandler(os.Stderr, nil))
	}
	tc := pgtarget.Config{DSN: p.TgtDS, Replication: p.Name, Lanes: 4, Journal: true}
	for _, o := range opts {
		o(&cfg, &tc)
	}
	tgt, err := pgtarget.New(ctx, tc)
	if err != nil {
		t.Fatal(err)
	}
	r := &run{t: t, p: p, e: engine.New(cfg, tgt), tgt: tgt, stop: stop, done: make(chan error, 1)}
	go func() {
		r.done <- r.e.Run(ctx)
		tgt.Close()
	}()
	t.Cleanup(r.close)
	return r
}

func (r *run) close() {
	r.stop()
	select {
	case err := <-r.done:
		r.done <- err
	case <-time.After(20 * time.Second):
		r.t.Error("engine didn't stop")
	}
}

// failed returns the engine's error if it has stopped by itself.
func (r *run) failed() error {
	select {
	case err := <-r.done:
		r.done <- err
		return err
	default:
		return nil
	}
}

func (r *run) waitFor(what string, limit time.Duration, ok func() bool) {
	r.t.Helper()
	end := time.Now().Add(limit)
	for !ok() {
		if err := r.failed(); err != nil {
			r.t.Fatalf("waiting for %s: engine stopped: %v", what, err)
		}
		if time.Now().After(end) {
			r.t.Fatalf("waiting for %s: gave up after %v; status %+v", what, limit, r.e.Status())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (r *run) ready() {
	r.t.Helper()
	r.waitFor("ready", 30*time.Second, func() bool { return r.e.Status().Phase == target.Ready })
}

// caughtUp waits until everything committed at the source by now is applied.
func (r *run) caughtUp() {
	r.t.Helper()
	var pos string
	if err := r.p.Source.QueryRow(context.Background(), `select pg_current_wal_lsn()::text`).Scan(&pos); err != nil {
		r.t.Fatal(err)
	}
	at, err := lsn.Parse(pos)
	if err != nil {
		r.t.Fatal(err)
	}
	r.waitFor("position "+pos, 30*time.Second, func() bool { return r.e.Status().Applied >= at })
}

func (r *run) same(tables ...string) {
	r.t.Helper()
	for _, tb := range tables {
		d, err := harness.Diff(context.Background(), r.p, tb)
		if err != nil {
			r.t.Fatal(err)
		}
		if len(d) > 0 {
			r.t.Errorf("%s differs: %v", tb, d)
		}
	}
}

func srcExec(t testing.TB, p *harness.Pair, sql string, args ...any) {
	t.Helper()
	if _, err := p.Source.Exec(context.Background(), sql, args...); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}
