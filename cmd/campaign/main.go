// Command campaign runs the measurements in the README. Each prints what it
// found and writes it to results/<name>.json.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/harness"
	"grantline/internal/lsn"
)

type campaign struct {
	run  func(ctx context.Context, n int) (map[string]any, error)
	n    int
	help string
}

var campaigns = map[string]campaign{
	"races":    {races, 10000, "revokes forced to race a chunk"},
	"kills":    {kills, 200, "workers killed at random times"},
	"pauses":   {pauses, 50, "workers paused, replaced and resumed"},
	"failover": {failover, 5, "primaries lost under load"},
	"lag":      {lag, 60, "seconds of load while revokes are timed"},
	"lanes":    {lanes, 40000, "transactions drained with one lane and with four"},
	"copy":     {copyRace, 5000000, "rows copied, against pg_dump piped to psql"},
	"drill":    {drill, 1, "a ticket to solve with the runbook, needs a build with the bug"},
	"bigtx":    {bigtx, 5000000, "rows in one transaction, in 512 MiB"},
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 || campaigns[os.Args[1]].run == nil {
		for name, c := range campaigns {
			fmt.Fprintf(os.Stderr, "campaign %-9s [-n %d]  %s\n", name, c.n, c.help)
		}
		os.Exit(2)
	}
	name := os.Args[1]
	c := campaigns[name]
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	n := fs.Int("n", c.n, c.help)
	out := fs.String("out", "results", "directory for the result")
	_ = fs.Parse(os.Args[2:])
	began := time.Now()
	res, err := c.run(ctx, *n)
	if res == nil {
		res = map[string]any{}
	}
	res["seconds"] = int(time.Since(began).Seconds())
	if err != nil {
		res["error"] = err.Error()
	}
	b, _ := json.MarshalIndent(res, "", "  ")
	fmt.Println(string(b))
	_ = os.MkdirAll(*out, 0o755)
	if werr := os.WriteFile(filepath.Join(*out, name+".json"), append(b, '\n'), 0o644); werr != nil {
		fmt.Fprintln(os.Stderr, werr)
	}
	if err != nil {
		os.Exit(1)
	}
}

// servers is a source and a target, each on its own server.
type servers struct {
	src, dst *harness.Postgres
	S, D     *pgxpool.Pool
}

func start(ctx context.Context, srcSettings, dstSettings []string, dstTmpfs ...string) (*servers, func(), error) {
	var s servers
	var err error
	if s.src, err = harness.StartPostgres(ctx, srcSettings...); err != nil {
		return nil, nil, err
	}
	if len(dstTmpfs) > 0 && dstTmpfs[0] != "" {
		_ = os.Setenv("GRANTLINE_TMPFS", dstTmpfs[0])
	}
	if s.dst, err = harness.StartPostgres(ctx, dstSettings...); err != nil {
		return nil, nil, err
	}
	if s.S, err = pgxpool.New(ctx, s.src.DSN("postgres")); err != nil {
		return nil, nil, err
	}
	if s.D, err = pgxpool.New(ctx, s.dst.DSN("postgres")); err != nil {
		return nil, nil, err
	}
	return &s, func() {
		s.S.Close()
		s.D.Close()
		_ = s.src.C.Terminate(context.Background())
		_ = s.dst.C.Terminate(context.Background())
	}, nil
}

// process is a worker in a process of its own.
type process struct {
	cmd  *exec.Cmd
	done chan struct{}
	err  error
	log  string
}

func spawn(dir, config string) (*process, error) {
	bin, err := filepath.Abs("bin/grantline")
	if err != nil {
		return nil, err
	}
	p := &process{cmd: exec.Command(bin, "run", "-config", config), done: make(chan struct{})}
	f, err := os.CreateTemp(dir, "worker-*.log")
	if err != nil {
		return nil, err
	}
	p.log = f.Name()
	p.cmd.Stderr, p.cmd.Stdout = f, f
	if err := p.cmd.Start(); err != nil {
		return nil, err
	}
	go func() {
		p.err = p.cmd.Wait()
		f.Close()
		close(p.done)
	}()
	return p, nil
}

func (p *process) signal(s syscall.Signal) { _ = p.cmd.Process.Signal(s) }

func (p *process) tail() string {
	b, _ := os.ReadFile(p.log)
	if len(b) > 1500 {
		b = b[len(b)-1500:]
	}
	return string(b)
}

// settled waits until the target has everything the source has committed.
func settled(ctx context.Context, src, dst *pgxpool.Pool, replication string, p *process, limit time.Duration) error {
	// The insert position: a commit that didn't wait for the disk is visible
	// before the write position has passed it.
	var s string
	if err := src.QueryRow(ctx, `select pg_current_wal_insert_lsn()::text`).Scan(&s); err != nil {
		return err
	}
	want, err := lsn.Parse(s)
	if err != nil {
		return err
	}
	for end := time.Now().Add(limit); ; {
		var at, phase string
		if err := dst.QueryRow(ctx, `select lsn::text, phase from grantline.progress where replication = $1`, replication).Scan(&at, &phase); err == nil {
			if got, err := lsn.Parse(at); err == nil && got >= want && phase == "ready" {
				return nil
			}
		}
		if p != nil {
			select {
			case <-p.done:
				return fmt.Errorf("worker ended (%v) before catching up:\n%s", p.err, p.tail())
			default:
			}
		}
		if time.Now().After(end) {
			return fmt.Errorf("target at %s (%s) after %v, want %s", at, phase, limit, want)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// same compares both tables and describes a difference.
func same(ctx context.Context, src, dst *pgxpool.Pool) error {
	for _, t := range []string{"grants", "members"} {
		a, err := harness.Digest(ctx, src, t)
		if err != nil {
			return err
		}
		b, err := harness.Digest(ctx, dst, t)
		if err != nil {
			return err
		}
		if a != b {
			d, _ := harness.Diff(ctx, &harness.Pair{Source: src, Target: dst}, t)
			return fmt.Errorf("%s differs: source %s, target %s: %v", t, a, b, d)
		}
	}
	return nil
}

func writeConfig(dir, name, src, dst, more string) (string, error) {
	path := filepath.Join(dir, name+".yaml")
	return path, os.WriteFile(path, []byte(fmt.Sprintf(`name: %s
source: {dsn: "%s", publication: grants_pub}
target: {kind: postgres, dsn: "%s", lanes: 4%s}
spillDir: %s
heartbeat: 100ms
chunkRows: 4000
copiers: 3
`, name, src, dst, more, dir)), 0o600)
}
