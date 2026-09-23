package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/config"
	"grantline/internal/harness"
	"grantline/internal/load"
	"grantline/internal/target"
	"grantline/internal/worker"
)

func freePort() string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return ""
	}
	defer l.Close()
	_, p, _ := net.SplitHostPort(l.Addr().String())
	return p
}

func failoverOnce(ctx context.Context, wait bool) (err error) {
	id := fmt.Sprint(time.Now().UnixNano() % 1e7)
	net, pri, sby := "grantline-net-"+id, "grantline-pri-"+id, "grantline-sby-"+id
	p1, p2 := freePort(), freePort()
	defer func() {
		c := context.Background()
		_, _ = sh(c, "docker", "rm", "-f", "-v", pri)
		_, _ = sh(c, "docker", "rm", "-f", "-v", sby)
		_, _ = sh(c, "docker", "network", "rm", net)
	}()
	if _, err := sh(ctx, "docker", "network", "create", net); err != nil {
		return err
	}
	slots := ""
	if wait {
		slots = "sb"
	}
	if _, err := sh(ctx, "docker", "run", "-d", "--name", pri, "--network", net, "-p", "127.0.0.1:"+p1+":5432",
		"-e", "POSTGRES_PASSWORD=x", "-e", "POSTGRES_HOST_AUTH_METHOD=trust", harness.PostgresImage,
		"-c", "wal_level=logical", "-c", "synchronized_standby_slots="+slots, "-c", "max_wal_size=256MB"); err != nil {
		return err
	}
	dsn := func(port string) string {
		return "postgres://postgres:x@127.0.0.1:" + port + "/postgres?sslmode=disable&connect_timeout=2"
	}
	both := "postgres://postgres:x@127.0.0.1:" + p1 + ",127.0.0.1:" + p2 + "/postgres?sslmode=disable&target_session_attrs=read-write&connect_timeout=2"
	up := func(port string) (*pgxpool.Pool, error) {
		for end := time.Now().Add(40 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			p, err := pgxpool.New(ctx, dsn(port))
			if err == nil {
				if err = p.Ping(ctx); err == nil {
					time.Sleep(300 * time.Millisecond)
					if err = p.Ping(ctx); err == nil {
						return p, nil
					}
				}
				p.Close()
			}
			if time.Now().After(end) {
				return nil, fmt.Errorf("no answer on port %s: %w", port, err)
			}
		}
	}
	// The image restarts the server once during setup, and only then listens on TCP.
	S, err := up(p1)
	if err != nil {
		return err
	}
	defer S.Close()
	if _, err := S.Exec(ctx, `select pg_create_physical_replication_slot('sb', true)`); err != nil {
		return err
	}
	if _, err := sh(ctx, "docker", "exec", pri, "bash", "-c", `echo "host replication all all trust" >> $PGDATA/pg_hba.conf`); err != nil {
		return err
	}
	if _, err := S.Exec(ctx, `select pg_reload_conf()`); err != nil {
		return err
	}
	if _, err := S.Exec(ctx, load.Schema); err != nil {
		return err
	}
	if err := load.Fill(ctx, S, 10000); err != nil {
		return err
	}
	if _, err := sh(ctx, "docker", "run", "-d", "--name", sby, "--network", net, "-p", "127.0.0.1:"+p2+":5432", "-u", "postgres",
		"-e", "PGDATA=/tmp/sb", harness.PostgresImage, "bash", "-c",
		"pg_basebackup -c fast -h "+pri+" -U postgres -D /tmp/sb -S sb -X stream && touch /tmp/sb/standby.signal && chmod 700 /tmp/sb && exec postgres "+
			"-c wal_level=logical -c hot_standby_feedback=on -c sync_replication_slots=on -c primary_slot_name=sb "+
			"-c synchronized_standby_slots= "+
			"-c \"primary_conninfo=host="+pri+" user=postgres dbname=postgres\""); err != nil {
		return err
	}
	// A standby opens for readers at the first snapshot of running transactions
	// it replays, and an idle primary logs one rarely.
	sctx, sstop := context.WithCancel(ctx)
	go func() {
		for sctx.Err() == nil {
			_, _ = S.Exec(sctx, `select pg_log_standby_snapshot()`)
			time.Sleep(500 * time.Millisecond)
		}
	}()
	B, err := up(p2)
	sstop()
	if err != nil {
		out, _ := exec.Command("docker", "logs", "--tail", "20", sby).CombinedOutput()
		return fmt.Errorf("%w\n%s", err, out)
	}
	defer B.Close()
	dst, err := harness.StartPostgres(ctx, "fsync=off")
	if err != nil {
		return err
	}
	defer dst.C.Terminate(context.Background())
	D, err := pgxpool.New(ctx, dst.DSN("postgres"))
	if err != nil {
		return err
	}
	defer D.Close()

	dir, err := os.MkdirTemp("", "failover")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	path, err := writeConfig(dir, "fo", both, dst.DSN("postgres"), "")
	if err != nil {
		return err
	}
	cfg, err := config.Load(path)
	if err != nil {
		return err
	}
	logf, _ := os.Create(dir + "/log")
	w := &worker.Worker{Cfg: cfg, Log: slog.New(slog.NewTextHandler(logf, nil))}
	wctx, wstop := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- w.Run(wctx) }()
	defer wstop()
	M, err := pgxpool.New(ctx, both)
	if err != nil {
		return err
	}
	defer M.Close()
	m := &load.Mix{Pool: M, Rows: 10000, Workers: 3, Pause: 3 * time.Millisecond}
	m.Start(ctx)
	stopped := false
	defer func() {
		if !stopped {
			m.Stop()
		}
	}()
	// Ready, and the standby has the slot.
	for end := time.Now().Add(60 * time.Second); ; time.Sleep(100 * time.Millisecond) {
		var ok bool
		_ = B.QueryRow(ctx, `select exists (select 1 from pg_replication_slots where slot_name = 'grantline_fo' and synced and not temporary)`).Scan(&ok)
		if e := w.Engine(); ok && e != nil && e.Status().Phase == target.Ready {
			break
		}
		select {
		case err := <-done:
			return fmt.Errorf("worker ended: %w", err)
		default:
		}
		if time.Now().After(end) {
			b, _ := os.ReadFile(dir + "/log")
			return fmt.Errorf("not ready with a synced slot after a minute; worker failure %v\n%s", w.Failure(), b)
		}
	}
	time.Sleep(2 * time.Second)
	if !wait {
		// The standby stands still while the primary writes more than the
		// connection between them can hold, and the engine applies it. Then the
		// primary is lost. The copy has changes nobody will have again.
		if _, err := sh(ctx, "docker", "pause", sby); err != nil {
			return err
		}
		for range 3 {
			if _, err := M.Exec(ctx, `update grants set policy = repeat(md5(random()::text), 120) where id % 2 = 0`); err != nil {
				return err
			}
		}
		time.Sleep(5 * time.Second)
	}
	if _, err := sh(ctx, "docker", "kill", "-s", "KILL", pri); err != nil {
		return err
	}
	if !wait {
		if _, err := sh(ctx, "docker", "unpause", sby); err != nil {
			return err
		}
		time.Sleep(time.Second)
	}
	if _, err := B.Exec(ctx, `select pg_promote()`); err != nil {
		return err
	}
	if wait {
		time.Sleep(3 * time.Second)
		m.Stop()
		stopped = true
		for end := time.Now().Add(60 * time.Second); ; time.Sleep(200 * time.Millisecond) {
			err := settled(ctx, B, D, "fo", nil, 2*time.Second)
			if err == nil {
				break
			}
			select {
			case werr := <-done:
				return fmt.Errorf("worker ended: %w", werr)
			default:
			}
			if time.Now().After(end) {
				b, _ := os.ReadFile(dir + "/log")
				return fmt.Errorf("%w\n%s", err, b)
			}
		}
		return same(ctx, B, D)
	}
	select {
	case err := <-done:
		return err
	case <-time.After(60 * time.Second):
		b, _ := os.ReadFile(dir + "/log")
		return fmt.Errorf("the worker went on as if nothing had happened\n%s", b)
	}
}
