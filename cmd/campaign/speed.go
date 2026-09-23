package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/harness"
	"grantline/internal/load"
)

func pick(d []time.Duration, q float64) float64 {
	sort.Slice(d, func(i, j int) bool { return d[i] < d[j] })
	if len(d) == 0 {
		return 0
	}
	return float64(d[int(float64(len(d)-1)*q)].Microseconds()) / 1000
}

// lag times revokes: from the moment the source's commit has answered to the
// moment a reader of the target no longer finds the row. The engine runs beside
// the target and reaches the source through a proxy that adds 40 ms each way.
func lag(ctx context.Context, n int) (map[string]any, error) {
	s, stop, err := start(ctx, nil, nil)
	if err != nil {
		return nil, err
	}
	defer stop()
	port, api := freePort(), freePort()
	name := "grantline-proxy-" + port
	if _, err := sh(ctx, "docker", "run", "-d", "--rm", "--name", name, "--network", "host", harness.ProxyImage, "-host=127.0.0.1", "-port="+api); err != nil {
		return nil, err
	}
	defer sh(context.Background(), "docker", "rm", "-f", name)
	post := func(path string, body any) error {
		b, _ := json.Marshal(body)
		for try := 0; ; try++ {
			r, err := http.Post("http://127.0.0.1:"+api+path, "application/json", bytes.NewReader(b))
			if err == nil {
				r.Body.Close()
				if r.StatusCode >= 300 {
					return fmt.Errorf("proxy: %s", r.Status)
				}
				return nil
			}
			if try > 50 {
				return err
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	if err := post("/proxies", map[string]any{"name": "pg", "listen": "127.0.0.1:" + port, "upstream": s.src.Host + ":" + s.src.Port, "enabled": true}); err != nil {
		return nil, err
	}
	for _, dir := range []string{"upstream", "downstream"} {
		if err := post("/proxies/pg/toxics", map[string]any{"type": "latency", "stream": dir, "attributes": map[string]any{"latency": 40}}); err != nil {
			return nil, err
		}
	}
	if _, err := s.S.Exec(ctx, load.Schema); err != nil {
		return nil, err
	}
	if err := load.Fill(ctx, s.S, 100000); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "lag")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	cfg, err := writeConfig(dir, "lag", "postgres://postgres:x@127.0.0.1:"+port+"/postgres?sslmode=disable", s.dst.DSN("postgres"), "")
	if err != nil {
		return nil, err
	}
	p, err := spawn(dir, cfg)
	if err != nil {
		return nil, err
	}
	defer p.signal(syscall.SIGTERM)
	if err := settled(ctx, s.S, s.D, "lag", p, 90*time.Second); err != nil {
		return nil, err
	}
	// Ten changes a transaction, 500 transactions a second.
	lctx, lstop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	var mu sync.Mutex
	done := 0
	const workers = 4
	for w := range workers {
		wg.Go(func() {
			t := time.NewTicker(time.Second * workers / 500)
			defer t.Stop()
			for lctx.Err() == nil {
				ids := make([]int64, 10)
				for i := range ids { // each worker has rows of its own
					ids[i] = int64(rand.IntN(20000)*workers+w) + 1
				}
				tag, err := s.S.Exec(lctx, `update grants set state = case state when 'active' then 'held' else 'active' end where id = any($1)`, ids)
				if err == nil {
					mu.Lock()
					done += int(tag.RowsAffected())
					mu.Unlock()
				}
				select {
				case <-t.C:
				case <-lctx.Done():
				}
			}
		})
	}
	time.Sleep(5 * time.Second)
	mu.Lock()
	base := done
	mu.Unlock()
	began := time.Now()
	var lags []time.Duration
	var pw sync.WaitGroup
	for k := range 4 {
		pw.Go(func() {
			for i := 0; time.Since(began) < time.Duration(n)*time.Second && ctx.Err() == nil; i++ {
				id := 90000 + k*2500 + i
				if _, err := s.S.Exec(ctx, `delete from grants where id = $1`, id); err != nil {
					return
				}
				t0 := time.Now()
				for {
					var there bool
					if err := s.D.QueryRow(ctx, `select exists (select 1 from grants where id = $1)`, id).Scan(&there); err != nil {
						return
					}
					if !there {
						break
					}
					time.Sleep(2 * time.Millisecond)
				}
				d := time.Since(t0)
				mu.Lock()
				lags = append(lags, d)
				mu.Unlock()
				time.Sleep(50 * time.Millisecond)
			}
		})
	}
	pw.Wait()
	secs := time.Since(began).Seconds()
	mu.Lock()
	rate := float64(done-base) / secs
	mu.Unlock()
	lstop()
	wg.Wait()
	res := map[string]any{"revokes_timed": len(lags), "changes_per_second": int(rate),
		"p50_ms": pick(lags, 0.5), "p99_ms": pick(lags, 0.99), "max_ms": pick(lags, 1), "delay_each_way_ms": 40}
	if err := settled(ctx, s.S, s.D, "lag", p, 60*time.Second); err != nil {
		return res, err
	}
	return res, same(ctx, s.S, s.D)
}

// lanes drains the same backlog with one lane and with four. The target has the
// indexes its readers use, and the keys are spread over a million rows. With a
// small table every group of 1,000 changes overlaps the one before it, and then
// lanes have nothing to do side by side.
func lanes(ctx context.Context, n int) (map[string]any, error) {
	res := map[string]any{"transactions": n, "changes": n * 10}
	for _, rtt := range []int{0, 5} {
		var secs [2]float64
		var digest [2]string
		for i, k := range []int{1, 4} {
			s, d, err := drain(ctx, n, k, rtt)
			if err != nil {
				return res, err
			}
			secs[i], digest[i] = s, d
			fmt.Fprintf(os.Stderr, "rtt %d ms, %d lanes: %.1f s\n", rtt, k, s)
		}
		key := fmt.Sprintf("target_round_trip_%dms", rtt)
		res[key] = map[string]any{"one_lane_s": secs[0], "four_lanes_s": secs[1], "times": secs[0] / secs[1],
			"four_lanes_changes_per_s": int(float64(n*10) / secs[1]), "same_rows": digest[0] == digest[1]}
	}
	return res, nil
}

const laneRows = 1_000_000

func drain(ctx context.Context, n, k, rtt int) (float64, string, error) {
	s, stop, err := start(ctx, []string{"max_wal_size=1GB"}, []string{"max_wal_size=1GB"})
	if err != nil {
		return 0, "", err
	}
	defer stop()
	dst := s.dst.DSN("postgres")
	if rtt > 0 {
		port, api := freePort(), freePort()
		name := "grantline-proxy-" + port
		if _, err := sh(ctx, "docker", "run", "-d", "--rm", "--name", name, "--network", "host", harness.ProxyImage, "-host=127.0.0.1", "-port="+api); err != nil {
			return 0, "", err
		}
		defer sh(context.Background(), "docker", "rm", "-f", name)
		time.Sleep(700 * time.Millisecond)
		for path, body := range map[string]string{
			"/proxies": fmt.Sprintf(`{"name":"pg","listen":"127.0.0.1:%s","upstream":"%s:%s","enabled":true}`, port, s.dst.Host, s.dst.Port)} {
			r, err := http.Post("http://127.0.0.1:"+api+path, "application/json", strings.NewReader(body))
			if err != nil {
				return 0, "", err
			}
			r.Body.Close()
		}
		r, err := http.Post("http://127.0.0.1:"+api+"/proxies/pg/toxics", "application/json", strings.NewReader(fmt.Sprintf(`{"type":"latency","stream":"downstream","attributes":{"latency":%d}}`, rtt)))
		if err != nil {
			return 0, "", err
		}
		r.Body.Close()
		dst = "postgres://postgres:x@127.0.0.1:" + port + "/postgres?sslmode=disable"
	}
	if _, err := s.S.Exec(ctx, load.Schema); err != nil {
		return 0, "", err
	}
	if _, err := s.S.Exec(ctx, fmt.Sprintf(`insert into grants select i, 'user-' || i %% 5000, 'dash-' || i %% 400, 'active' from generate_series(1, %d) i`, laneRows)); err != nil {
		return 0, "", err
	}
	dir, err := os.MkdirTemp("", "lanes")
	if err != nil {
		return 0, "", err
	}
	defer os.RemoveAll(dir)
	cfg, err := writeConfig(dir, "lanes", s.src.DSN("postgres"), dst, "")
	if err != nil {
		return 0, "", err
	}
	b, _ := os.ReadFile(cfg)
	if err := os.WriteFile(cfg, []byte(strings.Replace(string(b), "lanes: 4", "lanes: "+strconv.Itoa(k), 1)+os.Getenv("CAMPAIGN_CONFIG")), 0o600); err != nil {
		return 0, "", err
	}
	p, err := spawn(dir, cfg)
	if err != nil {
		return 0, "", err
	}
	if err := settled(ctx, s.S, s.D, "lanes", p, 60*time.Second); err != nil {
		return 0, "", err
	}
	p.signal(syscall.SIGTERM)
	<-p.done
	if _, err := s.D.Exec(ctx, `create index on grants (principal); create index on grants (resource); create index on grants (state, resource)`); err != nil {
		return 0, "", err
	}
	// The same backlog each time: the generator is seeded.
	r := rand.New(rand.NewPCG(1, 2))
	conn, err := pgx.Connect(ctx, s.src.DSN("postgres"))
	if err != nil {
		return 0, "", err
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, `set synchronous_commit = off`); err != nil {
		return 0, "", err
	}
	for done := 0; done < n; done += 500 {
		bt := &pgx.Batch{}
		for range 500 {
			ids := make([]int64, 10)
			for i := range ids {
				ids[i] = int64(r.IntN(laneRows)) + 1
			}
			bt.Queue("begin")
			bt.Queue(`update grants set state = case state when 'active' then 'held' else 'active' end, resource = resource || '.' where id = any($1)`, ids)
			bt.Queue("commit")
		}
		if err := conn.SendBatch(ctx, bt).Close(); err != nil {
			return 0, "", err
		}
	}
	began := time.Now()
	p, err = spawn(dir, cfg)
	if err != nil {
		return 0, "", err
	}
	defer p.signal(syscall.SIGTERM)
	if err := settled(ctx, s.S, s.D, "lanes", p, 10*time.Minute); err != nil {
		return 0, "", err
	}
	secs := time.Since(began).Seconds()
	d, err := harness.Digest(ctx, s.D, "grants")
	if err != nil {
		return 0, "", err
	}
	if err := same(ctx, s.S, s.D); err != nil {
		return 0, "", err
	}
	return secs, d, nil
}

// copyRace copies a table with the engine and with pg_dump piped to psql, into
// fresh databases on the same two servers.
func copyRace(ctx context.Context, n int) (map[string]any, error) {
	s, stop, err := start(ctx, []string{"max_wal_size=512MB"}, []string{"max_wal_size=512MB"})
	if err != nil {
		return nil, err
	}
	defer stop()
	if _, err := s.S.Exec(ctx, load.Schema); err != nil {
		return nil, err
	}
	if _, err := s.S.Exec(ctx, fmt.Sprintf(`insert into grants select i, 'user-' || i %% 5000, 'dash-' || i %% 400, 'active', null, null from generate_series(1, %d) i`, n)); err != nil {
		return nil, err
	}
	if _, err := s.S.Exec(ctx, `vacuum analyze grants`); err != nil {
		return nil, err
	}
	if _, err := s.S.Exec(ctx, `checkpoint`); err != nil {
		return nil, err
	}
	res := map[string]any{"rows": n}
	dir, err := os.MkdirTemp("", "copy")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	srcIP, err := s.src.C.ContainerIP(ctx)
	if err != nil {
		return res, err
	}
	dump := func() (float64, error) {
		if err := s.dst.Exec(ctx, "postgres", `create database dumped`); err != nil {
			return 0, err
		}
		defer s.dst.Exec(ctx, "postgres", `drop database dumped with (force)`)
		began := time.Now()
		out, err := sh(ctx, "docker", "exec", "-e", "PGPASSWORD=x", s.dst.C.GetContainerID(), "bash", "-c",
			"set -o pipefail; pg_dump -h "+srcIP+" -U postgres -t grants postgres | psql -q -U postgres -v ON_ERROR_STOP=1 dumped >/dev/null")
		if err != nil {
			return 0, fmt.Errorf("%w: %s", err, out)
		}
		return time.Since(began).Seconds(), nil
	}
	ours := func(i int) (float64, error) {
		db := fmt.Sprintf("ours%d", i)
		if err := s.dst.Exec(ctx, "postgres", "create database "+db); err != nil {
			return 0, err
		}
		defer s.dst.Exec(ctx, "postgres", "drop database "+db+" with (force)")
		cfg, err := writeConfig(dir, db, s.src.DSN("postgres"), s.dst.DSN(db), "")
		if err != nil {
			return 0, err
		}
		b, _ := os.ReadFile(cfg)
		c := strings.NewReplacer("chunkRows: 4000", "chunkRows: 100000", "copiers: 3", "copiers: 4").Replace(string(b))
		if err := os.WriteFile(cfg, []byte(c), 0o600); err != nil {
			return 0, err
		}
		D, err := pgxpool.New(ctx, s.dst.DSN(db))
		if err != nil {
			return 0, err
		}
		defer D.Close()
		began := time.Now()
		p, err := spawn(dir, cfg)
		if err != nil {
			return 0, err
		}
		defer func() { p.signal(syscall.SIGTERM); <-p.done }()
		for {
			var phase string
			_ = D.QueryRow(ctx, `select phase from grantline.progress`).Scan(&phase)
			if phase == "ready" {
				break
			}
			select {
			case <-p.done:
				return 0, fmt.Errorf("worker ended: %s", p.tail())
			case <-time.After(20 * time.Millisecond):
			}
		}
		secs := time.Since(began).Seconds()
		a, err := harness.Digest(ctx, s.S, "grants")
		if err != nil {
			return 0, err
		}
		if b, err := harness.Digest(ctx, D, "grants"); err != nil || a != b {
			return 0, fmt.Errorf("copies differ: %s and %s (%v)", a, b, err)
		}
		return secs, nil
	}
	var mine, theirs []float64
	for i := range 3 {
		a, err := ours(i)
		if err != nil {
			return res, err
		}
		b, err := dump()
		if err != nil {
			return res, err
		}
		mine, theirs = append(mine, a), append(theirs, b)
		fmt.Fprintf(os.Stderr, "run %d: engine %.1f s, pg_dump | psql %.1f s\n", i, a, b)
	}
	sort.Float64s(mine)
	sort.Float64s(theirs)
	res["engine_s"], res["pg_dump_psql_s"] = mine, theirs
	res["times_faster_by_median"] = theirs[1] / mine[1]
	res["engine_rows_per_s"] = int(float64(n) / mine[1])
	return res, nil
}

// bigtx sends one very large transaction through a worker in a 512 MiB
// container and reads the container's peak memory.
func bigtx(ctx context.Context, n int) (map[string]any, error) {
	// Only the worker's memory is measured, so with GRANTLINE_BIGTX_TMPFS the
	// target may keep its data in memory on a machine that is short of disk.
	_ = os.Unsetenv("GRANTLINE_TMPFS")
	s, stop, err := start(ctx, []string{"max_wal_size=256MB", "min_wal_size=80MB"}, nil, os.Getenv("GRANTLINE_BIGTX_TMPFS"))
	if err != nil {
		return nil, err
	}
	defer stop()
	if _, err := s.S.Exec(ctx, `create table grants (id bigint primary key, principal text not null, state text not null);
		create table members (team text primary key); create publication grants_pub for table grants, members`); err != nil {
		return nil, err
	}
	dir, err := os.MkdirTemp("", "bigtx")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)
	cfg, err := writeConfig(dir, "big", s.src.DSN("postgres"), s.dst.DSN("postgres"), ", journal: false")
	if err != nil {
		return nil, err
	}
	b, _ := os.ReadFile(cfg)
	if err := os.WriteFile(cfg, []byte(strings.ReplaceAll(string(b), "localhost", "127.0.0.1")), 0o644); err != nil {
		return nil, err
	}
	name := "grantline-big-" + freePort()
	pwd, _ := os.Getwd()
	if _, err := sh(ctx, "docker", "run", "-d", "--name", name, "--network", "host", "--memory", "512m", "--memory-swap", "512m", "-e", "GOMEMLIMIT=400MiB",
		"-v", pwd+"/bin:/b:ro", "-v", dir+":"+dir, "alpine:3.23.3", "/b/grantline", "run", "-config", cfg); err != nil {
		return nil, err
	}
	defer sh(context.Background(), "docker", "rm", "-f", name)
	if err := settled(ctx, s.S, s.D, "big", nil, 60*time.Second); err != nil {
		out, _ := sh(ctx, "docker", "logs", name)
		return nil, fmt.Errorf("%w\n%s", err, out)
	}
	began := time.Now()
	if _, err := s.S.Exec(ctx, fmt.Sprintf(`insert into grants select i, 'user-' || i %% 5000, 'active' from generate_series(1, %d) i`, n)); err != nil {
		return nil, err
	}
	written := time.Since(began).Seconds()
	res := map[string]any{"rows": n, "limit_mib": 512, "source_s": written}
	if err := settled(ctx, s.S, s.D, "big", nil, 20*time.Minute); err != nil {
		out, _ := exec.Command("docker", "logs", "--tail", "20", name).CombinedOutput()
		return res, fmt.Errorf("%w\n%s", err, out)
	}
	res["applied_s"] = time.Since(began).Seconds()
	id, _ := sh(ctx, "docker", "inspect", "-f", "{{.State.Pid}} {{.State.OOMKilled}} {{.RestartCount}}", name)
	f := strings.Fields(id)
	res["killed_for_memory"] = f[1] == "true"
	st, err := os.ReadFile("/proc/" + f[0] + "/status")
	if err != nil {
		return res, err
	}
	for line := range strings.Lines(string(st)) {
		if v, ok := strings.CutPrefix(line, "VmHWM:"); ok {
			kb, _ := strconv.Atoi(strings.Fields(v)[0])
			res["peak_resident_mib"] = kb / 1024
		}
	}
	a, err := harness.Digest(ctx, s.S, "grants")
	if err != nil {
		return res, err
	}
	if b, err := harness.Digest(ctx, s.D, "grants"); err != nil || a != b {
		return res, fmt.Errorf("copies differ: %s and %s (%v)", a, b, err)
	}
	return res, nil
}
