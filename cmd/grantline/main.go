// Command grantline replicates Postgres tables to Postgres and Kafka.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime/trace"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"grantline/gen/grantline/v1/grantlinev1connect"
	"grantline/internal/config"
	"grantline/internal/control"
	"grantline/internal/events"
	"grantline/internal/gateway"
	"grantline/internal/lease"
	"grantline/internal/obs"
	"grantline/internal/preflight"
	"grantline/internal/source"
	"grantline/internal/target"
	"grantline/internal/target/pgtarget"
	"grantline/internal/worker"
)

const usage = `grantline <command> [flags]

  run        run a replication
  preflight  look for what would go wrong, before installing anything
  explain    say what happened to one key:  explain -config c.yaml grants 8814
  recopy     copy every table again, with a new slot
  serve      the console and a gateway to the workers
`

// Exit codes a supervisor can act on.
const (
	exitError  = 1
	exitFenced = 3 // another worker owns the replication
	exitHelp   = 4 // a person has to decide: lost slot or diverged target
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level()}))
	cmds := map[string]func(context.Context, *slog.Logger, []string) error{
		"run": run, "preflight": runPreflight, "explain": explain, "recopy": recopy, "serve": serve,
	}
	cmd := cmds[os.Args[1]]
	if cmd == nil {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	err := cmd(ctx, log, os.Args[2:])
	switch {
	case err == nil, errors.Is(err, context.Canceled):
	case errors.Is(err, target.ErrFenced):
		log.Error("fenced", "error", err)
		os.Exit(exitFenced)
	case worker.Terminal(err):
		log.Error("needs a decision", "error", err)
		os.Exit(exitHelp)
	default:
		fmt.Fprintln(os.Stderr, "grantline:", err)
		os.Exit(exitError)
	}
}

func level() slog.Level {
	var l slog.Level
	if l.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))) != nil {
		return slog.LevelInfo
	}
	return l
}

func load(name string, args []string, more func(*flag.FlagSet)) (*config.Config, *flag.FlagSet, error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	path := fs.String("config", "grantline.yaml", "replication settings")
	if more != nil {
		more(fs)
	}
	_ = fs.Parse(args)
	c, err := config.Load(*path)
	return c, fs, err
}

func run(ctx context.Context, log *slog.Logger, args []string) error {
	c, _, err := load("run", args, nil)
	if err != nil {
		return err
	}
	log = log.With("replication", c.Name)
	stopTraces, err := obs.Traces(ctx, c.OTLP, c.Name)
	if err != nil {
		return err
	}
	defer stopTraces()
	w := &worker.Worker{Cfg: c, Log: log}
	if c.NATS != "" {
		if w.Events, err = events.Connect(ctx, c.NATS, log); err != nil {
			return err
		}
		defer w.Events.Close()
	}
	s := &control.Server{W: w}
	if s.Source, err = source.Open(ctx, c.Source.DSN, slot(c), c.Source.Publication); err != nil {
		return err
	}
	defer s.Source.Close()
	if c.Target.Kind == "postgres" {
		if s.Target, err = pgxpool.New(ctx, c.Target.DSN); err != nil {
			return err
		}
		defer s.Target.Close()
	}
	if c.Listen != "" {
		stopHTTP, err := listen(c.Listen, s, log)
		if err != nil {
			return err
		}
		defer stopHTTP()
	}
	// When a person has to decide, the worker stays up to say so. It keeps its
	// lease: another worker would only find the same thing.
	work := func(ctx context.Context) error {
		err := w.Run(ctx)
		if worker.Terminal(err) && !errors.Is(err, target.ErrFenced) && c.Listen != "" {
			<-ctx.Done()
		}
		return err
	}
	if c.Lease.Name == "" {
		return work(ctx)
	}
	client, err := lease.Client()
	if err != nil {
		return err
	}
	return lease.Run(ctx, client, c.Lease.Namespace, c.Lease.Name, lease.Identity(), log, work)
}

func slot(c *config.Config) string {
	if c.Source.Slot != "" {
		return c.Source.Slot
	}
	return "grantline_" + c.Name
}

// listen serves the API, metrics, health, profiles and a trace of the last
// seconds. Connect answers gRPC on the same port, so HTTP/2 without TLS is on.
func listen(addr string, s *control.Server, log *slog.Logger) (func(), error) {
	mux := http.NewServeMux()
	mux.Handle(grantlinev1connect.NewControlServiceHandler(s))
	reg := prometheus.NewRegistry()
	reg.MustRegister(obs.Collector{Status: s.Status}, collectors.NewGoCollector(), collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if st := s.Status(r.Context()); st.Failure != "" {
			http.Error(w, st.Failure, http.StatusServiceUnavailable)
		}
	})
	// The pod that holds the lease is the only one the Service should send to.
	mux.HandleFunc("/leading", func(w http.ResponseWriter, _ *http.Request) {
		if s.W.Engine() == nil && s.W.Failure() == nil {
			http.Error(w, "waiting for the lease", http.StatusServiceUnavailable)
		}
	})
	mux.Handle("/debug/pprof/", http.DefaultServeMux)
	fr := trace.NewFlightRecorder(trace.FlightRecorderConfig{MinAge: 10 * time.Second, MaxBytes: 16 << 20})
	if err := fr.Start(); err != nil {
		return nil, err
	}
	mux.HandleFunc("/debug/flight", func(w http.ResponseWriter, _ *http.Request) { _, _ = fr.WriteTo(w) })
	var p http.Protocols
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	srv := &http.Server{Addr: addr, Handler: mux, Protocols: &p, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
			log.Error("http", "error", err)
		}
	}()
	return func() { fr.Stop(); _ = srv.Close() }, nil
}

func runPreflight(ctx context.Context, _ *slog.Logger, args []string) error {
	asJSON := false
	c, _, err := load("preflight", args, func(fs *flag.FlagSet) { fs.BoolVar(&asJSON, "json", false, "print JSON") })
	if err != nil {
		return err
	}
	src, err := source.Open(ctx, c.Source.DSN, slot(c), c.Source.Publication)
	if err != nil {
		return err
	}
	defer src.Close()
	var tgt *pgxpool.Pool
	if c.Target.Kind == "postgres" {
		if tgt, err = pgxpool.New(ctx, c.Target.DSN); err != nil {
			return err
		}
		defer tgt.Close()
	}
	found, err := preflight.Run(ctx, src, tgt)
	if err != nil {
		return err
	}
	if asJSON {
		if err := json.NewEncoder(os.Stdout).Encode(found); err != nil {
			return err
		}
	} else {
		for _, f := range found {
			fmt.Printf("%-5s %s\n", f.Level, f.What)
			if f.Fix != "" {
				fmt.Printf("      fix: %s\n", f.Fix)
			}
		}
	}
	if preflight.Worst(found) == preflight.Stop {
		return errors.New("not ready to install")
	}
	return nil
}

func explain(ctx context.Context, _ *slog.Logger, args []string) error {
	c, fs, err := load("explain", args, nil)
	if err != nil {
		return err
	}
	if fs.NArg() < 2 {
		return errors.New("usage: explain -config c.yaml <table> <key> [<key part> ...]")
	}
	src, err := source.Open(ctx, c.Source.DSN, slot(c), c.Source.Publication)
	if err != nil {
		return err
	}
	defer src.Close()
	tgt, err := pgxpool.New(ctx, c.Target.DSN)
	if err != nil {
		return err
	}
	defer tgt.Close()
	x, err := control.Explain(ctx, src, tgt, c.Name, fs.Arg(0), fs.Args()[1:])
	if err != nil {
		return err
	}
	for _, s := range x.Steps {
		at := "                       "
		if !s.At.IsZero() {
			at = s.At.UTC().Format("2006-01-02 15:04:05.000")
		}
		switch s.What {
		case "chunk":
			fmt.Printf("%s  chunk     %d, epoch %d\n", at, s.Chunk, s.Epoch)
		case "refused":
			fmt.Printf("%s  refused   the row from chunk %d\n", at, s.Chunk)
		default:
			fmt.Printf("%s  %-8s  transaction %d, committed at %s, epoch %d\n", at, s.What, s.XID, s.LSN, s.Epoch)
		}
		if s.Reason != "" {
			fmt.Printf("%25s%s\n", "", s.Reason)
		}
	}
	if len(x.Steps) == 0 {
		fmt.Println("nothing recorded for this key")
	}
	show := func(side, row string) {
		if row == "" {
			row = "no row"
		}
		fmt.Printf("%s: %s\n", side, row)
	}
	show("source now", x.SourceRow)
	show("target now", x.TargetRow)
	return nil
}

func recopy(ctx context.Context, _ *slog.Logger, args []string) error {
	yes := false
	c, _, err := load("recopy", args, func(fs *flag.FlagSet) { fs.BoolVar(&yes, "yes", false, "do it") })
	if err != nil {
		return err
	}
	if c.Target.Kind != "postgres" {
		return errors.New("only a postgres target holds a copy")
	}
	if !yes {
		return errors.New("this drops the slot and copies every table again over the rows that are there; add -yes")
	}
	t, err := pgtarget.New(ctx, pgtarget.Config{DSN: c.Target.DSN, Replication: c.Name, Lanes: 1})
	if err != nil {
		return err
	}
	defer t.Close()
	return t.Recopy(ctx)
}

func serve(ctx context.Context, log *slog.Logger, args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	addr := fs.String("listen", ":8080", "address")
	static := fs.String("console", "", "directory with the built console")
	nats := fs.String("nats", "", "NATS URL for the event feed")
	workers := map[string]string{}
	fs.Func("worker", "name=url, once per replication", func(v string) error {
		name, u, ok := strings.Cut(v, "=")
		if !ok {
			return errors.New("want name=url")
		}
		workers[name] = u
		return nil
	})
	_ = fs.Parse(args)
	h, err := gateway.Handler(ctx, gateway.Config{Workers: workers, Static: *static, NATS: *nats, Log: log})
	if err != nil {
		return err
	}
	srv := &http.Server{Addr: *addr, Handler: h, ReadHeaderTimeout: 5 * time.Second}
	context.AfterFunc(ctx, func() { _ = srv.Close() })
	log.Info("serving", "addr", *addr, "replications", len(workers))
	if err := srv.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
