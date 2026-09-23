// Command loadgen fills a source with synthetic grants and keeps changing them.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/load"
)

func main() {
	dsn := flag.String("dsn", os.Getenv("SOURCE_DSN"), "source database")
	rows := flag.Int("rows", 200000, "grants to start with")
	workers := flag.Int("workers", 2, "connections that change them")
	pause := flag.Duration("pause", 20*time.Millisecond, "between a worker's transactions")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	p, err := pgxpool.New(ctx, *dsn)
	if err == nil {
		err = p.Ping(ctx)
	}
	if err == nil {
		_, err = p.Exec(ctx, load.Schema)
	}
	if err == nil {
		err = load.Fill(ctx, p, *rows)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	m := &load.Mix{Pool: p, Rows: *rows, Workers: *workers, Pause: *pause}
	m.Start(ctx)
	fmt.Println("changing", *rows, "grants")
	<-ctx.Done()
	m.Stop()
}
