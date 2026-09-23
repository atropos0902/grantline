package integration

import (
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"grantline/internal/harness"
	"grantline/internal/target"
	"grantline/internal/worker"
)

// A worker whose takeover succeeded at the target and failed at the source can
// still go on when the source is back. Another worker's takeover stops it.
func TestWorkerGoesOnAfterHalfATakeover(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	c := conf(t, p)
	c.Source.DSN = strings.Replace(c.Source.DSN, "postgres:x@", "late:x@", 1) // refused until the role exists
	w := &worker.Worker{Cfg: c, Log: slog.New(slog.DiscardHandler)}
	done := make(chan error, 1)
	go func() { done <- w.Run(t.Context()) }()
	for end := time.Now().Add(10 * time.Second); w.Failure() == nil; time.Sleep(5 * time.Millisecond) {
		if time.Now().After(end) {
			t.Fatal("it should have failed")
		}
	}
	time.Sleep(500 * time.Millisecond) // a few tries
	srcExec(t, p, `create role late superuser login`)
	t.Cleanup(func() { srcExec(t, p, `alter role late nologin`) })
	for end := time.Now().Add(15 * time.Second); w.Engine() == nil || w.Engine().Status().Phase != target.Ready; time.Sleep(5 * time.Millisecond) {
		select {
		case err := <-done:
			t.Fatalf("it gave up: %v", err)
		default:
		}
		if time.Now().After(end) {
			t.Fatal(w.Failure())
		}
	}
	other := &worker.Worker{Cfg: c, Log: slog.New(slog.DiscardHandler)}
	go func() { _ = other.Run(t.Context()) }()
	select {
	case err := <-done:
		if !errors.Is(err, target.ErrFenced) {
			t.Errorf("ended with %v, want fenced", err)
		}
	case <-time.After(20 * time.Second):
		t.Error("the first worker is still trying")
	}
}
