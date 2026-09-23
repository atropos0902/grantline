// Package gateway serves the console and passes its calls on to the workers.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sort"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"grantline/internal/events"
)

// Config for the gateway.
type Config struct {
	Workers map[string]string // replication name to base URL
	Static  string            // directory with the built console
	NATS    string
	Log     *slog.Logger
}

// Handler builds the routes: /w/<name>/ for a worker's API, /api/replications
// for their names, /api/events for a live feed, and the console.
func Handler(ctx context.Context, cfg Config) (http.Handler, error) {
	mux := http.NewServeMux()
	names := make([]string, 0, len(cfg.Workers))
	for name, base := range cfg.Workers {
		u, err := url.Parse(base)
		if err != nil {
			return nil, err
		}
		names = append(names, name)
		mux.Handle("/w/"+name+"/", http.StripPrefix("/w/"+name, &httputil.ReverseProxy{
			Rewrite:       func(r *httputil.ProxyRequest) { r.SetURL(u) },
			FlushInterval: -1,
		}))
	}
	sort.Strings(names)
	mux.HandleFunc("GET /api/replications", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("content-type", "application/json")
		_ = json.NewEncoder(w).Encode(names)
	})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprintln(w, "ok") })
	if cfg.NATS != "" {
		nc, err := nats.Connect(cfg.NATS, nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
		if err != nil {
			return nil, err
		}
		context.AfterFunc(ctx, nc.Close)
		js, err := jetstream.New(nc)
		if err != nil {
			return nil, err
		}
		mux.HandleFunc("GET /api/events", func(w http.ResponseWriter, r *http.Request) { feed(w, r, js, cfg.Log) })
	}
	if cfg.Static != "" {
		mux.Handle("/", http.FileServer(http.Dir(cfg.Static)))
	}
	return mux, nil
}

// feed sends the last hour's events and then new ones, as server-sent events.
func feed(w http.ResponseWriter, r *http.Request, js jetstream.JetStream, log *slog.Logger) {
	f, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming isn't supported", http.StatusInternalServerError)
		return
	}
	since := time.Now().Add(-time.Hour)
	c, err := js.OrderedConsumer(r.Context(), events.StreamName, jetstream.OrderedConsumerConfig{
		DeliverPolicy: jetstream.DeliverByStartTimePolicy, OptStartTime: &since,
	})
	if err != nil {
		log.Debug("event feed", "error", err)
		http.Error(w, "events aren't available", http.StatusServiceUnavailable)
		return
	}
	it, err := c.Messages()
	if err != nil {
		http.Error(w, "events aren't available", http.StatusServiceUnavailable)
		return
	}
	defer it.Stop()
	context.AfterFunc(r.Context(), it.Stop)
	w.Header().Set("content-type", "text/event-stream")
	w.Header().Set("cache-control", "no-store")
	f.Flush()
	for {
		m, err := it.Next()
		if err != nil {
			return
		}
		if _, err := fmt.Fprintf(w, "data: %s\n\n", m.Data()); err != nil {
			return
		}
		f.Flush()
	}
}
