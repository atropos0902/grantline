// Package lease picks the worker that runs a replication, with a Kubernetes
// Lease. The lease is only there to keep workers from getting in each other's
// way. A worker can lose it without noticing, so nothing in the engine trusts
// it: the epoch in the target decides who may commit.
package lease

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// Run waits for the lease, runs fn while holding it, and returns when fn does or
// the lease is lost.
func Run(ctx context.Context, client kubernetes.Interface, namespace, name, identity string, log *slog.Logger, fn func(context.Context) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := fmt.Errorf("lease %s/%s lost", namespace, name)
	done := make(chan struct{})
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Namespace: namespace, Name: name},
			Client:     client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: identity},
		},
		ReleaseOnCancel: true,
		LeaseDuration:   10 * time.Second,
		RenewDeadline:   7 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				defer close(done)
				log.Info("lease held", "lease", name)
				if err := fn(ctx); ctx.Err() == nil || err == nil {
					result = err
				}
				cancel()
			},
			OnStoppedLeading: func() { log.Info("lease over", "lease", name) },
		},
	})
	select {
	case <-done:
	case <-time.After(15 * time.Second):
	}
	return result
}

// Client talks to the cluster the pod runs in.
func Client() (kubernetes.Interface, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(cfg)
}

// Identity is the pod's name.
func Identity() string {
	h, _ := os.Hostname()
	return h
}
