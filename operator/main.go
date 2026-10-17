// Command operator runs Replications on Kubernetes.
package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"grantline/operator/api"
	"grantline/operator/controller"
)

func main() {
	image := flag.String("worker-image", "", "image the workers run")
	nats := flag.String("nats", "", "NATS URL for events")
	flag.Parse()
	ctrl.SetLogger(zap.New())
	log := ctrl.Log.WithName("setup")
	check := func(err error, what string) {
		if err != nil {
			log.Error(err, what)
			os.Exit(1)
		}
	}
	s := runtime.NewScheme()
	check(clientgoscheme.AddToScheme(s), "scheme")
	check(api.AddToScheme(s), "scheme")
	m, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme: s, Metrics: server.Options{BindAddress: ":8080"}, HealthProbeBindAddress: ":8081",
		LeaderElection: true, LeaderElectionID: "grantline-operator",
	})
	check(err, "manager")
	check((&controller.Reconciler{Client: m.GetClient(), Scheme: s, Image: *image, NATS: *nats, Ask: controller.Ask}).SetupWithManager(m), "controller")
	check(m.AddHealthzCheck("ping", healthz.Ping), "health")
	check(m.Start(ctrl.SetupSignalHandler()), "run")
}
