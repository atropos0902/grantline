package controller

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "grantline/gen/grantline/v1"
	"grantline/internal/config"
	"grantline/operator/api"
	"os"
)

func setup(t *testing.T, ask func() (*v1.Status, error), objs ...client.Object) (*Reconciler, types.NamespacedName) {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{api.AddToScheme, corev1.AddToScheme, appsv1.AddToScheme, rbacv1.AddToScheme, coordv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	rep := &api.Replication{ObjectMeta: metav1.ObjectMeta{Name: "grants", Namespace: "sec", UID: "u"}, Spec: api.ReplicationSpec{
		Source:    api.Source{DSN: api.SecretKey{Name: "db", Key: "src"}, Publication: "grants_pub"},
		Target:    api.Target{Kind: "postgres", DSN: &api.SecretKey{Name: "db", Key: "dst"}, Lanes: 4},
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("512Mi")}},
	}}
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(append(objs, rep)...).WithStatusSubresource(rep).Build()
	return &Reconciler{Client: c, Scheme: s, Image: "grantline:test", Ask: func(context.Context, string) (*v1.Status, error) { return ask() }},
		types.NamespacedName{Namespace: "sec", Name: "grants"}
}

func reconcile(t *testing.T, r *Reconciler, n types.NamespacedName) *api.Replication {
	t.Helper()
	if _, err := r.Reconcile(t.Context(), ctrl.Request{NamespacedName: n}); err != nil {
		t.Fatal(err)
	}
	var rep api.Replication
	if err := r.Get(t.Context(), n, &rep); err != nil {
		t.Fatal(err)
	}
	return &rep
}

func TestWorkersMatchTheSpec(t *testing.T) {
	r, n := setup(t, func() (*v1.Status, error) { return nil, errors.New("unused") })
	rep := reconcile(t, r, n)
	if c := rep.Status.Conditions[0]; c.Reason != "NoWorker" || c.Status != metav1.ConditionFalse {
		t.Errorf("condition %+v", c)
	}
	key := types.NamespacedName{Namespace: "sec", Name: "grantline-grants"}
	var d appsv1.Deployment
	if err := r.Get(t.Context(), key, &d); err != nil {
		t.Fatal(err)
	}
	ctr := d.Spec.Template.Spec.Containers[0]
	if *d.Spec.Replicas != 2 || len(ctr.Env) != 3 || ctr.Env[2].Name != "GOMEMLIMIT" || !*ctr.SecurityContext.ReadOnlyRootFilesystem {
		t.Errorf("deployment %+v", ctr)
	}
	// The file the worker gets is one the worker accepts.
	var cm corev1.ConfigMap
	if err := r.Get(t.Context(), key, &cm); err != nil {
		t.Fatal(err)
	}
	path := t.TempDir() + "/grantline.yaml"
	if err := os.WriteFile(path, []byte(cm.Data["grantline.yaml"]), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("SOURCE_DSN", "postgres://s")
	t.Setenv("TARGET_DSN", "postgres://t")
	c, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Lease.Name != "grantline-grants" || c.Target.DSN != "postgres://t" || c.Target.Lanes != 4 {
		t.Errorf("config %+v", c)
	}
	// No password ends up outside the Secret.
	if strings.Contains(cm.Data["grantline.yaml"], "postgres://") {
		t.Error("a connection string is in the ConfigMap")
	}
	// A changed spec restarts the workers.
	was := d.Spec.Template.Annotations["grantline.dev/config"]
	rep.Spec.Paused = true
	if err := r.Update(t.Context(), rep); err != nil {
		t.Fatal(err)
	}
	reconcile(t, r, n)
	if err := r.Get(t.Context(), key, &d); err != nil {
		t.Fatal(err)
	}
	if d.Spec.Template.Annotations["grantline.dev/config"] == was {
		t.Error("pausing didn't reach the workers")
	}
}

func TestStatusComesFromTheLeaseHolder(t *testing.T) {
	holder := "grantline-grants-abc"
	lease := &coordv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "grantline-grants", Namespace: "sec"}, Spec: coordv1.LeaseSpec{HolderIdentity: &holder}}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: holder, Namespace: "sec"}, Status: corev1.PodStatus{PodIP: "10.0.0.9"}}
	st := &v1.Status{Phase: v1.Phase_PHASE_READY, Epoch: 3, AppliedLsn: "0/1"}
	var fail error
	r, n := setup(t, func() (*v1.Status, error) { return st, fail }, lease, pod)
	for _, c := range []struct {
		set  func()
		want string
	}{
		{func() {}, "Ready"},
		{func() { st.WaitingStatement = "alter table grants ..." }, "SchemaChange"},
		{func() { st.WaitingStatement, st.Paused = "", true }, "Paused"},
		{func() { st.Failure = "replication slot lost" }, "Stopped"},
		{func() { fail = errors.New("timeout") }, "WorkerSilent"},
	} {
		c.set()
		rep := reconcile(t, r, n)
		if got := rep.Status.Conditions[0]; got.Reason != c.want || (got.Status == metav1.ConditionTrue) != (c.want == "Ready") {
			t.Errorf("want %s, got %+v", c.want, got)
		}
		if rep.Status.Worker != holder || rep.Status.Epoch != 3 {
			t.Errorf("status %+v", rep.Status)
		}
	}
	_ = time.Second
}
