// Package controller turns a Replication into workers and reports what they say.
package controller

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"connectrpc.com/connect"
	appsv1 "k8s.io/api/apps/v1"
	coordv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/yaml"

	v1 "grantline/gen/grantline/v1"
	"grantline/gen/grantline/v1/grantlinev1connect"
	"grantline/internal/config"
	"grantline/operator/api"
)

// Port the workers listen on.
const Port = 7400

// Reconciler watches Replications.
type Reconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Image  string
	NATS   string
	// Ask gets a worker's status. Tests replace it.
	Ask func(ctx context.Context, addr string) (*v1.Status, error)
}

// Ask calls a worker's API.
func Ask(ctx context.Context, addr string) (*v1.Status, error) {
	c := grantlinev1connect.NewControlServiceClient(&http.Client{Timeout: 3 * time.Second}, "http://"+addr)
	r, err := c.GetStatus(ctx, connect.NewRequest(&v1.GetStatusRequest{}))
	if err != nil {
		return nil, err
	}
	return r.Msg.Status, nil
}

// SetupWithManager registers the reconciler.
func (r *Reconciler) SetupWithManager(m ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(m).For(&api.Replication{}).
		Owns(&appsv1.Deployment{}).Owns(&corev1.ConfigMap{}).Owns(&corev1.Service{}).Complete(r)
}

// Reconcile makes the workers match the spec and copies their status back.
func (r *Reconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var rep api.Replication
	if err := r.Get(ctx, req.NamespacedName, &rep); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if rep.DeletionTimestamp != nil {
		return ctrl.Result{}, nil // owned objects go with it; the slot stays, see docs/runbook.md
	}
	cm, err := r.configMap(&rep)
	if err != nil {
		return ctrl.Result{}, err
	}
	for _, o := range []struct {
		obj    client.Object
		mutate func()
	}{
		{cm.obj, cm.set},
		r.serviceAccount(&rep), r.role(&rep), r.roleBinding(&rep), r.service(&rep), r.deployment(&rep, cm.hash),
	} {
		if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, o.obj, func() error {
			o.mutate()
			return controllerutil.SetControllerReference(&rep, o.obj, r.Scheme)
		}); err != nil {
			return ctrl.Result{}, fmt.Errorf("%T: %w", o.obj, err)
		}
	}
	return ctrl.Result{RequeueAfter: 5 * time.Second}, r.report(ctx, &rep)
}

type owned = struct {
	obj    client.Object
	mutate func()
}

func name(rep *api.Replication) string { return "grantline-" + rep.Name }

func om(rep *api.Replication) metav1.ObjectMeta {
	return metav1.ObjectMeta{Name: name(rep), Namespace: rep.Namespace}
}

func labels(rep *api.Replication) map[string]string {
	return map[string]string{"app.kubernetes.io/name": "grantline", "grantline.dev/replication": rep.Name}
}

type configObject struct {
	obj  *corev1.ConfigMap
	set  func()
	hash string
}

func (r *Reconciler) configMap(rep *api.Replication) (*configObject, error) {
	var c config.Config
	c.Name = rep.Name
	c.Source.DSN, c.Source.Publication = "${SOURCE_DSN}", rep.Spec.Source.Publication
	t := rep.Spec.Target
	c.Target.Kind, c.Target.Lanes, c.Target.Brokers = t.Kind, t.Lanes, t.Brokers
	c.Target.Prefix, c.Target.Replicas, c.Target.Parts = t.Prefix, t.Replicas, t.Partitions
	if t.DSN != nil {
		c.Target.DSN = "${TARGET_DSN}"
	}
	c.ChunkRows, c.Copiers, c.BudgetMiB = rep.Spec.ChunkRows, rep.Spec.Copiers, rep.Spec.BudgetMiB
	c.SpillDir, c.Listen, c.NATS, c.Paused = "/spill", fmt.Sprintf(":%d", Port), r.NATS, rep.Spec.Paused
	c.Lease.Name, c.Lease.Namespace = name(rep), rep.Namespace
	b, err := yaml.Marshal(c)
	if err != nil {
		return nil, err
	}
	cm := &corev1.ConfigMap{ObjectMeta: om(rep)}
	return &configObject{obj: cm, hash: fmt.Sprintf("%x", fnv(b)), set: func() {
		cm.Labels = labels(rep)
		cm.Data = map[string]string{"grantline.yaml": string(b)}
	}}, nil
}

func fnv(b []byte) uint64 {
	h := uint64(14695981039346656037)
	for _, c := range b {
		h = (h ^ uint64(c)) * 1099511628211
	}
	return h
}

func (r *Reconciler) serviceAccount(rep *api.Replication) owned {
	o := &corev1.ServiceAccount{ObjectMeta: om(rep)}
	return owned{o, func() { o.Labels = labels(rep) }}
}

func (r *Reconciler) role(rep *api.Replication) owned {
	o := &rbacv1.Role{ObjectMeta: om(rep)}
	return owned{o, func() {
		o.Labels = labels(rep)
		o.Rules = []rbacv1.PolicyRule{
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, Verbs: []string{"create"}},
			{APIGroups: []string{"coordination.k8s.io"}, Resources: []string{"leases"}, ResourceNames: []string{name(rep)}, Verbs: []string{"get", "update", "patch"}},
		}
	}}
}

func (r *Reconciler) roleBinding(rep *api.Replication) owned {
	o := &rbacv1.RoleBinding{ObjectMeta: om(rep)}
	return owned{o, func() {
		o.Labels = labels(rep)
		o.RoleRef = rbacv1.RoleRef{APIGroup: "rbac.authorization.k8s.io", Kind: "Role", Name: name(rep)}
		o.Subjects = []rbacv1.Subject{{Kind: "ServiceAccount", Name: name(rep), Namespace: rep.Namespace}}
	}}
}

func (r *Reconciler) service(rep *api.Replication) owned {
	o := &corev1.Service{ObjectMeta: om(rep)}
	return owned{o, func() {
		o.Labels = labels(rep)
		o.Spec.Selector = labels(rep)
		o.Spec.Ports = []corev1.ServicePort{{Name: "api", Port: Port, TargetPort: intstr.FromInt32(Port)}}
	}}
}

func (r *Reconciler) deployment(rep *api.Replication, hash string) owned {
	o := &appsv1.Deployment{ObjectMeta: om(rep)}
	return owned{o, func() {
		n := int32(2)
		if rep.Spec.Workers != nil {
			n = *rep.Spec.Workers
		}
		o.Labels = labels(rep)
		o.Spec.Replicas = &n
		o.Spec.Selector = &metav1.LabelSelector{MatchLabels: labels(rep)}
		// Only the worker with the lease is ready, so a rolling update would wait
		// forever for the new one.
		o.Spec.Strategy = appsv1.DeploymentStrategy{Type: appsv1.RecreateDeploymentStrategyType}
		o.Spec.Template.Labels = labels(rep)
		o.Spec.Template.Annotations = map[string]string{"grantline.dev/config": hash}
		env := []corev1.EnvVar{secretEnv("SOURCE_DSN", rep.Spec.Source.DSN)}
		if d := rep.Spec.Target.DSN; d != nil {
			env = append(env, secretEnv("TARGET_DSN", *d))
		}
		if q := rep.Spec.Resources.Limits.Memory(); q != nil && !q.IsZero() {
			env = append(env, corev1.EnvVar{Name: "GOMEMLIMIT", Value: fmt.Sprint(q.Value() * 8 / 10)})
		}
		yes, no := true, false
		user := int64(65532)
		probe := func(path string) *corev1.Probe {
			return &corev1.Probe{ProbeHandler: corev1.ProbeHandler{HTTPGet: &corev1.HTTPGetAction{Path: path, Port: intstr.FromInt32(Port)}}, PeriodSeconds: 5, TimeoutSeconds: 3}
		}
		o.Spec.Template.Spec = corev1.PodSpec{
			ServiceAccountName: name(rep),
			SecurityContext:    &corev1.PodSecurityContext{RunAsNonRoot: &yes, RunAsUser: &user, FSGroup: &user, SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}},
			Containers: []corev1.Container{{
				Name: "worker", Image: r.Image, Args: []string{"run", "-config", "/etc/grantline/grantline.yaml"},
				Env: env, Resources: rep.Spec.Resources,
				Ports:          []corev1.ContainerPort{{Name: "api", ContainerPort: Port}},
				LivenessProbe:  probe("/healthz"),
				ReadinessProbe: probe("/leading"),
				SecurityContext: &corev1.SecurityContext{AllowPrivilegeEscalation: &no, ReadOnlyRootFilesystem: &yes,
					Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}}},
				VolumeMounts: []corev1.VolumeMount{{Name: "config", MountPath: "/etc/grantline"}, {Name: "spill", MountPath: "/spill"}},
			}},
			Volumes: []corev1.Volume{
				{Name: "config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: name(rep)}}}},
				{Name: "spill", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
			},
		}
	}}
}

func secretEnv(env string, k api.SecretKey) corev1.EnvVar {
	return corev1.EnvVar{Name: env, ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: k.Name}, Key: k.Key}}}
}

// report asks the worker that holds the lease.
func (r *Reconciler) report(ctx context.Context, rep *api.Replication) error {
	before := rep.DeepCopyObject().(*api.Replication)
	st := &rep.Status
	st.Generation = rep.Generation
	reason, message := r.read(ctx, rep)
	ok := metav1.ConditionFalse
	if reason == "Ready" {
		ok = metav1.ConditionTrue
	}
	meta.SetStatusCondition(&st.Conditions, metav1.Condition{Type: "Ready", Status: ok, Reason: reason, Message: message, ObservedGeneration: rep.Generation})
	return client.IgnoreNotFound(r.Status().Patch(ctx, rep, client.MergeFrom(before)))
}

func (r *Reconciler) read(ctx context.Context, rep *api.Replication) (reason, message string) {
	st := &rep.Status
	var l coordv1.Lease
	if err := r.Get(ctx, types.NamespacedName{Namespace: rep.Namespace, Name: name(rep)}, &l); err != nil || l.Spec.HolderIdentity == nil || *l.Spec.HolderIdentity == "" {
		st.Worker = ""
		return "NoWorker", "no worker holds the lease"
	}
	st.Worker = *l.Spec.HolderIdentity
	var pod corev1.Pod
	if err := r.Get(ctx, types.NamespacedName{Namespace: rep.Namespace, Name: st.Worker}, &pod); err != nil || pod.Status.PodIP == "" {
		return "WorkerGone", "the worker that holds the lease has no pod"
	}
	s, err := r.Ask(ctx, fmt.Sprintf("%s:%d", pod.Status.PodIP, Port))
	if err != nil {
		return "WorkerSilent", err.Error()
	}
	st.Epoch, st.AppliedLSN, st.Waiting = s.Epoch, s.AppliedLsn, s.WaitingStatement
	st.Phase = map[v1.Phase]string{v1.Phase_PHASE_NEW: "new", v1.Phase_PHASE_COPYING: "copying", v1.Phase_PHASE_READY: "ready"}[s.Phase]
	if s.Lag != nil {
		// Rounded, so that the object isn't rewritten for every millisecond.
		st.Lag = s.Lag.AsDuration().Round(time.Second).String()
	}
	switch {
	case s.Failure != "":
		return "Stopped", s.Failure
	case s.WaitingStatement != "":
		return "SchemaChange", "waiting for approval of: " + s.WaitingStatement
	case s.Paused:
		return "Paused", "paused"
	case s.Phase == v1.Phase_PHASE_READY:
		return "Ready", "following the source"
	}
	return "Copying", fmt.Sprintf("%d of %d chunks landed", s.ChunksLanded, s.Chunks)
}
