// Package api is the Replication resource.
package api

import (
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// GroupVersion of the resource.
var GroupVersion = schema.GroupVersion{Group: "grantline.dev", Version: "v1alpha1"}

// AddToScheme registers the types.
func AddToScheme(s *runtime.Scheme) error {
	s.AddKnownTypes(GroupVersion, &Replication{}, &ReplicationList{})
	metav1.AddToGroupVersion(s, GroupVersion)
	return nil
}

// SecretKey names one value in a Secret.
type SecretKey struct {
	Name string `json:"name"`
	Key  string `json:"key"`
}

// Source is the database that owns the data.
type Source struct {
	DSN         SecretKey `json:"dsn"`
	Publication string    `json:"publication"`
}

// Target is where changes go.
type Target struct {
	Kind       string     `json:"kind"`
	DSN        *SecretKey `json:"dsn,omitempty"`
	Brokers    []string   `json:"brokers,omitempty"`
	Prefix     string     `json:"prefix,omitempty"`
	Replicas   int16      `json:"replicas,omitempty"`
	Partitions int32      `json:"partitions,omitempty"`
	Lanes      int        `json:"lanes,omitempty"`
}

// ReplicationSpec is what the owner asks for.
type ReplicationSpec struct {
	Source    Source                      `json:"source"`
	Target    Target                      `json:"target"`
	Paused    bool                        `json:"paused,omitempty"`
	ChunkRows int                         `json:"chunkRows,omitempty"`
	Copiers   int                         `json:"copiers,omitempty"`
	BudgetMiB int64                       `json:"budgetMiB,omitempty"`
	Workers   *int32                      `json:"workers,omitempty"` // one runs, the rest wait for its lease
	Resources corev1.ResourceRequirements `json:"resources,omitempty"`
}

// ReplicationStatus is what the worker that holds the lease says.
type ReplicationStatus struct {
	Phase      string             `json:"phase,omitempty"`
	Worker     string             `json:"worker,omitempty"`
	Epoch      int64              `json:"epoch,omitempty"`
	AppliedLSN string             `json:"appliedLSN,omitempty"`
	Lag        string             `json:"lag,omitempty"`
	Waiting    string             `json:"waitingStatement,omitempty"`
	Generation int64              `json:"observedGeneration,omitempty"`
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// Replication is one source copied to one target.
type Replication struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`
	Spec              ReplicationSpec   `json:"spec"`
	Status            ReplicationStatus `json:"status,omitempty"`
}

// ReplicationList is a list of them.
type ReplicationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Replication `json:"items"`
}

// DeepCopyObject implements runtime.Object. The types are plain data, so a trip
// through JSON is a full copy.
func (r *Replication) DeepCopyObject() runtime.Object { return deep(r, &Replication{}) }

// DeepCopyObject implements runtime.Object.
func (l *ReplicationList) DeepCopyObject() runtime.Object { return deep(l, &ReplicationList{}) }

func deep[T any](in, out *T) *T {
	b, err := json.Marshal(in)
	if err == nil {
		err = json.Unmarshal(b, out)
	}
	if err != nil {
		panic(err) // nothing in the types can fail to marshal
	}
	return out
}
