// Package config reads a replication's settings.
package config

import (
	"fmt"
	"os"
	"time"

	"sigs.k8s.io/yaml"
)

// Config is one replication. Values may use ${NAME} for environment variables.
type Config struct {
	Name   string `json:"name"`
	Source struct {
		DSN         string `json:"dsn"`
		Publication string `json:"publication"`
		Slot        string `json:"slot,omitempty"`
	} `json:"source"`
	Target struct {
		Kind     string   `json:"kind"` // postgres or kafka
		DSN      string   `json:"dsn,omitempty"`
		Lanes    int      `json:"lanes,omitempty"`
		Journal  *bool    `json:"journal,omitempty"`
		KeepOld  bool     `json:"keepOldSessions,omitempty"`
		Brokers  []string `json:"brokers,omitempty"`
		Prefix   string   `json:"prefix,omitempty"`
		Replicas int16    `json:"replicas,omitempty"`
		Parts    int32    `json:"partitions,omitempty"`
	} `json:"target"`
	ChunkRows int    `json:"chunkRows,omitempty"`
	Copiers   int    `json:"copiers,omitempty"`
	GroupMax  int    `json:"groupMax,omitempty"`
	BudgetMiB int64  `json:"budgetMiB,omitempty"`
	SpillDir  string `json:"spillDir,omitempty"`
	Heartbeat string `json:"heartbeat,omitempty"`
	Listen    string `json:"listen,omitempty"`
	NATS      string `json:"nats,omitempty"`
	OTLP      string `json:"otlp,omitempty"`
	Lease     struct {
		Namespace string `json:"namespace,omitempty"`
		Name      string `json:"name,omitempty"`
	} `json:"lease,omitempty"`
	Paused bool `json:"paused,omitempty"`
}

// Load reads and checks a file.
func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.UnmarshalStrict([]byte(os.ExpandEnv(string(b))), &c); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	switch {
	case c.Name == "":
		return nil, fmt.Errorf("%s: name is missing", path)
	case c.Source.DSN == "" || c.Source.Publication == "":
		return nil, fmt.Errorf("%s: source needs dsn and publication", path)
	case c.Target.Kind == "postgres" && c.Target.DSN == "":
		return nil, fmt.Errorf("%s: a postgres target needs dsn", path)
	case c.Target.Kind == "kafka" && len(c.Target.Brokers) == 0:
		return nil, fmt.Errorf("%s: a kafka target needs brokers", path)
	case c.Target.Kind != "postgres" && c.Target.Kind != "kafka":
		return nil, fmt.Errorf("%s: target kind is %q, want postgres or kafka", path, c.Target.Kind)
	}
	if c.Target.Lanes == 0 {
		c.Target.Lanes = 4
	}
	if c.Target.Replicas == 0 {
		c.Target.Replicas = 1
	}
	if c.Target.Parts == 0 {
		c.Target.Parts = 1
	}
	if c.Heartbeat != "" {
		if _, err := time.ParseDuration(c.Heartbeat); err != nil {
			return nil, err
		}
	}
	return &c, nil
}

// Beat is the heartbeat interval.
func (c *Config) Beat() time.Duration {
	d, err := time.ParseDuration(c.Heartbeat)
	if err != nil {
		return time.Second
	}
	return d
}
