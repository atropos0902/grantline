// Package harness starts the databases the tests and campaigns run against.
package harness

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// Images the project is built and tested against.
const (
	PostgresImage = "postgres:18.3"
	KafkaImage    = "apache/kafka-native:4.2.0"
	NATSImage     = "nats:2.12.8-alpine"
	ProxyImage    = "ghcr.io/shopify/toxiproxy:2.12.0"
)

// Postgres is a running server.
type Postgres struct {
	C    testcontainers.Container
	Host string
	Port string
}

// StartPostgres runs a server with the given settings, which are "name=value".
//
// GRANTLINE_TMPFS keeps the data in memory, in that many megabytes, for
// machines that are short of disk. Measurements are never taken that way.
func StartPostgres(ctx context.Context, settings ...string) (*Postgres, error) {
	var tmpfs map[string]string
	if mb := os.Getenv("GRANTLINE_TMPFS"); mb != "" {
		tmpfs = map[string]string{"/var/lib/postgresql": "size=" + mb + "m"}
		settings = append([]string{"max_wal_size=128MB", "min_wal_size=64MB"}, settings...)
	}
	cmd := []string{"-c", "wal_level=logical", "-c", "max_replication_slots=60", "-c", "max_wal_senders=60",
		"-c", "max_connections=200", "-c", "shared_buffers=128MB"}
	for _, s := range settings {
		cmd = append(cmd, "-c", s)
	}
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: PostgresImage, Cmd: append([]string{"postgres"}, cmd...),
			Env:          map[string]string{"POSTGRES_PASSWORD": "x", "POSTGRES_HOST_AUTH_METHOD": "trust"},
			ExposedPorts: []string{"5432/tcp"}, Tmpfs: tmpfs,
			WaitingFor: wait.ForAll(wait.ForListeningPort("5432/tcp"),
				wait.ForLog("database system is ready to accept connections").WithOccurrence(2)).WithDeadline(90 * time.Second),
		},
		Started: true,
	})
	if err != nil {
		return nil, err
	}
	host, err := c.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return nil, err
	}
	return &Postgres{C: c, Host: host, Port: port.Port()}, nil
}

// DSN for a database on the server.
func (p *Postgres) DSN(db string) string {
	return fmt.Sprintf("postgres://postgres:x@%s:%s/%s?sslmode=disable", p.Host, p.Port, db)
}

// Exec runs SQL against a database, outside any transaction.
func (p *Postgres) Exec(ctx context.Context, db, sql string) error {
	c, err := pgx.Connect(ctx, p.DSN(db))
	if err != nil {
		return err
	}
	defer c.Close(ctx)
	_, err = c.Exec(ctx, sql)
	return err
}

// Pair is an empty source database and an empty target database on two servers.
type Pair struct {
	Name             string
	Src, Dst         *Postgres
	Source, Target   *pgxpool.Pool
	SourceDSN, TgtDS string
}

// NewPair makes the databases and drops them when the test ends.
func NewPair(t testing.TB, src, dst *Postgres) *Pair {
	t.Helper()
	ctx := context.Background()
	name := strings.ToLower(strings.NewReplacer("/", "_", "-", "_", " ", "_").Replace(t.Name()))
	if len(name) > 40 {
		name = name[:40]
	}
	name = fmt.Sprintf("%s_%d", name, time.Now().UnixNano()%1e6)
	p := &Pair{Name: name, Src: src, Dst: dst, SourceDSN: src.DSN(name), TgtDS: dst.DSN(name)}
	for _, s := range []*Postgres{src, dst} {
		if err := s.Exec(ctx, "postgres", "create database "+name); err != nil {
			t.Fatal(err)
		}
	}
	var err error
	if p.Source, err = pgxpool.New(ctx, p.SourceDSN); err != nil {
		t.Fatal(err)
	}
	if p.Target, err = pgxpool.New(ctx, p.TgtDS); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		p.Source.Close()
		p.Target.Close()
		_ = src.Exec(ctx, "postgres", fmt.Sprintf(`select pg_terminate_backend(active_pid) from pg_replication_slots where database = '%s' and active_pid is not null`, name))
		time.Sleep(100 * time.Millisecond)
		_ = src.Exec(ctx, "postgres", fmt.Sprintf(`select pg_drop_replication_slot(slot_name) from pg_replication_slots where database = '%s'`, name))
		for _, s := range []*Postgres{src, dst} {
			_ = s.Exec(ctx, "postgres", "drop database if exists "+name+" with (force)")
		}
	})
	return p
}

// Digest sums a table's rows so two copies can be compared.
func Digest(ctx context.Context, p *pgxpool.Pool, table string) (string, error) {
	var d string
	err := p.QueryRow(ctx, fmt.Sprintf(`select count(*) || ':' || coalesce(sum(hashtextextended(t::text, 0)::numeric), 0) from %s t`, table)).Scan(&d)
	return d, err
}

// Diff lists rows that aren't the same on both sides, at most ten of them.
func Diff(ctx context.Context, p *Pair, table string) ([]string, error) {
	read := func(pool *pgxpool.Pool) (map[string]bool, error) {
		rows, err := pool.Query(ctx, fmt.Sprintf("select t::text from %s t", table))
		if err != nil {
			return nil, err
		}
		m := map[string]bool{}
		var s string
		_, err = pgx.ForEachRow(rows, []any{&s}, func() error { m[s] = true; return nil })
		return m, err
	}
	a, err := read(p.Source)
	if err != nil {
		return nil, err
	}
	b, err := read(p.Target)
	if err != nil {
		return nil, err
	}
	var out []string
	for r := range a {
		if !b[r] && len(out) < 10 {
			out = append(out, "missing "+r)
		}
	}
	for r := range b {
		if !a[r] && len(out) < 10 {
			out = append(out, "extra "+r)
		}
	}
	return out, nil
}

// StartKafka runs a one-broker cluster and returns its address.
func StartKafka(ctx context.Context) (testcontainers.Container, string, error) {
	// The broker tells clients its address, so it has to know it: host network,
	// on ports that are free now.
	free := func() string {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return ""
		}
		defer l.Close()
		return l.Addr().String()
	}
	addr, ctl := free(), free()
	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: KafkaImage,
			Env: map[string]string{
				"KAFKA_NODE_ID": "1", "KAFKA_PROCESS_ROLES": "broker,controller",
				"KAFKA_LISTENERS":                                "PLAINTEXT://" + addr + ",CONTROLLER://" + ctl,
				"KAFKA_ADVERTISED_LISTENERS":                     "PLAINTEXT://" + addr,
				"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP":           "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
				"KAFKA_CONTROLLER_LISTENER_NAMES":                "CONTROLLER",
				"KAFKA_CONTROLLER_QUORUM_VOTERS":                 "1@" + ctl,
				"KAFKA_INTER_BROKER_LISTENER_NAME":               "PLAINTEXT",
				"KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR":         "1",
				"KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
				"KAFKA_TRANSACTION_STATE_LOG_MIN_ISR":            "1",
				"KAFKA_TRANSACTION_STATE_LOG_NUM_PARTITIONS":     "4",
				"KAFKA_AUTO_CREATE_TOPICS_ENABLE":                "false",
			},
			HostConfigModifier: func(h *container.HostConfig) { h.NetworkMode = "host" },
			WaitingFor:         wait.ForLog("Kafka Server started").WithStartupTimeout(60 * time.Second),
		},
		Started: true,
	})
	return c, addr, err
}
