package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgxpool"

	"grantline/internal/bugs"
	"grantline/internal/load"
)

// drill leaves a revoked grant in the target and waits. Whoever takes the drill
// gets the ticket it prints, docs/runbook.md and the explain command, and has
// to say what put the row back.
func drill(ctx context.Context, _ int) (map[string]any, error) {
	if !bugs.LogPositionGuard {
		return nil, errors.New("build with -tags bug_log_position_guard: nothing goes wrong otherwise")
	}
	s, stop, err := start(ctx, []string{"fsync=off"}, []string{"fsync=off"})
	if err != nil {
		return nil, err
	}
	defer stop()
	for _, p := range []*pgxpool.Pool{s.S, s.D} {
		if _, err := p.Exec(ctx, load.Schema); err != nil {
			return nil, err
		}
	}
	if _, _, err := raceRound(ctx, s, 0); err != nil {
		return nil, err
	}
	var id int64
	rows, err := s.D.Query(ctx, `select id from grants where state = 'active' order by id`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var k int64
		if err := rows.Scan(&k); err != nil {
			return nil, err
		}
		if id != 0 {
			continue
		}
		var there bool
		if err := s.S.QueryRow(ctx, `select exists (select 1 from grants where id = $1)`, k).Scan(&there); err != nil {
			return nil, err
		}
		if !there {
			id = k
		}
	}
	if id == 0 {
		return nil, errors.New("no revoked grant came back")
	}
	dir, err := filepath.Abs("results")
	if err != nil {
		return nil, err
	}
	cfg, err := writeConfig(dir, "race0", s.src.DSN("postgres"), s.dst.DSN("postgres"), "")
	if err != nil {
		return nil, err
	}
	defer os.Remove(cfg)
	fmt.Printf(`
Ticket: "Grant %d was revoked and the person still gets in."

  bin/grantline explain -config %s grants %d

Start the clock. Stop this command when the drill is over.
`, id, cfg, id)
	<-ctx.Done()
	return map[string]any{"grant": id}, nil
}
