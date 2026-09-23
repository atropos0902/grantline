// Package load makes synthetic access grants and changes to them.
package load

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Schema makes the tables and the publication.
const Schema = `
create table if not exists grants (
  id bigint primary key, principal text not null, resource text not null,
  state text not null, badge int unique, policy text);
alter table grants alter column policy set storage external;
create table if not exists members (
  team text, principal text, role text not null, since timestamptz not null default now(), primary key (team, principal));
do $$ begin
  if not exists (select 1 from pg_publication where pubname = 'grants_pub') then
    create publication grants_pub for table grants, members;
  end if;
end $$;`

// Fill inserts n grants and n/10 members.
func Fill(ctx context.Context, p *pgxpool.Pool, n int) error {
	if _, err := p.Exec(ctx, fmt.Sprintf(`insert into grants select i, 'user-' || i %% 5000, 'dash-' || i %% 400, 'active',
		i, case when i %% 20 = 0 then repeat('p', 2500) end from generate_series(1, %d) i on conflict do nothing`, n)); err != nil {
		return err
	}
	_, err := p.Exec(ctx, fmt.Sprintf(`insert into members select 'team-' || i %% 97, 'user-' || i, 'reader' from generate_series(1, %d) i on conflict do nothing`, n/10))
	return err
}

// Mix applies random changes until it is stopped.
type Mix struct {
	Pool    *pgxpool.Pool
	Rows    int           // keys are drawn from here
	Workers int           // connections
	Pause   time.Duration // between a worker's transactions
	Changes atomic.Int64

	wg   sync.WaitGroup
	stop context.CancelFunc
	next atomic.Int64
}

// Start begins.
func (m *Mix) Start(ctx context.Context) {
	ctx, m.stop = context.WithCancel(ctx)
	m.next.CompareAndSwap(0, int64(m.Rows)*10)
	for range m.Workers {
		m.wg.Go(func() {
			for ctx.Err() == nil {
				n, err := m.one(ctx)
				var pe *pgconn.PgError
				if err == nil {
					m.Changes.Add(int64(n))
				} else if !errors.As(err, &pe) && ctx.Err() == nil {
					time.Sleep(100 * time.Millisecond) // the server went away
				}
				if m.Pause > 0 {
					time.Sleep(m.Pause)
				}
			}
		})
	}
}

// Stop ends the load and waits for transactions in flight.
func (m *Mix) Stop() {
	m.stop()
	m.wg.Wait()
}

func (m *Mix) one(ctx context.Context) (n int, err error) {
	// The change in hand finishes even when the load is being stopped, so the
	// source's state is settled afterwards.
	c, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err = pgx.BeginFunc(c, m.Pool, func(tx pgx.Tx) error {
		for range 1 + rand.IntN(5) {
			n++
			id := 1 + rand.IntN(m.Rows)
			sql, args := "", []any{id}
			switch rand.IntN(12) {
			case 0, 1, 2:
				sql = `update grants set state = case state when 'active' then 'revoked' else 'active' end where id = $1`
			case 3, 4:
				sql = `delete from grants where id = $1`
			case 5, 6:
				sql, args = `insert into grants values ($1, 'new', 'dash-1', 'active', null, null) on conflict (id) do update set principal = 'again'`, []any{m.next.Add(1)}
			case 7:
				sql = `update grants set policy = repeat(md5($1::text), 100) where id = $1::bigint`
			case 8: // moves the key and leaves the large value alone
				sql, args = `update grants set id = $2 where id = $1`, []any{id, m.next.Add(1)}
			case 9: // hands a badge from one row to another
				sql, args = `with f as (update grants set badge = null where id = $1 and badge is not null returning id)
					update grants set state = 'badge' where id = $2 and exists (select 1 from f)`, []any{id, 1 + rand.IntN(m.Rows)}
			case 10:
				sql, args = `insert into members values ('team-' || $1 % 97, 'user-' || $2::int, 'writer') on conflict (team, principal) do update set role = excluded.role`, []any{id, rand.IntN(m.Rows)}
			case 11:
				sql, args = `delete from members where team = 'team-' || $1 % 97 and principal = 'user-' || $2::int`, []any{id, rand.IntN(m.Rows)}
			}
			if _, err := tx.Exec(c, sql, args...); err != nil {
				return err
			}
		}
		if rand.IntN(20) == 0 {
			return errRollback
		}
		return nil
	})
	if errors.Is(err, errRollback) {
		return 0, nil
	}
	return n, err
}

var errRollback = errors.New("rolled back on purpose")
