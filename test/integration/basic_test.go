package integration

import (
	"testing"

	"grantline/internal/harness"
)

func TestCopyThenFollow(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, grantsSQL)
	srcExec(t, p, `insert into grants select i, 'user-' || i, 'dash-' || i % 50, 'active', i, repeat('p', 3000) from generate_series(1, 5000) i`)
	r := start(t, p, small)
	r.ready()
	srcExec(t, p, `insert into grants values (9001, 'new', 'dash-1', 'active', 9001, null)`)
	srcExec(t, p, `update grants set state = 'revoked' where id % 7 = 0`)
	srcExec(t, p, `delete from grants where id % 11 = 0`)
	srcExec(t, p, `update grants set id = id + 100000, badge = null where id % 13 = 0`)
	srcExec(t, p, `update grants set policy = 'changed', principal = E'quote '' back\\slash "q" {}' where id % 17 = 0`)
	r.caughtUp()
	r.same("grants")
	srcExec(t, p, `truncate grants`)
	srcExec(t, p, `insert into grants values (1, 'a', 'b', 'active', 1, 'x')`)
	r.caughtUp()
	r.same("grants")
}

func TestCompositeKeyAndTypes(t *testing.T) {
	p := harness.NewPair(t, srv.src, srv.dst)
	srcExec(t, p, `create table grants (org text, at timestamptz, n numeric(20, 6), b bytea, j jsonb, u uuid, f float8, arr text[], iv interval, primary key (org, at, u));
		create publication grants_pub for table grants`)
	srcExec(t, p, `insert into grants select 'org:' || i % 5, now() + i * interval '1 ms', i / 7.0, sha256(i::text::bytea), jsonb_build_object('k', i),
		gen_random_uuid(), i / 3.0, array['a"b', 'c\d', null, i::text], i * interval '1 minute' from generate_series(1, 3000) i`)
	r := start(t, p, small)
	r.ready()
	srcExec(t, p, `update grants set f = f * 2, arr = arr || '{x}' where n > 100`)
	srcExec(t, p, `delete from grants where org = 'org:3'`)
	srcExec(t, p, `update grants set org = 'moved' where org = 'org:4'`)
	r.caughtUp()
	r.same("grants")
}
