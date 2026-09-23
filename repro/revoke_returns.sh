#!/usr/bin/env bash
# Shows a chunk read returning a row whose delete is already in the change
# stream, at an earlier log position than a marker written before the read.
set -euo pipefail
N=revoke-returns-$$
docker run -d --rm --name "$N" -e POSTGRES_PASSWORD=x postgres:18.3 -c wal_level=logical >/dev/null
trap 'docker stop "$N" >/dev/null' EXIT
P() { docker exec -i "$N" psql -U postgres -qAt -v ON_ERROR_STOP=1 "$@"; }
# TCP check: the image's setup-time server listens on the socket only.
until docker exec "$N" pg_isready -U postgres -h 127.0.0.1 -q; do sleep 1; done

P <<'SQL'
create table grants (id bigint primary key, state text);
insert into grants values (8814, 'active');
select from pg_create_logical_replication_slot('s', 'test_decoding');
alter system set synchronous_standby_names = 'ghost';
select from pg_reload_conf();
SQL
sleep 1

P -c "delete from grants where id = 8814" >/dev/null 2>&1 &
W=$!
until [ "$(P -c "select count(*) from pg_stat_activity where wait_event = 'SyncRep'")" = 1 ]; do sleep 0.2; done

MARK=$(P -c "set synchronous_commit = local" -c "select pg_logical_emit_message(true, 'chunk', 'low')")
READ=$(P -c "select pg_current_snapshot() || ' ' || coalesce((select state from grants where id = 8814), 'missing')")
SNAP=${READ% *}; STATE=${READ#* }
DEL=$(P -c "select xid || ' ' || lsn from pg_logical_slot_peek_changes('s', null, null) where data like 'COMMIT%' order by lsn limit 1")
XID=${DEL% *}; LSN=${DEL#* }

echo "chunk read of 8814:        $STATE (snapshot $SNAP)"
echo "delete in the stream:      transaction $XID, committed by $LSN"
echo "marker before the read:    $MARK"
echo "log position guard:        $(P -c "select case when '$MARK'::pg_lsn > '$LSN'::pg_lsn then 'chunk wins, the revoked grant is back' else 'delete wins' end")"
echo "snapshot guard:            $(P -c "select case when pg_visible_in_snapshot('$XID'::xid8, '$SNAP'::pg_snapshot) then 'chunk wins' else 'delete wins, the grant stays revoked' end")"

P -c "alter system reset synchronous_standby_names" -c "select from pg_reload_conf()"
wait "$W"
