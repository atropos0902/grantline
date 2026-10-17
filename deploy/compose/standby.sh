#!/bin/bash
# A standby that keeps the replication slots too.
set -e
D=/var/lib/postgresql/18/docker
if [ ! -s "$D/PG_VERSION" ]; then
  until pg_basebackup -c fast -h primary -U postgres -D "$D" -S sb -X stream; do rm -rf "${D:?}"/*; sleep 1; done
  touch "$D/standby.signal"
  chmod 700 "$D"
fi
exec postgres -D "$D" -c wal_level=logical -c hot_standby_feedback=on -c sync_replication_slots=on \
  -c primary_slot_name=sb -c synchronized_standby_slots= \
  -c "primary_conninfo=host=primary user=postgres dbname=postgres"
