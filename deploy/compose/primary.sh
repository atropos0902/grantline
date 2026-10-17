#!/bin/bash
# Runs once, when the primary's data directory is made.
set -e
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
psql -v ON_ERROR_STOP=1 -U postgres -c "select pg_create_physical_replication_slot('sb', true)"
