# Runbook

Every page alert links here. `explain` works while the engine is down, because it only reads.

## A revoked user still has access
<a id="revoked"></a>

1. Find the row's key, then run `grantline explain -config copy.yaml grants <id>`.
2. Compare the last two lines. If the source still has the row, or has it as active, nobody revoked it at the source. The ticket goes to the team that owns grants.
3. If the source has no row and the target does, look at the newest change above them.
   - The delete is listed. Something put the row back afterwards. A `chunk` line after the delete names the chunk. If it says FAULT, that is our bug: keep the output and the target's `grantline.chunk` and `grantline.ledger` rows, and page the owner.
   - The delete isn't listed. It hasn't arrived. Check lag and the slot below.
   - A `refused` line is the guard doing its job. The row from that chunk was older than a change and was left out.
4. If both sides agree that the row is gone, the copy is right. Check what the access service caches.

## Late
<a id="late"></a>

`grantline_lag_seconds` is the age of the newest heartbeat the target has.

- `grantline_paused` is 1: see schema below, or someone paused it.
- `grantline_yields_total` climbs: two kinds of transaction keep taking each other's unique values. It is slow and still right. `replica identity full` on that table lets the overlap test see them.
- One very large transaction shows as `grantline_spilled_transactions_total` going up once, and lag growing until it has been applied.
- None of these: `curl :7400/debug/flight > f.trace` and open it with `go tool trace`.

## Stopped
<a id="stopped"></a>

Exit code 3 means another worker owns the replication. Leave it.

When a person has to decide, the worker stays up, `GetStatus` and the console show why, and nothing is applied. (Without `listen` in its settings it exits with code 4.)

- `replication slot lost`: the changes since the last commit are gone. `recopy -yes` makes a new slot and copies over the rows that are there. Tell the customer first. Until it is `ready` again the copy may be behind.
- `diverged`: after a failover the copy has changes the new primary doesn't, or a row that has to be there isn't. Keep the log line, then `recopy -yes`.

Restart the worker after a `recopy`.

Anything else is retried, with a pause that grows to five seconds.

## The slot keeps too much log
<a id="slot"></a>

The source keeps log until the target has committed it. Past `max_slot_wal_keep_size` the slot is lost, which saves the source's disk. If the engine can't be running again soon, that is the right outcome.

Deleting a Replication leaves its slot. Drop it by hand, `select pg_drop_replication_slot('grantline_<name>')`, when you are sure.

## Schema
<a id="schema"></a>

The console and `GetStatus` show the waiting statement. Approve it if the target should look like that. A column with a volatile default, `now()` or a random value, will differ on rows that were there before: approve and then `recopy`.

## Failover

Nothing to do when `synchronized_standby_slots` is set. The worker reconnects to whichever server takes writes.
