# 1. Chunks are checked by visibility

A table is copied in chunks while its changes stream. When a chunk lands, some of its rows are older than changes the stream has already applied, and those rows have to stay out.

The common method writes a marker into the log before and after each chunk and compares positions. On Postgres that isn't safe. A commit goes into the log first and becomes visible to readers afterwards, and with a synchronous standby the gap between the two lasts as long as the standby takes. In that gap a marker can be written, and a chunk read, behind a commit the chunk can't see. The positions say the chunk is newer. `repro/revoke_returns.sh` shows it.

So each chunk keeps the snapshot it was read under. While a table is being copied the target keeps a ledger: for each key the stream has touched, the last transaction that touched it. Deleted keys keep their entry. A chunk row lands only if that transaction is visible in the snapshot, which Postgres can answer anywhere with `pg_visible_in_snapshot`, because it only compares numbers.

The ledger costs one upsert for each change while copying goes on, and it is dropped when the table is finished. Comparing positions would have been free.

Holding one snapshot for the whole copy would also be safe. It stops vacuum on the source until the copy ends, and a restart has to begin again, so we don't.

## The same gap at the start

A commit can also be in the log ahead of the slot's starting position and still be invisible. It is then in no chunk and not in the stream. Before copying starts, the engine waits for every transaction that was open when the slot was made.

The first version of that wait was wrong. It took the edge from `pg_snapshot_xmax`, which is one past the last transaction that finished. A transaction that is still running can sit right on that number, and the wait let it through. `TestCommitOlderThanSlot` caught it. The edge now comes from `pg_current_xact_id`, which is behind everything that has started.

## When a table is finished

A chunk can be newer than the stream. A row may move and then be deleted, the chunk is read after that and has neither, and only then does the stream get to the move. Finding no row is right at that moment and a sign of divergence later. So the ledger stays, and a missing row is only put on the list to read again, until the stream has passed a mark written after the last read. The first version finished a table when its last chunk had landed. It worked on one machine and stopped with `diverged` as soon as 40 ms lay between the two.

## What a reader may do meanwhile

A chunk can hold rows newer than the point the stream has reached. Until the stream gets to the marker written after the last chunk, a row may step back for a moment before it goes forward again. The copy isn't `ready` until then, and the access checks don't read it before that.
