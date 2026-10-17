# 3. The position lives in the target

The position is a row in the target, written by the transaction that writes the rows it stands for. For Kafka it is a record in a compacted topic, in the same Kafka transaction.

The slot at the source is told later, and only about positions the target has committed. After a crash the source may send more than is needed. It never sends less.

A position kept anywhere else has to be saved before the rows or after them. Before loses changes on a crash. After repeats them, which an upsert hides in Postgres and nothing hides in Kafka.

## Who may commit

A worker takes over by raising the epoch in that same row. Every commit checks the epoch, so a worker that was paused and replaced can't commit what it had prepared, no matter what it believes about its lease. For Kafka the transactional producer ID does the same.

A worker that restarts its engine says who it is. If another process has taken over since, it stops. Without that, a worker that came back from a pause would take the replication away from its replacement, and they would pass it back and forth.

The Kubernetes Lease is only there so that two workers don't get in each other's way.

The first version compared the epoch the worker remembered. A takeover raises the epoch and may fail right after, for example because the source is down too. The worker then remembered an epoch that was no longer current and fenced itself out. The failover campaign found it.
