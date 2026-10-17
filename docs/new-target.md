# Writing a new target

A target implements `target.Target` in `internal/target/target.go`. The Kafka target is the example to read, it is one file.

Three rules hold for all of them.

1. The position is saved in the same commit as the changes. If your system can't do that, it can't be a target without duplicates or losses, and the interface isn't the place to hide it.
2. `Open` fences. When it returns, a worker that was there before must fail with `target.ErrFenced` the next time it commits. Read the position after fencing, not before.
3. `Prepare` can be called again after `Rollback`. Transactions can be iterated as often as you like. They may be on disk, and then a change's body is only valid inside the callback.

`Item` says what a group touches. A target with one lane can return `Barrier: true`.

Tests to copy: `TestKafkaOnceAndInOrder`, which restarts a worker with a transaction in hand and keeps the old worker around. Then add a known bug for rule 1 or 2 to your own code and check that the test fails.
