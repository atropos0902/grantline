# Where AI helped, and where it was wrong

Claude Code wrote drafts of the code, the tests and the fault ideas. This is what checking them found.

**The start barrier was wrong and looked right.** The draft took the edge from `pg_snapshot_xmax`, and the comment beside it was convincing. The test was drafted separately, from the failure rather than from the code, and it failed.

**A callback ran under a lock.** The scheduler's draft called `Done` with its mutex held. Every unit test passed, with the race detector on. The race campaign hung after 22 rounds. There is a test for it now, and it hangs on the old code.

**A resumed worker took the replication back.** The draft retried after any failure and fenced whoever was there. Found while writing the pause campaign, before it ran.

**One known bug couldn't be caught.** "Apply resent transactions again" was on the list of twelve. Postgres doesn't resend them when the start position is given, so no test could fail. It was replaced by one that can happen: the position saved outside the rows' commit.

**One test passed for the wrong reason.** The chunk order test didn't make sure the delete was prepared before the chunk landed, and it let its known bug through. `scripts/known-bugs.sh` said so.

**The checker's first version was too quick.** It called a row different when it differed in the same place three times, even if both values had changed in between. The test with a row that is written all the time broke it.

**Time went into** the Kafka broker's `CONCURRENT_TRANSACTIONS` answer during a takeover. The client can't be used after it, which wasn't in the draft.

**The fix for the resumed worker had a hole.** It compared remembered epochs, and a takeover that failed halfway made the worker refuse itself. Found by the first run of the failover campaign.

**A moved key could hit a row that was already there.** An update that changes a key and doesn't carry the large value was written as an update of the key. If a chunk read after the move had landed first, the new key was taken, and the worker kept failing on it until its luck changed. The kill campaign passed once, 200 times out of 200, before it showed. `TestKeyMovesBetweenChunks` shows it every time.

**A table was finished too early.** The draft dropped the ledger when the last chunk had landed, and after that called a missing row a divergence. A chunk can be newer than the stream, so it was wrong now and then. No test and no campaign had shown it. Starting everything with 40 ms between the data centers did, in the first minute.
