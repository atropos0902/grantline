# 2. Commits are in source order

Four lanes apply changes, and the target still sees the source's commits in the order the source made them. A reader never finds a later transaction without an earlier one.

The other choice was to split keys over lanes and let each lane commit when it likes. It is faster. It also means a reader can see a grant given by a later transaction before the revoke that came first, and our readers decide who gets in.

Lanes do the work in parallel and wait for their turn to commit. The position is part of the commit, so a restart needs nothing else.

## Overlap

Two transactions run together when the keys they touch don't overlap: primary keys, unique values and, when the target enforces them, foreign keys. Chunk ranges count as keys.

Primary keys alone aren't enough. One transaction can take and release a badge number and the next can take it for another row. If the second runs first, it holds the unique entry and waits for its turn. The first waits for the entry. Postgres sees one of those waits, so nothing breaks the circle.

The stream doesn't always say which unique value an update freed. With the default replica identity the old row isn't sent. So the overlap test can miss, and there has to be a way out: a lane that hits a lock timeout or a unique error makes every later lane give up its locks, and all of them run again in order. `TestLaterWorkGivesUpItsLock` is the smallest case.

## Chunks stand in line too

Landing a chunk reads the ledger and then writes rows. A delete that commits in between would be undone. So a landing takes a place in the commit order like any transaction, and declares its key range.

This is why chunk work stays in one process. Handing chunks to other workers through NATS was in the first plan. Their landings would have needed a place in this order, and the order lives in memory.
