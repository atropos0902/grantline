# Performance log

One entry for each change made for speed, with the profile that led to it, a comparison and the decision. Changes that didn't help stay.

The machine has four cores of a Ryzen 9 5950X and runs other work at the same time, so every comparison alternates the two builds run by run. An early comparison that didn't said the second change had made things 13% slower. The raw output is in `data/`.

## 1. Runs of rows instead of one statement a row

Profile: `data/apply-cpu-base.txt`. The lane waits for the target most of the time (4.2 s of CPU in 23 s), and the target spends it on 250 separate statements for a group of 250 changes.

Change: a stretch of upserts, or of deletes, on one table goes as one statement with an array for each column. A key that comes up again starts a new stretch, because an upsert can't touch a row twice. Rows still go through one at a time and in order.

| | before | after | |
| --- | --- | --- | --- |
| time for a group | 5.45 ms | 4.19 ms | -23%, p=0.000, n=8 |
| changes a second on one lane | 45,900 | 59,700 | +30%, p=0.000, n=8 |
| allocations for a group | 5,145 | 4,967 | -3%, p=0.000, n=8 |

Kept.

## 2. Fewer allocations for each change

Profile: `data/apply-allocs-runs.txt`. One `Change` allocated for every row because it escapes through the callback, the table's name joined again for every row, and the transaction's ID and position turned into text once for each of its rows.

Change: the `Change` is reused, the name is kept in the plan, and the text is made once a transaction.

| | before | after | |
| --- | --- | --- | --- |
| allocations for a group | 4,967 | 3,342 | -33%, p=0.000, n=8 |
| bytes for a group | 383 KiB | 346 KiB | -10%, p=0.000, n=8 |
| time for a group | 4.19 ms | 4.03 ms | no difference shown, p=0.382, n=8 |

Kept for the memory. It isn't faster, as far as anyone can tell here.

## 3. Binary instead of text for chunks

Rows go from COPY to COPY without being decoded, so binary should save the server its text conversion on both sides.

| | text | binary | |
| --- | --- | --- | --- |
| time for a 20,000 row chunk | 143.6 ms | 175.6 ms | no difference shown, p=0.201, n=20 |

Not shown to help. These runs vary by 22%. Binary stays because it is written and tested, and text takes over when the types differ. If someone measures this on a quiet machine and text wins, drop the binary path.

## 4. Plain inserts where the ledger is empty

The copy of 5 million rows took 26 seconds, and `pg_dump` piped to `psql` took 9. Timed by hand for a million rows:

| step | time |
| --- | --- |
| stage with COPY | 0.4 s |
| upsert with the guard | 4.0 s |
| plain insert | 1.5 s |
| COPY straight into the table with its key | 1.3 s |

The upsert was the cost. The guard's `key = $1 or key = '*'` was a problem of its own: it can't be hash joined, so with many entries in the ledger every staged row would have been compared with all of them.

Change: the landing looks at the ledger first. In a table that began empty, a key that isn't in the ledger has no row, so its row goes in with a plain insert. Only keys in the ledger take the guarded upsert. The truncate marker is checked once, outside the join.

| | before | after |
| --- | --- | --- |
| 5 million rows, median of 3 | 26.1 s | 11.6 s |
| `pg_dump` piped to `psql`, in the same run | 9.1 s | 6.1 s |

Kept. On the last run, with the machine nearly idle and 2.5 million rows, it was 3.6 s against 2.2. Still slower than the pipe. What is left is staging, the index kept up row by row, and that four chunks at once need cores that weren't free. COPY straight into the table would save the staging, and has to deal with rows the stream put there first. Loading before the index exists would save more. It means holding the stream back until the copy is done, and the log that keeps at the source is what chunks are there to avoid.

## 5. Groups of 250 instead of 1,000

Four lanes were no faster than one. Two groups of 1,000 keys out of a million share at least one key 63% of the time, so most groups waited for the one in front. Groups of 250 overlap 6% of the time.

| 200,000 changes, four lanes | 1,000 | 250 | 100 |
| --- | --- | --- | --- |
| no delay to the target | 6.7 s | 6.5 s | 5.7 s |
| 5 ms to the target and back | 9.6 s | 9.5 s | not measured |
| one lane, 5 ms | 8.5 s | 16.0 s | not measured |

One run each, so read them as rough. 250 is the default. It makes one lane slower when the target is far, which is the trade.
