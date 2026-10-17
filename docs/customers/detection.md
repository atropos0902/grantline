# Detection

The detection team reads every change from Kafka and looks for patterns in who gets access and who loses it.

## What they told us

- Nothing dropped, nothing twice, and each table's changes in order. A delete after a grant tells a different story from a grant after a delete.
- Seconds are fine. They process in windows.
- They need the transaction and its place in it, to see what was granted together.
- They don't need a copy of what was there before.

## What we agreed

| | |
| --- | --- |
| Delivery | one Kafka transaction for each group of changes, read with `read_committed` |
| Order | one partition a table. More partitions keep order for each key only |
| Content | operation, key, row, transaction, position, place in the transaction |
| Large values | listed under `unchanged` when the update didn't touch them |
| Sign-off | their data owner, after the restart test |

## Where they differ from access checks

If Kafka is down, detection waits and access checks must not. So each has a slot of its own. Both slots keep log at the source, and the cap on that log applies to each.
