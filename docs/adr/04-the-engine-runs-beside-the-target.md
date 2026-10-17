# 4. The engine runs beside the target

There are 40 ms between the data centers. Commits are in order, one after another, so a commit that crosses the gap can't happen more than 12 times a second.

Beside the target, the long way is the change stream, which is a one-way flow and doesn't care about distance. Commits are local. The price is that a chunk's rows cross the gap as they are read, with a connection for each copier, and that a heartbeat's answer takes 80 ms.

Groups help as well. Whole transactions that are waiting are put into one target commit, in order, up to 250 changes. Under light load a group is one transaction.
