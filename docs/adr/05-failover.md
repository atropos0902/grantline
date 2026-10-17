# 5. Failover

Slots are made with `failover` on, so a standby with `sync_replication_slots` has them when it is promoted.

That isn't enough. The stream can be ahead of the standby, and then the copy holds changes the promoted server never had. With `synchronized_standby_slots` set, the source doesn't send a commit before the standby has it. Preflight warns when a standby is connected and the setting is empty.

We don't trust the setting to be there. After the source's timeline changes, the engine reads where the new primary left the old timeline. If the target is past that point it stops with `diverged`. It doesn't guess which changes to take back.

A slot that the source invalidated is reported and stays lost. Making a new one silently would skip whatever happened in between.
