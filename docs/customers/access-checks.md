# Access checks

The services that decide who gets in read the copy in their own data center.

## What they told us

- A revoke has to reach the copy within a second. They measure it from the commit at the source.
- A missing grant is a ticket. A revoked grant that still works is an incident.
- They read one row by key, and all rows of a principal. They never write.
- They can't have a window where the copy is empty.

## What we agreed

| | |
| --- | --- |
| Revoke lag | p99 under 1 second at 5,000 changes a second |
| Reads before `ready` | none, they check the phase when they start |
| Recopy | over the rows that are there, so the copy is never empty |
| Schema changes | a nullable column goes through, anything else waits for their approval |
| Sign-off | their on-call lead, after the support drill |

## Open

They asked for a per-row age on each read. The copy has one position for all rows, so the honest answer is the replication's lag. They are thinking about it.
