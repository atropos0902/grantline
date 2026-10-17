# 6. Schema changes

The stream describes a table before the first row in each new shape. The engine compares that with the target, not with the source's catalog, which may be ahead.

A new nullable column without a default is added, in commit order. Everything else stops the replication and shows the statement it wants to run: a type change, a dropped column, a column with a default (rows that were there get the default without being sent), a column that can't be null. Someone approves that exact statement, or doesn't.

The whole replication stops, not only the one table. Commits are in source order, and a table that is held back would fall out of it.
