# Contributing

`make build test` needs Go 1.26. `make integration known-bugs` needs Docker and takes a few minutes. `make checker` needs uv, `make console` needs Node 24.

Start with `docs/team/first-issues.md`, or write a target with `docs/new-target.md`. Decisions are in `docs/adr`. If a change goes against one, the change includes the new record.

Bugs found in pglogrepl, pgx or franz-go are reported there with a test that fails.
