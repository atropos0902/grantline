# Review guide

A pull request that touches commit order, the chunk guard or the epoch check says which rule it relies on and names the test that breaks without it. If there is no such test, that test is the pull request.

Read the failure path first. What is committed when this line returns an error? Who has the locks?

Look twice at:

- A lock held while calling something that was passed in. The scheduler once ran a commit callback under its mutex while a status ticker took the same two locks the other way round. It took 22 rounds of the race campaign to show.
- A value from a snapshot used as if it were the next transaction ID.
- A wait without a context.
- Bytes that alias a message and outlive it.

Changes made for speed need an entry in `docs/performance` with a profile from before. If the profile doesn't show what the change fixes, close it.

## Tests drafted by an AI

They are read like anyone's. Before they merge, build them against the known bug they claim to cover (`scripts/known-bugs.sh` is the list) and see them fail. `docs/team/ai-log.md` has how that went.
