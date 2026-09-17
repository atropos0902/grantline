//go:build bug_unordered_commit

package bugs

// UnorderedCommit: Lanes commit as soon as they finish, without waiting for their turn.
const UnorderedCommit = true
