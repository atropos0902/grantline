//go:build !bug_ack_before_commit

package bugs

// AckBeforeCommit: The slot moves forward when a transaction is received, before the target has committed it.
const AckBeforeCommit = false
