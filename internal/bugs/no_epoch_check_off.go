//go:build !bug_no_epoch_check

package bugs

// NoEpochCheck: A commit doesn't check that its worker still owns the replication.
const NoEpochCheck = false
