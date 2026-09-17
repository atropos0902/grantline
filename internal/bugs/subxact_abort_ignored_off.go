//go:build !bug_subxact_abort_ignored

package bugs

// SubxactAbortIgnored: Changes from a rolled back savepoint in a streamed transaction are kept.
const SubxactAbortIgnored = false
