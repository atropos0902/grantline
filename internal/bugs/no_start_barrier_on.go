//go:build bug_no_start_barrier

package bugs

// NoStartBarrier: Copying begins without waiting for transactions that were open when the slot was made.
const NoStartBarrier = true
