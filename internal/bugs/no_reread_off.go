//go:build !bug_no_reread

package bugs

// NoReread: An update that arrives before its row, without its large values, is dropped and not read again.
const NoReread = false
