//go:build !bug_no_yield

package bugs

// NoYield: A blocked transaction doesn't make later ones give up their locks.
const NoYield = false
