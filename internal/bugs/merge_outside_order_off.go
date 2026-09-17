//go:build !bug_merge_outside_order

package bugs

// MergeOutsideOrder: A chunk merge commits on its own, without a place in the commit order.
const MergeOutsideOrder = false
