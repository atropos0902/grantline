//go:build !bug_toast_as_null

package bugs

// ToastAsNull: A value Postgres left out of an update is written as NULL.
const ToastAsNull = false
