//go:build !bug_log_position_guard

package bugs

// LogPositionGuard: A chunk row wins when the chunk's marker has a later log position than the last change to that key.
const LogPositionGuard = false
