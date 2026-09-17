// Package xid turns the 32-bit transaction IDs in the change stream into the
// 64-bit IDs that Postgres snapshots use.
package xid

import "grantline/internal/bugs"

// Widen picks the 64-bit ID with these low 32 bits that is nearest to ref. ref
// has to be within two billion transactions of the real answer, which a recent
// pg_snapshot_xmax always is: Postgres itself stops before that limit.
func Widen(low uint32, ref uint64) uint64 {
	if bugs.XIDNotWidened {
		return uint64(low)
	}
	const span = 1 << 32
	c := ref&^uint64(span-1) | uint64(low)
	switch {
	case c > ref && c-ref > span/2 && c >= span:
		return c - span
	case c < ref && ref-c > span/2:
		return c + span
	}
	return c
}
