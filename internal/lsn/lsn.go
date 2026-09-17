// Package lsn has the write-ahead log position type used everywhere else.
package lsn

import (
	"fmt"
	"strconv"
	"strings"
)

// LSN is a byte position in the Postgres write-ahead log.
type LSN uint64

func (l LSN) String() string { return fmt.Sprintf("%X/%X", uint32(l>>32), uint32(l)) }

// Parse reads the X/Y form Postgres prints.
func Parse(s string) (LSN, error) {
	hi, lo, ok := strings.Cut(s, "/")
	if !ok {
		return 0, fmt.Errorf("lsn %q: no slash", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("lsn %q: %w", s, err)
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, fmt.Errorf("lsn %q: %w", s, err)
	}
	return LSN(h<<32 | l), nil
}
