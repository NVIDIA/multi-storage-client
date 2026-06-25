//go:build !linux

package main

import "os"

// punchHoleSyscall is a no-op on non-Linux platforms (no portable hole-punch).
// The disk cache backend still functions; evicted lines simply keep their disk
// space until the inode's backing file is removed. MSFS production targets are
// Linux, so this path is for local dev/test builds (e.g. macOS) only.
func punchHoleSyscall(_ *os.File, _, _ int64) error {
	return nil
}
