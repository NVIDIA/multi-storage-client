//go:build linux

package main

import (
	"os"
	"syscall"
)

// punchHoleSyscall releases [offset, offset+length) of f back to the filesystem
// via fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE), so an evicted cache
// line stops consuming disk space while the file's logical size (and the offsets
// of other resident lines) are preserved.
func punchHoleSyscall(f *os.File, offset, length int64) error {
	const punchFlags = 0x02 | 0x01 // FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE
	return syscall.Fallocate(int(f.Fd()), punchFlags, offset, length)
}
