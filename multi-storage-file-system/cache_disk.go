package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
)

// Disk cache backend (cache_storage: "per-inode-file").
//
// Instead of holding cache-line bytes in the shared mmap'd content arena
// (globals.dataCacheLinesContent), this backend keeps one backing file per
// inode at <cacheDir>/cachelines/inode_<N>.bin. A "cache line" is just a byte
// range within that file: line lineNumber occupies the range
// [lineNumber*cacheLineSize, +cacheLineSize). Because each file's lines sit at
// their natural offsets they are physically contiguous, so kernel readahead
// helps sequential reads. The file is sparse — an evicted line is released with
// fallocate(PUNCH_HOLE|KEEP_SIZE), which frees its disk bytes while preserving
// the file size and the offsets of the surviving lines.
//
// Reads are served via pread (os.File.ReadAt) straight into the FUSE reply
// buffer, issued OUTSIDE the global lock (guarded by the
// dataCacheLineTracker.contentGeneration optimistic re-check in DoRead), so
// concurrent warm reads neither serialize on the lock nor pay the arena memcpy.
//
// All map/refcount mutations here assume the caller holds the global lock; the
// pwrite/punch-hole syscalls touch only the per-inode fd (positioned I/O, safe).

// inodeDiskCacheFileStruct tracks a single inode's on-disk cache file and how
// many of its cache lines are currently resident (so the file can be closed +
// removed when it drops to zero).
type inodeDiskCacheFileStruct struct {
	file          *os.File
	residentLines uint64
}

// diskReadsServed counts DoRead cache-line requests served via pread from the
// on-disk cache. Purely observational (sample-logged) to confirm the disk path
// is actually hot.
var diskReadsServed atomic.Uint64

// diskCacheDirPath returns <cacheDir>/cachelines.
func diskCacheDirPath() string {
	return filepath.Join(globals.cacheDir, "cachelines")
}

// diskCacheUp prepares the on-disk cache backend. Caller holds no lock (called
// once from dataCacheUp during startup, single-threaded).
func diskCacheUp() (err error) {
	globals.inodeDiskCacheFiles = make(map[uint64]*inodeDiskCacheFileStruct)
	if err = os.MkdirAll(diskCacheDirPath(), 0o700); err != nil {
		return fmt.Errorf("os.MkdirAll(%q) failed: %v", diskCacheDirPath(), err)
	}
	return nil
}

// diskCacheDown closes every open per-inode cache file. The cachelines/ dir
// itself is removed when fs.go tears down globals.cacheDir. Caller holds the
// global lock (called from dataCacheDown after dataCacheActivityWG drains).
func diskCacheDown() {
	for inodeNumber, idcf := range globals.inodeDiskCacheFiles {
		if idcf.file != nil {
			_ = idcf.file.Close()
		}
		delete(globals.inodeDiskCacheFiles, inodeNumber)
	}
}

// storeContentDisk writes a just-fetched cache line's bytes to the inode's
// backing file and records the location on the tracker. Caller holds the global
// lock. Returns the number of valid bytes stored; on any error it returns 0 and
// leaves the tracker disk fields cleared so DoRead falls through to EOF/empty
// rather than serving stale bytes.
func (dataCacheLineTracker *dataCacheLineTrackerStruct) storeContentDisk(buf []byte) (contentLength uint64) {
	dataCacheLineTracker.diskFile = nil
	dataCacheLineTracker.diskOffset = 0
	dataCacheLineTracker.diskLength = 0

	if len(buf) == 0 {
		return 0
	}

	idcf, ok := globals.inodeDiskCacheFiles[dataCacheLineTracker.inodeNumber]
	if !ok {
		path := filepath.Join(diskCacheDirPath(), fmt.Sprintf("inode_%d.bin", dataCacheLineTracker.inodeNumber))
		f, openErr := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
		if openErr != nil {
			globals.logger.Printf("[WARN] storeContentDisk: os.OpenFile(%q) failed: %v", path, openErr)
			return 0
		}
		idcf = &inodeDiskCacheFileStruct{file: f, residentLines: 0}
		globals.inodeDiskCacheFiles[dataCacheLineTracker.inodeNumber] = idcf
	}

	offset := int64(dataCacheLineTracker.lineNumber) * int64(globals.config.cacheLineSize)
	n, writeErr := idcf.file.WriteAt(buf, offset)
	if writeErr != nil {
		globals.logger.Printf("[WARN] storeContentDisk: WriteAt(inode=%d line=%d off=%d) failed: %v",
			dataCacheLineTracker.inodeNumber, dataCacheLineTracker.lineNumber, offset, writeErr)
		return 0
	}

	dataCacheLineTracker.diskFile = idcf.file
	dataCacheLineTracker.diskOffset = offset
	dataCacheLineTracker.diskLength = int64(n)
	idcf.residentLines++

	return uint64(n)
}

// punchHoleDisk releases the disk bytes of an evicted/freed cache line via
// fallocate(PUNCH_HOLE) and decrements the owning inode file's resident count,
// closing + removing the file when it reaches zero. No-op if the line has no
// disk backing. Caller holds the global lock.
func (dataCacheLineTracker *dataCacheLineTrackerStruct) punchHoleDisk() {
	if dataCacheLineTracker.diskFile == nil {
		return
	}

	if err := punchHoleSyscall(dataCacheLineTracker.diskFile, dataCacheLineTracker.diskOffset, int64(globals.config.cacheLineSize)); err != nil {
		globals.logger.Printf("[WARN] punchHoleDisk(inode=%d line=%d off=%d): %v",
			dataCacheLineTracker.inodeNumber, dataCacheLineTracker.lineNumber, dataCacheLineTracker.diskOffset, err)
	}

	if idcf, ok := globals.inodeDiskCacheFiles[dataCacheLineTracker.inodeNumber]; ok {
		if idcf.residentLines > 0 {
			idcf.residentLines--
		}
		if idcf.residentLines == 0 {
			path := idcf.file.Name()
			_ = idcf.file.Close()
			if removeErr := os.Remove(path); removeErr != nil && !os.IsNotExist(removeErr) {
				globals.logger.Printf("[WARN] punchHoleDisk: os.Remove(%q) failed: %v", path, removeErr)
			}
			delete(globals.inodeDiskCacheFiles, dataCacheLineTracker.inodeNumber)
		}
	}

	dataCacheLineTracker.diskFile = nil
	dataCacheLineTracker.diskOffset = 0
	dataCacheLineTracker.diskLength = 0
}
