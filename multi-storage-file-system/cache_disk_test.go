package main

import (
	"bytes"
	"log"
	"os"
	"path/filepath"
	"testing"
)

// diskTestSetup wires up the minimal globals the disk-cache helpers touch
// (config, cacheDir, logger, the per-inode file map) against a temp dir.
func diskTestSetup(t *testing.T, cacheLineSize, cacheLines uint64) {
	t.Helper()
	globals.logger = log.New(os.Stderr, "", 0)
	globals.cacheDir = t.TempDir()
	globals.config = &configStruct{
		cacheStorage:  cacheStoragePerInodeFile,
		cacheLineSize: cacheLineSize,
		cacheLines:    cacheLines,
	}
	if err := diskCacheUp(); err != nil {
		t.Fatalf("diskCacheUp() failed: %v", err)
	}
	t.Cleanup(diskCacheDown)
}

// TestDiskCacheStoreServePunch covers the round trip: store a line to the
// per-inode backing file, serve it back via pread (the DoRead serve path),
// then punch it and confirm the file is closed + removed at zero residency.
func TestDiskCacheStoreServePunch(t *testing.T) {
	const lineSize = uint64(4096)
	diskTestSetup(t, lineSize, 8)

	tracker := &dataCacheLineTrackerStruct{inodeNumber: 42, lineNumber: 3}
	payload := []byte("hello disk cache backend - served via pread")

	n := tracker.storeContentDisk(payload)
	if n != uint64(len(payload)) {
		t.Fatalf("storeContentDisk returned %d, want %d", n, len(payload))
	}
	if tracker.diskFile == nil {
		t.Fatal("tracker.diskFile is nil after store")
	}
	if tracker.diskOffset != int64(tracker.lineNumber)*int64(lineSize) {
		t.Fatalf("tracker.diskOffset = %d, want %d", tracker.diskOffset, int64(tracker.lineNumber)*int64(lineSize))
	}
	if tracker.diskLength != int64(len(payload)) {
		t.Fatalf("tracker.diskLength = %d, want %d", tracker.diskLength, len(payload))
	}

	idcf, ok := globals.inodeDiskCacheFiles[42]
	if !ok || idcf.residentLines != 1 {
		t.Fatalf("inode 42 disk file not tracked with residentLines==1 (ok=%v)", ok)
	}
	wantPath := filepath.Join(diskCacheDirPath(), "inode_42.bin")
	if _, err := os.Stat(wantPath); err != nil {
		t.Fatalf("backing file %q not present: %v", wantPath, err)
	}

	// Serve path: pread straight into a reply-sized buffer at diskOffset.
	out := make([]byte, len(payload))
	m, err := tracker.diskFile.ReadAt(out, tracker.diskOffset)
	if err != nil || m != len(payload) {
		t.Fatalf("ReadAt: n=%d err=%v", m, err)
	}
	if !bytes.Equal(out, payload) {
		t.Fatalf("served bytes mismatch: got %q want %q", out, payload)
	}

	// Punch: line evicted -> residency hits 0 -> file closed + removed.
	tracker.punchHoleDisk()
	if tracker.diskFile != nil {
		t.Fatal("tracker.diskFile not cleared after punchHoleDisk")
	}
	if _, ok := globals.inodeDiskCacheFiles[42]; ok {
		t.Fatal("inode 42 disk file still tracked after last line punched")
	}
	if _, err := os.Stat(wantPath); !os.IsNotExist(err) {
		t.Fatalf("backing file %q should be removed at zero residency (err=%v)", wantPath, err)
	}
}

// TestDiskCacheResidencyAccounting checks that multiple lines of one inode
// share a single backing file, residentLines tracks them, and the file is only
// removed once the last line is punched.
func TestDiskCacheResidencyAccounting(t *testing.T) {
	const lineSize = uint64(1024)
	diskTestSetup(t, lineSize, 16)

	lineA := &dataCacheLineTrackerStruct{inodeNumber: 7, lineNumber: 0}
	lineB := &dataCacheLineTrackerStruct{inodeNumber: 7, lineNumber: 5}

	bufA := bytes.Repeat([]byte{0xAA}, int(lineSize))
	bufB := bytes.Repeat([]byte{0xBB}, 512)

	lineA.storeContentDisk(bufA)
	lineB.storeContentDisk(bufB)

	idcf, ok := globals.inodeDiskCacheFiles[7]
	if !ok || idcf.residentLines != 2 {
		t.Fatalf("expected residentLines==2 for inode 7, got ok=%v residentLines=%v", ok, idcf.residentLines)
	}
	if lineA.diskFile != lineB.diskFile {
		t.Fatal("two lines of the same inode should share one backing file")
	}

	// Non-contiguous offsets must not collide: lineB at offset 5*lineSize.
	if lineB.diskOffset != int64(5)*int64(lineSize) {
		t.Fatalf("lineB.diskOffset = %d, want %d", lineB.diskOffset, int64(5)*int64(lineSize))
	}
	outB := make([]byte, len(bufB))
	if _, err := lineB.diskFile.ReadAt(outB, lineB.diskOffset); err != nil {
		t.Fatalf("ReadAt lineB: %v", err)
	}
	if !bytes.Equal(outB, bufB) {
		t.Fatal("lineB served bytes mismatch")
	}

	// Punch one line: file stays (residency 1).
	lineA.punchHoleDisk()
	if idcf, ok := globals.inodeDiskCacheFiles[7]; !ok || idcf.residentLines != 1 {
		t.Fatalf("after punching lineA expected residentLines==1, got ok=%v", ok)
	}

	// Punch the last line: file closed + removed.
	lineB.punchHoleDisk()
	if _, ok := globals.inodeDiskCacheFiles[7]; ok {
		t.Fatal("inode 7 disk file should be gone after both lines punched")
	}
}

// TestDiskCacheStoreEmpty confirms an empty payload stores nothing and leaves
// the tracker with no disk backing (so DoRead treats it as EOF, not garbage).
func TestDiskCacheStoreEmpty(t *testing.T) {
	diskTestSetup(t, 4096, 8)
	tracker := &dataCacheLineTrackerStruct{inodeNumber: 1, lineNumber: 0}
	if n := tracker.storeContentDisk([]byte{}); n != 0 {
		t.Fatalf("storeContentDisk(empty) = %d, want 0", n)
	}
	if tracker.diskFile != nil {
		t.Fatal("empty store should not set diskFile")
	}
	if len(globals.inodeDiskCacheFiles) != 0 {
		t.Fatal("empty store should not create a backing file")
	}
}
