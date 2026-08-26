// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"os"
	"testing"
	"time"
)

type blockingReadBackend struct {
	*ramContextStruct
	release chan struct{}
	started chan struct{}
}

func (backend *blockingReadBackend) readFile(input *readFileInputStruct) (*readFileOutputStruct, error) {
	close(backend.started)
	<-backend.release
	return backend.ramContextStruct.readFile(input)
}

func writeCacheTestUp(t *testing.T, storage string, cacheLines, cacheLineSize uint64) *backendStruct {
	t.Helper()
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))
	config := fmt.Sprintf(`{
		"msfs_version": 1,
		"cache_storage": %q,
		"cache_dir_path": %q,
		"cache_lines": %d,
		"cache_line_size": %d,
		"write_cache_promotion": true,
		"backends": [{
			"dir_name": "ram",
			"bucket_container_name": "ignored",
			"backend_type": "RAM",
			"readonly": false
		}]
	}`, storage, t.TempDir(), cacheLines, cacheLineSize)
	if err := os.WriteFile(globals.configFilePath, []byte(config), 0o600); err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}
	if err := checkConfigFile(); err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}
	initFS()
	processToMountList()
	backend := globals.config.backends["ram"]
	if backend == nil {
		t.Fatal("RAM backend was not mounted")
	}
	return backend
}

func writeCacheTestInode(t *testing.T, backend *backendStruct, name string, size uint64) *inodeStruct {
	t.Helper()
	globalsLock("write_cache_test.go:60:2:writeCacheTestInode")
	defer globalsUnlock()
	return backend.inode.createFileObjectInode(false, name, size, "", time.Now())
}

func readPromotedLine(t *testing.T, inode *inodeStruct, lineNumber uint64) []byte {
	t.Helper()
	globalsLock("write_cache_test.go:67:2:readPromotedLine")
	defer globalsUnlock()
	trackerNumber, ok := inode.cacheMap[lineNumber]
	if !ok {
		t.Fatalf("cache line %d not promoted", lineNumber)
	}
	tracker := &globals.dataCacheLinesTracker[trackerNumber]
	if tracker.state != CacheLineClean {
		t.Fatalf("cache line %d state = %d, expected Clean", lineNumber, tracker.state)
	}
	content := make([]byte, tracker.contentLength)
	if globals.config.cacheStorage == cacheStoragePerInodeFile {
		n, err := tracker.diskFile.ReadAt(content, tracker.diskOffset)
		if err != nil || n != len(content) {
			t.Fatalf("disk ReadAt() = (%d, %v), expected (%d, nil)", n, err, len(content))
		}
	} else {
		copy(content, globals.dataCacheLinesContent[tracker.contentStart:tracker.contentStart+tracker.contentLength])
	}
	return content
}

func TestWriteCachePromotionStorageModes(t *testing.T) {
	for _, storage := range []string{cacheStorageRAM, cacheStorageMappedFile, cacheStoragePerInodeFile} {
		t.Run(storage, func(t *testing.T) {
			backend := writeCacheTestUp(t, storage, 16, 4)
			defer drainFS()
			inode := writeCacheTestInode(t, backend, "promoted", 10)
			body := []byte("abcdefghij")

			globalsLock("write_cache_test.go:97:4:funcLit@91")
			promoteCommittedCacheLocked(inode, "committed-etag", uint64(len(body)), bytes.NewReader(body), nil)
			globalsUnlock()
			globals.dataCacheActivityWG.Wait()

			if got := string(readPromotedLine(t, inode, 0)); got != "abcd" {
				t.Fatalf("line 0 = %q, expected %q", got, "abcd")
			}
			if got := string(readPromotedLine(t, inode, 1)); got != "efgh" {
				t.Fatalf("line 1 = %q, expected %q", got, "efgh")
			}
			if got := string(readPromotedLine(t, inode, 2)); got != "ij" {
				t.Fatalf("line 2 = %q, expected %q", got, "ij")
			}
		})
	}
}

func TestWriteCachePromotionDisabledLeavesCacheEmpty(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 4)
	defer drainFS()
	globals.config.writeCachePromotion = false
	inode := writeCacheTestInode(t, backend, "disabled", 4)

	globalsLock("write_cache_test.go:121:2:TestWriteCachePromotionDisabledLeavesCacheEmpty")
	promoteCommittedCacheLocked(inode, "etag", 4, bytes.NewReader([]byte("data")), nil)
	globalsUnlock()

	if len(inode.cacheMap) != 0 {
		t.Fatalf("disabled promotion admitted %d cache lines", len(inode.cacheMap))
	}
}

func TestWriteCachePromotionStopsAtCapacity(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 4)
	defer drainFS()
	body := bytes.Repeat([]byte("abcd"), 17)
	inode := writeCacheTestInode(t, backend, "capacity", uint64(len(body)))

	globalsLock("write_cache_test.go:136:2:TestWriteCachePromotionStopsAtCapacity")
	promoteCommittedCacheLocked(inode, "etag", uint64(len(body)), bytes.NewReader(body), nil)
	globalsUnlock()
	globals.dataCacheActivityWG.Wait()

	if len(inode.cacheMap) != 16 {
		t.Fatalf("promoted lines = %d, expected 16", len(inode.cacheMap))
	}
	if _, ok := inode.cacheMap[16]; ok {
		t.Fatal("promotion retained line 16 after cache capacity was exhausted")
	}
	if got := string(readPromotedLine(t, inode, 0)); got != "abcd" {
		t.Fatalf("line 0 = %q, expected %q", got, "abcd")
	}
	if got := string(readPromotedLine(t, inode, 15)); got != "abcd" {
		t.Fatalf("line 15 = %q, expected %q", got, "abcd")
	}
}

func TestWriteCachePromotionEvictsOldestCleanLine(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 4)
	defer drainFS()
	firstBody := bytes.Repeat([]byte("1111"), 16)
	first := writeCacheTestInode(t, backend, "first", uint64(len(firstBody)))
	second := writeCacheTestInode(t, backend, "second", 4)

	globalsLock("write_cache_test.go:162:2:TestWriteCachePromotionEvictsOldestCleanLine")
	promoteCommittedCacheLocked(first, "one", uint64(len(firstBody)), bytes.NewReader(firstBody), nil)
	globalsUnlock()
	globals.dataCacheActivityWG.Wait()

	globalsLock("write_cache_test.go:167:2:TestWriteCachePromotionEvictsOldestCleanLine")
	promoteCommittedCacheLocked(second, "two", 4, bytes.NewReader([]byte("2222")), nil)
	globalsUnlock()
	globals.dataCacheActivityWG.Wait()

	if _, ok := first.cacheMap[0]; ok || len(first.cacheMap) != 15 {
		t.Fatalf("oldest clean line was not evicted correctly: remaining=%d", len(first.cacheMap))
	}
	if got := string(readPromotedLine(t, second, 0)); got != "2222" {
		t.Fatalf("second line = %q, expected %q", got, "2222")
	}
}

func TestWriteCachePromotionDoesNotWaitForInboundLines(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 4)
	defer drainFS()
	holder := writeCacheTestInode(t, backend, "holder", 64)
	target := writeCacheTestInode(t, backend, "target", 4)

	globalsLock("write_cache_test.go:186:2:TestWriteCachePromotionDoesNotWaitForInboundLines")
	for lineNumber := range uint64(16) {
		tracker := globals.dataCacheLineFreeLRU.popHead()
		tracker.inodeNumber = holder.inodeNumber
		tracker.lineNumber = lineNumber
		holder.cacheMap[lineNumber] = tracker.pos
		holder.inboundCacheLineCount++
		globals.dataCacheLineInboundLRU.pushTail(tracker)
	}
	promoteCommittedCacheLocked(target, "etag", 4, bytes.NewReader([]byte("data")), nil)
	globalsUnlock()

	if len(target.cacheMap) != 0 {
		t.Fatalf("promotion admitted %d lines while all slots were Inbound", len(target.cacheMap))
	}
}

func TestCompleteDirtyCacheLineNumbers(t *testing.T) {
	state := &writeState{segments: []writeSegment{
		{offset: 0, data: []byte("ab")},
		{offset: 2, data: []byte("cd")},
		{offset: 8, data: []byte("ij")},
	}}
	lines := completeDirtyCacheLineNumbers(state, 10, 4)
	if len(lines) != 2 || lines[0] != 0 || lines[1] != 2 {
		t.Fatalf("complete dirty lines = %v, expected [0 2]", lines)
	}
}

func TestWritePartsReaderAtCrossesPartBoundary(t *testing.T) {
	reader := &writePartsReaderAt{
		parts: map[int32]writePart{
			1: {data: []byte("abcd")},
			2: {data: []byte("efgh")},
		},
		partSize: 4,
		size:     8,
	}
	buf := make([]byte, 6)
	n, err := reader.ReadAt(buf, 2)
	if err != nil || n != 6 || string(buf) != "cdefgh" {
		t.Fatalf("ReadAt() = (%d, %v, %q), expected (6, nil, %q)", n, err, buf, "cdefgh")
	}
}

func TestWriteCachePromotionFromMultipartParts(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 6)
	defer drainFS()
	inode := writeCacheTestInode(t, backend, "multipart", 8)
	reader := &writePartsReaderAt{
		parts: map[int32]writePart{
			1: {data: []byte("abcd")},
			2: {data: []byte("efgh")},
		},
		partSize: 4,
		size:     8,
	}

	globalsLock("write_cache_test.go:244:2:TestWriteCachePromotionFromMultipartParts")
	promoteCommittedCacheLocked(inode, "multipart-etag", 8, reader, nil)
	globalsUnlock()
	globals.dataCacheActivityWG.Wait()

	if got := string(readPromotedLine(t, inode, 0)); got != "abcdef" {
		t.Fatalf("line 0 = %q, expected %q", got, "abcdef")
	}
	if got := string(readPromotedLine(t, inode, 1)); got != "gh" {
		t.Fatalf("line 1 = %q, expected %q", got, "gh")
	}
}

func TestWriteCachePromotionFromCompleteOverlayLines(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 4)
	defer drainFS()
	inode := writeCacheTestInode(t, backend, "overlay", 10)
	state := &writeState{segments: []writeSegment{
		{offset: 0, data: []byte("ab")},
		{offset: 2, data: []byte("cd")},
		{offset: 8, data: []byte("ij")},
	}}
	lines := completeDirtyCacheLineNumbers(state, 10, 4)

	globalsLock("write_cache_test.go:268:2:TestWriteCachePromotionFromCompleteOverlayLines")
	promoteCommittedCacheLocked(
		inode,
		"overlay-etag",
		10,
		&writeSegmentsReaderAt{segments: state.segments, size: 10},
		lines,
	)
	globalsUnlock()
	globals.dataCacheActivityWG.Wait()

	if got := string(readPromotedLine(t, inode, 0)); got != "abcd" {
		t.Fatalf("line 0 = %q, expected %q", got, "abcd")
	}
	if _, ok := inode.cacheMap[1]; ok {
		t.Fatal("partially overwritten line 1 was promoted")
	}
	if got := string(readPromotedLine(t, inode, 2)); got != "ij" {
		t.Fatalf("line 2 = %q, expected %q", got, "ij")
	}
}

func TestWriteCachePromotionDiscardsStaleInboundFetch(t *testing.T) {
	backend := writeCacheTestUp(t, cacheStorageRAM, 16, 4)
	defer drainFS()
	inode := writeCacheTestInode(t, backend, "stale", 4)
	ram := backend.context.(*ramContextStruct)
	if ok := ram.rootDir.fileMap.Put("stale", []byte("old!")); !ok {
		t.Fatal("RAM fileMap.Put() returned !ok")
	}
	blocking := &blockingReadBackend{
		ramContextStruct: ram,
		release:          make(chan struct{}),
		started:          make(chan struct{}),
	}
	backend.context = blocking

	globalsLock("write_cache_test.go:305:2:TestWriteCachePromotionDiscardsStaleInboundFetch")
	tracker := globals.dataCacheLineFreeLRU.popHead()
	if tracker == nil {
		globalsUnlock()
		t.Fatal("no free cache tracker")
	}
	tracker.inodeNumber = inode.inodeNumber
	tracker.lineNumber = 0
	inode.cacheMap[0] = tracker.pos
	inode.inboundCacheLineCount++
	globals.dataCacheLineInboundLRU.pushTail(tracker)
	globals.dataCacheActivityWG.Add(1)
	go tracker.fetch()
	globalsUnlock()

	select {
	case <-blocking.started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stale fetch to start")
	}

	globalsLock("write_cache_test.go:326:2:TestWriteCachePromotionDiscardsStaleInboundFetch")
	output := &writeFileOutputStruct{eTag: "new-etag", size: 4, mTime: time.Now()}
	inode.applyWriteOutputLocked(backend, output)
	promoteCommittedCacheLocked(inode, output.eTag, output.size, bytes.NewReader([]byte("new!")), nil)
	globalsUnlock()
	close(blocking.release)
	globals.dataCacheActivityWG.Wait()

	if got := string(readPromotedLine(t, inode, 0)); got != "new!" {
		t.Fatalf("promoted line = %q, expected %q", got, "new!")
	}
	globalsLock("write_cache_test.go:337:2:TestWriteCachePromotionDiscardsStaleInboundFetch")
	defer globalsUnlock()
	if tracker.state != CacheLineFree {
		t.Fatalf("stale tracker state = %d, expected Free", tracker.state)
	}
	if inode.inboundCacheLineCount != 0 {
		t.Fatalf("inbound count = %d, expected 0", inode.inboundCacheLineCount)
	}
}
