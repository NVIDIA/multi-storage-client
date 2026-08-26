// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func writeCommitTestUp(t *testing.T) {
	t.Helper()
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))
	if err := os.WriteFile(globals.configFilePath, []byte(`
	{
		"msfs_version": 1,
		"write_commit_workers": 4,
		"write_commit_queue_depth": 16,
		"backends": [
			{
				"dir_name": "ram",
				"bucket_container_name": "ignored",
				"backend_type": "RAM",
				"readonly": false
			}
		]
	}
	`), 0o600); err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}
	if err := checkConfigFile(); err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}
	initFS()
	processToMountList()
}

func writeCommitTestInode(t *testing.T, data []byte) *inodeStruct {
	t.Helper()
	inode := &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         FileObject,
		parentInodeNumber: FUSERootDirInodeNumber,
		objectPath:        "commit-test",
		basename:          "commit-test",
		sizeInMemory:      uint64(len(data)),
		mTime:             time.Now(),
		fhSet:             make(map[uint64]struct{}),
		writeState: writeState{
			segments:             []writeSegment{{offset: 0, data: data}},
			deferredBytesCounted: uint64(len(data)),
		},
		writeStateActive: true,
		writeDirty:       true,
	}
	writeCommitControlLocked(inode.inodeNumber).generation = 1
	globals.deferredWriteBytes += uint64(len(data))
	if ok := globals.inodeMap.put(inode); !ok {
		t.Fatalf("globals.inodeMap.put() returned !ok")
	}
	return inode
}

func TestWriteCommitPoolBoundsConcurrency(t *testing.T) {
	writeCommitTestUp(t)
	defer drainFS()

	pool := newWriteCommitPool(2, 4)
	var active atomic.Int32
	var maxActive atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{}, 4)
	jobs := make([]*writeCommitJob, 0, 4)

	globalsLock("write_commit_test.go:79:2:TestWriteCommitPoolBoundsConcurrency")
	for i := range 4 {
		inode := writeCommitTestInode(t, []byte{byte(i)})
		done := make(chan struct{})
		control := writeCommitControlLocked(inode.inodeNumber)
		control.inFlight = true
		control.commitGeneration = control.generation
		control.done = done
		job := &writeCommitJob{
			backend:     &backendStruct{},
			control:     control,
			done:        done,
			filePath:    inode.objectPath,
			generation:  control.generation,
			inodeNumber: inode.inodeNumber,
			size:        1,
		}
		job.upload = func() (*writeFileOutputStruct, error) {
			current := active.Add(1)
			for {
				seen := maxActive.Load()
				if current <= seen || maxActive.CompareAndSwap(seen, current) {
					break
				}
			}
			started <- struct{}{}
			<-release
			active.Add(-1)
			return &writeFileOutputStruct{size: 1, mTime: time.Now()}, nil
		}
		jobs = append(jobs, job)
	}
	globalsUnlock()

	for _, job := range jobs {
		if err := pool.submit(job); err != nil {
			t.Fatalf("pool.submit() failed: %v", err)
		}
	}
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for workers to start")
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := maxActive.Load(); got != 2 {
		t.Fatalf("max active uploads = %d, expected 2", got)
	}

	drained := make(chan struct{})
	go func() {
		pool.drain()
		close(drained)
	}()
	select {
	case <-drained:
		t.Fatal("pool drain returned while uploads were blocked")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	for _, job := range jobs {
		select {
		case <-job.done:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for commit completion")
		}
		if job.err != nil {
			t.Fatalf("commit failed: %v", job.err)
		}
	}
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out draining commit pool")
	}
}

func TestSmallWriteCommitWaitsAndAppliesResult(t *testing.T) {
	writeCommitTestUp(t)
	defer drainFS()
	globals.config.writeCachePromotion = true

	backend := &backendStruct{backendType: "S3"}
	release := make(chan struct{})

	globalsLock("write_commit_test.go:167:2:TestSmallWriteCommitWaitsAndAppliesResult")
	inode := writeCommitTestInode(t, []byte("data"))
	job, err := inode.prepareSmallWriteCommitLocked(backend)
	if err != nil {
		globalsUnlock()
		t.Fatalf("prepareSmallWriteCommitLocked() failed: %v", err)
	}
	job.upload = func() (*writeFileOutputStruct, error) {
		<-release
		return &writeFileOutputStruct{size: 4, eTag: "etag", mTime: time.Now()}, nil
	}
	globalsUnlock()

	if err := globals.writeCommitPool.submit(job); err != nil {
		t.Fatalf("submit() failed: %v", err)
	}

	waited := make(chan error, 1)
	go func() {
		globalsLock("write_commit_test.go:186:3:funcLit@185")
		_, waitErr := waitForWriteCommitLocked(inode.inodeNumber)
		waited <- waitErr
		globalsUnlock()
	}()

	select {
	case <-waited:
		t.Fatal("same-inode waiter returned before commit completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err = <-waited:
		if err != nil {
			t.Fatalf("waitForWriteCommitLocked() failed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for same-inode operation")
	}
	globals.dataCacheActivityWG.Wait()

	globalsLock("write_commit_test.go:209:2:TestSmallWriteCommitWaitsAndAppliesResult")
	defer globalsUnlock()
	if control := globals.writeCommitControls[inode.inodeNumber]; control != nil && control.inFlight {
		t.Fatal("commit remained in flight")
	}
	if inode.writeDirty || inode.writeStateActive {
		t.Fatalf("commit did not clear write state: dirty=%v active=%v", inode.writeDirty, inode.writeStateActive)
	}
	if inode.eTag != "etag" || inode.sizeInBackend != 4 {
		t.Fatalf("commit output not applied: etag=%q size=%d", inode.eTag, inode.sizeInBackend)
	}
	trackerNumber, ok := inode.cacheMap[0]
	if !ok {
		t.Fatal("successful commit did not promote cache line 0")
	}
	tracker := &globals.dataCacheLinesTracker[trackerNumber]
	if tracker.state != CacheLineClean || tracker.eTag != "etag" || tracker.contentLength != 4 {
		t.Fatalf("promoted tracker state=%d etag=%q length=%d", tracker.state, tracker.eTag, tracker.contentLength)
	}
}

func TestSmallWriteCommitFailurePreservesRetryState(t *testing.T) {
	writeCommitTestUp(t)
	defer drainFS()
	globals.config.writeCachePromotion = true

	backend := &backendStruct{backendType: "S3"}
	globalsLock("write_commit_test.go:236:2:TestSmallWriteCommitFailurePreservesRetryState")
	inode := writeCommitTestInode(t, []byte("retry"))
	job, err := inode.prepareSmallWriteCommitLocked(backend)
	if err != nil {
		globalsUnlock()
		t.Fatalf("prepareSmallWriteCommitLocked() failed: %v", err)
	}
	job.upload = func() (*writeFileOutputStruct, error) {
		return nil, errors.New("injected upload failure")
	}
	reloaded := &inodeStruct{
		inodeNumber:       inode.inodeNumber,
		inodeType:         inode.inodeType,
		backendNonce:      inode.backendNonce,
		parentInodeNumber: inode.parentInodeNumber,
		objectPath:        inode.objectPath,
		basename:          inode.basename,
		sizeInMemory:      inode.sizeInMemory,
		mode:              inode.mode,
		mTime:             inode.mTime,
		fhSet:             make(map[uint64]struct{}),
	}
	if ok := globals.inodeMap.delete(inode.inodeNumber); !ok {
		globalsUnlock()
		t.Fatal("globals.inodeMap.delete() returned !ok")
	}
	if ok := globals.inodeMap.put(reloaded); !ok {
		globalsUnlock()
		t.Fatal("globals.inodeMap.put(reloaded) returned !ok")
	}
	inode = reloaded
	globalsUnlock()

	if err = globals.writeCommitPool.submit(job); err != nil {
		t.Fatalf("submit() failed: %v", err)
	}
	select {
	case <-job.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for failed commit")
	}

	globalsLock("write_commit_test.go:278:2:TestSmallWriteCommitFailurePreservesRetryState")
	defer globalsUnlock()
	control := globals.writeCommitControls[inode.inodeNumber]
	if control == nil {
		t.Fatal("failed commit control was discarded")
	}
	if control.inFlight {
		t.Fatal("failed commit remained in flight")
	}
	if !inode.writeDirty || !inode.writeStateActive {
		t.Fatal("failed commit discarded retry state")
	}
	if control.err == nil || len(inode.writeState.segments) != 1 {
		t.Fatalf("failed commit state is incomplete: err=%v segments=%d", control.err, len(inode.writeState.segments))
	}
	if got := string(inode.writeState.segments[0].data); got != "retry" {
		t.Fatalf("retry body = %q, expected %q", got, "retry")
	}
	if len(inode.cacheMap) != 0 {
		t.Fatalf("failed commit promoted %d cache lines", len(inode.cacheMap))
	}
}

// submit runs after globals.Lock is released, so it races drainFS closing the
// job channel. Without the close guard a send on the closed channel panics, and
// the panic surfaces inside a FUSE callback during unmount.
func TestWriteCommitPoolSubmitDuringDrainDoesNotPanic(t *testing.T) {
	pool := newWriteCommitPool(2, 4)

	var (
		wg       sync.WaitGroup
		accepted atomic.Int64
		refused  atomic.Int64
		start    = make(chan struct{})
	)

	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for range 50 {
				job := &writeCommitJob{
					done:   make(chan struct{}),
					upload: func() (*writeFileOutputStruct, error) { return nil, errors.New("unused") },
				}
				if err := pool.submit(job); err != nil {
					refused.Add(1)
					continue
				}
				accepted.Add(1)
			}
		}()
	}

	close(start)
	// Drain concurrently with the submitters, which is the window drainFS opens.
	pool.drain()
	wg.Wait()

	if accepted.Load()+refused.Load() != 400 {
		t.Fatalf("accounted for %d of 400 submissions", accepted.Load()+refused.Load())
	}
	if refused.Load() == 0 {
		t.Log("drain completed before any submission was refused; the race window was not exercised this run")
	}

	// drain must be idempotent: drainFS nils the pool afterwards, but a second
	// call must not double-close.
	pool.drain()
}

// A job that never reached a worker leaves the control in flight with an open
// done channel, and waitForWriteCommitLocked then blocks on that inode forever.
func TestAbandonedWriteCommitReleasesTheInode(t *testing.T) {
	writeCommitTestUp(t)
	defer drainFS()

	backend := &backendStruct{backendType: "S3"}
	globalsLock("write_commit_test.go:357:2:TestAbandonedWriteCommitReleasesTheInode")
	inode := writeCommitTestInode(t, []byte("abandoned"))
	inodeNumber := inode.inodeNumber
	job, err := inode.prepareSmallWriteCommitLocked(backend)
	if err != nil {
		globalsUnlock()
		t.Fatalf("prepareSmallWriteCommitLocked() failed: %v", err)
	}
	if !globals.writeCommitControls[inodeNumber].inFlight {
		globalsUnlock()
		t.Fatal("control was not marked in flight before submission")
	}

	abandonWriteCommitLocked(job, errors.New("pool is draining"))
	globalsUnlock()

	select {
	case <-job.done:
	case <-time.After(2 * time.Second):
		t.Fatal("done channel was never closed; the inode would block forever")
	}

	globalsLock("write_commit_test.go:379:2:TestAbandonedWriteCommitReleasesTheInode")
	defer globalsUnlock()

	control := globals.writeCommitControls[inodeNumber]
	if control == nil || control.inFlight {
		t.Fatalf("control still in flight after abandonment: %+v", control)
	}
	// A waiter must now return rather than block.
	current, waitErr := waitForWriteCommitLocked(inodeNumber)
	if waitErr != nil {
		t.Fatalf("waitForWriteCommitLocked() failed: %v", waitErr)
	}
	// The buffered body must survive as retry state.
	if !current.writeDirty || !current.writeStateActive {
		t.Fatal("abandonment discarded the retry state")
	}
	if len(current.writeState.segments) != 1 ||
		string(current.writeState.segments[0].data) != "abandoned" {
		t.Fatalf("retry body was lost: %+v", current.writeState.segments)
	}
	// And the failure must be reportable at the next barrier.
	if latched := takeWriteCommitErrorLocked(inodeNumber); latched == nil {
		t.Fatal("abandoned commit did not latch an error for the next fsync")
	}
}

// The promotion pool registers with dataCacheActivityWG, which drainFS waits on
// straight after draining, so an Add racing that Wait is its own panic.
func TestWriteCachePromotionPoolTrySubmitAfterDrain(t *testing.T) {
	pool := newWriteCachePromotionPool(2, 4)
	pool.drain()

	if pool.trySubmit(&writeCachePromotionJob{}) {
		t.Fatal("trySubmit accepted a job after drain")
	}
	pool.drain()
}

// With flush_on_close false, close(2) is acknowledged before the upload runs, so
// a durability barrier is the only place the application can still learn the
// upload failed. The error must arrive once and then be forgotten, and a retry
// that succeeds must clear it rather than report a repaired failure forever.
func TestFailedDetachedCommitSurfacesOnceAtBarrier(t *testing.T) {
	writeCommitTestUp(t)
	defer drainFS()

	globalsLock("write_commit_test.go:425:2:TestFailedDetachedCommitSurfacesOnceAtBarrier")
	inode := writeCommitTestInode(t, []byte("latch"))
	inodeNumber := inode.inodeNumber
	globalsUnlock()

	globalsLock("write_commit_test.go:430:2:TestFailedDetachedCommitSurfacesOnceAtBarrier")
	control := writeCommitControlLocked(inodeNumber)
	control.err = errors.New("injected detached commit failure")
	globalsUnlock()

	globalsLock("write_commit_test.go:435:2:TestFailedDetachedCommitSurfacesOnceAtBarrier")
	first := takeWriteCommitErrorLocked(inodeNumber)
	second := takeWriteCommitErrorLocked(inodeNumber)
	globalsUnlock()

	if first == nil {
		t.Fatal("a failed detached commit was never reported")
	}
	if second != nil {
		t.Fatalf("the failure was reported twice; second = %v", second)
	}

	// An in-flight commit owns the outcome, so nothing may be reported yet.
	globalsLock("write_commit_test.go:448:2:TestFailedDetachedCommitSurfacesOnceAtBarrier")
	control = writeCommitControlLocked(inodeNumber)
	control.err = errors.New("stale failure")
	control.inFlight = true
	inFlight := takeWriteCommitErrorLocked(inodeNumber)
	control.inFlight = false
	globalsUnlock()

	if inFlight != nil {
		t.Fatalf("an in-flight commit reported a stale failure: %v", inFlight)
	}
}

// A successful commit discards its control, which is what keeps a repaired
// failure from being reported by a later barrier.
func TestSucceedingCommitClearsLatchedFailure(t *testing.T) {
	writeCommitTestUp(t)
	defer drainFS()

	backend := &backendStruct{backendType: "S3"}
	globalsLock("write_commit_test.go:468:2:TestSucceedingCommitClearsLatchedFailure")
	inode := writeCommitTestInode(t, []byte("repair"))
	inodeNumber := inode.inodeNumber
	writeCommitControlLocked(inodeNumber).err = errors.New("earlier detached failure")

	job, err := inode.prepareSmallWriteCommitLocked(backend)
	if err != nil {
		globalsUnlock()
		t.Fatalf("prepareSmallWriteCommitLocked() failed: %v", err)
	}
	job.upload = func() (*writeFileOutputStruct, error) {
		return &writeFileOutputStruct{eTag: "repaired", size: inode.sizeInMemory, mTime: time.Now()}, nil
	}
	globalsUnlock()

	if err = globals.writeCommitPool.submit(job); err != nil {
		t.Fatalf("submit() failed: %v", err)
	}
	select {
	case <-job.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the commit")
	}
	if job.err != nil {
		t.Fatalf("the commit unexpectedly failed: %v", job.err)
	}

	globalsLock("write_commit_test.go:495:2:TestSucceedingCommitClearsLatchedFailure")
	defer globalsUnlock()
	if latched := takeWriteCommitErrorLocked(inodeNumber); latched != nil {
		t.Fatalf("a repaired failure was still reported: %v", latched)
	}
}

// An inode removed while its commit is in flight takes its deferred-budget
// contribution with it. Those bytes are unreachable afterwards, and once enough
// accumulate shouldPromoteDeferredLocked always trips, silently sending every
// new object to multipart.
func TestSmallWriteCommitReleasesBudgetWhenInodeDisappears(t *testing.T) {
	var (
		body = []byte("gone")
		err  error
	)

	writeCommitTestUp(t)
	defer drainFS()

	globalsLock("write_commit_test.go:515:2:TestSmallWriteCommitReleasesBudgetWhenInodeDisappears")
	globals.deferredWriteBytes = 0
	inode := writeCommitTestInode(t, body)
	control := writeCommitControlLocked(inode.inodeNumber)
	control.inFlight = true
	control.commitGeneration = control.generation
	control.body = body
	control.deferredBytes = uint64(len(body))
	control.done = make(chan struct{})
	job := &writeCommitJob{
		backend:     &backendStruct{},
		body:        body,
		control:     control,
		done:        control.done,
		filePath:    inode.objectPath,
		generation:  control.generation,
		inodeNumber: inode.inodeNumber,
		size:        uint64(len(body)),
	}
	job.upload = func() (*writeFileOutputStruct, error) {
		return &writeFileOutputStruct{eTag: "etag", size: uint64(len(body)), mTime: time.Now()}, nil
	}
	if globals.deferredWriteBytes != uint64(len(body)) {
		globalsUnlock()
		t.Fatalf("setup: deferredWriteBytes = %d, expected %d", globals.deferredWriteBytes, len(body))
	}
	if ok := globals.inodeMap.delete(inode.inodeNumber); !ok {
		globalsUnlock()
		t.Fatal("globals.inodeMap.delete() returned !ok")
	}
	globalsUnlock()

	if err = globals.writeCommitPool.submit(job); err != nil {
		t.Fatalf("submit() failed: %v", err)
	}
	select {
	case <-job.done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for commit")
	}

	globalsLock("write_commit_test.go:556:2:TestSmallWriteCommitReleasesBudgetWhenInodeDisappears")
	defer globalsUnlock()
	if job.err == nil {
		t.Fatal("commit against a removed inode unexpectedly succeeded")
	}
	if globals.deferredWriteBytes != 0 {
		t.Fatalf("deferred budget leaked %d bytes after the inode disappeared", globals.deferredWriteBytes)
	}
}
