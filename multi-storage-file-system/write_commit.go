package main

import (
	"bytes"
	"errors"
	"fmt"
	"sync"
)

type writeCommitJob struct {
	backend     *backendStruct
	body        []byte
	control     *writeCommitControl
	done        chan struct{}
	err         error
	filePath    string
	generation  uint64
	ifMatch     string
	inodeNumber uint64
	size        uint64
	upload      func() (*writeFileOutputStruct, error)
}

type writeCommitControl struct {
	body             []byte
	commitGeneration uint64
	deferredBytes    uint64
	done             chan struct{}
	err              error
	generation       uint64
	inFlight         bool
}

type writeCommitPool struct {
	jobs     chan *writeCommitJob
	workerWG sync.WaitGroup
	// closeMu orders sends against the close. Submitters hold it for reading so
	// they do not serialize against each other; drain holds it for writing.
	closeMu sync.RWMutex
	closed  bool
}

func newWriteCommitPool(workerCount, queueDepth uint64) *writeCommitPool {
	pool := &writeCommitPool{
		jobs: make(chan *writeCommitJob, int(queueDepth)),
	}
	for range workerCount {
		pool.workerWG.Add(1)
		go func() {
			defer pool.workerWG.Done()
			for job := range pool.jobs {
				job.run()
			}
		}()
	}
	return pool
}

// submit hands a job to a worker. The caller has released globals.Lock, so an
// unsynchronized send races drainFS closing the channel, and a send on a closed
// channel is an unrecoverable panic raised inside a FUSE callback.
//
// The read lock is held across the send. A submitter blocked on a full queue
// keeps it, but the channel is still open at that point so workers keep draining
// and the submitter makes progress; drain's write lock is therefore never
// starved. Taking the lock only to test `closed` and releasing it before the
// send would reintroduce the race.
func (pool *writeCommitPool) submit(job *writeCommitJob) error {
	if pool == nil {
		return errors.New("write commit pool is not initialized")
	}
	pool.closeMu.RLock()
	defer pool.closeMu.RUnlock()
	if pool.closed {
		return errors.New("write commit pool is draining")
	}
	pool.jobs <- job
	return nil
}

func (pool *writeCommitPool) drain() {
	if pool == nil {
		return
	}
	pool.closeMu.Lock()
	if !pool.closed {
		pool.closed = true
		close(pool.jobs)
	}
	pool.closeMu.Unlock()
	// Outside the lock: drainFS has not taken globals.Lock yet, but a worker
	// still needs it to finish, so nothing here may block a submitter that is
	// holding it.
	pool.workerWG.Wait()
}

func (job *writeCommitJob) run() {
	var (
		output *writeFileOutputStruct
		err    error
	)
	if job.upload != nil {
		output, err = job.upload()
	} else {
		output, err = writeFileWrapper(job.backend.context, &writeFileInputStruct{
			filePath:       job.filePath,
			ifMatch:        job.ifMatch,
			body:           bytes.NewReader(job.body),
			readerAt:       bytes.NewReader(job.body),
			size:           job.size,
			forceSinglePut: true,
		})
	}

	globalsLock("write_commit.go:115:2:(*writeCommitJob).run")
	inode, ok := globals.inodeMap.get(job.inodeNumber)
	control, controlOK := globals.writeCommitControls[job.inodeNumber]
	switch {
	case !ok:
		// The inode carried this contribution to the deferred budget and is now
		// gone, so nothing else can release it. Every other branch leaves a live
		// inode holding it.
		if globals.deferredWriteBytes >= job.control.deferredBytes {
			globals.deferredWriteBytes -= job.control.deferredBytes
		} else {
			globals.deferredWriteBytes = 0
		}
		job.control.deferredBytes = 0
		err = fmt.Errorf("inode %d disappeared during write commit", job.inodeNumber)
	case !controlOK || control != job.control || !control.inFlight:
		err = fmt.Errorf("inode %d has no matching in-flight write commit", job.inodeNumber)
	case control.commitGeneration != job.generation || control.generation != job.generation:
		err = fmt.Errorf("inode %d write generation changed during commit", job.inodeNumber)
	case err == nil:
		inode.applyWriteOutputLocked(job.backend, output)
		promoteCommittedCacheLocked(inode, output.eTag, output.size, bytes.NewReader(job.body), nil)
		if globals.deferredWriteBytes >= control.deferredBytes {
			globals.deferredWriteBytes -= control.deferredBytes
		} else {
			globals.deferredWriteBytes = 0
		}
		inode.writeState = writeState{}
		inode.writeStateActive = false
		inode.touch(nil)
	default:
		inode.sizeInMemory = job.size
		inode.writeState.segments = []writeSegment{{offset: 0, data: control.body}}
		inode.writeState.deferredBytesCounted = control.deferredBytes
		inode.writeStateActive = true
		inode.writeDirty = true
	}
	if controlOK && control == job.control {
		control.err = err
		control.inFlight = false
		if err == nil {
			delete(globals.writeCommitControls, job.inodeNumber)
		}
	}
	job.err = err
	close(job.done)
	if err != nil {
		globals.logger.Printf("[WARN] detached write commit failed for %q: %s", job.filePath, redactSecrets(job.backend, err.Error()))
	}
	globalsUnlock()
}

func writeCommitControlLocked(inodeNumber uint64) *writeCommitControl {
	control := globals.writeCommitControls[inodeNumber]
	if control == nil {
		control = &writeCommitControl{}
		globals.writeCommitControls[inodeNumber] = control
	}
	return control
}

func noteWriteMutationLocked(inode *inodeStruct) {
	writeCommitControlLocked(inode.inodeNumber).generation++
}

// abandonWriteCommitLocked releases a commit that never reached a worker.
//
// prepareSmallWriteCommitLocked marks the control in flight and creates its done
// channel before the job is submitted, and every same-inode operation waits on
// that channel. A failed submit therefore leaves the inode blocked for the life
// of the mount unless the control is released here. The buffered body is kept as
// retry state, matching the failure branch of (*writeCommitJob).run.
func abandonWriteCommitLocked(job *writeCommitJob, cause error) {
	control, ok := globals.writeCommitControls[job.inodeNumber]
	if ok && control == job.control {
		control.inFlight = false
		control.err = cause
		if inode, inodeOK := globals.inodeMap.get(job.inodeNumber); inodeOK {
			inode.sizeInMemory = job.size
			inode.writeState.segments = []writeSegment{{offset: 0, data: control.body}}
			inode.writeState.deferredBytesCounted = control.deferredBytes
			inode.writeStateActive = true
			inode.writeDirty = true
		}
	}
	job.err = cause
	close(job.done)
}

// takeWriteCommitErrorLocked reports a failed detached commit once, then forgets
// it. With flush_on_close false, release acknowledges close(2) before the upload
// runs, so a durability barrier is the only place the application can still learn
// that the upload failed. A commit that later succeeds discards its control and
// this error with it, so a failure that a retry repaired is never reported.
func takeWriteCommitErrorLocked(inodeNumber uint64) error {
	control := globals.writeCommitControls[inodeNumber]
	if control == nil || control.inFlight || control.err == nil {
		return nil
	}
	err := control.err
	control.err = nil
	return err
}

func clearWriteCommitControlLocked(inodeNumber uint64) {
	control := globals.writeCommitControls[inodeNumber]
	if control != nil && !control.inFlight {
		delete(globals.writeCommitControls, inodeNumber)
	}
}

func (inode *inodeStruct) canDetachSmallWriteCommitLocked(backend *backendStruct) bool {
	control := globals.writeCommitControls[inode.inodeNumber]
	return globals.writeCommitPool != nil &&
		backend != nil &&
		backend.backendType == "S3" &&
		inode.writeStateActive &&
		inode.writeDirty &&
		!inode.writeState.streaming &&
		inode.sizeInBackend == 0 &&
		(control == nil || !control.inFlight)
}

func (inode *inodeStruct) prepareSmallWriteCommitLocked(backend *backendStruct) (*writeCommitJob, error) {
	if !inode.canDetachSmallWriteCommitLocked(backend) {
		return nil, nil
	}
	body, err := inode.materializeWriteStateLocked(backend)
	if err != nil {
		return nil, err
	}

	// Keep the materialized image as the retry state. The commit job and inode
	// share this immutable buffer while same-inode operations wait.
	inode.writeState.segments = []writeSegment{{offset: 0, data: body}}
	inode.writeState.parts = nil
	inode.writeState.uploadedParts = nil
	control := writeCommitControlLocked(inode.inodeNumber)
	control.inFlight = true
	control.body = body
	control.commitGeneration = control.generation
	control.deferredBytes = inode.writeState.deferredBytesCounted
	control.done = make(chan struct{})
	control.err = nil

	return &writeCommitJob{
		backend:     backend,
		body:        body,
		control:     control,
		done:        control.done,
		filePath:    inode.objectPath,
		generation:  control.generation,
		ifMatch:     inode.eTag,
		inodeNumber: inode.inodeNumber,
		size:        inode.sizeInMemory,
	}, nil
}

// waitForWriteCommitLocked waits without holding globals.Lock and returns the
// current pageable inode with globals.Lock held again.
func waitForWriteCommitLocked(inodeNumber uint64) (*inodeStruct, error) {
	for {
		control := globals.writeCommitControls[inodeNumber]
		if control == nil || !control.inFlight {
			inode, ok := globals.inodeMap.get(inodeNumber)
			if !ok {
				return nil, fmt.Errorf("inode %d disappeared while waiting for write commit", inodeNumber)
			}
			return inode, nil
		}
		done := control.done
		globalsUnlock()
		<-done
		globalsLock("write_commit.go:288:3:waitForWriteCommitLocked")
	}
}

// flushWriteFileConcurrentlyLocked uses the worker pool for deferred small S3
// objects and falls back to the existing locked path for multipart, overlay,
// and non-S3 writes. It always returns with globals.Lock held.
func (inode *inodeStruct) flushWriteFileConcurrentlyLocked(backend *backendStruct) error {
	inodeNumber := inode.inodeNumber
	if control := globals.writeCommitControls[inodeNumber]; control != nil && control.inFlight {
		current, err := waitForWriteCommitLocked(inodeNumber)
		if err != nil {
			return err
		}
		inode = current
	}
	job, err := inode.prepareSmallWriteCommitLocked(backend)
	if err != nil {
		return err
	}
	if job == nil {
		if err = inode.flushWriteFileLocked(backend); err != nil {
			return err
		}
		// Nothing was dirty, so nothing retried an earlier detached failure.
		return takeWriteCommitErrorLocked(inodeNumber)
	}

	pool := globals.writeCommitPool
	globalsUnlock()
	submitErr := pool.submit(job)
	if submitErr == nil {
		<-job.done
	}
	globalsLock("write_commit.go:322:2:(*inodeStruct).flushWriteFileConcurrentlyLocked")
	if submitErr != nil {
		// No worker will ever complete this job, so nothing else closes the done
		// channel the whole inode is waiting on.
		abandonWriteCommitLocked(job, submitErr)
		return submitErr
	}
	if job.err != nil {
		return job.err
	}
	return takeWriteCommitErrorLocked(inodeNumber)
}
