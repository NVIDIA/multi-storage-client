package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
)

const (
	DataCacheFileName = "MSFS_data_cache"
)

func dataCacheUp() (err error) {
	var (
		dataCacheLineContentSize uint64
		dataCacheLineIndex       uint64
		dataCacheLineTracker     *dataCacheLineTrackerStruct
	)

	dataCacheLineContentSize = globals.config.cacheLines * globals.config.cacheLineSize

	if globals.config.mappedCache {
		globals.dataCacheLinesFile, err = os.OpenFile(filepath.Join(globals.cacheDir, DataCacheFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			err = fmt.Errorf("os.OpenFile(filepath.Join(globals.cacheDir, DataCacheFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600) failed: %v", err)
			return
		}

		err = globals.dataCacheLinesFile.Truncate(int64(dataCacheLineContentSize))
		if err != nil {
			err = fmt.Errorf("globals.dataCacheLinesFile.Truncate(dataCacheLineContentSize) failed: %v", err)
			return
		}

		globals.dataCacheLinesContent, err = syscall.Mmap(
			int(globals.dataCacheLinesFile.Fd()), 0, int(dataCacheLineContentSize),
			syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
		)
		if err != nil {
			err = fmt.Errorf("syscall.Mmap(int(globals.dataCacheLinesFile.Fd()),,,,,) failed: %v", err)
			return
		}
	} else {
		globals.dataCacheLinesFile = nil
		globals.dataCacheLinesContent = make([]byte, dataCacheLineContentSize)
	}

	globals.dataCacheLinesTracker = make([]dataCacheLineTrackerStruct, globals.config.cacheLines)

	globals.dataCacheLineFreeLRU = dataCacheLineLRUStruct{
		head:     0, // not yet applicable
		tail:     0, // not yet applicable
		lruCount: 0,
		state:    CacheLineFree,
	}

	for dataCacheLineIndex = range globals.config.cacheLines {
		dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineIndex]

		dataCacheLineTracker.pos = dataCacheLineIndex
		dataCacheLineTracker.state = CacheLineNotNotOnLRU
		dataCacheLineTracker.waiters = make([]*sync.WaitGroup, 0, 1)
		dataCacheLineTracker.contentStart = dataCacheLineIndex * globals.config.cacheLineSize
		dataCacheLineTracker.contentLength = 0     // not yet applicable
		dataCacheLineTracker.contentGeneration = 0 // not yet applicable
		dataCacheLineTracker.inodeNumber = 0       // not yet applicable
		dataCacheLineTracker.lineNumber = 0        // not yet applicable
		dataCacheLineTracker.eTag = ""             // not yet applicable

		globals.dataCacheLineFreeLRU.pushTail(dataCacheLineTracker)
	}

	globals.dataCacheLineInboundLRU = dataCacheLineLRUStruct{
		head:     0, // not yet applicable
		tail:     0, // not yet applicable
		lruCount: 0,
		state:    CacheLineInbound,
	}

	globals.dataCacheLineCleanLRU = dataCacheLineLRUStruct{
		head:     0, // not yet applicable
		tail:     0, // not yet applicable
		lruCount: 0,
		state:    CacheLineClean,
	}

	globals.dataCacheLineOutboundLRU = dataCacheLineLRUStruct{
		head:     0, // not yet applicable
		tail:     0, // not yet applicable
		lruCount: 0,
		state:    CacheLineOutbound,
	}

	globals.dataCacheLineDirtyLRU = dataCacheLineLRUStruct{
		head:     0, // not yet applicable
		tail:     0, // not yet applicable
		lruCount: 0,
		state:    CacheLineDirty,
	}

	return
}

func dataCacheDown() (err error) {
	globals.dataCacheActivityWG.Wait()

	if globals.config.mappedCache {
		err = syscall.Munmap(globals.dataCacheLinesContent)
		if err != nil {
			err = fmt.Errorf("syscall.Munmap(globals.dataCacheLinesContent) failed: %v", err)
			return
		}

		err = globals.dataCacheLinesFile.Close()
		if err != nil {
			err = fmt.Errorf("globals.dataCacheLinesFile.Close() failed: %v", err)
			return
		}
	}

	globals.dataCacheLinesContent = nil
	globals.dataCacheLinesFile = nil
	globals.dataCacheLinesTracker = nil

	return
}

func (dataCacheLineLRU *dataCacheLineLRUStruct) pushTail(dataCacheLineTracker *dataCacheLineTrackerStruct) {
	// if dataCacheLineTracker.state != CacheLineNotNotOnLRU {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.state(%v) != CacheLineNotNotOnLRU(%v)", dataCacheLineTracker.state, CacheLineNotNotOnLRU)
	// }

	dataCacheLineTracker.next = 0 // not yet applicable
	dataCacheLineTracker.state = dataCacheLineLRU.state

	if dataCacheLineLRU.lruCount == 0 {
		dataCacheLineTracker.prev = 0 // not yet applicable

		dataCacheLineLRU.head = dataCacheLineTracker.pos
		dataCacheLineLRU.tail = dataCacheLineTracker.pos
		dataCacheLineLRU.lruCount = 1
	} else {
		globals.dataCacheLinesTracker[dataCacheLineLRU.tail].next = dataCacheLineTracker.pos

		dataCacheLineTracker.prev = dataCacheLineLRU.tail

		dataCacheLineLRU.tail = dataCacheLineTracker.pos
		dataCacheLineLRU.lruCount++
	}
}

func (dataCacheLineLRU *dataCacheLineLRUStruct) peekHead() (dataCacheLineTracker *dataCacheLineTrackerStruct) {
	if dataCacheLineLRU.lruCount == 0 {
		dataCacheLineTracker = nil
		return
	}

	dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineLRU.head]

	// if dataCacheLineTracker.pos != dataCacheLineLRU.head {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.pos(%v) != dataCacheLineLRU.head(%v)", dataCacheLineTracker.pos, dataCacheLineLRU.head)
	// }
	// if dataCacheLineTracker.state != dataCacheLineLRU.state {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.state(%v) != dataCacheLineLRU.state(%v)", dataCacheLineTracker.state, dataCacheLineLRU.state)
	// }

	return
}

func (dataCacheLineLRU *dataCacheLineLRUStruct) popHead() (dataCacheLineTracker *dataCacheLineTrackerStruct) {
	if dataCacheLineLRU.lruCount == 0 {
		dataCacheLineTracker = nil
		return
	}

	dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineLRU.head]

	// if dataCacheLineTracker.pos != dataCacheLineLRU.head {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.pos(%v) != dataCacheLineLRU.head(%v)", dataCacheLineTracker.pos, dataCacheLineLRU.head)
	// }
	// if dataCacheLineTracker.state != dataCacheLineLRU.state {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.state(%v) != dataCacheLineLRU.state(%v)", dataCacheLineTracker.state, dataCacheLineLRU.state)
	// }

	if dataCacheLineLRU.lruCount == 1 {
		dataCacheLineLRU.head = 0 // not yet applicable
		dataCacheLineLRU.tail = 0 // not yet applicable
		dataCacheLineLRU.lruCount = 0
	} else {
		dataCacheLineLRU.head = dataCacheLineTracker.next
		globals.dataCacheLinesTracker[dataCacheLineLRU.head].prev = 0 // not yet applicable
		dataCacheLineLRU.lruCount--
	}

	dataCacheLineTracker.next = 0 // not yet applicable
	dataCacheLineTracker.prev = 0 // not yet applicable
	dataCacheLineTracker.state = CacheLineNotNotOnLRU

	return
}

func (dataCacheLineLRU *dataCacheLineLRUStruct) popTail() (dataCacheLineTracker *dataCacheLineTrackerStruct) {
	if dataCacheLineLRU.lruCount == 0 {
		dataCacheLineTracker = nil
		return
	}

	dataCacheLineTracker = &globals.dataCacheLinesTracker[dataCacheLineLRU.tail]

	// if dataCacheLineTracker.pos != dataCacheLineLRU.tail {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.pos(%v) != dataCacheLineLRU.tail(%v)", dataCacheLineTracker.pos, dataCacheLineLRU.tail)
	// }
	// if dataCacheLineTracker.state != dataCacheLineLRU.state {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.state(%v) != dataCacheLineLRU.state(%v)", dataCacheLineTracker.state, dataCacheLineLRU.state)
	// }

	if dataCacheLineLRU.lruCount == 1 {
		dataCacheLineLRU.head = 0 // not yet applicable
		dataCacheLineLRU.tail = 0 // not yet applicable
		dataCacheLineLRU.lruCount = 0
	} else {
		dataCacheLineLRU.tail = dataCacheLineTracker.prev
		globals.dataCacheLinesTracker[dataCacheLineLRU.tail].next = 0 // not yet applicable
		dataCacheLineLRU.lruCount--
	}

	dataCacheLineTracker.next = 0 // not yet applicable
	dataCacheLineTracker.prev = 0 // not yet applicable
	dataCacheLineTracker.state = CacheLineNotNotOnLRU

	return
}

func (dataCacheLineLRU *dataCacheLineLRUStruct) popThis(dataCacheLineTracker *dataCacheLineTrackerStruct) {
	// if dataCacheLineLRU.lruCount == 0 {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineLRU.lruCount == 0")
	// }
	// if dataCacheLineTracker.state != dataCacheLineLRU.state {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.state(%v) != dataCacheLineLRU.state(%v)", dataCacheLineTracker.state, dataCacheLineLRU.state)
	// }

	if dataCacheLineLRU.lruCount == 1 {
		dataCacheLineLRU.head = 0 // not yet applicable
		dataCacheLineLRU.tail = 0 // not yet applicable
		dataCacheLineLRU.lruCount = 0
	} else {
		switch dataCacheLineTracker.pos {
		case dataCacheLineLRU.head:
			dataCacheLineLRU.head = dataCacheLineTracker.next
			globals.dataCacheLinesTracker[dataCacheLineLRU.head].prev = 0 // not yet applicable
		case dataCacheLineLRU.tail:
			dataCacheLineLRU.tail = dataCacheLineTracker.prev
			globals.dataCacheLinesTracker[dataCacheLineLRU.tail].next = 0 // not yet applicable
		default:
			globals.dataCacheLinesTracker[dataCacheLineTracker.prev].next = dataCacheLineTracker.next
			globals.dataCacheLinesTracker[dataCacheLineTracker.next].prev = dataCacheLineTracker.prev
		}

		dataCacheLineLRU.lruCount--
	}

	dataCacheLineTracker.next = 0 // not yet applicable
	dataCacheLineTracker.prev = 0 // not yet applicable
	dataCacheLineTracker.state = CacheLineNotNotOnLRU
}

func (dataCacheLineLRU *dataCacheLineLRUStruct) touchThis(dataCacheLineTracker *dataCacheLineTrackerStruct) {
	// if dataCacheLineLRU.lruCount == 0 {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineLRU.lruCount == 0")
	// }
	// if dataCacheLineTracker.state != dataCacheLineLRU.state {
	// 	dumpStack()
	// 	globals.logger.Fatalf("dataCacheLineTracker.state(%v) != dataCacheLineLRU.state(%v)", dataCacheLineTracker.state, dataCacheLineLRU.state)
	// }

	if dataCacheLineTracker.pos == dataCacheLineLRU.tail {
		return
	}

	if dataCacheLineTracker.pos == dataCacheLineLRU.head {
		dataCacheLineLRU.head = dataCacheLineTracker.next
		globals.dataCacheLinesTracker[dataCacheLineTracker.next].prev = 0 // not yet applicable
	} else {
		globals.dataCacheLinesTracker[dataCacheLineTracker.prev].next = dataCacheLineTracker.next
		globals.dataCacheLinesTracker[dataCacheLineTracker.next].prev = dataCacheLineTracker.prev
	}

	globals.dataCacheLinesTracker[dataCacheLineLRU.tail].next = dataCacheLineTracker.pos

	dataCacheLineTracker.next = 0 // not yet applicable
	dataCacheLineTracker.prev = dataCacheLineLRU.tail

	dataCacheLineLRU.tail = dataCacheLineTracker.pos
}

// `allocateDataCacheLines` is called while holding the globals lock to provision the
// specified `count` data cache lines. Ideally, the required number of data cache lines
// are available (i.e. from either the Free or Clean LRUs) in which case `neededToBlock`
// will be false. But if data cache lines needed to be obtained by simply awaiting their
// availability (as in the case where Inbound data cache lines transition to Clean) or
// more extensive efforts are needed, the value of `neededToBlock` will be true and the
// caller's globals lock will have been released requiring their logic to restart.
func allocateDataCacheLines(count uint64) (cacheLineNumbers []uint64, neededToBlock bool) {
	var (
		cacheLineWaiter      sync.WaitGroup
		dataCacheLineTracker *dataCacheLineTrackerStruct
		inode                *inodeStruct
		ok                   bool
	)

	cacheLineNumbers = make([]uint64, 0, count)
	neededToBlock = false

	for {
		for uint64(len(cacheLineNumbers)) < count {
			dataCacheLineTracker = globals.dataCacheLineFreeLRU.popHead()
			if dataCacheLineTracker == nil {
				break
			}

			cacheLineNumbers = append(cacheLineNumbers, dataCacheLineTracker.pos)
		}

		for uint64(len(cacheLineNumbers)) < count {
			dataCacheLineTracker = globals.dataCacheLineCleanLRU.popHead()
			if dataCacheLineTracker == nil {
				break
			}

			inode, ok = globals.inodeMap.get(dataCacheLineTracker.inodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.get(dataCacheLineTracker.inodeNumber[%v]) returned !ok", dataCacheLineTracker.inodeNumber)
			}

			delete(inode.cacheMap, dataCacheLineTracker.lineNumber)

			cacheLineNumbers = append(cacheLineNumbers, dataCacheLineTracker.pos)
		}

		if uint64(len(cacheLineNumbers)) == count {
			// Fortunately, we didn't need to await any other data cache activity

			globalsUnlock()
			return
		}

		// We need to pause while one or more data cache lines in .state CacheLineInbound transition

		neededToBlock = true

		dataCacheLineTracker = globals.dataCacheLineInboundLRU.peekHead()
		if dataCacheLineTracker == nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.dataCacheLineInboundLRU.peekHead() returned nil")
		}

		cacheLineWaiter.Add(1)
		dataCacheLineTracker.waiters = append(dataCacheLineTracker.waiters, &cacheLineWaiter)

		globalsUnlock()

		cacheLineWaiter.Wait()

		globalsLock("cache.go:377:3:allocateDataCacheLines")
	}
}

// `releaseDataCacheLines` is the companion to `allocateDataCacheLines` that anticipates
// that its caller may ultimately not consume all of the allocated data cache lines. Thus,
// the remaining cacheLineNumbers slice is simply passed to this function. The caller
// must hold the globals lock.
func releaseDataCacheLines(cacheLineNumbers []uint64) {
	var (
		cacheLineNumber      uint64
		dataCacheLineTracker *dataCacheLineTrackerStruct
	)

	for _, cacheLineNumber = range cacheLineNumbers {
		dataCacheLineTracker = &globals.dataCacheLinesTracker[cacheLineNumber]
		dataCacheLineTracker.free()
	}
}

// `free` resets a data cache line that is not currently on any LRU and returns
// it to the Free LRU. The caller must hold the globals lock.
func (dataCacheLineTracker *dataCacheLineTrackerStruct) free() {
	dataCacheLineTracker.contentLength = 0
	dataCacheLineTracker.contentGeneration++
	dataCacheLineTracker.inodeNumber = 0 // not yet applicable
	dataCacheLineTracker.lineNumber = 0  // not yet applicable
	dataCacheLineTracker.eTag = ""       // not yet applicable
	dataCacheLineTracker.waiters = make([]*sync.WaitGroup, 0, 1)
	globals.dataCacheLineFreeLRU.pushTail(dataCacheLineTracker)
}

// `fetch` is run in a goroutine for an allocated dataCacheLineTrackerStruct that
// is to be populated with a portion of the object's contents. Completion of the
// fetch operation is indicated by notifying the tracker waiters.
func (dataCacheLineTracker *dataCacheLineTrackerStruct) fetch() {
	var (
		backend        *backendStruct
		content        []byte
		err            error
		inode          *inodeStruct
		ok             bool
		readFileInput  *readFileInputStruct
		readFileOutput *readFileOutputStruct
	)

	defer globals.dataCacheActivityWG.Done()

	globalsLock("cache.go:425:2:(*dataCacheLineTrackerStruct).fetch")

	inode, ok = globals.inodeMap.get(dataCacheLineTracker.inodeNumber)
	if !ok {
		globals.logger.Printf("[WARN] [TODO] (*dataCacheLineTrackerStruct) fetch() needs to handle missing inodeStruct [case 1]")
		globals.dataCacheLineInboundLRU.popThis(dataCacheLineTracker)
		dataCacheLineTracker.free()
		dataCacheLineTracker.notifyWaiters()
		globalsUnlock()
		return
	}

	backend, ok = globals.backendMap[inode.backendNonce]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.backendMap[inode.backendNonce] returned !ok")
	}

	readFileInput = &readFileInputStruct{
		filePath:        inode.objectPath,
		offsetCacheLine: dataCacheLineTracker.lineNumber,
		ifMatch:         "",
	}

	globalsUnlock()

	readFileOutput, err = readFileWrapper(backend.context, readFileInput)
	if err == nil {
		content = globals.dataCacheLinesContent[dataCacheLineTracker.contentStart : dataCacheLineTracker.contentStart+globals.config.cacheLineSize]
		dataCacheLineTracker.contentLength = uint64(copy(content, readFileOutput.buf))
	}

	globalsLock("cache.go:457:2:(*dataCacheLineTrackerStruct).fetch")
	inode, ok = globals.inodeMap.get(dataCacheLineTracker.inodeNumber)
	if ok {
		inode.inboundCacheLineCount--
	} else {
		globals.logger.Printf("[WARN] [TODO] (*dataCacheLineTrackerStruct) fetch() needs to handle missing inodeStruct [case 2]")
		globals.dataCacheLineInboundLRU.popThis(dataCacheLineTracker)
		dataCacheLineTracker.free()
		dataCacheLineTracker.notifyWaiters()
		globalsUnlock()
		return
	}

	globals.dataCacheLineInboundLRU.popThis(dataCacheLineTracker)
	dataCacheLineTracker.contentGeneration++

	if err != nil {
		globals.logger.Printf("[WARN] [TODO] (*dataCacheLineTrackerStruct) fetch() needs to handle error reading cache line")
		dataCacheLineTracker.contentLength = 0
		dataCacheLineTracker.eTag = ""
	} else {
		dataCacheLineTracker.eTag = readFileOutput.eTag
	}

	globals.dataCacheLineCleanLRU.pushTail(dataCacheLineTracker)
	dataCacheLineTracker.notifyWaiters()
	globalsUnlock()
}

// `touch` is called while globals.Lock() is held to update the placement of
// a dataCacheLineTrackerStruct on the state-corresponding LRU.
func (dataCacheLineTracker *dataCacheLineTrackerStruct) touch() {
	switch dataCacheLineTracker.state {
	case CacheLineInbound:
		// Nothing to do here
	case CacheLineClean:
		globals.dataCacheLineCleanLRU.touchThis(dataCacheLineTracker)
	case CacheLineOutbound:
		// Nothing to do here
	case CacheLineDirty:
		globals.dataCacheLineDirtyLRU.touchThis(dataCacheLineTracker)
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] dataCacheLineTracker.state (%v) unexpected", dataCacheLineTracker.state)
	}
}

// `notifyWaiters` is called while holding globals.Lock() to notify all those
// in the .waiters slice awaiting a state change of this data cache line. Upon
// return, the .waiters slice will be emptied.
func (dataCacheLineTracker *dataCacheLineTrackerStruct) notifyWaiters() {
	var (
		waiter *sync.WaitGroup
	)

	for _, waiter = range dataCacheLineTracker.waiters {
		waiter.Done()
	}

	dataCacheLineTracker.waiters = make([]*sync.WaitGroup, 0, 1)
}
