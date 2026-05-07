package main

import (
	"container/list"
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
		dataCacheLineIndex   uint64
		dataCacheLineTracker *dataCacheLineTrackerStruct
	)

	globals.dataCacheLinesFile, err = os.OpenFile(filepath.Join(globals.cacheDir, DataCacheFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666)
	if err != nil {
		err = fmt.Errorf("os.OpenFile(filepath.Join(globals.cacheDir, DataCacheFileName), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o666) failed: %v", err)
		return
	}

	globals.dataCacheLinesContent, err = syscall.Mmap(
		int(globals.dataCacheLinesFile.Fd()), 0, int(globals.config.cacheLines*globals.config.cacheLineSize),
		syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED,
	)
	if err != nil {
		err = fmt.Errorf("syscall.Mmap(int(globals.dataCacheLinesFile.Fd()),,,,,) failed: %v", err)
		return
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
		dataCacheLineLRU.tail = dataCacheLineTracker.prev
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
// specified `count` data cache lines. As these are typically not available, the caller's
// globals lock will be released but all data cache lines will now be returned in the
// `cacheLineNumbers` slice. The caller would then need to reacquire the globals lock
// and reestablish it's state where some or all of those data cache lines would be
// utilized. The pattern of consuming them would be to read cacheLineNumbers[0] and then
// adjust cacheLineNumbers to now equal cacheLineNumbers[1:].
func allocateDataCacheLines(count uint64) (cacheLineNumbers []uint64) {
	var (
		cacheLineWaiter      sync.WaitGroup
		dataCacheLineTracker *dataCacheLineTrackerStruct
		inode                *inodeStruct
		ok                   bool
	)

	cacheLineNumbers = make([]uint64, 0, count)

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

		dataCacheLineTracker = globals.dataCacheLineInboundLRU.peekHead()
		if dataCacheLineTracker == nil {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.dataCacheLineInboundLRU.peekHead() returned nil")
		}

		cacheLineWaiter.Add(1)
		dataCacheLineTracker.waiters = append(dataCacheLineTracker.waiters, &cacheLineWaiter)

		globalsUnlock()

		cacheLineWaiter.Wait()

		globalsLock("cache.go:317:3:allocateDataCacheLines")
	}
}

// `releaseDataCacheLines` is the companion to `allocateDataCacheLines` that anticipates
// that its caller may ultimately not consume all of the allocated data cache lines. Thus,
// the remaining cacheLineNumbers slice is simply passed to this function. The caller
// must hold the globals lock.
func releaseDataCacheLines(cacheLineNumbers []uint64) {
	var (
		cacheLineNumber uint64
	)

	for _, cacheLineNumber = range cacheLineNumbers {
		globals.dataCacheLineFreeLRU.pushTail(&globals.dataCacheLinesTracker[cacheLineNumber])
	}
}

// `fetch` is run in a goroutine for an allocated cacheLineStruct that
// is to be populated with a portion of the object's contents. Completion of
// the fetch operation is indicated by signaling as done the sync.WaitGroup
// in the cacheLineStruct itself.
func (cacheLine *cacheLineStruct) fetch() {
	var (
		backend        *backendStruct
		err            error
		inode          *inodeStruct
		ok             bool
		readFileInput  *readFileInputStruct
		readFileOutput *readFileOutputStruct
	)

	globalsLock("cache.go:349:2:(*cacheLineStruct).fetch")

	inode, ok = globals.inodeMap.get(cacheLine.inodeNumber)
	if !ok {
		globals.logger.Printf("[WARN] [TODO] (*cacheLineStruct) fetch() needs to handle missing inodeStruct [case 1]")
		cacheLine.state = CacheLineClean
		cacheLine.eTag = ""
		cacheLine.content = make([]byte, 0)
		_ = globals.inboundCacheLineList.Remove(cacheLine.listElement)
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
		cacheLine.notifyWaiters()
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
		offsetCacheLine: cacheLine.lineNumber,
		ifMatch:         "",
	}

	globalsUnlock()

	readFileOutput, err = readFileWrapper(backend.context, readFileInput)
	if err != nil {
		globalsLock("cache.go:380:3:(*cacheLineStruct).fetch")
		globals.logger.Printf("[WARN] [TODO] (*cacheLineStruct) fetch() needs to handle error reading cache line")
		inode, ok = globals.inodeMap.get(cacheLine.inodeNumber)
		if ok {
			inode.inboundCacheLineCount--
		} else {
			globals.logger.Printf("[WARN] [TODO] (*cacheLineStruct) fetch() needs to handle missing inodeStruct [case 2]")
		}
		cacheLine.state = CacheLineClean
		cacheLine.eTag = ""
		cacheLine.content = make([]byte, 0)
		_ = globals.inboundCacheLineList.Remove(cacheLine.listElement)
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
		cacheLine.notifyWaiters()
		globalsUnlock()
		return
	}

	globalsLock("cache.go:398:2:(*cacheLineStruct).fetch")
	inode, ok = globals.inodeMap.get(cacheLine.inodeNumber)
	if ok {
		inode.inboundCacheLineCount--
	} else {
		globals.logger.Printf("[WARN] [TODO] (*cacheLineStruct) fetch() needs to handle missing inodeStruct [case 3]")
	}
	cacheLine.state = CacheLineClean
	cacheLine.eTag = readFileOutput.eTag
	cacheLine.content = readFileOutput.buf
	_ = globals.inboundCacheLineList.Remove(cacheLine.listElement)
	cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
	cacheLine.notifyWaiters()
	globalsUnlock()
}

// `touch` is called while globals.Lock() is held to update the placement of
// a cacheLineStruct on globals.{clean|dirty}CacheLineLRU if it is currently
// on either.
func (cacheLine *cacheLineStruct) touch() {
	switch cacheLine.state {
	case CacheLineInbound:
		// Nothing to do here
	case CacheLineClean:
		globals.cleanCacheLineLRU.Remove(cacheLine.listElement)
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
	case CacheLineOutbound:
		// Nothing to do here
	case CacheLineDirty:
		globals.dirtyCacheLineLRU.Remove(cacheLine.listElement)
		cacheLine.listElement = globals.dirtyCacheLineLRU.PushBack(cacheLine)
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] cacheLine.state (%v) unexpected", cacheLine.state)
	}
}

// `notifyWaiters` is called while holding glohbals.Lock() to notify all those
// in the .waiters slice awaiting a state change of this cacheLine. Upon return,
// // the .waiters slice will be emptied.
func (cacheLine *cacheLineStruct) notifyWaiters() {
	var (
		waiter *sync.WaitGroup
	)

	for _, waiter = range cacheLine.waiters {
		waiter.Done()
	}

	cacheLine.waiters = make([]*sync.WaitGroup, 0, 1)
}

// `cachePrune` is called to immediately attempt to trim globals.cleanCacheLineLRU
// in an attempt to keep the sum of all cache lines at or below the configured cap.
// Note: This call must be made while holding the globals.Lock().
func cachePrune() {
	var (
		cacheLineToEvict *cacheLineStruct
		inode            *inodeStruct
		listElement      *list.Element
		ok               bool
	)

	for (uint64(globals.inboundCacheLineList.Len()) + uint64(globals.cleanCacheLineLRU.Len())) > globals.config.cacheLines {
		listElement = globals.cleanCacheLineLRU.Front()
		if listElement == nil {
			return
		}

		cacheLineToEvict, ok = listElement.Value.(*cacheLineStruct)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] listElement.Value.(*cacheLineStruct) returned !ok")
		}

		_ = globals.cleanCacheLineLRU.Remove(listElement)
		cacheLineToEvict.listElement = nil

		inode, ok = globals.inodeMap.get(cacheLineToEvict.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(cacheLineToEvict.inodeNumber) returned !ok [cachePrune()]")
		}

		_, ok = inode.cacheMap[cacheLineToEvict.lineNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] inode.cache[cacheLineToEvict.lineNumber] returned !ok")
		}
		_, ok = globals.cacheMap[cacheLineToEvict.nonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.cacheMap[cacheLineToEvict.nonce] returned !ok")
		}

		delete(inode.cacheMap, cacheLineToEvict.lineNumber)
		delete(globals.cacheMap, cacheLineToEvict.nonce)
	}
}
