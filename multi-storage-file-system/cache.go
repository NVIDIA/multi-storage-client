package main

import (
	"container/list"
	"sync"
)

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

	globalsLock("cache.go:22:2:(*cacheLineStruct).fetch")

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
		globalsLock("cache.go:53:3:(*cacheLineStruct).fetch")
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

	globalsLock("cache.go:71:2:(*cacheLineStruct).fetch")
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

	for (uint64(globals.inboundCacheLineList.Len()) + uint64(globals.cleanCacheLineLRU.Len())) >= globals.config.cacheLines {
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
