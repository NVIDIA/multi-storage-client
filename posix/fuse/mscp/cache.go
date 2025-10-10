package main

import (
	"fmt"
)

func (cacheLine *cacheLineStruct) fetch() {
	var (
		backend        *backendStruct
		err            error
		inode          *inodeStruct
		ok             bool
		readFileInput  *readFileInputStruct
		readFileOutput *readFileOutputStruct
	)

	globals.Lock()

	inode, ok = globals.inodeMap[cacheLine.inodeNumber]
	if !ok {
		fmt.Printf("[TODO] (*cacheLineStruct) fetch() needs to handle missing inodeStruct\n")
		cacheLine.state = CacheLineClean
		cacheLine.eTag = ""
		cacheLine.content = make([]byte, 0)
		globals.inboundCacheLineCount--
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
		globals.Unlock()
		cacheLine.Done()
		return
	}

	backend = inode.backend

	readFileInput = &readFileInputStruct{
		filePath:        inode.objectPath,
		offsetCacheLine: cacheLine.lineNumber,
		ifMatch:         "",
	}

	globals.Unlock()

	readFileOutput, err = backend.context.readFile(readFileInput)
	if err != nil {
		globals.Lock()
		fmt.Printf("[TODO] (*cacheLineStruct) fetch() needs to handle error reading cache line\n")
		cacheLine.state = CacheLineClean
		cacheLine.eTag = ""
		cacheLine.content = make([]byte, 0)
		globals.inboundCacheLineCount--
		cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)
		globals.Unlock()
		cacheLine.Done()
		return
	}

	globals.Lock()

	cacheLine.state = CacheLineClean
	cacheLine.eTag = readFileOutput.eTag
	cacheLine.content = readFileOutput.buf

	globals.inboundCacheLineCount--
	cacheLine.listElement = globals.cleanCacheLineLRU.PushBack(cacheLine)

	globals.Unlock()

	cacheLine.Done()
}

// touch is called while globals.Lock() is held to update the placement of
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
		globals.logger.Fatalf("cacheLine.state (%v) unexpected", cacheLine.state)
	}
}
