package main

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"syscall"
	"time"
)

// `initFS` initializes the root of the FUSE file system.
func initFS() {
	var (
		err              error
		fuseRootDirInode *inodeStruct
		ok               bool
		timeNow          time.Time
	)

	globalsLock("fs.go:24:2:initFS")

	globals.backendMap = make(map[uint64]*backendStruct)

	globals.lastNonce = FUSERootDirInodeNumber

	globals.cacheDir, err = os.MkdirTemp(globals.config.cacheDirPath, "MSFS_")
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] os.MkdirTemp(globals.config.cacheDirPath, \"MSFS\") failed: %v", err)
	}
	globals.logger.Printf("[INFO] cache dir: \"%s\"", globals.cacheDir)

	err = dataCacheUp()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] dataCacheUp() failed: %v", err)
	}

	err = metadataCacheUp()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] metadataCacheUp() failed: %v", err)
	}

	globals.inodeMap = inodeNumberToInodeStructMapStructCreate("globals.inodeMap", globals.config.inodeMapKeysPerPageMax, globals.config.inodeMapPageEvictLowLimit, globals.config.inodeMapPageEvictHighLimit, globals.config.inodeMapPageDirtyFlushTrigger, globals.config.inodeMapFlushedPerGC)
	globals.inodeEvictionQueue = xTimeInodeNumberSetStructCreate("globals.inodeEvictionQueue", globals.config.inodeEvictionQueueKeysPerPageMax, globals.config.inodeEvictionQueuePageEvictLowLimit, globals.config.inodeEvictionQueuePageEvictHighLimit, globals.config.inodeEvictionQueuePageDirtyFlushTrigger, globals.config.inodeEvictionQueueFlushedPerGC)
	globals.physChildDirEntryMap = parentInodeNumberChildBasenameToChildInodeNumberStructCreate("globals.physChildDirEntryMap", globals.config.physChildDirEntryMapKeysPerPageMax, globals.config.physChildDirEntryMapPageEvictLowLimit, globals.config.physChildDirEntryMapPageEvictHighLimit, globals.config.physChildDirEntryMapPageDirtyFlushTrigger, globals.config.physChildDirEntryMapFlushedPerGC)
	globals.virtChildDirEntryMap = parentInodeNumberChildBasenameToChildInodeNumberStructCreate("globals.virtChildDirEntryMap", globals.config.virtChildDirEntryMapKeysPerPageMax, globals.config.virtChildDirEntryMapPageEvictLowLimit, globals.config.virtChildDirEntryMapPageEvictHighLimit, globals.config.virtChildDirEntryMapPageDirtyFlushTrigger, globals.config.virtChildDirEntryMapFlushedPerGC)

	if globals.config.processMemoryLimit > 0 {
		_ = debug.SetMemoryLimit(int64(globals.config.processMemoryLimit))
	}

	timeNow = time.Now()

	fuseRootDirInode = &inodeStruct{
		inodeNumber:            FUSERootDirInodeNumber,
		inodeType:              FUSERootDir,
		backendNonce:           0,
		parentInodeNumber:      FUSERootDirInodeNumber,
		isVirt:                 true,
		objectPath:             "",
		basename:               "",
		sizeInBackend:          0,
		sizeInMemory:           0,
		eTag:                   "",
		mode:                   uint32(syscall.S_IFDIR | globals.config.dirPerm),
		mTime:                  timeNow,
		xTime:                  time.Time{},
		isPrefetchInProgress:   false,
		cacheMap:               nil,
		inboundCacheLineCount:  0,
		outboundCacheLineCount: 0,
		dirtyCacheLineCount:    0,
		fhSet:                  make(map[uint64]struct{}),
		pendingDelete:          false,
	}

	ok = globals.inodeMap.put(fuseRootDirInode)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.put(fuseRootDirInode) returned !ok")
	}

	ok = globals.virtChildDirEntryMap.put(FUSERootDirInodeNumber, DotDirEntryBasename, FUSERootDirInodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(FUSERootDirInodeNumber, DotDirEntryBasename, FUSERootDirInodeNumber) returned !ok")
	}
	ok = globals.virtChildDirEntryMap.put(FUSERootDirInodeNumber, DotDotDirEntryBasename, FUSERootDirInodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(FUSERootDirInodeNumber, DotDotDirEntryBasename, FUSERootDirInodeNumber) returned !ok")
	}

	globals.inodeEvictorContext, globals.inodeEvictorCancelFunc = context.WithCancel(context.Background())
	globals.inodeEvictorWaitGroup.Go(inodeEvictor)

	globals.inboundCacheLineList = list.New()
	globals.cleanCacheLineLRU = list.New()
	globals.outboundCacheLineList = list.New()
	globals.dirtyCacheLineLRU = list.New()

	globals.cacheMap = make(map[uint64]*cacheLineStruct, globals.config.cacheLines)
	globals.fhMap = make(map[uint64]*fhStruct)

	globals.fissionMetrics = newFissionMetrics()
	globals.backendMetrics = newBackendMetrics()

	globalsUnlock()
}

// `drainFS` awaits all backend/asynchronous traffic to complete before
func drainFS() {
	var (
		backend *backendStruct
		dirName string
		err     error
	)

	globals.inodeEvictorCancelFunc()
	globals.inodeEvictorWaitGroup.Wait()

	globalsLock("fs.go:122:2:drainFS")

	for dirName, backend = range globals.config.backends {
		globals.backendsToUnmount[dirName] = backend
	}

	processToUnmountListAlreadyLocked()

	err = metadataCacheDown()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] metadataCacheDown() failed: %v", err)
	}

	err = dataCacheDown()
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL] dataCacheDown() failed: %v", err)
	}

	err = os.RemoveAll(globals.cacheDir)
	if err != nil {
		dumpStack()
		globals.logger.Fatalf("[FATAL]s.RemoveAll(globals.cacheDir:\"%s\") failed: %v", globals.cacheDir, err)
	}
	globals.logger.Printf("[INFO] cache dir (\"%s\") removed", globals.cacheDir)

	globalsUnlock()
}

// `processToMountList` creates a backend subdirectory of the FUSE
// file system's root directory that maps to each backend on the
// globals.backendsToMount list.
func processToMountList() {
	var (
		backend *backendStruct
		dirName string
		err     error
		ok      bool
		timeNow time.Time
	)

	globalsLock("fs.go:158:2:processToMountList")

	timeNow = time.Now()

	for dirName, backend = range globals.backendsToMount {
		delete(globals.backendsToMount, dirName)

		err = backend.setupContext()
		if err != nil {
			globals.logger.Printf("[WARN] unable to setup backend context: %s (err: %v) [skipping]", dirName, err)
			continue
		}

		backend.nonce = fetchNonce()

		backend.inode = &inodeStruct{
			inodeNumber:            fetchNonce(),
			inodeType:              BackendRootDir,
			backendNonce:           backend.nonce,
			parentInodeNumber:      FUSERootDirInodeNumber,
			isVirt:                 true,
			objectPath:             "",
			basename:               dirName,
			sizeInBackend:          0,
			sizeInMemory:           0,
			eTag:                   "",
			mode:                   uint32(syscall.S_IFDIR | backend.dirPerm),
			mTime:                  timeNow,
			xTime:                  time.Time{},
			isPrefetchInProgress:   false,
			cacheMap:               nil,
			inboundCacheLineCount:  0,
			outboundCacheLineCount: 0,
			dirtyCacheLineCount:    0,
			fhSet:                  make(map[uint64]struct{}),
			pendingDelete:          false,
		}

		ok = globals.inodeMap.put(backend.inode)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.put(backend.inode) returned !ok")
		}

		ok = globals.virtChildDirEntryMap.put(FUSERootDirInodeNumber, backend.inode.basename, backend.inode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(FUSERootDirInodeNumber, backend.inode.basename[\"%s\"], backend.inode.inodeNumber) returned !ok", backend.inode.basename)
		}

		ok = globals.virtChildDirEntryMap.put(backend.inode.inodeNumber, DotDirEntryBasename, backend.inode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(backend.inode.inodeNumber, DotDirEntryBasename, backend.inode.inodeNumber) returned !ok")
		}
		ok = globals.virtChildDirEntryMap.put(backend.inode.inodeNumber, DotDotDirEntryBasename, FUSERootDirInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(backend.inode.inodeNumber, DotDotDirEntryBasename, FUSERootDirInodeNumber) returned !ok")
		}

		backend.fissionMetrics = newFissionMetrics()
		backend.backendMetrics = newBackendMetrics()

		backend.mounted = true

		globals.config.backends[dirName] = backend
		globals.backendMap[backend.nonce] = backend
	}

	globalsUnlock()
}

// `processToUnmountList` is called to remove each backend subdirectory of the FUSE
// file system's root directory found on the globals.backendsToUnmount list.
func processToUnmountList() {
	globalsLock("fs.go:234:2:processToUnmountList")
	processToUnmountListAlreadyLocked()
	globalsUnlock()
}

// `processToUnmountListAlreadyLocked` is called while globals.Lock() is held to
// remove each backend subdirectory of the FUSE file system's root directory found
// on the globals.backendsToUnmount list.
func processToUnmountListAlreadyLocked() {
	var (
		backend *backendStruct
		dirName string
		ok      bool
	)

	for dirName, backend = range globals.backendsToUnmount {
		delete(globals.backendsToUnmount, dirName)

		backend.inode.emptyChildInodes()

		ok = globals.virtChildDirEntryMap.delete(FUSERootDirInodeNumber, backend.dirName)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(FUSERootDirInodeNumber, backend.dirName[\"%s\"]) returned !ok", backend.dirName)
		}

		ok = globals.inodeMap.delete(backend.inode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(backend.inode.inodeNumber) returned !ok")
		}

		backend.mounted = false

		delete(globals.config.backends, dirName)
		delete(globals.backendMap, backend.nonce)
	}
}

// `emptyChildInodes` is called to remove all child inodes.
func (parentInode *inodeStruct) emptyChildInodes() {
	var (
		childInode                           *inodeStruct
		childInodeBasename                   string
		childInodeNumber                     uint64
		parentInodePhysChildDirEntryMapIndex uint64
		parentInodePhysChildDirEntryMapLimit uint64
		parentInodePhysChildDirEntryMapStart uint64
		parentInodeVirtChildDirEntryMapIndex uint64
		parentInodeVirtChildDirEntryMapLimit uint64
		parentInodeVirtChildDirEntryMapStart uint64
		ok                                   bool
	)

	parentInodePhysChildDirEntryMapStart, parentInodePhysChildDirEntryMapLimit = globals.physChildDirEntryMap.getIndexRange(parentInode.inodeNumber)

	if parentInodePhysChildDirEntryMapStart < parentInodePhysChildDirEntryMapLimit {
		parentInodePhysChildDirEntryMapIndex = parentInodePhysChildDirEntryMapLimit

		for {
			parentInodePhysChildDirEntryMapIndex--

			// for parentInodePhysChildDirEntryMapIndex = parentInodePhysChildDirEntryMapLimit - 1; parentInodePhysChildDirEntryMapIndex >= parentInodePhysChildDirEntryMapStart; parentInodePhysChildDirEntryMapIndex-- {
			_, childInodeBasename, childInodeNumber, ok = globals.physChildDirEntryMap.getByIndex(parentInodePhysChildDirEntryMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.getByIndex(parentInodePhysChildDirEntryMapIndex) returned !ok")
			}

			childInode, ok = globals.inodeMap.get(childInodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok [case physChildDirEntryMap]")
			}

			if childInode.inodeType == PseudoDir {
				childInode.emptyChildInodes()
			}

			if !childInode.xTime.IsZero() {
				ok = globals.inodeEvictionQueue.remove(childInode)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(childInode) returned !ok [case physChildDirEntryMap]")
				}
			}

			ok = globals.inodeMap.delete(childInodeNumber)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(childInodeNumber) returned !ok [case physChildDirEntryMap]")
			}

			ok = globals.physChildDirEntryMap.delete(parentInode.inodeNumber, childInodeBasename)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.delete(parentInode.inodeNumber, childInodeBasename) returned !ok")
			}

			if parentInodePhysChildDirEntryMapIndex == parentInodePhysChildDirEntryMapStart {
				break
			}
		}
	}

	parentInodeVirtChildDirEntryMapStart, parentInodeVirtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(parentInode.inodeNumber)

	if parentInodeVirtChildDirEntryMapStart < parentInodeVirtChildDirEntryMapLimit {
		parentInodeVirtChildDirEntryMapIndex = parentInodeVirtChildDirEntryMapLimit

		for {
			parentInodeVirtChildDirEntryMapIndex--

			// for parentInodeVirtChildDirEntryMapIndex = parentInodeVirtChildDirEntryMapLimit - 1; parentInodeVirtChildDirEntryMapIndex >= parentInodeVirtChildDirEntryMapStart; parentInodeVirtChildDirEntryMapIndex-- {
			_, childInodeBasename, childInodeNumber, ok = globals.virtChildDirEntryMap.getByIndex(parentInodeVirtChildDirEntryMapIndex)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.getByIndex(parentInodeVirtChildDirEntryMapIndex) returned !ok")
			}

			if (childInodeBasename != DotDirEntryBasename) && (childInodeBasename != DotDotDirEntryBasename) {
				childInode, ok = globals.inodeMap.get(childInodeNumber)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok [case virtChildDirEntryMap]")
				}

				if childInode.inodeType == PseudoDir {
					childInode.emptyChildInodes()
				}

				if !childInode.xTime.IsZero() {
					ok = globals.inodeEvictionQueue.remove(childInode)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(childInode) returned !ok [case virtChildDirEntryMap]")
					}
				}

				ok = globals.inodeMap.delete(childInodeNumber)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(childInodeNumber) returned !ok [case virtChildDirEntryMap]")
				}
			}

			ok = globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, childInodeBasename)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, childInodeBasename) returned !ok")
			}

			if parentInodeVirtChildDirEntryMapIndex == parentInodeVirtChildDirEntryMapStart {
				break
			}
		}
	}
}

// `convertToPhysInodeIfNecessary` is called while globals.Lock() is held to convert
// the supplied inode from "virt" to "phys" if necessary. It is the caller's responsibility
// to ensure that the directory path leading down to this now assuredly "phys" inode has
// already been ensured to be "phys".
func (childInode *inodeStruct) convertToPhysInodeIfNecessary() {
	var (
		ok bool
	)

	if !childInode.isVirt || ((childInode.inodeType != FileObject) && (childInode.inodeType != PseudoDir)) {
		return
	}

	if !childInode.xTime.IsZero() {
		ok = globals.inodeEvictionQueue.remove(childInode)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(childInode) returned !ok")
		}
		childInode.xTime = time.Time{}
	}

	childInode.isVirt = false

	ok = globals.virtChildDirEntryMap.delete(childInode.parentInodeNumber, childInode.basename)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(childInode.parentInodeNumber, childInode.basename) returned !ok")
	}

	ok = globals.physChildDirEntryMap.put(childInode.parentInodeNumber, childInode.basename, childInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.put(childInode.parentInodeNumber, childInode.basename, childInode.inodeNumber) returned !ok")
	}

	childInode.touch(nil)
}

// `createPseudoDirInode` is called while globals.Lock() is held to create a new PsuedoDir inodeStruct.
func (parentInode *inodeStruct) createPseudoDirInode(isVirt bool, basename string) (pseudoDirInode *inodeStruct) {
	var (
		backend *backendStruct
		ok      bool
		timeNow = time.Now()
	)

	backend, ok = globals.backendMap[parentInode.backendNonce]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce] returned !ok")
	}

	pseudoDirInode = &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         PseudoDir,
		backendNonce:      backend.nonce,
		parentInodeNumber: parentInode.inodeNumber,
		isVirt:            isVirt,
		// objectPath: filled in below
		basename:               basename,
		sizeInBackend:          0,
		sizeInMemory:           0,
		eTag:                   "",
		mode:                   uint32(syscall.S_IFDIR | backend.dirPerm),
		mTime:                  timeNow,
		xTime:                  time.Time{},
		isPrefetchInProgress:   false,
		cacheMap:               nil,
		inboundCacheLineCount:  0,
		outboundCacheLineCount: 0,
		dirtyCacheLineCount:    0,
		fhSet:                  make(map[uint64]struct{}),
		pendingDelete:          false,
	}

	if parentInode.objectPath == "" {
		pseudoDirInode.objectPath = basename + "/"
	} else {
		pseudoDirInode.objectPath = parentInode.objectPath + basename + "/"
	}

	ok = globals.inodeMap.put(pseudoDirInode)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.put(pseudoDirInode) returned !ok")
	}

	if isVirt {
		ok = globals.virtChildDirEntryMap.put(parentInode.inodeNumber, pseudoDirInode.basename, pseudoDirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(parentInode.inodeNumber, pseudoDirInode.basename, pseudoDirInode.inodeNumber) returned !ok")
		}
	} else {
		ok = globals.physChildDirEntryMap.put(parentInode.inodeNumber, pseudoDirInode.basename, pseudoDirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.put(parentInode.inodeNumber, pseudoDirInode.basename, pseudoDirInode.inodeNumber) returned !ok")
		}
	}

	ok = globals.virtChildDirEntryMap.put(pseudoDirInode.inodeNumber, DotDirEntryBasename, pseudoDirInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(pseudoDirInode.inodeNumber, DotDirEntryBasename, pseudoDirInode.inodeNumber) returned !ok")
	}
	ok = globals.virtChildDirEntryMap.put(pseudoDirInode.inodeNumber, DotDotDirEntryBasename, parentInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(pseudoDirInode.inodeNumber, DotDotDirEntryBasename, parentInode.inodeNumber) returned !ok")
	}

	parentInode.touch(nil)
	pseudoDirInode.touch(nil)

	return
}

// `createFileObjectInode` is called while globals.Lock() is held to create a new FileObject inodeStruct.
func (parentInode *inodeStruct) createFileObjectInode(isVirt bool, basename string, size uint64, eTag string, mTime time.Time) (fileObjectInode *inodeStruct) {
	var (
		backend *backendStruct
		ok      bool
	)

	backend, ok = globals.backendMap[parentInode.backendNonce]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce] returned !ok")
	}

	fileObjectInode = &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         FileObject,
		backendNonce:      backend.nonce,
		parentInodeNumber: parentInode.inodeNumber,
		isVirt:            isVirt,
		// objectPath: filled in below
		basename:               basename,
		sizeInBackend:          size,
		sizeInMemory:           size,
		eTag:                   eTag,
		mode:                   uint32(syscall.S_IFREG | backend.filePerm),
		mTime:                  mTime,
		xTime:                  time.Time{},
		isPrefetchInProgress:   false,
		cacheMap:               make(map[uint64]uint64),
		inboundCacheLineCount:  0,
		outboundCacheLineCount: 0,
		dirtyCacheLineCount:    0,
		fhSet:                  make(map[uint64]struct{}),
		pendingDelete:          false,
	}

	if parentInode.objectPath == "" {
		fileObjectInode.objectPath = basename
	} else {
		fileObjectInode.objectPath = parentInode.objectPath + basename
	}

	ok = globals.inodeMap.put(fileObjectInode)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.put(fileObjectInode) returned !ok")
	}

	if isVirt {
		ok = globals.virtChildDirEntryMap.put(parentInode.inodeNumber, fileObjectInode.basename, fileObjectInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(parentInode.inodeNumber, fileObjectInode.basename, fileObjectInode.inodeNumber) returned !ok")
		}
	} else {
		ok = globals.physChildDirEntryMap.put(parentInode.inodeNumber, fileObjectInode.basename, fileObjectInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.put(parentInode.inodeNumber, fileObjectInode.basename, fileObjectInode.inodeNumber) returned !ok")
		}
	}

	parentInode.touch(nil)
	fileObjectInode.touch(nil)

	return
}

// clearFileCacheLinesLocked removes all cache lines from a file inode and updates LRU tracking.
//
// Preconditions (caller must ensure):
// - globals.Lock() is held
// - Inode must be eviction-ready:
//   - No inbound cache lines (inode.inboundCacheLineCount == 0)
//   - No outbound cache lines (inode.outboundCacheLineCount == 0)
//   - No dirty cache lines (inode.dirtyCacheLineCount == 0)
//   - No open file handles (len(inode.fhMap) == 0)
//
// - Only clean cache lines should remain
func clearFileCacheLinesLocked(inode *inodeStruct) {
	var (
		cacheLine       *cacheLineStruct
		cacheLineNonce  uint64
		cacheLineNumber uint64
		ok              bool
	)

	if inode == nil || inode.inodeType != FileObject {
		return
	}

	for cacheLineNumber, cacheLineNonce = range inode.cacheMap {
		cacheLine, ok = globals.cacheMap[cacheLineNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.cacheMap[cacheLineNonce] returned !ok")
		}
		if cacheLine.state != CacheLineClean {
			dumpStack()
			globals.logger.Fatalf("[FATAL] cacheLine.state(%v) != CacheLineClean(%v)", cacheLine.state, CacheLineClean)
		}

		_ = globals.cleanCacheLineLRU.Remove(cacheLine.listElement)
		cacheLine.listElement = nil

		delete(inode.cacheMap, cacheLineNumber)
		delete(globals.cacheMap, cacheLineNonce)
	}
}

// convertDirectoryToVirtual converts a physical directory to virtual when it becomes empty.
// This maintains POSIX semantics where directories persist after their last file is deleted.
//
// Preconditions (caller must ensure):
// - globals.Lock() is held
// - dirInode is a PseudoDir
// - dirInode has no physical children (physChildInodeMap is empty)
// - dirInode is currently physical (isVirt == false)
//
// The function:
// 1. Moves directory from parent's physChildInodeMap to virtChildInodeMap
// 2. Marks directory as virtual (isVirt = true)
// 3. Recursively converts ancestor directories if they also become empty
func convertDirectoryToVirtual(dirInode *inodeStruct) {
	var (
		childInodePhysChildDirEntryMapLimit uint64
		childInodePhysChildDirEntryMapStart uint64
		ok                                  bool
		parentInode                         *inodeStruct
	)

	if dirInode == nil {
		return
	}

	if dirInode.inodeType != PseudoDir {
		return
	}

	if dirInode.isVirt {
		return
	}

	childInodePhysChildDirEntryMapStart, childInodePhysChildDirEntryMapLimit = globals.physChildDirEntryMap.getIndexRange(dirInode.inodeNumber)
	if (childInodePhysChildDirEntryMapLimit - childInodePhysChildDirEntryMapStart) > 0 {
		return
	}

	parentInode, ok = globals.inodeMap.get(dirInode.parentInodeNumber)
	if ok {
		ok = globals.physChildDirEntryMap.delete(dirInode.parentInodeNumber, dirInode.basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.delete(dirInode.parentInodeNumber, dirInode.basename) returned !ok")
		}
		ok = globals.virtChildDirEntryMap.put(dirInode.parentInodeNumber, dirInode.basename, dirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.put(dirInode.parentInodeNumber, dirInode.basename, dirInode.inodeNumber) returned !ok")
		}
	}

	dirInode.isVirt = true
	dirInode.touch(nil)

	if parentInode != nil && parentInode.inodeType == PseudoDir {
		convertAncestorDirectoriesToVirtual(parentInode)
	}
}

// convertAncestorDirectoriesToVirtual recursively converts ancestor directories to virtual
// when they have no remaining physical children.
//
// Preconditions:
// - globals.Lock() is held
// - Called after a directory is converted to virtual
//
// This maintains the invariant that if a directory has no physical descendants,
// it should also be virtual (unless it's a BackendRootDir).
func convertAncestorDirectoriesToVirtual(dirInode *inodeStruct) {
	var (
		childInodePhysChildDirEntryMapLimit uint64
		childInodePhysChildDirEntryMapStart uint64
	)

	if dirInode == nil {
		return
	}

	if dirInode.inodeType == BackendRootDir || dirInode.inodeType == FUSERootDir {
		return
	}

	if dirInode.inodeType != PseudoDir {
		return
	}

	if dirInode.isVirt {
		return
	}

	childInodePhysChildDirEntryMapStart, childInodePhysChildDirEntryMapLimit = globals.physChildDirEntryMap.getIndexRange(dirInode.inodeNumber)
	if (childInodePhysChildDirEntryMapLimit - childInodePhysChildDirEntryMapStart) > 0 {
		return
	}

	convertDirectoryToVirtual(dirInode)
}

// `touch` is called to ensure an inode that should be on globals.inodeEvictionLRU has the
// appropriate .xTime. `touch` will optionally update .mTime as well. If the inode should
// not be on globals.inodeEvictionLRU, its .listElement will be nil.
func (inode *inodeStruct) touch(mTimeAsInterface interface{}) {
	var (
		ok                        bool
		physChildDirEntryMapLimit uint64
		physChildDirEntryMapStart uint64
		virtChildDirEntryMapLimit uint64
		virtChildDirEntryMapStart uint64
	)

	if mTimeAsInterface != nil {
		inode.mTime, ok = mTimeAsInterface.(time.Time)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] mTimeAsInterface.(time.Time) returned !ok")
		}
	}

	if !inode.xTime.IsZero() {
		ok = globals.inodeEvictionQueue.remove(inode)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(inode) returned !ok")
		}

		inode.xTime = time.Time{}
	}

	switch inode.inodeType {
	case FileObject:
		if !inode.pendingDelete && (len(inode.fhSet) == 0) && ((inode.inboundCacheLineCount + inode.outboundCacheLineCount + inode.dirtyCacheLineCount) == 0) {
			if inode.isVirt {
				inode.xTime = time.Now().Add(globals.config.virtualFileTTL)
			} else {
				inode.xTime = time.Now().Add(globals.config.evictableInodeTTL)
			}

			ok = globals.inodeEvictionQueue.insert(inode)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.insert(inode) returned !ok")
			}
		}
	case FUSERootDir:
		// Never placed on any of globals.inodeEvictionLRU
	case BackendRootDir:
		// Never placed on any of globals.inodeEvictionLRU
	case PseudoDir:
		physChildDirEntryMapStart, physChildDirEntryMapLimit = globals.physChildDirEntryMap.getIndexRange(inode.inodeNumber)
		virtChildDirEntryMapStart, virtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(inode.inodeNumber)

		if (len(inode.fhSet) == 0) && ((physChildDirEntryMapLimit - physChildDirEntryMapStart) == 0) && ((virtChildDirEntryMapLimit - virtChildDirEntryMapStart) == 2) {
			if inode.isVirt {
				inode.xTime = time.Now().Add(globals.config.virtualDirTTL)
			} else {
				inode.xTime = time.Now().Add(globals.config.evictableInodeTTL)
			}

			ok = globals.inodeEvictionQueue.insert(inode)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.insert(inode) returned !ok")
			}
		}
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] inode.inodeType(%v) must be one of FileObject(%v), FUSERootDir(%v), BackendRootDir(%v), or PseudoDir(%v)", inode.inodeType, FileObject, FUSERootDir, BackendRootDir, PseudoDir)
	}

	globals.inodeMap.touch(inode)
}

// `inodeEvictor` is a goroutine that periodically monitors the cache and globals.inodeEvictionLRU
// to see if cache limits need to be enforced or any "phys"/"virt" inodes should be evicted/expired.
func inodeEvictor() {
	var (
		childInode       *inodeStruct
		childInodeNumber uint64
		ok               bool
		parentInode      *inodeStruct
		ticker           *time.Ticker
		timeNow          time.Time
		xTime            time.Time
	)

	ticker = time.NewTicker(globals.config.ttlCheckInterval)

	for {
		select {
		case <-ticker.C:
			globalsLock("fs.go:813:4:inodeEvictor")

			// Trim globals.cleanCacheLineLRU as possible/necessary

			cachePrune()

			// Scan globals.inodeEvictionLRU looking for expired inodes to evict

			timeNow = time.Now()

			for {
				xTime, childInodeNumber, ok = globals.inodeEvictionQueue.front()
				if !ok || (xTime.After(timeNow)) {
					break
				}

				childInode, ok = globals.inodeMap.get(childInodeNumber)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok")
				}

				ok = globals.inodeEvictionQueue.remove(childInode)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(childInode) returned !ok")
				}

				clearFileCacheLinesLocked(childInode)

				parentInode, ok = globals.inodeMap.get(childInode.parentInodeNumber)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInode.parentInodeNumber) returned !ok")
				}

				if childInode.isVirt {
					ok = globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename) returned !ok")
					}
				} else {
					ok = globals.physChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename) returned !ok")
					}
				}

				ok = globals.inodeMap.delete(childInodeNumber)
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(childInodeNumber) returned !ok")
				}

				parentInode.touch(nil)
			}

			globalsUnlock()
		case <-globals.inodeEvictorContext.Done():
			ticker.Stop()
			return
		}
	}
}

// `inodeEvictorForceDrain` is called to forcibly drain globals.inodeEvictionLRU.
//
// Note 1: Callers must hold globals.lock
// Note 2: Calls should not be made until after globals.config.entryAttrTTL idle time
func inodeEvictorForceDrain() (numDrained uint64) {
	var (
		childInode       *inodeStruct
		childInodeNumber uint64
		ok               bool
		parentInode      *inodeStruct
	)

	numDrained = 0

	for {
		_, childInodeNumber, ok = globals.inodeEvictionQueue.front()
		if !ok {
			break
		}

		numDrained++

		childInode, ok = globals.inodeMap.get(childInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok")
		}

		ok = globals.inodeEvictionQueue.remove(childInode)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeEvictionQueue.remove(childInode) returned !ok")
		}

		clearFileCacheLinesLocked(childInode)

		parentInode, ok = globals.inodeMap.get(childInode.parentInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInode.parentInodeNumber) returned !ok")
		}

		if childInode.isVirt {
			ok = globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename) returned !ok")
			}
		} else {
			ok = globals.physChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.delete(parentInode.inodeNumber, childInode.basename) returned !ok")
			}
		}

		ok = globals.inodeMap.delete(childInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(childInodeNumber) returned !ok")
		}

		parentInode.touch(nil)
	}

	return
}

// `findChildInode` is called to locate or create a child's inodeStruct. The return `ok` indicates
// that either the child's inodeStruct was already known or has been created in the cases where
// an existing object or object prefix is found. Callers should already hold globals.Lock().
func (parentInode *inodeStruct) findChildInode(basename string) (childInode *inodeStruct, ok bool) {
	var (
		backend            *backendStruct
		childInodeNumber   uint64
		dirOrFilePath      string
		err                error
		statDirectoryInput *statDirectoryInputStruct
		statFileInput      *statFileInputStruct
		statFileOutput     *statFileOutputStruct
	)

	defer func() {
		parentInode.touch(nil)

		if ok {
			childInode.touch(nil)
		}
	}()

	// First see if we already know about the childInode

	childInodeNumber, ok = globals.physChildDirEntryMap.getByBasename(parentInode.inodeNumber, basename)
	if ok {
		childInode, ok = globals.inodeMap.get(childInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok [findChildInode() case 1]")
		}

		// [TODO] We might want to (1) validate the object or prefix exists and (2) if it doesn't and this is a PseudoDir, convert it & all descendents to "virt"

		return
	}

	childInodeNumber, ok = globals.virtChildDirEntryMap.getByBasename(parentInode.inodeNumber, basename)
	if ok {
		childInode, ok = globals.inodeMap.get(childInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok [findChildInode() case 2]")
		}

		// [TODO] We might want to (1) validate the object or prefix doesn't exist and (2) if it does and this is a PseudoDir, convert it to "phys"

		return
	}

	// We didn't already know about the childInode, so let's first look for an existing object in the backend

	backend, ok = globals.backendMap[parentInode.backendNonce]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.backendMap[parentInode.backendNonce] returned !ok")
	}

	if parentInode.objectPath == "" {
		dirOrFilePath = basename
	} else {
		dirOrFilePath = parentInode.objectPath + basename
	}

	statFileInput = &statFileInputStruct{
		filePath: dirOrFilePath,
		ifMatch:  "",
	}

	statFileOutput, err = statFileWrapper(backend.context, statFileInput)
	if err == nil {
		// We found an existing object in the backend, so let's create a FileObject inode for it

		childInode = parentInode.createFileObjectInode(false, basename, statFileOutput.size, statFileOutput.eTag, statFileOutput.mTime)

		if !parentInode.isPrefetchInProgress {
			parentInode.isPrefetchInProgress = true
			go prefetchDirectory(parentInode.inodeNumber)
		}

		ok = true
		return
	}

	// No object found in the backend... what about an object prefix?
	// Note: By convention, we must modify dirOrFileOPath to end in "/"

	dirOrFilePath += "/"

	statDirectoryInput = &statDirectoryInputStruct{
		dirPath: dirOrFilePath,
	}

	_, err = statDirectoryWrapper(backend.context, statDirectoryInput)
	if err == nil {
		// We found an existing object prefix in the backend, so let's create a PseudoDir inode for it

		childInode = parentInode.createPseudoDirInode(false, basename)

		if !parentInode.isPrefetchInProgress {
			parentInode.isPrefetchInProgress = true
			go prefetchDirectory(parentInode.inodeNumber)
		}

		ok = true
		return
	}

	// We found neither an object nor an object prefix in the backend... so we fail

	childInode = nil
	ok = false

	return
}

// `prefetchDirectory` is run as a background worker to populate globals.inodeMap
// with inodeStruct's as would occur in DoReadDir() and DoReadDirPlus() to handle
// the use cases where paths are known by users without the need to discover them
// via directory listings that would normally trigger such population.
func prefetchDirectory(dirInodeNumber uint64) {
	var (
		backend                 *backendStruct
		basename                string
		continuationToken       = string("")
		dirInode                *inodeStruct
		err                     error
		latency                 float64
		listDirectoryOutputFile listDirectoryOutputFileStruct
		listDirectoryInput      *listDirectoryInputStruct
		listDirectoryOutput     *listDirectoryOutputStruct
		ok                      bool
		startTime               = time.Now()
	)

	globalsLock("fs.go:1083:2:prefetchDirectory")

	dirInode, ok = globals.inodeMap.get(dirInodeNumber)
	if !ok {
		// For any reason, the directory inode has been evicted and no longer needs to be prefetched [case 1]
		globalsUnlock()
		return
	}

	for {
		backend, ok = globals.backendMap[dirInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[dirInode.backendNonce] returned !ok [case 1]")
		}

		listDirectoryInput = &listDirectoryInputStruct{
			continuationToken: continuationToken,
			maxItems:          backend.directoryPageSize,
			dirPath:           dirInode.objectPath,
		}

		globalsUnlock()

		listDirectoryOutput, err = listDirectoryWrapper(backend.context, listDirectoryInput)
		if err != nil {
			globals.logger.Printf("[WARN] listDirectoryWrapper(dirInode.backend.context, listDirectoryInput) failed: %v", err)
		}

		globalsLock("fs.go:1112:3:prefetchDirectory")

		dirInode, ok = globals.inodeMap.get(dirInodeNumber)
		if !ok {
			// For any reason, the directory inode has been evicted and no longer needs to be prefetched [case 2]
			globalsUnlock()
			return
		}

		backend, ok = globals.backendMap[dirInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[dirInode.backendNonce] returned !ok [case 2]")
		}

		// Check first to see if we should continue or the directory inode has been evicted since we last checked

		if !ok {
			// For any reason, the directory inode has been evicted and no longer needs to be prefetched
			globalsUnlock()
			return
		}

		// Now we should also cleanly exit if we reported that warning above

		if err != nil {
			dirInode.isPrefetchInProgress = false
			globalsUnlock()
			return
		}

		for _, basename = range listDirectoryOutput.subdirectory {
			// The following will only create the childDirInode if necessary
			_ = dirInode.findChildDirInode(basename)
		}

		for _, listDirectoryOutputFile = range listDirectoryOutput.file {
			// The following will only create the childFileInode if necessary
			_ = dirInode.findChildFileInode(listDirectoryOutputFile.basename, listDirectoryOutputFile.eTag, listDirectoryOutputFile.mTime, listDirectoryOutputFile.size)
		}

		dirInode.touch(nil)

		if listDirectoryOutput.isTruncated {
			continuationToken = listDirectoryOutput.nextContinuationToken
		} else {
			// Finished prefetching directory
			dirInode.isPrefetchInProgress = false
			latency = time.Since(startTime).Seconds()
			globals.backendMetrics.DirectoryPrefetchLatencies.Observe(latency)
			backend.backendMetrics.DirectoryPrefetchLatencies.Observe(latency)
			globalsUnlock()
			return
		}
	}
}

// `findChildDirInode` is called to locate, or create if missing, a child directory inodeStruct.
func (parentInode *inodeStruct) findChildDirInode(basename string) (childDirInode *inodeStruct) {
	var (
		childDirInodeNumber uint64
		ok                  bool
	)

	defer func() {
		parentInode.touch(nil)
		childDirInode.touch(nil)
	}()

	// First see if we already know about the childInode

	childDirInodeNumber, ok = globals.physChildDirEntryMap.getByBasename(parentInode.inodeNumber, basename)
	if ok {
		childDirInode, ok = globals.inodeMap.get(childDirInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInodeNumber) returned !ok [findChildDirInode() case 1]")
		}

		// [TODO] We might want to validate that childDirInode.inodeType == PseudoDir
		// [TODO] We might want to (1) validate the prefix exists and (2) if it doesn't, convert it & all descendents to "virt"

		return
	}

	childDirInodeNumber, ok = globals.virtChildDirEntryMap.getByBasename(parentInode.inodeNumber, basename)
	if ok {
		childDirInode, ok = globals.inodeMap.get(childDirInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childDirInodeNumber) returned !ok [findChildDirInode() case 2]")
		}

		// [TODO] We might want to validate that childDirInode.inodeType == PseudoDir
		// [TODO] We might want to (1) validate the prefix doesn't exist and (2) if it does, convert it to "phys"

		return
	}

	// We didn't already know about the childInode... so just create it

	childDirInode = parentInode.createPseudoDirInode(false, basename)

	return
}

// `findChildFileInode` is called to locate, or create if missing, a child file inodeStruct.
func (parentInode *inodeStruct) findChildFileInode(basename, eTag string, mTime time.Time, size uint64) (childFileInode *inodeStruct) {
	var (
		childFileInodeNumber uint64
		ok                   bool
	)

	defer func() {
		parentInode.touch(nil)
		childFileInode.touch(nil)
	}()

	// First see if we already know about the childInode

	childFileInodeNumber, ok = globals.physChildDirEntryMap.getByBasename(parentInode.inodeNumber, basename)
	if ok {
		childFileInode, ok = globals.inodeMap.get(childFileInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childFileInodeNumber) returned !ok [findChildFileInode() case 1]")
		}

		// [TODO] We might want to validate that childFileInode.inodeType == FileObject
		// [TODO] We might want to (1) validate the object exists and (2) if it doesn't, convert it to "virt"

		return
	}

	childFileInodeNumber, ok = globals.virtChildDirEntryMap.getByBasename(parentInode.inodeNumber, basename)
	if ok {
		childFileInode, ok = globals.inodeMap.get(childFileInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childFileInodeNumber) returned !ok [findChildFileInode() case 1]")
		}

		// [TODO] We might want to validate that childFileInode.inodeType == FileObject
		// [TODO] We might want to (1) validate the object doesn't exist and (2) if it does, convert it to "phys"

		return
	}

	// We didn't already know about the childFileInode... so just create it

	childFileInode = parentInode.createFileObjectInode(false, basename, size, eTag, mTime)

	return
}

const (
	DUMP_FS_DIR_INDENT = "    "
)

// `dumpFS` logs the entire file system. It must be called without holding globals.Lock().
func dumpFS(w io.Writer) {
	var (
		ok           bool
		rootDirInode *inodeStruct
	)

	globalsLock("fs.go:1278:2:dumpFS")

	rootDirInode, ok = globals.inodeMap.get(FUSERootDirInodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.get(FUSERootDirInodeNumber) returned !ok")
	}
	if rootDirInode.inodeType != FUSERootDir {
		dumpStack()
		globals.logger.Fatalf("[FATAL] rootDirInode.inodeType(%v) should have been FUSERootDir(%v)", rootDirInode.inodeType, FUSERootDir)
	}

	rootDirInode.dumpFS(w, "", FUSERootDirInodeNumber, "")

	globalsUnlock()
}

// `dumpFS` called on a particular inode recursively dumps a file system element.
func (thisInode *inodeStruct) dumpFS(w io.Writer, indent string, expectedInodeNumber uint64, expectedBasename string) {
	var (
		backend                   *backendStruct
		childInode                *inodeStruct
		childInodeBasename        string
		childInodeNumber          uint64
		nextIndent                = indent + DUMP_FS_DIR_INDENT
		ok                        bool
		physChildDirEntryMapIndex uint64
		physChildDirEntryMapLimit uint64
		physChildDirEntryMapStart uint64
		thisInodeBasename         string
		virtChildDirEntryMapIndex uint64
		virtChildDirEntryMapLimit uint64
		virtChildDirEntryMapStart uint64
	)

	if thisInode.inodeType != FUSERootDir {
		backend, ok = globals.backendMap[thisInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[thisInode.backendNonce] returned !ok")
		}
	}
	if thisInode.inodeNumber != expectedInodeNumber {
		dumpStack()
		globals.logger.Fatalf("[FATAL] thisInode.inodeNumber(%v) != expectedInodeNumber(%v)", thisInode.inodeNumber, expectedInodeNumber)
	}
	if thisInode.basename != expectedBasename {
		dumpStack()
		globals.logger.Fatalf("[FATAL] thisInode.basename(\"%s\") != expectedBasename(\"%s\")", thisInode.basename, expectedBasename)
	}

	switch thisInode.inodeType {
	case FileObject:
		thisInodeBasename = "\"" + thisInode.basename + "\""
	case FUSERootDir:
		thisInodeBasename = "[FUSERootDir]"
	case BackendRootDir:
		thisInodeBasename = "[BackendRootDir] \"" + thisInode.basename + "\" (" + backend.backendPath + ")"
	case PseudoDir:
		thisInodeBasename = "[PseudoDir]      \"" + thisInode.basename + "\" (" + thisInode.objectPath + ")"
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] dirInode.inodeType should == FUSERootDir(%v) or BackendRootDir(%v) or PseudoDir(%v), not FileObject(%v) nor what was found: %v", FUSERootDir, BackendRootDir, PseudoDir, FileObject, thisInode.inodeType)
	}

	fmt.Fprintf(w, "%s%5d %s\n", indent, thisInode.inodeNumber, thisInodeBasename)

	if thisInode.inodeType == FileObject {
		return
	}

	virtChildDirEntryMapStart, virtChildDirEntryMapLimit = globals.virtChildDirEntryMap.getIndexRange(thisInode.inodeNumber)

	for virtChildDirEntryMapIndex = virtChildDirEntryMapStart; virtChildDirEntryMapIndex < virtChildDirEntryMapLimit; virtChildDirEntryMapIndex++ {
		_, childInodeBasename, childInodeNumber, ok = globals.virtChildDirEntryMap.getByIndex(virtChildDirEntryMapIndex)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.virtChildDirEntryMap.getByIndex(virtChildDirEntryMapIndex) returned !ok")
		}

		if childInodeBasename == DotDirEntryBasename {
			if childInodeNumber != thisInode.inodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.inodeNumber(%v) [case virt]", childInodeNumber, thisInode.inodeNumber)
			}
			fmt.Fprintf(w, "%s%5d \"%s\"\n", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		if childInodeBasename == DotDotDirEntryBasename {
			if childInodeNumber != thisInode.parentInodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.parentInodeNumber(%v) [case virt]", childInodeNumber, thisInode.parentInodeNumber)
			}
			fmt.Fprintf(w, "%s%5d \"%s\"\n", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		childInode, ok = globals.inodeMap.get(childInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok [case virt]")
		}

		childInode.dumpFS(w, nextIndent, childInodeNumber, childInodeBasename)
	}

	physChildDirEntryMapStart, physChildDirEntryMapLimit = globals.physChildDirEntryMap.getIndexRange(thisInode.inodeNumber)

	for physChildDirEntryMapIndex = physChildDirEntryMapStart; physChildDirEntryMapIndex < physChildDirEntryMapLimit; physChildDirEntryMapIndex++ {
		_, childInodeBasename, childInodeNumber, ok = globals.physChildDirEntryMap.getByIndex(physChildDirEntryMapIndex)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.getByIndex(physChildDirEntryMapIndex) returned !ok")
		}

		if childInodeBasename == DotDirEntryBasename {
			if childInodeNumber != thisInode.inodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.inodeNumber(%v) [case phys]", childInodeNumber, thisInode.inodeNumber)
			}
			fmt.Fprintf(w, "%s%5d \"%s\"\n", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		if childInodeBasename == DotDotDirEntryBasename {
			if childInodeNumber != thisInode.parentInodeNumber {
				dumpStack()
				globals.logger.Fatalf("[FATAL] childInodeNumber(%v) != thisInode.parentInodeNumber(%v) [case phys]", childInodeNumber, thisInode.parentInodeNumber)
			}
			fmt.Fprintf(w, "%s%5d \"%s\"\n", nextIndent, childInodeNumber, childInodeBasename)
			continue
		}

		childInode, ok = globals.inodeMap.get(childInodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap.get(childInodeNumber) returned !ok [case phys]")
		}

		childInode.dumpFS(w, nextIndent, childInodeNumber, childInodeBasename)
	}
}

// `finishPendingDelete` is called to finish the deletion of a
// FileInode that includes removing the corresponding backend
// object (if any). As this may involve blocking (e.g. to await
// various cache line operations), this function must be called
// while unlocked.
func (thisInode *inodeStruct) finishPendingDelete() {
	var (
		backend         *backendStruct
		cacheLine       *cacheLineStruct
		cacheLineNonce  uint64
		cacheLineNumber uint64
		cacheLineWaiter sync.WaitGroup
		deleteFileInput *deleteFileInputStruct
		err             error
		ok              bool
		parentInode     *inodeStruct
	)

Restart:

	globalsLock("fs.go:1442:2:(*inodeStruct).finishPendingDelete")

	// Let's just drop cache lines that are either "clean" io "dirty"

	for cacheLineNumber, cacheLineNonce = range thisInode.cacheMap {
		cacheLine, ok = globals.cacheMap[cacheLineNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.cacheMap[cacheLineNonce] returned !ok [case 1]")
		}
		switch cacheLine.state {
		case CacheLineClean:
			delete(thisInode.cacheMap, cacheLineNumber)
			delete(globals.cacheMap, cacheLineNonce)
			_ = globals.cleanCacheLineLRU.Remove(cacheLine.listElement)
			cacheLine.listElement = nil
		case CacheLineDirty:
			delete(thisInode.cacheMap, cacheLineNumber)
			delete(globals.cacheMap, cacheLineNonce)
			_ = globals.dirtyCacheLineLRU.Remove(cacheLine.listElement)
			cacheLine.listElement = nil
		default:
			// Nothing for now
		}
	}

	// Now we need to await any pending "inbound" or "outbound" cache lines (though this shouldn't happen)

	for _, cacheLineNonce = range thisInode.cacheMap {
		cacheLine, ok = globals.cacheMap[cacheLineNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.cacheMap[cacheLineNonce] returned !ok [case 2]")
		}
		cacheLineWaiter.Add(1)
		cacheLine.waiters = append(cacheLine.waiters, &cacheLineWaiter)
		globalsUnlock()
		cacheLineWaiter.Wait()
		goto Restart
	}

	// Once we make it here, we need to atomically delete the object (if any)

	if !thisInode.isVirt {
		backend, ok = globals.backendMap[thisInode.backendNonce]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.backendMap[thisInode.backendNonce] returned !ok")
		}

		deleteFileInput = &deleteFileInputStruct{
			filePath: thisInode.objectPath,
			ifMatch:  "",
		}

		// It's actually ok if the object is already gone
		_, err = deleteFileWrapper(backend.context, deleteFileInput)
		if err != nil {
			globals.logger.Printf("[WARN] deleteBackendObjectWhenAndIfNecessary() got deleteFileWrapper(thisInode.backend.context, deleteFileInput) err: %v", err)
		}
	}

	// Finally remove thisInode from its parent and globals.inodeMap

	parentInode, ok = globals.inodeMap.get(thisInode.parentInodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.get(thisInode.parentInodeNumber) returned !ok")
	}

	if thisInode.isVirt {
		ok = globals.virtChildDirEntryMap.delete(thisInode.parentInodeNumber, thisInode.basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] virtChildDirEntryMap.delete(thisInode.parentInodeNumber, thisInode.basename) returned !ok")
		}
	} else {
		ok = globals.physChildDirEntryMap.delete(thisInode.parentInodeNumber, thisInode.basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.physChildDirEntryMap.delete(thisInode.parentInodeNumber, thisInode.basename) returned !ok")
		}
	}

	ok = globals.inodeMap.delete(thisInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap.delete(thisInode.inodeNumber) returned !ok")
	}

	parentInode.touch(nil)

	globalsUnlock()
}
