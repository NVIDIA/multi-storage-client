package main

import (
	"container/list"
	"context"
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"
)

// `initFS` initializes the root of the FUSE file system.
func initFS() {
	var (
		timeNow time.Time
	)

	globals.Lock()

	timeNow = time.Now()

	globals.backendMap = make(map[uint64]*backendStruct)

	globals.lastNonce = FUSERootDirInodeNumber

	globals.inode = &inodeStruct{
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
		listElement:            nil,
		physChildInodeMap:      newStringToUint64Map(PhysChildInodeMap),
		virtChildInodeMap:      newStringToUint64Map(VirtChildInodeMap),
		isPrefetchInProgress:   false,
		cacheMap:               nil,
		inboundCacheLineCount:  0,
		outboundCacheLineCount: 0,
		dirtyCacheLineCount:    0,
		fhSet:                  make(map[uint64]struct{}),
		pendingDelete:          false,
	}

	globals.inodeMap = make(map[uint64]*inodeStruct)

	_ = globals.inode.virtChildInodeMap.Put(DotDirEntryBasename, FUSERootDirInodeNumber)
	_ = globals.inode.virtChildInodeMap.Put(DotDotDirEntryBasename, FUSERootDirInodeNumber)

	globals.inodeMap[FUSERootDirInodeNumber] = globals.inode

	globals.inodeEvictionLRU = newTimeToUint64Queue(InodeEvictionLRU)

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

	globals.Unlock()
}

// `drainFS` awaits all backend/asynchronous traffic to complete before
func drainFS() {
	var (
		dirName string
		backend *backendStruct
	)

	globals.inodeEvictorCancelFunc()
	globals.inodeEvictorWaitGroup.Wait()

	globals.Lock()

	for dirName, backend = range globals.config.backends {
		globals.backendsToUnmount[dirName] = backend
	}

	processToUnmountListAlreadyLocked()

	globals.Unlock()
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

	globals.Lock()

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
			listElement:            nil,
			physChildInodeMap:      newStringToUint64Map(PhysChildInodeMap),
			virtChildInodeMap:      newStringToUint64Map(VirtChildInodeMap),
			isPrefetchInProgress:   false,
			cacheMap:               nil,
			inboundCacheLineCount:  0,
			outboundCacheLineCount: 0,
			dirtyCacheLineCount:    0,
			fhSet:                  make(map[uint64]struct{}),
			pendingDelete:          false,
		}

		ok = globals.inode.virtChildInodeMap.Put(backend.dirName, backend.inode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] put of \"%s\" into backend.inode.virtChildInodeMap returned !ok", backend.dirName)
		}

		_ = backend.inode.virtChildInodeMap.Put(DotDirEntryBasename, backend.inode.inodeNumber)
		_ = backend.inode.virtChildInodeMap.Put(DotDotDirEntryBasename, FUSERootDirInodeNumber)

		globals.inodeMap[backend.inode.inodeNumber] = backend.inode

		backend.fissionMetrics = newFissionMetrics()
		backend.backendMetrics = newBackendMetrics()

		backend.mounted = true

		globals.config.backends[dirName] = backend
		globals.backendMap[backend.nonce] = backend
	}

	globals.Unlock()
}

// `processToUnmountList` is called to remove each backend subdirectory of the FUSE
// file system's root directory found on the globals.backendsToUnmount list.
func processToUnmountList() {
	globals.Lock()
	processToUnmountListAlreadyLocked()
	globals.Unlock()
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

		ok = globals.inode.virtChildInodeMap.DeleteByKey(backend.dirName)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] delete of \"%s\" from globals.inode.virtChildInodeMap returned !ok", backend.dirName)
		}

		delete(globals.inodeMap, backend.inode.inodeNumber)

		backend.mounted = false

		delete(globals.config.backends, dirName)
		delete(globals.backendMap, backend.nonce)
	}
}

// `emptyChildInodes` is called to remove all child inodes.
func (parentInode *inodeStruct) emptyChildInodes() {
	var (
		childInode         *inodeStruct
		childInodeBasename string
		childInodeNumber   uint64
		ok                 bool
	)

	for {
		childInodeBasename, childInodeNumber, ok = parentInode.physChildInodeMap.GetByIndex(0)
		if !ok {
			break
		}

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case physChildInodeMap]")
		}

		if childInode.inodeType == PseudoDir {
			childInode.emptyChildInodes()
		}

		if childInode.listElement != nil {
			globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
		}

		delete(globals.inodeMap, childInodeNumber)

		ok = parentInode.physChildInodeMap.DeleteByKey(childInodeBasename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.DeleteByKey(childInodeBasename) returned !ok")
		}
	}

	for {
		childInodeBasename, childInodeNumber, ok = parentInode.virtChildInodeMap.GetByIndex(0)
		if !ok {
			break
		}

		if (childInodeBasename != DotDirEntryBasename) && (childInodeBasename != DotDotDirEntryBasename) {
			childInode, ok = globals.inodeMap[childInodeNumber]
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case virtChildInodeMap]")
			}

			if childInode.inodeType == PseudoDir {
				childInode.emptyChildInodes()
			}

			if childInode.listElement != nil {
				globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
			}

			delete(globals.inodeMap, childInodeNumber)
		}

		ok = parentInode.virtChildInodeMap.DeleteByKey(childInodeBasename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInodeBasename) returned !ok")
		}
	}
}

// `convertToPhysInodeIfNecessary` is called while globals.Lock() is held to convert
// the supplied inode from "virt" to "phys" if necessary. It is the caller's responsibility
// to ensure that the directory path leading down to this now assuredly "phys" inode has
// already been ensured to be "phys".
func (childInode *inodeStruct) convertToPhysInodeIfNecessary() {
	var (
		ok          bool
		parentInode *inodeStruct
	)

	if !childInode.isVirt || ((childInode.inodeType != FileObject) && (childInode.inodeType != PseudoDir)) {
		return
	}

	if childInode.listElement != nil {
		globals.inodeEvictionLRU.Remove(childInode.xTime, childInode.listElement)
		childInode.xTime = time.Time{}
		childInode.listElement = nil
	}

	childInode.isVirt = false

	parentInode, ok = globals.inodeMap[childInode.parentInodeNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap[childInode.parentInodeNumber] returned !ok")
	}

	ok = parentInode.virtChildInodeMap.DeleteByKey(childInode.basename)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
	}

	ok = parentInode.physChildInodeMap.Put(childInode.basename, childInode.inodeNumber)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.Put(childInode.basename, childInode.inodeNumber) returned !ok")
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
		listElement:            nil,
		physChildInodeMap:      newStringToUint64Map(PhysChildInodeMap),
		virtChildInodeMap:      newStringToUint64Map(VirtChildInodeMap),
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

	_ = pseudoDirInode.virtChildInodeMap.Put(DotDirEntryBasename, pseudoDirInode.inodeNumber)
	_ = pseudoDirInode.virtChildInodeMap.Put(DotDotDirEntryBasename, parentInode.inodeNumber)

	if isVirt {
		ok = parentInode.virtChildInodeMap.Put(basename, pseudoDirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.Put(basename, pseudoDirInode.inodeNumber) returned !ok")
		}
	} else {
		ok = parentInode.physChildInodeMap.Put(basename, pseudoDirInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.Put(basename, pseudoDirInode.inodeNumber) returned !ok")
		}
	}

	globals.inodeMap[pseudoDirInode.inodeNumber] = pseudoDirInode

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
		basename:      basename,
		sizeInBackend: size,
		sizeInMemory:  size,
		eTag:          eTag,
		mode:          uint32(syscall.S_IFREG | backend.filePerm),
		mTime:         mTime,
		xTime:         time.Time{},
		// listElement: filled in below
		physChildInodeMap:      nil,
		virtChildInodeMap:      nil,
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

	if isVirt {
		ok = parentInode.virtChildInodeMap.Put(basename, fileObjectInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.Put(basename, fileObjectInode.inodeNumber) returned !ok")
		}
	} else {
		ok = parentInode.physChildInodeMap.Put(basename, fileObjectInode.inodeNumber)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.Put(basename, fileObjectInode.inodeNumber) returned !ok")
		}
	}

	globals.inodeMap[fileObjectInode.inodeNumber] = fileObjectInode

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
		ok          bool
		parentInode *inodeStruct
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

	if dirInode.physChildInodeMap.Len() > 0 {
		return
	}

	parentInode, ok = globals.inodeMap[dirInode.parentInodeNumber]
	if ok {
		ok = parentInode.physChildInodeMap.DeleteByKey(dirInode.basename)
		if ok {
			parentInode.virtChildInodeMap.Put(dirInode.basename, dirInode.inodeNumber)
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

	if dirInode.physChildInodeMap.Len() > 0 {
		return
	}

	convertDirectoryToVirtual(dirInode)
}

// `touch` is called to ensure an inode that should be on globals.inodeEvictionLRU has the
// appropriate .xTime. `touch` will optionally update .mTime as well. If the inode should
// not be on globals.inodeEvictionLRU, its .listElement will be nil.
func (inode *inodeStruct) touch(mTimeAsInterface interface{}) {
	var (
		ok bool
	)

	if mTimeAsInterface != nil {
		inode.mTime, ok = mTimeAsInterface.(time.Time)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] mTimeAsInterface.(time.Time) returned !ok")
		}
	}

	if inode.listElement != nil {
		globals.inodeEvictionLRU.Remove(inode.xTime, inode.listElement)
		inode.xTime = time.Time{}
		inode.listElement = nil
	}

	switch inode.inodeType {
	case FileObject:
		if !inode.pendingDelete && (len(inode.fhSet) == 0) && ((inode.inboundCacheLineCount + inode.outboundCacheLineCount + inode.dirtyCacheLineCount) == 0) {
			if inode.isVirt {
				inode.xTime = time.Now().Add(globals.config.virtualFileTTL)
			} else {
				inode.xTime = time.Now().Add(globals.config.evictableInodeTTL)
			}
			inode.listElement = globals.inodeEvictionLRU.Put(inode.xTime, inode.inodeNumber)
		}
	case FUSERootDir:
		// Never placed on any of globals.inodeEvictionLRU
	case BackendRootDir:
		// Never placed on any of globals.inodeEvictionLRU
	case PseudoDir:
		if (len(inode.fhSet) == 0) && (inode.physChildInodeMap.Len() == 0) && (inode.virtChildInodeMap.Len() == 2) {
			if inode.isVirt {
				inode.xTime = time.Now().Add(globals.config.virtualDirTTL)
			} else {
				inode.xTime = time.Now().Add(globals.config.evictableInodeTTL)
			}
			inode.listElement = globals.inodeEvictionLRU.Put(inode.xTime, inode.inodeNumber)
		}
	default:
		dumpStack()
		globals.logger.Fatalf("[FATAL] inode.inodeType(%v) must be one of FileObject(%v), FUSERootDir(%v), BackendRootDir(%v), or PseudoDir(%v)", inode.inodeType, FileObject, FUSERootDir, BackendRootDir, PseudoDir)
	}
}

// `inodeEvictor` is a goroutine that periodically monitors the cache and globals.inodeEvictionLRU
// to see if cache limits need to be enforced or any "phys"/"virt" inodes should be evicted/expired.
func inodeEvictor() {
	var (
		childInode       *inodeStruct
		childInodeNumber uint64
		listElement      *list.Element
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
			globals.Lock()

			// Trim globals.cleanCacheLineLRU as possible/necessary

			cachePrune()

			// Scan globals.inodeEvictionLRU looking for expired inodes to evict

			timeNow = time.Now()

			for {
				xTime, listElement, childInodeNumber, ok = globals.inodeEvictionLRU.Front()
				if !ok || (xTime.After(timeNow)) {
					break
				}

				globals.inodeEvictionLRU.Remove(xTime, listElement)

				childInode, ok = globals.inodeMap[childInodeNumber]
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok")
				}

				clearFileCacheLinesLocked(childInode)

				parentInode, ok = globals.inodeMap[childInode.parentInodeNumber]
				if !ok {
					dumpStack()
					globals.logger.Fatalf("[FATAL] globals.inodeMap[childInode.parentInodeNumber] returned !ok")
				}

				if childInode.isVirt {
					ok = parentInode.virtChildInodeMap.DeleteByKey(childInode.basename)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
					}
				} else {
					ok = parentInode.physChildInodeMap.DeleteByKey(childInode.basename)
					if !ok {
						dumpStack()
						globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
					}
				}

				delete(globals.inodeMap, childInodeNumber)

				parentInode.touch(nil)
			}

			globals.Unlock()
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
		listElement      *list.Element
		ok               bool
		parentInode      *inodeStruct
		xTime            time.Time
	)

	numDrained = 0

	for {
		xTime, listElement, childInodeNumber, ok = globals.inodeEvictionLRU.Front()
		if !ok {
			break
		}

		numDrained++

		globals.inodeEvictionLRU.Remove(xTime, listElement)

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok")
		}

		clearFileCacheLinesLocked(childInode)

		parentInode, ok = globals.inodeMap[childInode.parentInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInode.parentInodeNumber] returned !ok")
		}

		if childInode.isVirt {
			ok = parentInode.virtChildInodeMap.DeleteByKey(childInode.basename)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
			}
		} else {
			ok = parentInode.physChildInodeMap.DeleteByKey(childInode.basename)
			if !ok {
				dumpStack()
				globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(childInode.basename) returned !ok")
			}
		}

		delete(globals.inodeMap, childInodeNumber)

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

	childInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(basename)
	if ok {
		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [findChildInode() case 1]")
		}

		// [TODO] We might want to (1) validate the object or prefix exists and (2) if it doesn't and this is a PseudoDir, convert it & all descendents to "virt"

		return
	}

	childInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(basename)
	if ok {
		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [findChildInode() case 2]")
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

	globals.Lock()

	dirInode, ok = globals.inodeMap[dirInodeNumber]
	if !ok {
		// For any reason, the directory inode has been evicted and no longer needs to be prefetched
		globals.Unlock()
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

		globals.Unlock()

		listDirectoryOutput, err = listDirectoryWrapper(backend.context, listDirectoryInput)
		if err != nil {
			globals.logger.Printf("[WARN] listDirectoryWrapper(dirInode.backend.context, listDirectoryInput) failed: %v", err)
		}

		globals.Lock()

		dirInode, ok = globals.inodeMap[dirInodeNumber]
		if !ok {
			// For any reason, the directory inode has been evicted and no longer needs to be prefetched
			globals.Unlock()
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
			globals.Unlock()
			return
		}

		// Now we should also cleanly exit if we reported that warning above

		if err != nil {
			dirInode.isPrefetchInProgress = false
			globals.Unlock()
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
			globals.Unlock()
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

	childDirInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(basename)
	if ok {
		childDirInode, ok = globals.inodeMap[childDirInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childDirInodeNumber] returned !ok [findChildDirInode() case 1]")
		}

		// [TODO] We might want to validate that childDirInode.inodeType == PseudoDir
		// [TODO] We might want to (1) validate the prefix exists and (2) if it doesn't, convert it & all descendents to "virt"

		return
	}

	childDirInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(basename)
	if ok {
		childDirInode, ok = globals.inodeMap[childDirInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childDirInodeNumber] returned !ok [findChildDirInode() case 1]")
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

	childFileInodeNumber, ok = parentInode.physChildInodeMap.GetByKey(basename)
	if ok {
		childFileInode, ok = globals.inodeMap[childFileInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childFileInodeNumber] returned !ok [findChildFileInode() case 1]")
		}

		// [TODO] We might want to validate that childFileInode.inodeType == FileObject
		// [TODO] We might want to (1) validate the object exists and (2) if it doesn't, convert it to "virt"

		return
	}

	childFileInodeNumber, ok = parentInode.virtChildInodeMap.GetByKey(basename)
	if ok {
		childFileInode, ok = globals.inodeMap[childFileInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childFileInodeNumber] returned !ok [findChildFileInode() case 1]")
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

	globals.Lock()

	rootDirInode, ok = globals.inodeMap[FUSERootDirInodeNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap[FUSERootDirInodeNumber] returned !ok")
	}
	if rootDirInode.inodeType != FUSERootDir {
		dumpStack()
		globals.logger.Fatalf("[FATAL] rootDirInode.inodeType(%v) should have been FUSERootDir(%v)", rootDirInode.inodeType, FUSERootDir)
	}

	rootDirInode.dumpFS(w, "", FUSERootDirInodeNumber, "")

	globals.Unlock()
}

// `dumpFS` called on a particular inode recursively dumps a file system element.
func (thisInode *inodeStruct) dumpFS(w io.Writer, indent string, expectedInodeNumber uint64, expectedBasename string) {
	var (
		backend            *backendStruct
		childInode         *inodeStruct
		childInodeBasename string
		childInodeMapIndex int
		childInodeMapLen   int
		childInodeNumber   uint64
		nextIndent         = indent + DUMP_FS_DIR_INDENT
		ok                 bool
		thisInodeBasename  string
	)

	backend, ok = globals.backendMap[thisInode.backendNonce]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.backendMap[thisInode.backendNonce] returned !ok")
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

	childInodeMapLen = thisInode.virtChildInodeMap.Len()

	for childInodeMapIndex = range childInodeMapLen {
		childInodeBasename, childInodeNumber, ok = thisInode.virtChildInodeMap.GetByIndex(childInodeMapIndex)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] thisInode.virtChildInodeMap.GetByIndex(childInodeMapIndex) returned !ok")
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

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case virt]")
		}

		childInode.dumpFS(w, nextIndent, childInodeNumber, childInodeBasename)
	}

	childInodeMapLen = thisInode.physChildInodeMap.Len()

	for childInodeMapIndex = range childInodeMapLen {
		childInodeBasename, childInodeNumber, ok = thisInode.physChildInodeMap.GetByIndex(childInodeMapIndex)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] thisInode.physChildInodeMap.GetByIndex(childInodeMapIndex) returned !ok")
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

		childInode, ok = globals.inodeMap[childInodeNumber]
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] globals.inodeMap[childInodeNumber] returned !ok [case phys]")
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

	globals.Lock()

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
		globals.Unlock()
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

	parentInode, ok = globals.inodeMap[thisInode.parentInodeNumber]
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] globals.inodeMap[thisInode.parentInodeNumber] returned !ok")
	}

	if thisInode.isVirt {
		ok = parentInode.virtChildInodeMap.DeleteByKey(thisInode.basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.virtChildInodeMap.DeleteByKey(thisInode.basename) returned !ok")
		}
	} else {
		ok = parentInode.physChildInodeMap.DeleteByKey(thisInode.basename)
		if !ok {
			dumpStack()
			globals.logger.Fatalf("[FATAL] parentInode.physChildInodeMap.DeleteByKey(thisInode.basename) returned !ok")
		}
	}

	delete(globals.inodeMap, thisInode.inodeNumber)

	parentInode.touch(nil)

	globals.Unlock()
}
