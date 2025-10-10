package main

import (
	"container/list"
	"context"
	"syscall"
	"time"
)

func initFS() {
	var (
		timeNow time.Time
	)

	globals.Lock()

	timeNow = time.Now()

	globals.lastNonce = FUSERootDirInodeNumber

	globals.inode = &inodeStruct{
		inodeNumber:       FUSERootDirInodeNumber,
		inodeType:         FUSERootDir,
		backend:           nil,
		parentInodeNumber: FUSERootDirInodeNumber,
		isVirt:            false,
		objectPath:        "",
		basename:          "",
		sizeInBackend:     0,
		sizeInMemory:      0,
		eTag:              "",
		mode:              uint32(syscall.S_IFDIR | globals.config.dirPerm),
		mTime:             timeNow,
		lTime:             timeNow,
		listElement:       nil,
		fhMap:             make(map[uint64]*fhStruct),
		virtChildDirMap:   newStringToUint64Map(VirtChildDirMap),
		virtChildFileMap:  newStringToUint64Map(VirtChildFileMap),
		cache:             nil,
	}

	globals.inodeMap = make(map[uint64]*inodeStruct)

	_ = globals.inode.virtChildDirMap.Put(DotDirEntryBasename, FUSERootDirInodeNumber)
	_ = globals.inode.virtChildDirMap.Put(DotDotDirEntryBasename, FUSERootDirInodeNumber)

	globals.inodeMap[FUSERootDirInodeNumber] = globals.inode

	globals.inodeLRU = list.New()

	globals.inodeEvictorContext, globals.inodeEvictorCancelFunc = context.WithCancel(context.Background())
	globals.inodeEvictorWaitGroup.Go(inodeEvictor)

	globals.inboundCacheLineCount = 0
	globals.cleanCacheLineLRU = list.New()
	globals.outboundCacheLineCount = 0
	globals.dirtyCacheLineLRU = list.New()

	globals.Unlock()
}

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

	processNextToUnmountListAlreadyLocked()

	globals.Unlock()
}

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
			globals.logger.Printf("unable to setup backend context: %v [skipping]", err)
			continue
		}

		backend.inode = &inodeStruct{
			inodeNumber:       fetchNonce(),
			inodeType:         BackendRootDir,
			backend:           backend,
			parentInodeNumber: FUSERootDirInodeNumber,
			isVirt:            false,
			objectPath:        "",
			basename:          dirName,
			sizeInBackend:     0,
			sizeInMemory:      0,
			eTag:              "",
			mode:              uint32(syscall.S_IFDIR | backend.dirPerm),
			mTime:             timeNow,
			lTime:             timeNow,
			listElement:       nil,
			fhMap:             make(map[uint64]*fhStruct),
			virtChildDirMap:   newStringToUint64Map(VirtChildDirMap),
			virtChildFileMap:  newStringToUint64Map(VirtChildFileMap),
			cache:             nil,
		}

		ok = globals.inode.virtChildDirMap.Put(backend.dirName, backend.inode.inodeNumber)
		if !ok {
			globals.logger.Fatalf("put of \"%s\" into backend.inode.virtChildDirMap returned !ok", backend.dirName)
		}

		_ = backend.inode.virtChildDirMap.Put(DotDirEntryBasename, backend.inode.inodeNumber)
		_ = backend.inode.virtChildDirMap.Put(DotDotDirEntryBasename, FUSERootDirInodeNumber)

		globals.inodeMap[backend.inode.inodeNumber] = backend.inode

		backend.inodeMap = make(map[string]*inodeStruct)

		backend.mounted = true

		globals.config.backends[dirName] = backend
	}

	globals.Unlock()
}

func processNextToUnmountList() {
	globals.Lock()
	processNextToUnmountListAlreadyLocked()
	globals.Unlock()
}

func processNextToUnmountListAlreadyLocked() {
	var (
		backend *backendStruct
		dirName string
		ok      bool
	)

	for dirName, backend = range globals.backendsToUnmount {
		delete(globals.backendsToUnmount, dirName)

		backend.inodeMapEmpty()

		delete(globals.inodeMap, backend.inode.inodeNumber)

		ok = globals.inode.virtChildDirMap.DeleteByKey(backend.dirName)
		if !ok {
			globals.logger.Fatalf("delete of \"%s\" from globals.inode.virtChildDirMap returned !ok", backend.dirName)
		}

		backend.mounted = false

		delete(globals.config.backends, dirName)
	}
}

func (backend *backendStruct) inodeMapEmpty() {
	var (
		inode *inodeStruct
	)

	for _, inode = range backend.inodeMap {
		// [TODO] (*backendStruct) inodeMapEmpty() should attempt to flush dirty cache lines
		delete(globals.inodeMap, inode.inodeNumber)
		if inode.listElement != nil {
			globals.inodeLRU.Remove(inode.listElement)
		}
	}

	clear(backend.inodeMap)
}

// createPseudoDirInode is called while globals.Lock() is held to create a new PsuedoDir inodeStruct.
func (parentInode *inodeStruct) createPseudoDirInode(isVirt bool, basename string) (pseudoDirInode *inodeStruct) {
	var (
		ok      bool
		timeNow = time.Now()
	)

	pseudoDirInode = &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         PseudoDir,
		backend:           parentInode.backend,
		parentInodeNumber: parentInode.inodeNumber,
		isVirt:            isVirt,
		// objectPath: filled in below
		basename:      basename,
		sizeInBackend: 0,
		sizeInMemory:  0,
		eTag:          "",
		mode:          uint32(syscall.S_IFDIR | parentInode.backend.dirPerm),
		mTime:         timeNow,
		lTime:         timeNow,
		// listElement: filled in below
		fhMap:            make(map[uint64]*fhStruct),
		virtChildDirMap:  newStringToUint64Map(VirtChildDirMap), // populated with {Dot|DotDot}DirEntryBasename below
		virtChildFileMap: newStringToUint64Map(VirtChildFileMap),
		cache:            nil,
	}

	if parentInode.objectPath == "" {
		pseudoDirInode.objectPath = basename + "/"
	} else {
		pseudoDirInode.objectPath = parentInode.objectPath + basename + "/"
	}

	if isVirt {
		ok = parentInode.virtChildDirMap.Put(pseudoDirInode.basename, pseudoDirInode.inodeNumber)
		if !ok {
			globals.logger.Fatalf("parentInode.virtChildDirMap.Put(pseudoDirInode.basename, pseudoDirInode.inodeNumber) returned !ok")
		}

		pseudoDirInode.listElement = nil
	} else {
		pseudoDirInode.listElement = globals.inodeLRU.PushBack(pseudoDirInode)
	}

	_ = pseudoDirInode.virtChildDirMap.Put(DotDirEntryBasename, pseudoDirInode.inodeNumber)
	_ = pseudoDirInode.virtChildDirMap.Put(DotDotDirEntryBasename, parentInode.inodeNumber)

	globals.inodeMap[pseudoDirInode.inodeNumber] = pseudoDirInode

	pseudoDirInode.backend.inodeMap[pseudoDirInode.objectPath] = pseudoDirInode

	return
}

// createFileObjectInode is called while globals.Lock() is held to create a new FileObject inodeStruct.
func (parentInode *inodeStruct) createFileObjectInode(isVirt bool, basename string, size uint64, eTag string, mTime time.Time) (fileObjectInode *inodeStruct) {
	var (
		ok      bool
		timeNow = time.Now()
	)

	fileObjectInode = &inodeStruct{
		inodeNumber:       fetchNonce(),
		inodeType:         FileObject,
		backend:           parentInode.backend,
		parentInodeNumber: parentInode.inodeNumber,
		isVirt:            isVirt,
		// objectPath: filled in below
		basename:      basename,
		sizeInBackend: size,
		sizeInMemory:  size,
		eTag:          eTag,
		mode:          uint32(syscall.S_IFREG | parentInode.backend.filePerm),
		mTime:         mTime,
		lTime:         timeNow,
		// listElement: filled in below
		fhMap:            make(map[uint64]*fhStruct),
		virtChildDirMap:  nil,
		virtChildFileMap: nil,
		cache:            make(map[uint64]*cacheLineStruct),
	}

	if parentInode.objectPath == "" {
		fileObjectInode.objectPath = basename
	} else {
		fileObjectInode.objectPath = parentInode.objectPath + basename
	}

	if isVirt {
		ok = parentInode.virtChildFileMap.Put(fileObjectInode.basename, fileObjectInode.inodeNumber)
		if !ok {
			globals.logger.Fatalf("parentInode.virtChildFileMap.Put(fileObjectInode.basename, fileObjectInode.inodeNumber) returned !ok")
		}

		fileObjectInode.listElement = nil
	} else {
		fileObjectInode.listElement = globals.inodeLRU.PushBack(fileObjectInode)
	}

	globals.inodeMap[fileObjectInode.inodeNumber] = fileObjectInode

	fileObjectInode.backend.inodeMap[fileObjectInode.objectPath] = fileObjectInode

	return
}

func (inode *inodeStruct) isEvictable() (evictable bool) {
	switch inode.inodeType {
	case FileObject:
		// [TODO] FileObject's can't be evicted if dirty
		evictable = !inode.isVirt && (len(inode.fhMap) == 0)
	case FUSERootDir:
		evictable = false
	case BackendRootDir:
		evictable = false
	case PseudoDir:
		evictable = !inode.isVirt && (len(inode.fhMap) == 0) && (inode.virtChildDirMap.Len() == 2) && (inode.virtChildFileMap.Len() == 0)
	default:
		globals.logger.Fatalf("unrecognized inodeType (%v)", inode.inodeType)
	}

	return
}

func (inode *inodeStruct) touch(mTimeAsInterface, lTimeAsInterface interface{}) {
	var (
		ok bool
	)

	if mTimeAsInterface != nil {
		inode.mTime, ok = mTimeAsInterface.(time.Time)
		if !ok {
			globals.logger.Fatalf("mTimeAsInterface.(time.Time) returned !ok")
		}
	}

	if inode.listElement != nil {
		_ = globals.inodeLRU.Remove(inode.listElement)
		inode.listElement = nil
	}

	if lTimeAsInterface == nil {
		inode.lTime = time.Now()
	} else {
		inode.lTime, ok = lTimeAsInterface.(time.Time)
		if !ok {
			globals.logger.Fatalf("lTimeAsInterface.(time.Time) returned !ok")
		}
	}

	if inode.isEvictable() {
		inode.listElement = globals.inodeLRU.PushBack(inode)
	}
}

func inodeEvictor() {
	var (
		listElement *list.Element
		ok          bool
		oldInode    *inodeStruct
		parentInode *inodeStruct
		ticker      *time.Ticker
		timeNow     time.Time
	)

	ticker = time.NewTicker(globals.config.evictableInodeTTL)

	for {
		select {
		case <-ticker.C:
			globals.Lock()
			timeNow = time.Now()
			for {
				listElement = globals.inodeLRU.Front()
				if listElement == nil {
					break
				}

				oldInode, ok = listElement.Value.(*inodeStruct)
				if !ok {
					globals.logger.Fatalf("listElement.Value.(*inodeStruct) returned !ok")
				}

				if oldInode.lTime.Add(globals.config.evictableInodeTTL).After(timeNow) {
					break
				}

				delete(globals.inodeMap, oldInode.inodeNumber)
				_ = globals.inodeLRU.Remove(oldInode.listElement)

				delete(oldInode.backend.inodeMap, oldInode.objectPath)

				parentInode, ok = globals.inodeMap[oldInode.parentInodeNumber]
				if ok {
					switch oldInode.inodeType {
					case FileObject:
						_ = parentInode.virtChildFileMap.DeleteByKey(oldInode.basename)
					case PseudoDir:
						_ = parentInode.virtChildDirMap.DeleteByKey(oldInode.basename)
					default:
						globals.logger.Fatalf("oldInode.inodeType(%v) must be either FileObject(%v) or PseudoDir(%v)", oldInode.inodeType, FileObject, PseudoDir)
					}
				}
			}
			globals.Unlock()
		case <-globals.inodeEvictorContext.Done():
			ticker.Stop()
			return
		}
	}
}

// findChildInode is called while globals.Lock() is held to locate the child's inodeStruct.
func (parentInode *inodeStruct) findChildInode(basename string) (childInode *inodeStruct, ok bool) {
	var (
		childInodeNumber   uint64
		dirOrFilePath      string
		err                error
		statDirectoryInput *statDirectoryInputStruct
		statFileInput      *statFileInputStruct
		statFileOutput     *statFileOutputStruct
	)

	defer func() {
		parentInode.touch(nil, nil)

		if ok {
			childInode.touch(nil, nil)
		}
	}()

	if parentInode.objectPath == "" {
		dirOrFilePath = basename
	} else {
		dirOrFilePath = parentInode.objectPath + basename
	}

	// First look for an existing object in the backend

	statFileInput = &statFileInputStruct{
		filePath: dirOrFilePath,
		ifMatch:  "",
	}

	statFileOutput, err = parentInode.backend.context.statFile(statFileInput)
	if err == nil {
		// We found an existing object in the backend

		childInode, ok = parentInode.backend.inodeMap[dirOrFilePath]
		if ok {
			// We also found an existing childInode... if it was previously virtual, it certainly shouldn't anymore

			if childInode.isVirt {
				childInode.isVirt = false

				if childInode.inodeType == FileObject {
					ok = parentInode.virtChildFileMap.DeleteByKey(basename)
					if !ok {
						globals.logger.Fatalf("parentInode.virtChildFileMap.DeleteByKey(basename) returned !ok")
					}
				} else {
					ok = parentInode.virtChildDirMap.DeleteByKey(basename)
					if !ok {
						globals.logger.Fatalf("parentInode.virtChildDirMap.DeleteByKey(basename) returned !ok")
					}
				}
			}

			// Since backend contains an object, childInode must be a FileObject

			if childInode.inodeType == FileObject {
				// Make sure childInode matches statFileOutput before returning it

				childInode.sizeInBackend = statFileOutput.size
				childInode.eTag = statFileOutput.eTag
				childInode.mTime = statFileOutput.mTime

				ok = true
				return
			}

			// Current childInode must be orphaned as we will have a new FileObject childInode

			clear(childInode.fhMap)

			delete(parentInode.backend.inodeMap, dirOrFilePath)
		}

		// If we reach here, we need to create a new FileObject childInode from statFileOuput

		childInode = parentInode.createFileObjectInode(false, basename, statFileOutput.size, statFileOutput.eTag, statFileOutput.mTime)

		ok = true
		return
	}

	// No object found in the backend... what about an object prefix?

	statDirectoryInput = &statDirectoryInputStruct{
		dirPath: dirOrFilePath,
	}

	_, err = parentInode.backend.context.statDirectory(statDirectoryInput)
	if err == nil {
		// We found an existing object prefix in the backend
		// By convention, we modify dirOrFilePath to end in "/"

		dirOrFilePath += "/"

		childInode, ok = parentInode.backend.inodeMap[dirOrFilePath]
		if ok {
			// We also found an existing childInode... it it was previously virtual, it certainly shouldn't be anymore

			if childInode.isVirt {
				childInode.isVirt = false

				if childInode.inodeType == FileObject {
					ok = parentInode.virtChildFileMap.DeleteByKey(basename)
					if !ok {
						globals.logger.Fatalf("parentInode.virtChildFileMap.DeleteByKey(basename) returned !ok")
					}
				} else {
					ok = parentInode.virtChildDirMap.DeleteByKey(basename)
					if !ok {
						globals.logger.Fatalf("parentInode.virtChildDirMap.DeleteByKey(basename) returned !ok")
					}
				}
			}

			// Since backend contains an object prefix, childInode must be a PseudoDir

			if childInode.inodeType == PseudoDir {
				// We can simply return childInode

				ok = true
				return
			}

			// Current childInode must be orphaned as we will have a new PseudoDir childInode

			clear(childInode.fhMap)

			delete(parentInode.backend.inodeMap, dirOrFilePath)
		}

		// If we reach here, we need to create a new PseudoDir childInode

		childInode = parentInode.createPseudoDirInode(false, basename)

		ok = true
		return
	}

	// We found neither an object nor an object prefix in the backned... perhaps basename is virtual?

	childInodeNumber, ok = parentInode.virtChildDirMap.GetByKey(basename)
	if ok {
		// We have a virtual directory... return it

		childInode, ok = globals.inodeMap[childInodeNumber] // or parentInode.backend.inodeMap[dirOrFilePath]
		if !ok {
			globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok [case 1]")
		}

		return
	}

	// No virtual directory... how about a virtual file?

	childInodeNumber, ok = parentInode.virtChildFileMap.GetByKey(basename)
	if !ok {
		// No virtual file either... so we fail here

		return
	}

	childInode, ok = globals.inodeMap[childInodeNumber] // or parentInode.backend.inodeMap[dirOrFilePath]
	if !ok {
		globals.logger.Fatalf("globals.inodeMap[childInodeNumber] returned !ok [case 2]")
	}

	// We have a virtual file... return it

	return
}

func (parentInode *inodeStruct) findChildDirInode(basename string) (childDirInode *inodeStruct) {
	var (
		dirPath string
		ok      bool
	)

	defer func() {
		parentInode.touch(nil, nil)

		if ok {
			childDirInode.touch(nil, nil)
		}
	}()

	if parentInode.objectPath == "" {
		dirPath = basename + "/"
	} else {
		dirPath = parentInode.objectPath + basename + "/"
	}

	childDirInode, ok = parentInode.backend.inodeMap[dirPath]
	if ok {
		// We found an existing childInode... if it was previously virtual, it certainly shouldn't anymore

		if childDirInode.isVirt {
			childDirInode.isVirt = false

			if childDirInode.inodeType == FileObject {
				ok = parentInode.virtChildFileMap.DeleteByKey(basename)
				if !ok {
					globals.logger.Fatalf("parentInode.virtChildFileMap.DeleteByKey(basename) returned !ok")
				}
			} else {
				ok = parentInode.virtChildDirMap.DeleteByKey(basename)
				if !ok {
					globals.logger.Fatalf("parentInode.virtChildDirMap.DeleteByKey(basename) returned !ok")
				}
			}
		}

		// If childDirInode is, in fact, a Dir Inode, we can simply return it

		if childDirInode.inodeType != FileObject {
			return
		}

		// Current childDirInode must be orphaned as we will have a new PseudoDir childDirInode

		clear(childDirInode.fhMap)

		delete(parentInode.backend.inodeMap, dirPath)
	}

	// If we reach here, we need to create a new PseudoDir childDirInode

	childDirInode = parentInode.createPseudoDirInode(false, basename)

	return
}

func (parentInode *inodeStruct) findChildFileInode(basename, eTag string, mTime time.Time, size uint64) (childFileInode *inodeStruct) {
	var (
		filePath string
		ok       bool
	)

	defer func() {
		parentInode.touch(nil, nil)

		if ok {
			childFileInode.touch(nil, nil)
		}
	}()

	if parentInode.objectPath == "" {
		filePath = basename
	} else {
		filePath = parentInode.objectPath + basename
	}

	childFileInode, ok = parentInode.backend.inodeMap[filePath]
	if ok {
		// We found an existing childInode... if it was previously virtual, it certainly shouldn't anymore

		if childFileInode.isVirt {
			childFileInode.isVirt = false

			if childFileInode.inodeType == FileObject {
				ok = parentInode.virtChildFileMap.DeleteByKey(basename)
				if !ok {
					globals.logger.Fatalf("parentInode.virtChildFileMap.DeleteByKey(basename) returned !ok")
				}
			} else {
				ok = parentInode.virtChildDirMap.DeleteByKey(basename)
				if !ok {
					globals.logger.Fatalf("parentInode.virtChildDirMap.DeleteByKey(basename) returned !ok")
				}
			}
		}

		// If childFileInode is, in fact, a FileObject Inode, we can simply return it

		if childFileInode.inodeType == FileObject {
			// Make sure childFileInode matches supplied eTag/mTime/size
			// Make sure childInode matches statFileOutput before returning it

			childFileInode.sizeInBackend = size
			childFileInode.eTag = eTag
			childFileInode.mTime = mTime

			return
		}

		// Current childFileInode must be orphaned as we will have a new FileObject childFileInode

		clear(childFileInode.fhMap)

		delete(parentInode.backend.inodeMap, filePath)
	}

	// If we reach here, we need to create a new FileObject childFileInode

	childFileInode = parentInode.createFileObjectInode(false, basename, size, eTag, mTime)

	return
}
