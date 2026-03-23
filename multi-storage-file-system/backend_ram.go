package main

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/sortedmap"
)

// `ramDirEntryDirMapStruct`holds a sorted list of subdirectories of a ramDirStruct.
type ramDirEntryDirMapStruct struct {
	llrb sortedmap.LLRBTree // key == basename; value == *ramDirStruct
}

// `ramDirEntryFileMapStruct`holds a sorted list of files of a ramDirStruct.
type ramDirEntryFileMapStruct struct {
	llrb sortedmap.LLRBTree // key == basename; value == []byte

}

// `ramDirStruct` represents a directory.
type ramDirStruct struct {
	dirName string // rootDir will be ""
	dirMap  ramDirEntryDirMapStruct
	fileMap ramDirEntryFileMapStruct
}

// `ramContextStruct` holds the RAM-specific backend details.
type ramContextStruct struct {
	backend             *backendStruct
	rootDir             *ramDirStruct
	curTotalObjects     uint64
	curTotalObjectSpace uint64
}

// `backendCommon` is called to return a pointer to the context's common `backendStruct`.
func (backend *ramContextStruct) backendCommon() (backendCommon *backendStruct) {
	backendCommon = backend.backend
	return
}

// `setupRAMContext` establishes the RAM client context. Once set up, each
// method defined in the `backendConfigIf` interface may be invoked.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupRAMContext() (err error) {
	backend.context = &ramContextStruct{
		backend:             backend,
		rootDir:             newRamDir(""),
		curTotalObjects:     0,
		curTotalObjectSpace: 0,
	}

	backend.backendPath = "ram://"

	err = nil
	return
}

/*
func TODOmoveThisToATestBench() {
	const (
		doDeletes = false

		enableInodeMap             = true
		enableInodeEvictionQueue   = true
		enablePhysChildDirEntryMap = true
		enableVirtChildDirEntryMap = false

		inodeNumberBase  = uint64(10000000)  //  10M
		inodeNumberCount = uint64(100000000) // 100M

		tUpdateDuration = 10 * time.Second
	)
	var (
		inode       *inodeStruct
		inodeNumber uint64
		limit       uint64
		ok          bool
		parentInode *inodeStruct
		start       uint64
	)

	globals.lastNonce = inodeNumberBase + inodeNumberCount

	parentInode = &inodeStruct{
		inodeNumber:       1,
		parentInodeNumber: 1,
		isVirt:            true,
		objectPath:        "/somedir",
		basename:          "somedir",
		eTag:              "",
		mTime:             time.Time{},
		xTime:             time.Time{},
		cacheMap:          nil,
		fhSet:             make(map[uint64]struct{}),
	}

	inode = &inodeStruct{
		inodeNumber:       0, // to be computed
		parentInodeNumber: 1,
		isVirt:            true,
		objectPath:        "/somedir/somebase",
		basename:          "somebase",
		eTag:              "someETag",
		mTime:             time.Now(),
		xTime:             time.Now().Add(time.Second),
		cacheMap:          make(map[uint64]uint64),
		fhSet:             make(map[uint64]struct{}),
	}

	tNextUpdate := time.Now().Add(tUpdateDuration)

	t0 := time.Now()

	if enableInodeMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber

			ok = toAddToGlobals.inodeMap.put(inode)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.inodeMap.put(inode) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.inodeMap.put(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t1 := time.Now()

	if enableInodeMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			_, ok = toAddToGlobals.inodeMap.get(inodeNumber)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.inodeMap.get(inodeNumber) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.inodeMap.get(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t2 := time.Now()

	if enableInodeMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber

			ok = toAddToGlobals.inodeMap.touch(inode)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.inodeMap.touch(inode) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.inodeMap.touch(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t3 := time.Now()

	if enableInodeMap {
		if doDeletes {
			for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
				inode.inodeNumber = inodeNumber

				ok = toAddToGlobals.inodeMap.delete(inode)
				if !ok {
					globals.logger.Fatalf("[FATAL] toAddToGlobals.inodeMap.delete(inode) returned !ok")
				}

				if time.Now().After(tNextUpdate) {
					globals.logger.Printf("[BENCH] toAddToGlobals.inodeMap.delete(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
					tNextUpdate = tNextUpdate.Add(tUpdateDuration)
				}
			}
		}
	}

	t4 := time.Now()

	if enableInodeEvictionQueue {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber

			ok = toAddToGlobals.inodeEvictionQueue.insert(inode)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.inodeEvictionQueue.insert(inode) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.inodeEvictionQueue.insert(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t5 := time.Now()

	if enableInodeEvictionQueue {
		if doDeletes {
			for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
				inode.inodeNumber = inodeNumber

				ok = toAddToGlobals.inodeEvictionQueue.remove(inode)
				if !ok {
					globals.logger.Fatalf("[FATAL] toAddToGlobals.inodeEvictionQueue.remove(inode) returned !ok")
				}

				if time.Now().After(tNextUpdate) {
					globals.logger.Printf("[BENCH] toAddToGlobals.inodeEvictionQueue.remove(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
					tNextUpdate = tNextUpdate.Add(tUpdateDuration)
				}
			}
		}
	}

	t6 := time.Now()

	if enablePhysChildDirEntryMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber
			inode.basename = fmt.Sprintf("%016X", inodeNumber)

			ok = toAddToGlobals.physChildDirEntryMap.put(parentInode, inode)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.physChildDirEntryMap.put(parentInode, inode) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.physChildDirEntryMap.put(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t7 := time.Now()

	if enablePhysChildDirEntryMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber
			inode.basename = fmt.Sprintf("%016X", inodeNumber)

			_, ok = toAddToGlobals.physChildDirEntryMap.getByBasename(parentInode, inode.basename)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.physChildDirEntryMap.getByBasename(parentInode, inodeMap[inodeNumber].basename) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.physChildDirEntryMap.getByBasename(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t8 := time.Now()

	if enablePhysChildDirEntryMap {
		start, limit = toAddToGlobals.physChildDirEntryMap.getIndexRange(parentInode)
	}

	t9 := time.Now()

	if enablePhysChildDirEntryMap {
		for i := start; i < limit; i++ {
			_, ok = toAddToGlobals.physChildDirEntryMap.getByIndex(i)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.physChildDirEntryMap.getByIndex(i) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.physChildDirEntryMap.getByIndex(%v/%v)...", (i - start + 1), (limit - start))
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t10 := time.Now()

	if enablePhysChildDirEntryMap {
		if doDeletes {
			for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
				inode.inodeNumber = inodeNumber
				inode.basename = fmt.Sprintf("%016X", inodeNumber)

				ok = toAddToGlobals.physChildDirEntryMap.delete(parentInode, inode)
				if !ok {
					globals.logger.Fatalf("[FATAL] toAddToGlobals.physChildDirEntryMap.delete(parentInode, inode) returned !ok")
				}

				if time.Now().After(tNextUpdate) {
					globals.logger.Printf("[BENCH] toAddToGlobals.physChildDirEntryMap.delete(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
					tNextUpdate = tNextUpdate.Add(tUpdateDuration)
				}
			}
		}
	}

	t11 := time.Now()

	if enableVirtChildDirEntryMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber
			inode.basename = fmt.Sprintf("%016X", inodeNumber)

			ok = toAddToGlobals.virtChildDirEntryMap.put(parentInode, inode)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.virtChildDirEntryMap.put(parentInode, inode) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.virtChildDirEntryMap.put(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t12 := time.Now()

	if enableVirtChildDirEntryMap {
		for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
			inode.inodeNumber = inodeNumber
			inode.basename = fmt.Sprintf("%016X", inodeNumber)

			_, ok = toAddToGlobals.virtChildDirEntryMap.getByBasename(parentInode, inode.basename)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.virtChildDirEntryMap.getByBasename(parentInode, inodeMap[inodeNumber].basename) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.virtChildDirEntryMap.getByBasename(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t13 := time.Now()

	if enableVirtChildDirEntryMap {
		start, limit = toAddToGlobals.virtChildDirEntryMap.getIndexRange(parentInode)
	}

	t14 := time.Now()

	if enableVirtChildDirEntryMap {
		for i := start; i < limit; i++ {
			_, ok = toAddToGlobals.virtChildDirEntryMap.getByIndex(i)
			if !ok {
				globals.logger.Fatalf("[FATAL] toAddToGlobals.virtChildDirEntryMap.getByIndex(i) returned !ok")
			}

			if time.Now().After(tNextUpdate) {
				globals.logger.Printf("[BENCH] toAddToGlobals.virtChildDirEntryMap.getByIndex(%v/%v)...", (i - start + 1), (limit - start))
				tNextUpdate = tNextUpdate.Add(tUpdateDuration)
			}
		}
	}

	t15 := time.Now()

	if enableVirtChildDirEntryMap {
		if doDeletes {
			for inodeNumber = inodeNumberBase; inodeNumber < inodeNumberBase+inodeNumberCount; inodeNumber++ {
				inode.inodeNumber = inodeNumber
				inode.basename = fmt.Sprintf("%016X", inodeNumber)

				ok = toAddToGlobals.virtChildDirEntryMap.delete(parentInode, inode)
				if !ok {
					globals.logger.Fatalf("[FATAL] toAddToGlobals.virtChildDirEntryMap.delete(parentInode, inode) returned !ok")
				}

				if time.Now().After(tNextUpdate) {
					globals.logger.Printf("[BENCH] toAddToGlobals.virtChildDirEntryMap.delete(%v/%v)...", (inodeNumber - inodeNumberBase + 1), inodeNumberCount)
					tNextUpdate = tNextUpdate.Add(tUpdateDuration)
				}
			}
		}
	}

	t16 := time.Now()

	if enableInodeMap {
		if doDeletes {
			globals.logger.Printf("[BENCH] Performing %v put/get/touch/delete operations on toAddToGlobals.inodeMap:", inodeNumberCount)
		} else {
			globals.logger.Printf("[BENCH] Performing %v put/get/touch operations on toAddToGlobals.inodeMap:", inodeNumberCount)
		}
		globals.logger.Printf("[BENCH]   .put():           %v", t1.Sub(t0))
		globals.logger.Printf("[BENCH]   .get():           %v", t2.Sub(t1))
		globals.logger.Printf("[BENCH]   .touch():         %v", t3.Sub(t2))
		if doDeletes {
			globals.logger.Printf("[BENCH]   .delete():        %v", t4.Sub(t3))
		}
	}

	if enableInodeEvictionQueue {
		if doDeletes {
			globals.logger.Printf("[BENCH] Performing %v insert/remove operations on toAddToGlobals.inodeEvictionQueue:", inodeNumberCount)
		} else {
			globals.logger.Printf("[BENCH] Performing %v insert operations on toAddToGlobals.inodeEvictionQueue:", inodeNumberCount)
		}
		globals.logger.Printf("[BENCH]   .insert():        %v", t5.Sub(t4))
		if doDeletes {
			globals.logger.Printf("[BENCH]   .remove():        %v", t6.Sub(t5))
		}
	}

	if enablePhysChildDirEntryMap {
		if doDeletes {
			globals.logger.Printf("[BENCH] Performing %v put/getByBasename/getByIndex/delete operations on toAddToGlobals.physChildDirEntryMap:", inodeNumberCount)
		} else {
			globals.logger.Printf("[BENCH] Performing %v put/getByBasename/getByIndex operations on toAddToGlobals.physChildDirEntryMap:", inodeNumberCount)
		}
		globals.logger.Printf("[BENCH]   .put():           %v", t7.Sub(t6))
		globals.logger.Printf("[BENCH]   .getByBasename(): %v", t8.Sub(t7))
		globals.logger.Printf("[BENCH]   .getByIndex():    %v [single .getByIndex(): %v]", t10.Sub(t9), t9.Sub(t8))
		if doDeletes {
			globals.logger.Printf("[BENCH]   .delete():        %v", t11.Sub(t10))
		}
	}

	if enableVirtChildDirEntryMap {
		if doDeletes {
			globals.logger.Printf("[BENCH] Performing %v put/getByBasename/getByIndex/delete operations on toAddToGlobals.virtChildDirEntryMap:", inodeNumberCount)
		} else {
			globals.logger.Printf("[BENCH] Performing %v put/getByBasename/getByIndex operations on toAddToGlobals.virtChildDirEntryMap:", inodeNumberCount)
		}
		globals.logger.Printf("[BENCH]   .put():           %v", t12.Sub(t11))
		globals.logger.Printf("[BENCH]   .getByBasename(): %v", t13.Sub(t12))
		globals.logger.Printf("[BENCH]   .getByIndex():    %v [single .getByIndex(): %v]", t15.Sub(t14), t14.Sub(t13))
		if doDeletes {
			globals.logger.Printf("[BENCH]   .delete():        %v", t16.Sub(t15))
		}
	}
}
*/

// `deleteFile` is called to remove a "file" at the specified path.
// If a `subdirectory` or nothing is found at that path, an error will be returned.
func (ramContext *ramContextStruct) deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
	var (
		dirName     []string
		fileContent []byte
		fileName    string
		ok          bool
		ramDir      []*ramDirStruct
		ramDirIndex int
	)

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalFilePath(deleteFileInput.filePath))
	if (len(dirName) + 1) > len(ramDir) {
		// Not all directories in the path exist... so we know fileName does not exist
		err = errors.New("file not found")
		return
	}

	ramDirIndex = len(ramDir) - 1

	fileContent, ok = ramDir[ramDirIndex].fileMap.GetByKey(fileName)
	if !ok {
		// Didn't find fileName in leaf ramDir... so we know fileName does not exist
		err = errors.New("file not found")
		return
	}

	// At this point, we know we will succeed...

	ok = ramDir[ramDirIndex].fileMap.DeleteByKey(fileName)
	if !ok {
		// We know fileName should have been in leaf ramDir, so if it failes, this is fatal
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDir[ramDirIndex].fileMap.DeleteByKey(fileName) returned !ok")
	}

	ramContext.curTotalObjects--
	ramContext.curTotalObjectSpace -= uint64(len(fileContent))

	err = nil

	// ...but we possibly have emptied one or more directories

	for ramDirIndex > 0 {
		if (ramDir[ramDirIndex].dirMap.Len() > 0) || (ramDir[ramDirIndex].fileMap.Len() > 0) {
			// ramDir[ramDirIndex] not empty, so we are done
			break
		}

		ok = ramDir[ramDirIndex-1].dirMap.DeleteByKey(ramDir[ramDirIndex].dirName)
		if !ok {
			// We know dirName should have been in our parent directory, so if it fails, this is fatal
			dumpStack()
			globals.logger.Fatalf("[FATAL] ramDir[ramDirIndex-1].dirMap.DeleteByKey(ramDir[ramDirIndex].dirName) returned !ok")
		}

		ramDirIndex--
	}

	return
}

// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
// indicates the `directory` has been completely enumerated. The `isTruncated` field will also
// align with this convention.
func (ramContext *ramContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	var (
		continuationTokenAsUint64 uint64
		dirName                   []string
		fileContent               []byte
		fileName                  string
		itemIndex                 uint64
		itemLimit                 uint64
		maxItems                  uint64
		numDirToReturn            uint64
		numFileToReturn           uint64
		ok                        bool
		ramDir                    []*ramDirStruct
		ramDirLeaf                *ramDirStruct
		ramDirLeafDirMapLen       uint64
		ramDirLeafFileMapLen      uint64
		subdirectoryName          string
		timeNow                   = time.Now()
	)

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalDirPath(listDirectoryInput.dirPath))
	if (len(dirName)+1 > len(ramDir)) || (fileName != "") {
		// To align with other "real" object store backends, we just return an empty response

		listDirectoryOutput = &listDirectoryOutputStruct{
			subdirectory:          make([]string, 0),
			file:                  make([]listDirectoryOutputFileStruct, 0),
			nextContinuationToken: "",
			isTruncated:           false,
		}

		err = nil
		return
	}

	ramDirLeaf = ramDir[len(ramDir)-1]

	ramDirLeafDirMapLen = uint64(ramDirLeaf.dirMap.Len())
	ramDirLeafFileMapLen = uint64(ramDirLeaf.fileMap.Len())

	if listDirectoryInput.continuationToken == "" {
		continuationTokenAsUint64 = 0
	} else {
		continuationTokenAsUint64, err = strconv.ParseUint(listDirectoryInput.continuationToken, 10, 64)
		if err != nil {
			err = fmt.Errorf("strconv.ParseUint(listDirectoryInput.continuationToken, 10, 64) failed: %v", err)
			return
		}
	}

	// At this point, we know we will succeed

	if listDirectoryInput.maxItems == 0 {
		maxItems = ramContext.backend.directoryPageSize // Possibly also zero
	} else { // listDirectoryInput.maxItems != 0
		if ramContext.backend.directoryPageSize == 0 {
			maxItems = listDirectoryInput.maxItems
		} else {
			if listDirectoryInput.maxItems < ramContext.backend.directoryPageSize {
				maxItems = listDirectoryInput.maxItems
			} else {
				maxItems = ramContext.backend.directoryPageSize
			}
		}
	}

	if continuationTokenAsUint64 < ramDirLeafDirMapLen {
		numDirToReturn = ramDirLeafDirMapLen - continuationTokenAsUint64
	} else {
		numDirToReturn = 0
	}

	if maxItems != 0 {
		if maxItems <= numDirToReturn {
			numDirToReturn = maxItems
			numFileToReturn = 0
		} else {
			numFileToReturn = maxItems - numDirToReturn
		}
	} else {
		numFileToReturn = ramDirLeafFileMapLen
	}

	itemLimit = continuationTokenAsUint64 + numDirToReturn + numFileToReturn

	listDirectoryOutput = &listDirectoryOutputStruct{
		subdirectory:          make([]string, 0, numDirToReturn),
		file:                  make([]listDirectoryOutputFileStruct, 0, numFileToReturn),
		nextContinuationToken: strconv.FormatUint(itemLimit, 10),
		isTruncated:           (itemLimit < (ramDirLeafDirMapLen + ramDirLeafFileMapLen)),
	}

	for itemIndex = continuationTokenAsUint64; itemIndex < itemLimit; itemIndex++ {
		if itemIndex < ramDirLeafDirMapLen {
			subdirectoryName, _, ok = ramDirLeaf.dirMap.GetByIndex(int(itemIndex))
			if !ok {
				// Since we previously discovered ramDirLeafDirMapLen, and have bounds checked it above,
				// we know itemIndex is a valid index into dirMap... so this is a fatal condition
				dumpStack()
				globals.logger.Fatalf("[FATAL] ramDirLeaf.dirMap.GetByIndex(int(itemIndex)) returned !ok")
			}
			listDirectoryOutput.subdirectory = append(listDirectoryOutput.subdirectory, subdirectoryName)
		} else { // itemIndex >= ramDirLeafDirMapLen
			fileName, fileContent, ok = ramDirLeaf.fileMap.GetByIndex(int(itemIndex - ramDirLeafDirMapLen))
			if !ok {
				// Since we previously discovered ramDirLeafDirMapLen and ramDirLeafFileMapLen
				// to compute itemLimit and know that itemIndex >= ramDirLeafDirMapLen,
				// we know itemIndex-ramDirLEafDirMapLen is a valid index indo fileMap... so this is a fatal condition
				dumpStack()
				globals.logger.Fatalf("[FATAL] ramDirLeaf.fileMap.GetByIndex(int(itemIndex - ramDirLeafDirMapLen)) returned !ok")
			}
			listDirectoryOutput.file = append(listDirectoryOutput.file, listDirectoryOutputFileStruct{
				basename: fileName,
				eTag:     "",
				mTime:    timeNow,
				size:     uint64(len(fileContent)),
			})
		}
	}

	err = nil
	return
}

// `appendObjects` is a func to append objects listed in a ramDirStruct's .fileMap as well as
// recursively invoke itself for any child ramDirStruct's listed in .dirMap (prefix'd with
// that ramDirStruct's .dirName+"/").
func (ramContext *ramContextStruct) appendObjects(thisDir *ramDirStruct, thisDirPrefix string, objectList *[]listObjectsOutputObjectStruct) {
	var (
		childDir          *ramDirStruct
		childDirBasename  string
		childFileContent  []byte
		childDirPrefix    string
		childFileBasename string
		dirMapIndex       int
		dirMapLen         int
		fileMapIndex      int
		fileMapLen        int
		ok                bool
		timeNow           = time.Now()
	)

	fileMapLen = thisDir.fileMap.Len()

	for fileMapIndex = range fileMapLen {
		childFileBasename, childFileContent, ok = thisDir.fileMap.GetByIndex(fileMapIndex)
		if !ok {
			globals.logger.Fatalf("[FATAL] thisDir.fileMap.GetByIndex(fileMapIndex) returned !ok")
		}

		*objectList = append(*objectList, listObjectsOutputObjectStruct{
			path:  thisDirPrefix + childFileBasename,
			eTag:  "",
			mTime: timeNow,
			size:  uint64(len(childFileContent)),
		})
	}

	dirMapLen = thisDir.dirMap.Len()

	for dirMapIndex = range dirMapLen {
		childDirBasename, childDir, ok = thisDir.dirMap.GetByIndex(dirMapIndex)
		if !ok {
			globals.logger.Fatalf("[FATAL] thisDir.dirMap.GetByIndex(dirMapIndex) returned !ok")
		}

		childDirPrefix = thisDirPrefix + childDirBasename + "/"

		ramContext.appendObjects(childDir, childDirPrefix, objectList)
	}
}

// `listObjects` is called to fetch a `page` of the objects. An empty continuationToken or
// empty list of elements (`objects`) indicates the list of `objects` has been completely
// enumerated. The `isTruncated` field will also align with this convention.
func (ramContext *ramContextStruct) listObjects(listObjectsInput *listObjectsInputStruct) (listObjectsOutput *listObjectsOutputStruct, err error) {
	var (
		continuationTokenAsUint64 uint64
		dirName                   []string
		fileName                  string
		itemIndex                 uint64
		itemLimit                 uint64
		maxItems                  uint64
		numObjectToReturn         uint64
		objectList                []listObjectsOutputObjectStruct
		ramDir                    []*ramDirStruct
		ramDirLeaf                *ramDirStruct
	)

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalDirPath(""))
	if (len(dirName)+1 > len(ramDir)) || (fileName != "") {
		// To align with other "real" object store backends, we just return an empty response

		listObjectsOutput = &listObjectsOutputStruct{
			object:                make([]listObjectsOutputObjectStruct, 0),
			nextContinuationToken: "",
			isTruncated:           false,
		}

		err = nil
		return
	}

	if listObjectsInput.continuationToken == "" {
		continuationTokenAsUint64 = 0
	} else {
		continuationTokenAsUint64, err = strconv.ParseUint(listObjectsInput.continuationToken, 10, 64)
		if err != nil {
			err = fmt.Errorf("strconv.ParseUint(listObjectsInput.continuationToken, 10, 64) failed: %v", err)
			return
		}
	}

	// At this point, we know we will succeed

	ramDirLeaf = ramDir[len(ramDir)-1]

	objectList = make([]listObjectsOutputObjectStruct, 0)

	ramContext.appendObjects(ramDirLeaf, "", &objectList)

	if listObjectsInput.maxItems == 0 {
		maxItems = ramContext.backend.directoryPageSize // Possibly also zero
	} else { // listDirectoryInput.maxItems != 0
		if ramContext.backend.directoryPageSize == 0 {
			maxItems = listObjectsInput.maxItems
		} else {
			if listObjectsInput.maxItems < ramContext.backend.directoryPageSize {
				maxItems = listObjectsInput.maxItems
			} else {
				maxItems = ramContext.backend.directoryPageSize
			}
		}
	}

	if continuationTokenAsUint64 < uint64(len(objectList)) {
		numObjectToReturn = uint64(len(objectList)) - continuationTokenAsUint64
	} else {
		numObjectToReturn = 0
	}

	if maxItems == 0 {
		itemLimit = uint64(len(objectList))
	} else {
		if (continuationTokenAsUint64 + maxItems) > uint64(len(objectList)) {
			numObjectToReturn = uint64(len(objectList)) - continuationTokenAsUint64
		}
		itemLimit = continuationTokenAsUint64 + maxItems
	}

	listObjectsOutput = &listObjectsOutputStruct{
		object:                make([]listObjectsOutputObjectStruct, 0, numObjectToReturn),
		nextContinuationToken: strconv.FormatUint(itemLimit, 10),
		isTruncated:           (itemLimit < uint64(len(objectList))),
	}

	for itemIndex = continuationTokenAsUint64; itemIndex < itemLimit; itemIndex++ {
		listObjectsOutput.object = append(listObjectsOutput.object, objectList[itemIndex])
	}

	// err = nil
	return
}

// `readFile` is called to read a range of a `file` at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (ramContext *ramContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	var (
		dirName     []string
		fileContent []byte
		fileName    string
		limit       uint64
		offset      uint64
		ok          bool
		ramDir      []*ramDirStruct
		ramDirIndex int
	)

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalFilePath(readFileInput.filePath))
	if (len(dirName) + 1) > len(ramDir) {
		// Not all directories in the path exist... so we know fileName does not exist
		err = errors.New("file not found")
		return
	}

	ramDirIndex = len(ramDir) - 1

	fileContent, ok = ramDir[ramDirIndex].fileMap.GetByKey(fileName)
	if !ok {
		// Didn't find fileName in leaf ramDir... so we know fileName does not exist
		err = errors.New("file not found")
		return
	}

	// At this point, we know we will succeed

	err = nil

	// Fetch copy of bytes to return

	offset = readFileInput.offsetCacheLine * globals.config.cacheLineSize
	limit = offset + globals.config.cacheLineSize

	switch {
	case offset >= uint64(len(fileContent)):
		offset = 0
		limit = 0
	case limit > uint64(len(fileContent)):
		limit = uint64(len(fileContent))
	default:
		// offset and limit are fine
	}

	readFileOutput = &readFileOutputStruct{
		eTag: "",
		buf:  make([]byte, limit-offset),
	}

	_ = copy(readFileOutput.buf, fileContent[offset:limit])

	return
}

// `statDirectory` is called to verify that the specified path refers to a `directory`.
// An error is returned if either the specified path is not a `directory` or non-existent.
func (ramContext *ramContextStruct) statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
	var (
		dirName  []string
		fileName string
		ramDir   []*ramDirStruct
	)

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalDirPath(statDirectoryInput.dirPath))
	if (len(dirName)+1 > len(ramDir)) || (fileName != "") {
		// Either not all directories in the path exist... or this is actually a reference to a file... so we know directory does not exist
		err = errors.New("directory not found")
		return
	}

	statDirectoryOutput = &statDirectoryOutputStruct{}

	err = nil
	return
}

// `statFile` is called to fetch the `file` metadata at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (ramContext *ramContextStruct) statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
	var (
		dirName     []string
		fileContent []byte
		fileName    string
		ok          bool
		ramDir      []*ramDirStruct
	)

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalFilePath(statFileInput.filePath))
	if (len(dirName)+1 > len(ramDir)) || (fileName == "") {
		// Either not all directories in the path exist... or this is actually not a reference to a file... so we know file does not exist
		err = errors.New("file not found")
		return
	}

	fileContent, ok = ramDir[len(ramDir)-1].fileMap.GetByKey(fileName)
	if !ok {
		// Containing directory existed, but file didn't
		err = errors.New("file not found")
		return
	}

	statFileOutput = &statFileOutputStruct{
		eTag:  "",
		mTime: time.Now(),
		size:  uint64(len(fileContent)),
	}

	err = nil
	return
}

// `canonicalDirPath` converts the supplied dirPath to `/[dirName/]*` (including ramContext.backend.prefix).
func (ramContext *ramContextStruct) canonicalDirPath(dirPath string) (canonicalDirPath string) {
	if ramContext.backend.prefix == "" {
		if dirPath == "" {
			canonicalDirPath = "/"
		} else {
			if strings.HasSuffix(dirPath, "/") {
				canonicalDirPath = "/" + dirPath
			} else {
				canonicalDirPath = "/" + dirPath + "/"
			}
		}
	} else {
		if dirPath == "" {
			canonicalDirPath = "/" + ramContext.backend.prefix
		} else {
			if strings.HasSuffix(dirPath, "/") {
				canonicalDirPath = "/" + ramContext.backend.prefix + dirPath
			} else {
				canonicalDirPath = "/" + ramContext.backend.prefix + dirPath + "/"
			}
		}
	}
	return
}

// `canonicalFilePath` converts the supplied filePath to `/[dirName/]*fileName` (including ramContext.backend.prefix).
func (ramContext *ramContextStruct) canonicalFilePath(filePath string) (canonicalFilePath string) {
	if ramContext.backend.prefix == "" {
		canonicalFilePath = "/" + filePath
	} else {
		canonicalFilePath = "/" + ramContext.backend.prefix + filePath
	}
	return
}

// `findFullPathElements` splits the supplied fullPath and locates ramDirStruct's for directory path elements
// should they currently exist. Note that directory paths should end in "/".
func (ramContext *ramContextStruct) findFullPathElements(fullPath string) (dirName []string, fileName string, ramDir []*ramDirStruct) {
	var (
		dirNameElement    string
		fullPathSplit     []string
		lastRamDirElement *ramDirStruct
		nextRamDirElement *ramDirStruct
		ok                bool
	)

	fullPathSplit = strings.Split(fullPath, "/")

	dirName = fullPathSplit[1 : len(fullPathSplit)-1]
	fileName = fullPathSplit[len(fullPathSplit)-1]

	ramDir = make([]*ramDirStruct, 1, len(dirName)+1)

	ramDir[0] = ramContext.rootDir
	lastRamDirElement = ramContext.rootDir

	for _, dirNameElement = range dirName {
		nextRamDirElement, ok = lastRamDirElement.dirMap.GetByKey(dirNameElement)
		if !ok {
			break
		}

		ramDir = append(ramDir, nextRamDirElement)
		lastRamDirElement = nextRamDirElement
	}

	return
}

// `newRamDir` creates a new ramDirStruct.`
func newRamDir(dirName string) (ramDir *ramDirStruct) {
	ramDir = &ramDirStruct{
		dirName: dirName,
		dirMap:  ramDirEntryDirMapStruct{},
		fileMap: ramDirEntryFileMapStruct{},
	}

	ramDir.dirMap.llrb = sortedmap.NewLLRBTree(sortedmap.CompareString, &ramDir.dirMap)
	ramDir.fileMap.llrb = sortedmap.NewLLRBTree(sortedmap.CompareString, &ramDir.fileMap)
	return
}

// `DeleteByKey` removes the string:*ramDirStruct element from ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) DeleteByKey(keyAsString string) (ok bool) {
	ok, err := ramDirEntryDirMap.llrb.DeleteByKey(keyAsString)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryDirMap.llrb.DeleteByKey(keyAsString) failed: %v", err)
	}
	return
}

// `GetByIndex` retrieves the string key at the requested index of ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) GetByIndex(index int) (keyAsString string, valueAsRamDir *ramDirStruct, ok bool) {
	keyAsKey, valueAsValue, ok, err := ramDirEntryDirMap.llrb.GetByIndex(index)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryDirMap.llrb.GetByIndex(index) failed: %v", err)
	}
	if !ok {
		return
	}
	keyAsString, ok = keyAsKey.(string)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] keyAsKey.(string) returned !ok")
	}
	valueAsRamDir, ok = valueAsValue.(*ramDirStruct)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] valueAsValue.(*ramDirStruct) returned !ok")
	}
	return
}

// `GetByKey` returns the *ramDirStruct value corresponding to the string key of ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) GetByKey(keyAsString string) (valueAsRamDir *ramDirStruct, ok bool) {
	valueAsValue, ok, err := ramDirEntryDirMap.llrb.GetByKey(keyAsString)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryDirMap.llrb.GetByKey(keyAsString) failed: %v", err)
	}
	if !ok {
		return
	}
	valueAsRamDir, ok = valueAsValue.(*ramDirStruct)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] valueAsValue.(*ramDirStruct) returned !ok")
	}
	return
}

// `Len` returns how many string:*ramDirStruct elements are in ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) Len() (numberOfItems int) {
	numberOfItems, err := ramDirEntryDirMap.llrb.Len()
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryDirMap.llrb.Len() failed: %v", err)
	}
	return
}

// `Put` sets the string's *ramDirStruct value in ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) Put(keyAsString string, valueAsRamDir *ramDirStruct) (ok bool) {
	ok, err := ramDirEntryDirMap.llrb.Put(keyAsString, valueAsRamDir)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryDirMap.llrb.Put(keyAsString, valueAsRamDir) failed: %v", err)
	}
	return
}

// `DumpKey` is a callback to format the string key in *ramDirEntryDirMapStruct as a string.
func (*ramDirEntryDirMapStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)
	if ok {
		err = nil
	} else {
		err = errors.New("key.(string) returned !ok")
	}
	return
}

// `DumpValue` is a callback to format the *ramDirStruct value in *ramDirEntryFileMapStruct as a string.
func (*ramDirEntryDirMapStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsRamDir, ok := value.(*ramDirStruct)
	if !ok {
		err = errors.New("value.(*ramDirStruct) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("%#v", valueAsRamDir)
	err = nil
	return
}

// `DeleteByKey` removes the string:*ramDirStruct element from ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) DeleteByKey(keyAsString string) (ok bool) {
	ok, err := ramDirEntryFileMap.llrb.DeleteByKey(keyAsString)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryFileMap.llrb.DeleteByKey(keyAsString) failed: %v", err)
	}
	return
}

// `GetByIndex` retrieves the string key at the requested index of ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) GetByIndex(index int) (keyAsString string, valueAsByteSlice []byte, ok bool) {
	keyAsKey, valueAsValue, ok, err := ramDirEntryFileMap.llrb.GetByIndex(index)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryFileMap.llrb.GetByIndex(index) failed: %v", err)
	}
	if !ok {
		return
	}
	keyAsString, ok = keyAsKey.(string)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] keyAsKey.(string) returned !ok")
	}
	valueAsByteSlice, ok = valueAsValue.([]byte)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] valueAsValue.([]byte) returned !ok")
	}
	return
}

// `GetByKey` returns the []byte value corresponding to the string key of ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) GetByKey(keyAsString string) (valueAsByteSlice []byte, ok bool) {
	valueAsValue, ok, err := ramDirEntryFileMap.llrb.GetByKey(keyAsString)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryFileMap.llrb.GetByKey(keyAsString) failed: %v", err)
	}
	if !ok {
		return
	}
	valueAsByteSlice, ok = valueAsValue.([]byte)
	if !ok {
		dumpStack()
		globals.logger.Fatalf("[FATAL] valueAsValue.([]byte) returned !ok")
	}
	return
}

// `Len` returns how many string:[]byte elements are in ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) Len() (numberOfItems int) {
	numberOfItems, err := ramDirEntryFileMap.llrb.Len()
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryFileMap.llrb.Len() failed: %v", err)
	}
	return
}

// `Put` sets the string's []byte value in ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) Put(keyAsString string, valueAsByteSlice []byte) (ok bool) {
	ok, err := ramDirEntryFileMap.llrb.Put(keyAsString, valueAsByteSlice)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		dumpStack()
		globals.logger.Fatalf("[FATAL] ramDirEntryFileMap.llrb.Put(keyAsString, valueAsByteSlice) failed: %v", err)
	}
	return
}

// `DumpKey` is a callback to format the string key in *ramDirEntryFileMapStruct as a string.
func (*ramDirEntryFileMapStruct) DumpKey(key sortedmap.Key) (keyAsString string, err error) {
	keyAsString, ok := key.(string)
	if ok {
		err = nil
	} else {
		err = errors.New("key.(string) returned !ok")
	}
	return
}

// `DumpValue` is a callback to format the []byte value in *ramDirEntryFileMapStruct as a string.
func (*ramDirEntryFileMapStruct) DumpValue(value sortedmap.Value) (valueAsString string, err error) {
	valueAsByteSlice, ok := value.([]byte)
	if !ok {
		err = errors.New("value.([]byte) returned !ok")
		return
	}

	valueAsString = fmt.Sprintf("%#v", valueAsByteSlice)
	err = nil
	return
}
