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

	err = nil
	return
}

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

	defer func() {
		switch ramContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", ramContext.backend.dirName, deleteFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.deleteFile(%#v) succeeded", ramContext.backend.dirName, deleteFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", ramContext.backend.dirName, deleteFileInput, err)
			}
		}
	}()

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
		globals.logger.Fatalf("ramDir[ramDirIndex].fileMap.DeleteByKey(fileName) returned !ok")
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
			globals.logger.Fatalf("ramDir[ramDirIndex-1].dirMap.DeleteByKey(ramDir[ramDirIndex].dirName) returned !ok")
		}

		ramDirIndex--
	}

	return
}

// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
// indicates the `directory` has been completely enumerated. An error is returned if either the
// specified path is not a `directory` or non-existent.
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
	)

	defer func() {
		switch ramContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", ramContext.backend.dirName, listDirectoryInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.listDirectory(%#v) succeeded", ramContext.backend.dirName, listDirectoryInput)
			} else {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", ramContext.backend.dirName, listDirectoryInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.listDirectory(%#v) returning deleteFileOutput: %#v", ramContext.backend.dirName, listDirectoryInput, listDirectoryOutput)
			} else {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", ramContext.backend.dirName, listDirectoryInput, err)
			}
		}
	}()

	dirName, fileName, ramDir = ramContext.findFullPathElements(ramContext.canonicalDirPath(listDirectoryInput.dirPath))
	if (len(dirName)+1 > len(ramDir)) || (fileName != "") {
		// Either not all directories in the path exist... or this is actually a reference to a file... so we know directory does not exist
		err = errors.New("directory not found")
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
		isTruncated:           (itemLimit) < (ramDirLeafDirMapLen + ramDirLeafFileMapLen),
	}

	for itemIndex = continuationTokenAsUint64; itemIndex < itemLimit; itemIndex++ {
		if itemIndex < ramDirLeafDirMapLen {
			subdirectoryName, _, ok = ramDirLeaf.dirMap.GetByIndex(int(itemIndex))
			if !ok {
				// Since we previously discovered ramDirLeafDirMapLen, and have bounds checked it above,
				// we know itemIndex is a valid index into dirMap... so this is a fatal condition
				globals.logger.Fatalf("ramDirLeaf.dirMap.GetByIndex(int(itemIndex)) returned !ok")
			}
			listDirectoryOutput.subdirectory = append(listDirectoryOutput.subdirectory, subdirectoryName)
		} else { // itemIndex >= ramDirLeafDirMapLen
			fileName, fileContent, ok = ramDirLeaf.fileMap.GetByIndex(int(itemIndex - ramDirLeafDirMapLen))
			if !ok {
				// Since we previously discovered ramDirLeafDirMapLen and ramDirLeafFileMapLen
				// to compute itemLimit and know that itemIndex >= ramDirLeafDirMapLen,
				// we know itemIndex-ramDirLEafDirMapLen is a valid index indo fileMap... so this is a fatal condition
				globals.logger.Fatalf("ramDirLeaf.fileMap.GetByIndex(int(itemIndex - ramDirLeafDirMapLen)) returned !ok")
			}
			listDirectoryOutput.file = append(listDirectoryOutput.file, listDirectoryOutputFileStruct{
				basename: fileName,
				eTag:     "",
				mTime:    time.Now(),
				size:     uint64(len(fileContent)),
			})
		}
	}

	err = nil
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

	defer func() {
		switch ramContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", ramContext.backend.dirName, readFileInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.readFile(%#v) succeeded", ramContext.backend.dirName, readFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", ramContext.backend.dirName, readFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.readFile(%#v) returning deleteFileOutput: {\"eTag\":\"%s\",len(\"buf\":%v)}", ramContext.backend.dirName, readFileInput, readFileOutput.eTag, len(readFileOutput.buf))
			} else {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", ramContext.backend.dirName, readFileInput, err)
			}
		}
	}()

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

	defer func() {
		switch ramContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", ramContext.backend.dirName, statDirectoryInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statDirectory(%#v) succeeded", ramContext.backend.dirName, statDirectoryInput)
			} else {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", ramContext.backend.dirName, statDirectoryInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statDirectory(%#v) returning deleteFileOutput: %#v", ramContext.backend.dirName, statDirectoryInput, statDirectoryOutput)
			} else {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", ramContext.backend.dirName, statDirectoryInput, err)
			}
		}
	}()

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

	defer func() {
		switch ramContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", ramContext.backend.dirName, statFileInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statFile(%#v) succeeded", ramContext.backend.dirName, statFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", ramContext.backend.dirName, statFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statFile(%#v) returning deleteFileOutput: %#v", ramContext.backend.dirName, statFileInput, statFileOutput)
			} else {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", ramContext.backend.dirName, statFileInput, err)
			}
		}
	}()

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
		globals.logger.Fatalf("ramDirEntryDirMap.llrb.DeleteByKey(keyAsString) failed: %v", err)
	}
	return
}

// `GetByIndex` retrieves the string key at the requested index of ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) GetByIndex(index int) (keyAsString string, valueAsRamDir *ramDirStruct, ok bool) {
	keyAsKey, valueAsValue, ok, err := ramDirEntryDirMap.llrb.GetByIndex(index)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryDirMap.llrb.GetByIndex(index) failed: %v", err)
	}
	if !ok {
		return
	}
	keyAsString, ok = keyAsKey.(string)
	if !ok {
		globals.logger.Fatalf("keyAsKey.(string) returned !ok")
	}
	valueAsRamDir, ok = valueAsValue.(*ramDirStruct)
	if !ok {
		globals.logger.Fatalf("valueAsValue.(*ramDirStruct) returned !ok")
	}
	return
}

// `GetByKey` returns the *ramDirStruct value corresponding to the string key of ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) GetByKey(keyAsString string) (valueAsRamDir *ramDirStruct, ok bool) {
	valueAsValue, ok, err := ramDirEntryDirMap.llrb.GetByKey(keyAsString)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryDirMap.llrb.GetByKey(keyAsString) failed: %v", err)
	}
	if !ok {
		return
	}
	valueAsRamDir, ok = valueAsValue.(*ramDirStruct)
	if !ok {
		globals.logger.Fatalf("valueAsValue.(*ramDirStruct) returned !ok")
	}
	return
}

// `Len` returns how many string:*ramDirStruct elements are in ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) Len() (numberOfItems int) {
	numberOfItems, err := ramDirEntryDirMap.llrb.Len()
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryDirMap.llrb.Len() failed: %v", err)
	}
	return
}

// `Put` sets the string's *ramDirStruct value in ramDirEntryDirMap.
func (ramDirEntryDirMap *ramDirEntryDirMapStruct) Put(keyAsString string, valueAsRamDir *ramDirStruct) (ok bool) {
	ok, err := ramDirEntryDirMap.llrb.Put(keyAsString, valueAsRamDir)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryDirMap.llrb.Put(keyAsString, valueAsRamDir) failed: %v", err)
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
		globals.logger.Fatalf("ramDirEntryFileMap.llrb.DeleteByKey(keyAsString) failed: %v", err)
	}
	return
}

// `GetByIndex` retrieves the string key at the requested index of ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) GetByIndex(index int) (keyAsString string, valueAsByteSlice []byte, ok bool) {
	keyAsKey, valueAsValue, ok, err := ramDirEntryFileMap.llrb.GetByIndex(index)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryFileMap.llrb.GetByIndex(index) failed: %v", err)
	}
	if !ok {
		return
	}
	keyAsString, ok = keyAsKey.(string)
	if !ok {
		globals.logger.Fatalf("keyAsKey.(string) returned !ok")
	}
	valueAsByteSlice, ok = valueAsValue.([]byte)
	if !ok {
		globals.logger.Fatalf("valueAsValue.([]byte) returned !ok")
	}
	return
}

// `GetByKey` returns the []byte value corresponding to the string key of ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) GetByKey(keyAsString string) (valueAsByteSlice []byte, ok bool) {
	valueAsValue, ok, err := ramDirEntryFileMap.llrb.GetByKey(keyAsString)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryFileMap.llrb.GetByKey(keyAsString) failed: %v", err)
	}
	if !ok {
		return
	}
	valueAsByteSlice, ok = valueAsValue.([]byte)
	if !ok {
		globals.logger.Fatalf("valueAsValue.([]byte) returned !ok")
	}
	return
}

// `Len` returns how many string:[]byte elements are in ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) Len() (numberOfItems int) {
	numberOfItems, err := ramDirEntryFileMap.llrb.Len()
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryFileMap.llrb.Len() failed: %v", err)
	}
	return
}

// `Put` sets the string's []byte value in ramDirEntryFileMap.
func (ramDirEntryFileMap *ramDirEntryFileMapStruct) Put(keyAsString string, valueAsByteSlice []byte) (ok bool) {
	ok, err := ramDirEntryFileMap.llrb.Put(keyAsString, valueAsByteSlice)
	if err != nil {
		// A non-nil err indicates a fatally corrupt llrb
		globals.logger.Fatalf("ramDirEntryFileMap.llrb.Put(keyAsString, valueAsByteSlice) failed: %v", err)
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
