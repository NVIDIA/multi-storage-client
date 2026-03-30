package main

import (
	"errors"
	"fmt"
	"hash/crc32"
	"strconv"
	"strings"
	"time"
)

const (
	PSEUDOFileNameFormat         = "file_%08X" // Must lexigraphically sort after  PSEUDOSubdirectoryNameFormat and end with a unique "_" character followed by 8 uppercase Hex Digits
	PSEUDOSubdirectoryNameFormat = "dir_%08X"  // Must lexigraphically sort before PSEUDOFileNameFormat         and end with a unique "_" character followed by 8 uppercase Hex Digits

	PSEUDOFileCRC32ETagFormat = "%08X"
)

// `pseudoContextStruct` holds the PSEUDO-specific backend details.
type pseudoContextStruct struct {
	backend       *backendStruct
	backendPSEUDO *backendConfigPSEUDOStruct
}

// `backendCommon` is called to return a pointer to the context's common `backendStruct`.
func (backend *pseudoContextStruct) backendCommon() (backendCommon *backendStruct) {
	backendCommon = backend.backend
	return
}

// `setupPSEUDOContext` establishes the PSEUDO client context. Once set up, each
// method defined in the `backendConfigIf` interface may be invoked.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupPSEUDOContext() (err error) {
	var (
		pseudoContext = backend.backendTypeSpecifics.(*backendConfigPSEUDOStruct)
	)

	switch {
	case pseudoContext.filesAtDepth3 > 0:
		pseudoContext.maxPathDepth = 3
	case pseudoContext.filesAtDepth2 > 0:
		pseudoContext.maxPathDepth = 2
	case pseudoContext.filesAtDepth1 > 0:
		pseudoContext.maxPathDepth = 1
	default:
		pseudoContext.maxPathDepth = 0
	}

	pseudoContext.filesAtDepth[0] = pseudoContext.filesAtDepth0
	pseudoContext.filesAtDepth[1] = pseudoContext.filesAtDepth1
	pseudoContext.filesAtDepth[2] = pseudoContext.filesAtDepth2
	pseudoContext.filesAtDepth[3] = pseudoContext.filesAtDepth3

	pseudoContext.subdirectoriesAtDepth[0] = pseudoContext.subdirectoriesAtDepth0
	pseudoContext.subdirectoriesAtDepth[1] = pseudoContext.subdirectoriesAtDepth1
	pseudoContext.subdirectoriesAtDepth[2] = pseudoContext.subdirectoriesAtDepth2
	pseudoContext.subdirectoriesAtDepth[3] = 0

	pseudoContext.objectsInDirectoryAtDepth[3] = pseudoContext.filesAtDepth3
	pseudoContext.objectsInDirectoryAtDepth[2] = pseudoContext.filesAtDepth2 + (pseudoContext.subdirectoriesAtDepth2 * pseudoContext.objectsInDirectoryAtDepth[3])
	pseudoContext.objectsInDirectoryAtDepth[1] = pseudoContext.filesAtDepth1 + (pseudoContext.subdirectoriesAtDepth1 * pseudoContext.objectsInDirectoryAtDepth[2])
	pseudoContext.objectsInDirectoryAtDepth[0] = pseudoContext.filesAtDepth0 + (pseudoContext.subdirectoriesAtDepth0 * pseudoContext.objectsInDirectoryAtDepth[1])

	backend.context = &pseudoContextStruct{
		backend:       backend,
		backendPSEUDO: pseudoContext,
	}

	backend.backendPath = "pseudo://"

	err = nil
	return
}

// `deleteFile` is called to remove a "file" at the specified path.
// If a `subdirectory` or nothing is found at that path, an error will be returned.
func (pseudoContext *pseudoContextStruct) deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
	var (
		timeNotToReturnBefore = time.Now().Add(pseudoContext.backendPSEUDO.minLatencyDeleteFile)
	)

	defer func() {
		time.Sleep(time.Until(timeNotToReturnBefore))
	}()

	err = errors.New("PSEUDO backend is read-only")
	return
}

// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
// indicates the `directory` has been completely enumerated. The `isTruncated` field will also
// align with this convention.
func (pseudoContext *pseudoContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	var (
		continuationTokenAsUint64 uint64
		depth                     int
		dirIndex                  uint64
		dirIndexStart             uint64
		fileContent               []byte
		fileIndex                 uint64
		fileIndexStart            uint64
		fileName                  string
		fullDirPath               string
		fullDirPathElements       []uint64
		fullFilePath              string
		isTruncated               bool
		nextContinuationToken     string
		numDirFileToReturn        uint64
		numDirToReturn            uint64
		numFileToReturn           uint64
		ok                        bool
		timeNotToReturnBefore     = time.Now().Add(pseudoContext.backendPSEUDO.minLatencyListDirectory)
	)

	defer func() {
		time.Sleep(time.Until(timeNotToReturnBefore))
	}()

	if listDirectoryInput.continuationToken == "" {
		continuationTokenAsUint64 = 0
	} else {
		continuationTokenAsUint64, err = strconv.ParseUint(listDirectoryInput.continuationToken, 16, 64)
		if err != nil {
			err = fmt.Errorf("strconv.ParseUint(listDirectoryInput.continuationToken, 16, 64) failed: %v", err)
			return
		}
	}

	fullDirPath = pseudoContext.canonicalDirPath(listDirectoryInput.dirPath)

	fullDirPathElements, ok = pseudoContext.findFullDirPathElements(fullDirPath)
	if !ok {
		err = errors.New("pseudoContext.findFullDirPathElements(fullDirPath) returned !ok")
		return
	}

	depth = len(fullDirPathElements)

	numDirFileToReturn = pseudoContext.backendPSEUDO.subdirectoriesAtDepth[depth] + pseudoContext.backendPSEUDO.filesAtDepth[depth]

	if continuationTokenAsUint64 >= numDirFileToReturn {
		listDirectoryOutput = &listDirectoryOutputStruct{
			subdirectory:          make([]string, 0),
			file:                  make([]listDirectoryOutputFileStruct, 0),
			nextContinuationToken: "",
			isTruncated:           false,
		}

		err = nil
		return
	}

	numDirFileToReturn -= continuationTokenAsUint64

	nextContinuationToken = ""
	isTruncated = false

	if (listDirectoryInput.maxItems != 0) && (numDirFileToReturn > listDirectoryInput.maxItems) {
		numDirFileToReturn = listDirectoryInput.maxItems

		nextContinuationToken = fmt.Sprintf("%016X", continuationTokenAsUint64+numDirFileToReturn)
		isTruncated = true
	}
	if (pseudoContext.backend.directoryPageSize != 0) && (numDirFileToReturn > pseudoContext.backend.directoryPageSize) {
		numDirFileToReturn = pseudoContext.backend.directoryPageSize

		nextContinuationToken = fmt.Sprintf("%016X", continuationTokenAsUint64+numDirFileToReturn)
		isTruncated = true
	}

	if continuationTokenAsUint64 < pseudoContext.backendPSEUDO.subdirectoriesAtDepth[depth] {
		dirIndexStart = continuationTokenAsUint64
		fileIndexStart = 0

		if (continuationTokenAsUint64 + numDirFileToReturn) <= pseudoContext.backendPSEUDO.subdirectoriesAtDepth[depth] {
			numDirToReturn = numDirFileToReturn
			numFileToReturn = 0
		} else {
			numDirToReturn = pseudoContext.backendPSEUDO.subdirectoriesAtDepth[depth] - continuationTokenAsUint64
			numFileToReturn = numDirFileToReturn - numDirToReturn
		}
	} else {
		dirIndexStart = 0
		fileIndexStart = continuationTokenAsUint64 - pseudoContext.backendPSEUDO.subdirectoriesAtDepth[depth]

		numDirToReturn = 0
		numFileToReturn = numDirFileToReturn
	}

	listDirectoryOutput = &listDirectoryOutputStruct{
		subdirectory:          make([]string, 0, numDirToReturn),
		file:                  make([]listDirectoryOutputFileStruct, 0, numFileToReturn),
		nextContinuationToken: nextContinuationToken,
		isTruncated:           isTruncated,
	}

	for dirIndex = dirIndexStart; dirIndex < (dirIndexStart + numDirToReturn); dirIndex++ {
		listDirectoryOutput.subdirectory = append(listDirectoryOutput.subdirectory, fmt.Sprintf(PSEUDOSubdirectoryNameFormat, dirIndex))
	}

	for fileIndex = fileIndexStart; fileIndex < (fileIndexStart + numFileToReturn); fileIndex++ {
		fileName = fmt.Sprintf(PSEUDOFileNameFormat, fileIndex)
		fullFilePath = fullDirPath + fileName
		fileContent = []byte(fullFilePath + "\n")

		listDirectoryOutput.file = append(listDirectoryOutput.file, listDirectoryOutputFileStruct{
			basename: fileName,
			eTag:     fmt.Sprintf(PSEUDOFileCRC32ETagFormat, crc32.ChecksumIEEE(fileContent)),
			mTime:    time.Now(),
			size:     uint64(len(fileContent)),
		})
	}

	err = nil
	return
}

// `checkPathElement` checks pathElement against the allowable range [0:pageElementNumberMax]
// returning the pathElementNumber that is lexigraphically identical or just less than that
// contained in pageElement. If the match is exact, comparison will be zero. If pathElement is
// greater than the exact match for the returned pathElementNumber, comparison will be positive
// one. In the case where path element is lexigraphically less than what would be the minimum
// (i.e. zero) pathElementNumber, pathElementNumber will be zero and comparison will be minus one.
func checkPathElement(pathElementFormat, pathElement string, pathElementNumberLimit uint64) (pathElementNumber uint64, comparison int) {
	var (
		err                          error
		pad                          string
		pathElementAsByteSlice       = []byte(pathElement)
		pathElementByte              byte
		pathElementFormatAsByteSlice = []byte(pathElementFormat)
		pathElementIndex             int
		pathElementMax               = fmt.Sprintf(pathElementFormat, pathElementNumberLimit-1)
		pathElementMin               = fmt.Sprintf(pathElementFormat, 0)
		underscoreFound              = false
	)

	if pathElement < pathElementMin {
		pathElementNumber = 0
		comparison = -1

		return
	}

	if pathElement > pathElementMax {
		pathElementNumber = pathElementNumberLimit - 1
		comparison = 1

		return
	}

	for pathElementIndex = range pathElementFormatAsByteSlice {
		if (len(pathElementAsByteSlice) < (pathElementIndex + 1)) || (pathElementAsByteSlice[pathElementIndex] < pathElementFormatAsByteSlice[pathElementIndex]) {
			pathElementNumber = 0
			comparison = -1

			return
		}

		if pathElementAsByteSlice[pathElementIndex] < pathElementFormatAsByteSlice[pathElementIndex] {
			pathElementNumber = pathElementNumberLimit - 1
			comparison = 1

			return
		}

		// At this point, the character at position pathElementIndex in both byte slices are equal... so check for the terminating condition

		if pathElementFormatAsByteSlice[pathElementIndex] == '_' {
			// Trim prefix from pathElement and exit loop

			underscoreFound = true
			pathElement = pathElement[pathElementIndex+1:]
			pathElementAsByteSlice = pathElementAsByteSlice[pathElementIndex+1:]

			break
		}
	}

	if !underscoreFound {
		dumpStack()
		globals.logger.Fatalf("pathElementFormat:\"%s\" missing \"_\"", pathElementFormat)
	}

	if len(pathElement) == 8 {
		// Optimistically, pathElement should now be a 8 uppercase Hex Digit number that might be an exact match

		_, err = fmt.Sscanf(pathElement+"pad", "%08X%s", &pathElementNumber, &pad)
		if (err == nil) && (pad == "pad") {
			// We found a valid 8 uppercase Hex Digit number, so we can compute our return values immediately

			if pathElementNumber < pathElementNumberLimit {
				comparison = 0

				return
			}

			pathElementNumber = pathElementNumberLimit - 1
			comparison = 1

			return
		}
	}

	// Challenging case, so let's pad pathElement with characters just less than the minimum Hex digit and brute force the parsing

	pathElement += "////////"
	pathElementAsByteSlice = []byte(pathElement)[:8]

	pathElementNumber = 0

	for pathElementIndex, pathElementByte = range pathElementAsByteSlice {
		switch {
		case pathElementByte < '0':
			if pathElementNumber >= pathElementNumberLimit {
				pathElementNumber = pathElementNumberLimit - 1
				comparison = 1
			} else {
				comparison = -1
			}
			return
		case pathElementByte <= '9':
			pathElementNumber += uint64(pathElementByte-'0') * (uint64(0x1) << uint64(4*(7-pathElementIndex)))
			if pathElementNumber >= pathElementNumberLimit {
				pathElementNumber = pathElementNumberLimit - 1
				comparison = 1
				return
			}
		case pathElementByte < 'A':
			pathElementNumber += uint64(9) * (uint64(0x1) << uint64(4*(7-pathElementIndex)))
			if pathElementNumber >= pathElementNumberLimit {
				pathElementNumber = pathElementNumberLimit - 1
			}
			comparison = 1
			return
		case pathElementByte <= 'F':
			pathElementNumber += uint64(pathElementByte-'A'+10) * (uint64(0x1) << uint64(4*(7-pathElementIndex)))
			if pathElementNumber >= pathElementNumberLimit {
				pathElementNumber = pathElementNumberLimit - 1
				comparison = 1
				return
			}
		default: // pathElementByte > 'F'
			pathElementNumber += uint64(15) * (uint64(0x1) << uint64(4*(7-pathElementIndex)))
			if pathElementNumber >= pathElementNumberLimit {
				pathElementNumber = pathElementNumberLimit - 1
			}
			comparison = 1
			return
		}
	}

	// Since we performed the "exact match" check prior to this troublesome brute force path,
	// we must have truncated pathElement, so if we reach here, pathElement be greater than
	// the computed pathElementNumber.

	comparison = 1
	return
}

// `listObjects` is called to fetch a `page` of the objects. An empty continuationToken or
// empty list of elements (`objects`) indicates the list of `objects` has been completely
// enumerated. The `isTruncated` field will also align with this convention.
func (pseudoContext *pseudoContextStruct) listObjects(listObjectsInput *listObjectsInputStruct) (listObjectsOutput *listObjectsOutputStruct, err error) {
	var (
		checkPathElementForFile        bool
		comparison                     int
		continuationTokenAsUint64      uint64
		continuationTokenAsUint64Limit uint64
		ok                             bool
		prefixDepth                    int
		prefixDirPath                  string
		prefixDirPathElements          []uint64
		startAfterSplit                []string
		startAfterSplitElementAsString string
		startAfterSplitElementAsUint64 uint64
		startAfterSplitElementDepth    int
		startAfterSplitElementIndex    int
		suffixDirPathElements          []uint64
		timeNotToReturnBefore          = time.Now().Add(pseudoContext.backendPSEUDO.minLatencyListObjects)
	)

	defer func() {
		time.Sleep(time.Until(timeNotToReturnBefore))
	}()

	if (listObjectsInput.startAfter != "") && (listObjectsInput.continuationToken != "") {
		err = errors.New("[PSEUDO] .startAfter and .continuationToken can't both be non-empty strings")
		return
	}

	if listObjectsInput.continuationToken == "" {
		continuationTokenAsUint64 = 0
	} else {
		continuationTokenAsUint64, err = strconv.ParseUint(listObjectsInput.continuationToken, 16, 64)
		if err != nil {
			err = fmt.Errorf("strconv.ParseUint(listObjectsInput.continuationToken, 16, 64) failed: %v", err)
			return
		}
	}

	prefixDirPath = pseudoContext.canonicalDirPath("")

	prefixDirPathElements, ok = pseudoContext.findFullDirPathElements(prefixDirPath)
	if !ok {
		err = errors.New("pseudoContext.findFullDirPathElements(prefixDirPath) returned !ok")
		return
	}

	prefixDepth = len(prefixDirPathElements)

	continuationTokenAsUint64Limit = pseudoContext.backendPSEUDO.objectsInDirectoryAtDepth[prefixDepth]

	suffixDirPathElements = make([]uint64, 0, int(pseudoContext.backendPSEUDO.maxPathDepth+1)-prefixDepth)

	if listObjectsInput.startAfter != "" {
		// Need to convert listObjectsInput.startafter to continuationTokenAsUint64
		// So first we search for the closest match at or before listObjectsInput.startAfter

		startAfterSplit = strings.SplitN(listObjectsInput.startAfter, "/", cap(suffixDirPathElements))

		for startAfterSplitElementIndex, startAfterSplitElementAsString = range startAfterSplit {
			startAfterSplitElementDepth = prefixDepth + startAfterSplitElementIndex

			if pseudoContext.backendPSEUDO.subdirectoriesAtDepth[startAfterSplitElementDepth] > 0 {
				startAfterSplitElementAsUint64, comparison = checkPathElement(PSEUDOSubdirectoryNameFormat, startAfterSplitElementAsString, pseudoContext.backendPSEUDO.subdirectoriesAtDepth[startAfterSplitElementDepth])
				switch {
				case comparison == 0:
					continuationTokenAsUint64 += startAfterSplitElementAsUint64 * pseudoContext.backendPSEUDO.objectsInDirectoryAtDepth[startAfterSplitElementDepth+1]
					continue
				case comparison < 0:
					continuationTokenAsUint64 += startAfterSplitElementAsUint64 * pseudoContext.backendPSEUDO.objectsInDirectoryAtDepth[startAfterSplitElementDepth+1]
					checkPathElementForFile = false
				default: // comparison > 0
					continuationTokenAsUint64 += (startAfterSplitElementAsUint64 + 1) * pseudoContext.backendPSEUDO.objectsInDirectoryAtDepth[startAfterSplitElementDepth+1]
					checkPathElementForFile = ((startAfterSplitElementAsUint64 + 1) == pseudoContext.backendPSEUDO.subdirectoriesAtDepth[startAfterSplitElementDepth])
				}
			} else {
				checkPathElementForFile = true
			}

			if checkPathElementForFile && (pseudoContext.backendPSEUDO.filesAtDepth[startAfterSplitElementDepth] > 0) {
				startAfterSplitElementAsUint64, comparison = checkPathElement(PSEUDOFileNameFormat, startAfterSplitElementAsString, pseudoContext.backendPSEUDO.filesAtDepth[startAfterSplitElementDepth])
				if comparison == -1 {
					continuationTokenAsUint64 += startAfterSplitElementAsUint64
				} else {
					continuationTokenAsUint64 += startAfterSplitElementAsUint64 + 1
				}
			}

			break
		}
	}

	globals.logger.Printf("[UNDO] continuationTokenAsUint64:      %016X", continuationTokenAsUint64)
	globals.logger.Printf("[UNDO] continuationTokenAsUint64Limit: %016X", continuationTokenAsUint64Limit)

	listObjectsOutput = &listObjectsOutputStruct{object: make([]listObjectsOutputObjectStruct, 0), nextContinuationToken: "", isTruncated: false} // [UNDO]
	err = nil
	return
}

// `readFile` is called to read a range of a `file` at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (pseudoContext *pseudoContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	var (
		cacheLine             []byte
		fileContent           []byte
		fullFilePath          string
		limit                 uint64
		offset                uint64
		ok                    bool
		timeNotToReturnBefore = time.Now().Add(pseudoContext.backendPSEUDO.minLatencyReadFile)
	)

	defer func() {
		time.Sleep(time.Until(timeNotToReturnBefore))
	}()

	fullFilePath = pseudoContext.canonicalFilePath(readFileInput.filePath)

	_, ok = pseudoContext.findFullFilePathElements(fullFilePath)
	if !ok {
		err = errors.New("pseudoContext.findFullFilePathElements(fullFilePath) returned !ok")
		return
	}

	fileContent = []byte(fullFilePath + "\n")
	cacheLine = fileContent

	offset = readFileInput.offsetCacheLine * globals.config.cacheLineSize
	limit = offset + globals.config.cacheLineSize

	if limit < uint64(len(fileContent)) {
		cacheLine = cacheLine[:limit]
	}
	if offset < uint64(len(cacheLine)) {
		cacheLine = cacheLine[offset:]
	} else {
		cacheLine = cacheLine[:0]
	}

	readFileOutput = &readFileOutputStruct{
		eTag: fmt.Sprintf(PSEUDOFileCRC32ETagFormat, crc32.ChecksumIEEE(fileContent)),
		buf:  cacheLine,
	}

	err = nil
	return
}

// `statDirectory` is called to verify that the specified path refers to a `directory`.
// An error is returned if either the specified path is not a `directory` or non-existent.
func (pseudoContext *pseudoContextStruct) statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
	var (
		fullDirPath           string
		ok                    bool
		timeNotToReturnBefore = time.Now().Add(pseudoContext.backendPSEUDO.minLatencyStatDirectory)
	)

	defer func() {
		time.Sleep(time.Until(timeNotToReturnBefore))
	}()

	fullDirPath = pseudoContext.canonicalDirPath(statDirectoryInput.dirPath)

	_, ok = pseudoContext.findFullDirPathElements(fullDirPath)
	if !ok {
		err = errors.New("pseudoContext.findFullDirPathElements(fullDirPath) returned !ok")
		return
	}

	statDirectoryOutput = &statDirectoryOutputStruct{}

	err = nil
	return
}

// `statFile` is called to fetch the `file` metadata at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (pseudoContext *pseudoContextStruct) statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
	var (
		fileContent           []byte
		fullFilePath          string
		ok                    bool
		timeNotToReturnBefore = time.Now().Add(pseudoContext.backendPSEUDO.minLatencyStatFile)
	)

	defer func() {
		time.Sleep(time.Until(timeNotToReturnBefore))
	}()

	fullFilePath = pseudoContext.canonicalFilePath(statFileInput.filePath)

	_, ok = pseudoContext.findFullFilePathElements(fullFilePath)
	if !ok {
		err = errors.New("pseudoContext.findFullFilePathElements(fullFilePath) returned !ok")
		return
	}

	fileContent = []byte(fullFilePath + "\n")

	statFileOutput = &statFileOutputStruct{
		eTag:  fmt.Sprintf(PSEUDOFileCRC32ETagFormat, crc32.ChecksumIEEE(fileContent)),
		mTime: time.Now(),
		size:  uint64(len(fileContent)),
	}

	err = nil
	return
}

// `canonicalDirPath` converts the supplied dirPath to `/[dirName/]*` (including pseudoContext.backend.prefix).
func (pseudoContext *pseudoContextStruct) canonicalDirPath(dirPath string) (canonicalDirPath string) {
	if pseudoContext.backend.prefix == "" {
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
			canonicalDirPath = "/" + pseudoContext.backend.prefix
		} else {
			if strings.HasSuffix(dirPath, "/") {
				canonicalDirPath = "/" + pseudoContext.backend.prefix + dirPath
			} else {
				canonicalDirPath = "/" + pseudoContext.backend.prefix + dirPath + "/"
			}
		}
	}
	return
}

// `findFullDirPathElements` splits the supplied fullDirPath validating each path element for
// existence returning a slice of the path elements converted to uint64s.
func (pseudoContext *pseudoContextStruct) findFullDirPathElements(fullDirPath string) (elements []uint64, ok bool) {
	var (
		err                             error
		fullDirPathSplit                []string
		fullDirPathSplitElementAsString string
		fullDirPathSplitElementAsUint64 uint64
		fullDirPathSplitElementDepth    int
	)

	elements = make([]uint64, 0, 3)

	fullDirPathSplit = strings.Split(fullDirPath, "/")
	if (len(fullDirPathSplit) < 2) || (len(fullDirPathSplit) > 5) || (fullDirPathSplit[0] != "") || (fullDirPathSplit[len(fullDirPathSplit)-1] != "") {
		ok = false
		return
	}

	for fullDirPathSplitElementDepth, fullDirPathSplitElementAsString = range fullDirPathSplit[1 : len(fullDirPathSplit)-1] {
		_, err = fmt.Sscanf(fullDirPathSplitElementAsString, PSEUDOSubdirectoryNameFormat, &fullDirPathSplitElementAsUint64)
		if err != nil {
			ok = false
			return
		}

		if fullDirPathSplitElementAsUint64 >= pseudoContext.backendPSEUDO.subdirectoriesAtDepth[fullDirPathSplitElementDepth] {
			ok = false
			return
		}

		elements = append(elements, fullDirPathSplitElementAsUint64)
	}

	ok = true
	return
}

// `canonicalFilePath` converts the supplied filePath to `/[dirName/]*fileName` (including pseudoContext.backend.prefix).
func (pseudoContext *pseudoContextStruct) canonicalFilePath(filePath string) (canonicalFilePath string) {
	if pseudoContext.backend.prefix == "" {
		canonicalFilePath = "/" + filePath
	} else {
		canonicalFilePath = "/" + pseudoContext.backend.prefix + filePath
	}
	return
}

// `findFullFilePathElements` splits the supplied fullFilePath validating each path element for
// existence returning a slice of the path elements converted to uint64s.
func (pseudoContext *pseudoContextStruct) findFullFilePathElements(fullFilePath string) (elements []uint64, ok bool) {
	var (
		err                              error
		fullFilePathSplit                []string
		fullFilePathSplitElementAsString string
		fullFilePathSplitElementAsUint64 uint64
		fullFilePathSplitElementDepth    int
	)

	elements = make([]uint64, 0, 4)

	fullFilePathSplit = strings.Split(fullFilePath, "/")
	if (len(fullFilePathSplit) < 2) || (len(fullFilePathSplit) > 5) || (fullFilePathSplit[0] != "") || (fullFilePathSplit[len(fullFilePathSplit)-1] == "") {
		ok = false
		return
	}

	for fullFilePathSplitElementDepth, fullFilePathSplitElementAsString = range fullFilePathSplit[1 : len(fullFilePathSplit)-1] {
		_, err = fmt.Sscanf(fullFilePathSplitElementAsString, PSEUDOSubdirectoryNameFormat, &fullFilePathSplitElementAsUint64)
		if err != nil {
			ok = false
			return
		}

		if fullFilePathSplitElementAsUint64 >= pseudoContext.backendPSEUDO.subdirectoriesAtDepth[fullFilePathSplitElementDepth] {
			ok = false
			return
		}

		elements = append(elements, fullFilePathSplitElementAsUint64)
	}

	fullFilePathSplitElementDepth = len(fullFilePathSplit) - 2
	fullFilePathSplitElementAsString = fullFilePathSplit[fullFilePathSplitElementDepth+1]

	_, err = fmt.Sscanf(fullFilePathSplitElementAsString, PSEUDOFileNameFormat, &fullFilePathSplitElementAsUint64)
	if err != nil {
		ok = false
		return
	}

	if fullFilePathSplitElementAsUint64 >= pseudoContext.backendPSEUDO.filesAtDepth[fullFilePathSplitElementDepth] {
		ok = false
		return
	}

	elements = append(elements, fullFilePathSplitElementAsUint64)

	ok = true
	return
}
