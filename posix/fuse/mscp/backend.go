package main

import (
	"fmt"
	"time"
)

// `setupContext` is called to establish the client that will be used
// to access a backend. Once the context is established, each of the
// calls to func's defined in backendContextIf interface are callable.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupContext() (err error) {
	backend.backendPath = "<unknown>"

	switch backend.backendType {
	case "S3":
		err = backend.setupS3Context()
	default:
		err = fmt.Errorf("for backend.dir_name \"%s\", unexpected backend_type \"%s\" (must be \"S3\")", backend.dirName, backend.backendType)
	}

	return
}

// `backendContextIf` defines the methods available for each backend
// context. In order to set a backend (a struct of some sort), a
// backend type-specific implementation for each of these methods
// must be provided.
type backendContextIf interface {
	// `deleteFile` is called to remove a `file` at the specified path.
	// If a `subdirectory` or nothing is found at that path, an error will be returned.
	deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error)

	// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
	// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
	// indicates the `directory` has been completely enumerated. An error will result if either the
	// specified path is not a `directory` or non-existent.
	listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error)

	// `readFile` is called to read a range of a `file` at the specified path.
	// As error will result if either the specified path is not a `file` or non-existent.
	readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error)

	// `statDirectory` is called to verify that the specified path refers to a `directory`.
	// An error will result if either the specified path is not a `directory` or non-existent.
	statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error)

	// `statFile` is called to fetch the `file` metadata at the specified path.
	// As error will result if either the specified path is not a `file` or non-existent.
	statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error)

	// [TODO] writeFile equivalents: simple PUT as well as the exciting challenges of MPU
}

// `deleteFileInputStruct` lays out the fields provided as input
// to deleteFile().
type deleteFileInputStruct struct {
	filePath string // Relative to backend.prefix
	ifMatch  string // If == "", then always matches existing object; if != "", must match existing object's eTag
}

// `deleteFileOutputStruct` lays out the fields produced as output
// by deleteFile(). Currently, there are none.
type deleteFileOutputStruct struct{}

// `listDirectoryInputStruct` lays out the fields provided as input
// to listDirectory().
type listDirectoryInputStruct struct {
	continuationToken string // If != "", from prior listDirectoryOut.nextContinuationToken
	maxItems          uint64 // If == 0, limited instead by the object server
	dirPath           string // Relative to backend.prefix; if != "", should end with a trailing "/"
}

// `listDirectoryOutputFileStruct` lays out the fields produced as output
// by listDirectory() for each "file".
type listDirectoryOutputFileStruct struct {
	basename string // Relative to listDirectoryInputStruct.dirPath which is itself relative to backend.prefix
	eTag     string
	mTime    time.Time
	size     uint64
}

// `listDirectoryOutputStruct` lays out the fields produced as output
// by listDirectory().
type listDirectoryOutputStruct struct {
	subdirectory          []string // Relative to listDirectoryInputStruct.DirPath which is itself relative to backend.prefix; No trailing "/"
	file                  []listDirectoryOutputFileStruct
	nextContinuationToken string
	isTruncated           bool
}

// `readFileInputStruct` lays out the fields provided as input
// to readFile().
type readFileInputStruct struct {
	filePath        string // Relative to backend.prefix
	offsetCacheLine uint64 // Read byte range [offsetCacheLine * backend.config.cacheLineSize:min((offsetCacheLine+1) * backend.config.cacheLineSize, <object size>))
	ifMatch         string // If == "", then always matches existing object; if != "", must match existing object's eTag
}

// `readFileOutputStruct` lays out the fields produced as output
// by readFile().
type readFileOutputStruct struct {
	eTag string
	buf  []byte
}

// `statDirectoryInputStruct` lays out the fields provided as input
// to statDirectory().
type statDirectoryInputStruct struct {
	dirPath string // Relative to backend.prefix; if != "", should end with a trailing "/"
}

// `deleteFileOutputStruct` lays out the fields produced as output
// by deleteFile(). Currently, there are none. A successful return
// indicates the "subdirectory" exists. A failure may mean there
// is actually a "file" at that path or nothing.
type statDirectoryOutputStruct struct{}

// `statFileInputStruct` lays out the fields provided as input
// to statFile().
type statFileInputStruct struct {
	filePath string // Relative to backend.prefix
	ifMatch  string // If == "", then always matches existing object; if != "", must match existing object's eTag
}

// `statFileOutputStruct` lays out the fields produced as output
// by statFile(). A failure indicates either a "subdirectory"
// exists at that path or nothing does.
type statFileOutputStruct struct {
	eTag  string
	mTime time.Time
	size  uint64
}
