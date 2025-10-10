package main

import (
	"fmt"
	"time"
)

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

type backendContextIf interface {
	deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error)
	listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error)
	readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error)
	statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error)
	statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error)
	// [TODO] writeFile equivalents: simple PUT as well as the exciting challenges of MPU
}

type deleteFileInputStruct struct {
	filePath string // Relative to backend.prefix
	ifMatch  string // If == "", then always matches existing object; if != "", must match existing object's eTag
}

type deleteFileOutputStruct struct{}

type listDirectoryInputStruct struct {
	continuationToken string // If != "", from prior listDirectoryOut.nextContinuationToken
	maxItems          uint64 // If == 0, limited instead by the object server
	dirPath           string // Relative to backend.prefix; if != "", should end with a trailing "/"
}

type listDirectoryOutputFileStruct struct {
	basename string // Relative to listDirectoryInputStruct.dirPath which is itself relative to backend.prefix
	eTag     string
	mTime    time.Time
	size     uint64
}

type listDirectoryOutputStruct struct {
	subdirectory          []string // Relative to listDirectoryInputStruct.DirPath which is itself relative to backend.prefix; No trailing "/"
	file                  []listDirectoryOutputFileStruct
	nextContinuationToken string
	isTruncated           bool
}

type readFileInputStruct struct {
	filePath        string // Relative to backend.prefix
	offsetCacheLine uint64 // Read byte range [offsetCacheLine * backend.config.cacheLineSize:min((offsetCacheLine+1) * backend.config.cacheLineSize, <object size>))
	ifMatch         string // If == "", then always matches existing object; if != "", must match existing object's eTag
}

type readFileOutputStruct struct {
	eTag string
	buf  []byte
}

type statDirectoryInputStruct struct {
	dirPath string // Relative to backend.prefix; if != "", should end with a trailing "/"
}

type statDirectoryOutputStruct struct{}

type statFileInputStruct struct {
	filePath string // Relative to backend.prefix
	ifMatch  string // If == "", then always matches existing object; if != "", must match existing object's eTag
}

type statFileOutputStruct struct {
	eTag  string
	mTime time.Time
	size  uint64
}
