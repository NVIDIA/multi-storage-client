package main

import (
	"context"
	"fmt"
	"time"

	"github.com/NVIDIA/multi-storage-client/multi-storage-file-system/telemetry"
)

// `setupContext` is called to establish the client that will be used
// to access a backend. Once the context is established, each of the
// calls to func's defined in backendContextIf interface are callable.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupContext() (err error) {
	backend.backendPath = "<unknown>"

	switch backend.backendType {
	case "AIStore":
		err = backend.setupAIStoreContext()
	case "RAM":
		err = backend.setupRAMContext()
	case "S3":
		err = backend.setupS3Context()
	default:
		err = fmt.Errorf("for backend.dir_name \"%s\", unexpected backend_type \"%s\" (must be \"AIStore\", \"RAM\", or \"S3\")", backend.dirName, backend.backendType)
	}

	return
}

// `backendContextIf` defines the methods available for each backend
// context. In order to set a backend (a struct of some sort), a
// backend type-specific implementation for each of these methods
// must be provided.
type backendContextIf interface {
	// `backendCommon` is called to return a pointer to the context's common `backendStruct`.
	backendCommon() (backendCommon *backendStruct)

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

// `recordRequest` records the request counter at the START of an operation.
// Matches Python's behavior: request.sum is recorded BEFORE the operation executes (line 209).
// This should be called immediately at the start of each backend operation (not in defer).
// Note: Does not accept additional attributes to avoid high cardinality issues.
//
// Call Chain:
//
//	S3 Backend Operations (backend_s3.go)
//	  ↓
//	recordRequest() (backend.go line 27)
//	  ↓
//	metrics.RecordBackendRequest() (backend.go line 43)
//	  ↓
//	MSCPMetricsDiperiodic.RecordBackendRequest() (metrics_diperiodic.go line 135)
//	  ↓
//	requestSumCounter.Add() (metrics_diperiodic.go line 146)
func recordRequest(backendName, operation string) {
	if globals.metrics == nil {
		return
	}

	metrics, ok := globals.metrics.(telemetry.MSCPMetricsDiperiodic)
	if !ok {
		return
	}

	// Get version (matches Python's VERSION attribute)
	version := GitTag
	if version == "" {
		version = "dev" // Fallback for development builds
	}

	metrics.RecordBackendRequest(context.Background(), operation, version, backendName)
}

// `recordBackendMetrics` is a helper to record metrics for backend operations.
// It should be called in a defer statement at the beginning of each backend operation.
// This records latency, response, data_size, etc. AFTER the operation completes.
// Matches Python's behavior: these metrics are recorded in the finally block (lines 235-272).
// This function is in backend.go (not backend_s3.go) so it can be used by all backend types.
// Note: Does not accept additional attributes (like file paths) to avoid high cardinality issues.
func recordBackendMetrics(backendName, operation string, startTime time.Time, err error, bytesTransferred int64) {
	if globals.metrics == nil {
		return
	}

	metrics, ok := globals.metrics.(telemetry.MSCPMetricsDiperiodic)
	if !ok {
		return
	}

	duration := time.Since(startTime)
	success := (err == nil)

	// Get version (matches Python's VERSION attribute)
	version := GitTag
	if version == "" {
		version = "dev" // Fallback for development builds
	}

	metrics.RecordBackendOperation(context.Background(), operation, version, backendName, duration, success, bytesTransferred)
}

// `deleteFileWrapper` is a wrapper function around the supplied backendContext's `deleteFile` function enabling centralized metrics and tracing capture.
func deleteFileWrapper(backendContext backendContextIf, deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
	var (
		backendCommon = backendContext.backendCommon()
		latency       float64
		startTime     time.Time
	)

	recordRequest(backendCommon.dirName, "delete")

	startTime = time.Now()

	deleteFileOutput, err = backendContext.deleteFile(deleteFileInput)

	latency = time.Since(startTime).Seconds()

	go func(backend *backendStruct, latency float64) {
		globals.Lock()
		if err == nil {
			globals.backendMetrics.DeleteFileSuccesses.Inc()
			globals.backendMetrics.DeleteFileSuccessLatencies.Observe(latency)

			backend.backendMetrics.DeleteFileSuccesses.Inc()
			backend.backendMetrics.DeleteFileSuccessLatencies.Observe(latency)
		} else {
			globals.backendMetrics.DeleteFileFailures.Inc()
			globals.backendMetrics.DeleteFileFailureLatencies.Observe(latency)

			backend.backendMetrics.DeleteFileFailures.Inc()
			backend.backendMetrics.DeleteFileFailureLatencies.Observe(latency)
		}
		globals.Unlock()
	}(backendCommon, latency)

	recordBackendMetrics(backendCommon.dirName, "delete", startTime, err, 0)

	switch backendCommon.traceLevel {
	case 0:
		// Trace nothing
	case 1:
		if err != nil {
			globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", backendCommon.dirName, deleteFileInput, err)
		}
	default:
		if err == nil {
			globals.logger.Printf("[INFO] %s.deleteFile(%#v) succeeded", backendCommon.dirName, deleteFileInput)
		} else {
			globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", backendCommon.dirName, deleteFileInput, err)
		}
	}

	return
}

// `listDirectoryWrapper` is a wrapper function around the supplied backendContext's `listDirectory` function enabling centralized metrics and tracing capture.
func listDirectoryWrapper(backendContext backendContextIf, listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	var (
		backendCommon = backendContext.backendCommon()
		latency       float64
		startTime     time.Time
	)

	recordRequest(backendCommon.dirName, "list")

	startTime = time.Now()

	listDirectoryOutput, err = backendContext.listDirectory(listDirectoryInput)

	latency = time.Since(startTime).Seconds()

	go func(backend *backendStruct, latency float64) {
		globals.Lock()
		if err == nil {
			globals.backendMetrics.ListDirectorySuccesses.Inc()
			globals.backendMetrics.ListDirectorySuccessLatencies.Observe(latency)

			backend.backendMetrics.ListDirectorySuccesses.Inc()
			backend.backendMetrics.ListDirectorySuccessLatencies.Observe(latency)
		} else {
			globals.backendMetrics.ListDirectoryFailures.Inc()
			globals.backendMetrics.ListDirectoryFailureLatencies.Observe(latency)

			backend.backendMetrics.ListDirectoryFailures.Inc()
			backend.backendMetrics.ListDirectoryFailureLatencies.Observe(latency)
		}
		globals.Unlock()
	}(backendCommon, latency)

	recordBackendMetrics(backendCommon.dirName, "list", startTime, err, 0)

	switch backendCommon.traceLevel {
	case 0:
		// Trace nothing
	case 1:
		if err != nil {
			globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", backendCommon.dirName, listDirectoryInput, err)
		}
	case 2:
		if err == nil {
			globals.logger.Printf("[INFO] %s.listDirectory(%#v) succeeded", backendCommon.dirName, listDirectoryInput)
		} else {
			globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", backendCommon.dirName, listDirectoryInput, err)
		}
	default:
		if err == nil {
			globals.logger.Printf("[INFO] %s.listDirectory(%#v) returning listDirectoryOutput: {len(\"subdirectory\"):%v,len(\"file\"):%v}", backendCommon.dirName, listDirectoryInput, len(listDirectoryOutput.subdirectory), len(listDirectoryOutput.file))
		} else {
			globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", backendCommon.dirName, listDirectoryInput, err)
		}
	}

	return
}

// `readFileWrapper` is a wrapper function around the supplied backendContext's `readFile` function enabling centralized metrics and tracing capture.
func readFileWrapper(backendContext backendContextIf, readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	var (
		backendCommon = backendContext.backendCommon()
		bytesRead     = int64(0)
		latency       float64
		startTime     time.Time
	)

	recordRequest(backendCommon.dirName, "read")

	startTime = time.Now()

	readFileOutput, err = backendContext.readFile(readFileInput)

	latency = time.Since(startTime).Seconds()

	go func(backend *backendStruct, latency float64) {
		globals.Lock()
		if err == nil {
			globals.backendMetrics.ReadFileSuccesses.Inc()
			globals.backendMetrics.ReadFileSuccessLatencies.Observe(latency)

			backend.backendMetrics.ReadFileSuccesses.Inc()
			backend.backendMetrics.ReadFileSuccessLatencies.Observe(latency)
		} else {
			globals.backendMetrics.ReadFileFailures.Inc()
			globals.backendMetrics.ReadFileFailureLatencies.Observe(latency)

			backend.backendMetrics.ReadFileFailures.Inc()
			backend.backendMetrics.ReadFileFailureLatencies.Observe(latency)
		}
		globals.Unlock()
	}(backendCommon, latency)

	if (err == nil) && (readFileOutput != nil) {
		bytesRead = int64(len(readFileOutput.buf))
	}
	recordBackendMetrics(backendCommon.dirName, "read", startTime, err, bytesRead)

	switch backendCommon.traceLevel {
	case 0:
		// Trace nothing
	case 1:
		if err != nil {
			globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", backendCommon.dirName, readFileInput, err)
		}
	case 2:
		if err == nil {
			globals.logger.Printf("[INFO] %s.readFile(%#v) succeeded", backendCommon.dirName, readFileInput)
		} else {
			globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", backendCommon.dirName, readFileInput, err)
		}
	default:
		if err == nil {
			globals.logger.Printf("[INFO] %s.readFile(%#v) returning readFileOutput: {\"eTag\":\"%s\",len(\"buf\":%v)}", backendCommon.dirName, readFileInput, readFileOutput.eTag, len(readFileOutput.buf))
		} else {
			globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", backendCommon.dirName, readFileInput, err)
		}
	}

	return
}

// `statDirectoryWrapper` is a wrapper function around the supplied backendContext's `statDirectory` function enabling centralized metrics and tracing capture.
func statDirectoryWrapper(backendContext backendContextIf, statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
	var (
		backendCommon = backendContext.backendCommon()
		latency       float64
		startTime     time.Time
	)

	recordRequest(backendCommon.dirName, "info")

	startTime = time.Now()

	statDirectoryOutput, err = backendContext.statDirectory(statDirectoryInput)

	go func(backend *backendStruct, latency float64) {
		latency = time.Since(startTime).Seconds()
		globals.Lock()
		if err == nil {
			globals.backendMetrics.StatDirectorySuccesses.Inc()
			globals.backendMetrics.StatDirectorySuccessLatencies.Observe(latency)

			backend.backendMetrics.StatDirectorySuccesses.Inc()
			backend.backendMetrics.StatDirectorySuccessLatencies.Observe(latency)
		} else {
			globals.backendMetrics.StatDirectoryFailures.Inc()
			globals.backendMetrics.StatDirectoryFailureLatencies.Observe(latency)

			backend.backendMetrics.StatDirectoryFailures.Inc()
			backend.backendMetrics.StatDirectoryFailureLatencies.Observe(latency)
		}
		globals.Unlock()
	}(backendCommon, latency)

	recordBackendMetrics(backendCommon.dirName, "info", startTime, err, 0)

	switch backendCommon.traceLevel {
	case 0:
		// Trace nothing
	case 1:
		if err != nil {
			globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", backendCommon.dirName, statDirectoryInput, err)
		}
	case 2:
		if err == nil {
			globals.logger.Printf("[INFO] %s.statDirectory(%#v) succeeded", backendCommon.dirName, statDirectoryInput)
		} else {
			globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", backendCommon.dirName, statDirectoryInput, err)
		}
	default:
		if err == nil {
			globals.logger.Printf("[INFO] %s.statDirectory(%#v) returning statDirectoryOutput: %#v", backendCommon.dirName, statDirectoryInput, statDirectoryOutput)
		} else {
			globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", backendCommon.dirName, statDirectoryInput, err)
		}
	}

	return
}

// `statFileWrapper` is a wrapper function around the supplied backendContext's `statFile` function enabling centralized metrics and tracing capture.
func statFileWrapper(backendContext backendContextIf, statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
	var (
		backendCommon = backendContext.backendCommon()
		bytesReported = int64(0)
		latency       float64
		startTime     time.Time
	)

	recordRequest(backendCommon.dirName, "info")

	startTime = time.Now()

	statFileOutput, err = backendContext.statFile(statFileInput)

	latency = time.Since(startTime).Seconds()

	go func(backend *backendStruct, latency float64) {
		globals.Lock()
		if err == nil {
			globals.backendMetrics.StatFileSuccesses.Inc()
			globals.backendMetrics.StatFileSuccessLatencies.Observe(latency)

			backend.backendMetrics.StatFileSuccesses.Inc()
			backend.backendMetrics.StatFileSuccessLatencies.Observe(latency)
		} else {
			globals.backendMetrics.StatFileFailures.Inc()
			globals.backendMetrics.StatFileFailureLatencies.Observe(latency)

			backend.backendMetrics.StatFileFailures.Inc()
			backend.backendMetrics.StatFileFailureLatencies.Observe(latency)
		}
		globals.Unlock()
	}(backendCommon, latency)

	if (err == nil) && (statFileOutput != nil) {
		bytesReported = int64(statFileOutput.size)
	}
	recordBackendMetrics(backendCommon.dirName, "info", startTime, err, bytesReported)

	switch backendCommon.traceLevel {
	case 0:
		// Trace nothing
	case 1:
		if err != nil {
			globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", backendCommon.dirName, statFileInput, err)
		}
	case 2:
		if err == nil {
			globals.logger.Printf("[INFO] %s.statFile(%#v) succeeded", backendCommon.dirName, statFileInput)
		} else {
			globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", backendCommon.dirName, statFileInput, err)
		}
	default:
		if err == nil {
			globals.logger.Printf("[INFO] %s.statFile(%#v) returning statFileOutput: %#v", backendCommon.dirName, statFileInput, statFileOutput)
		} else {
			globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", backendCommon.dirName, statFileInput, err)
		}
	}

	return
}

// [TODO] writeFileWrapper equivalents
