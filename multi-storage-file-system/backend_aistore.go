package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/authn"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// `aistoreContextStruct` holds the AIStore-specific backend details.
// Note: Unlike S3 SDK which bundles everything into s3.Client, AIStore SDK
// separates baseParams (connection) from bck (bucket metadata). We store
// both since bucket info is reused across all operations.
type aistoreContextStruct struct {
	backend    *backendStruct
	baseParams api.BaseParams // Connection parameters
	bck        cmn.Bck        // Bucket metadata/ structure
}

// `setupAIStoreContext` establishes the AIStore client context. Once set up, each
// method defined in the `backendConfigIf` interface may be invoked.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupAIStoreContext() (err error) {
	var (
		authnToken     string
		backendAIStore = backend.backendTypeSpecifics.(*backendConfigAIStoreStruct)
		httpClient     *http.Client
	)

	// Create HTTP client with custom timeout and TLS config (matches S3 backend pattern)
	transport := &http.Transport{}
	httpClient = &http.Client{
		Timeout:   backendAIStore.timeout,
		Transport: transport,
	}

	// Skip TLS certificate verification if specified
	if backendAIStore.skipTLSCertificateVerify {
		if transport.TLSClientConfig == nil {
			transport.TLSClientConfig = &tls.Config{}
		}
		transport.TLSClientConfig.InsecureSkipVerify = true
		transport.TLSClientConfig.MinVersion = tls.VersionTLS13
	}

	// Fetch  AuthN Token from either backendAIStore.authnToken or backendAIStore.authnTokenFile
	if backendAIStore.authnToken == "" {
		if backendAIStore.authnTokenFile == "" {
			authnToken = ""
		} else {
			authnToken, err = authn.LoadToken(backendAIStore.authnTokenFile)
			if err != nil {
				// Unreadable/loadable... just default to empty authnToken
				authnToken = ""
				err = nil
			}
		}
	} else {
		authnToken = backendAIStore.authnToken
	}

	// Create base parameters for AIStore API
	baseParams := api.BaseParams{
		Client: httpClient,
		URL:    backendAIStore.endpoint,
		Token:  authnToken,
		UA:     "multi-storage-file-system", // User-Agent string for identification
	}

	// Create bucket reference
	bck := cmn.Bck{
		Name:     backend.bucketContainerName, // Bucket name from configuration
		Provider: backendAIStore.provider,     // Provider type (ais, aws, gcp, azure, ht)
	}

	// Store context
	backend.context = &aistoreContextStruct{
		backend:    backend,
		baseParams: baseParams,
		bck:        bck,
	}

	return
}

// Note on Retry Logic:
// Unlike S3 backend which implements aws.Retryer interface (IsErrorRetryable, MaxAttempts,
// RetryDelay, GetRetryToken, GetInitialToken, GetAttemptToken), AIStore backend does NOT
// need these methods. The AIStore SDK handles retry logic internally via cmn.RetryArgs with
// hardcoded sensible defaults (5 retries, 100ms base delay, exponential backoff up to 4s).
// This is an architectural difference between AWS SDK (user-controlled retries) and AIStore
// SDK (SDK-controlled retries). Both approaches work correctly, just different design patterns.
// See: https://github.com/NVIDIA/aistore/tree/main/aistore/cmn/retry.go and
// https://github.com/NVIDIA/aistore/tree/main/aistore/api/client.go:215-222

// `deleteFile` is called to remove a "file" at the specified path.
// If a `subdirectory` or nothing is found at that path, an error will be returned.
func (aisContext *aistoreContextStruct) deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
	// Observability: Record request at START
	recordRequest(aisContext.backend.dirName, "delete")

	// Observability: Record latency/response at END
	startTime := time.Now()
	defer func() {
		recordBackendMetrics(aisContext.backend.dirName, "delete", startTime, err, 0)
	}()

	var (
		backend      = aisContext.backend
		fullFilePath = backend.prefix + deleteFileInput.filePath
	)

	defer func() {
		switch aisContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", aisContext.backend.dirName, deleteFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.deleteFile(%#v) succeeded", aisContext.backend.dirName, deleteFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", aisContext.backend.dirName, deleteFileInput, err)
			}
		}
	}()

	// If ifMatch is specified, verify ETag first
	if deleteFileInput.ifMatch != "" {
		var props *cmn.ObjectProps
		props, err = api.HeadObject(aisContext.baseParams, aisContext.bck, fullFilePath, api.HeadArgs{
			Silent: true,
		})
		if err != nil {
			return
		}
		if props.Cksum != nil && props.Cksum.Value() != deleteFileInput.ifMatch {
			err = errors.New("eTag mismatch")
			return
		}
	}

	// Delete the object
	err = api.DeleteObject(aisContext.baseParams, aisContext.bck, fullFilePath)

	return
}

// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
// indicates the `directory` has been completely enumerated. An error is returned if either the
// specified path is not a `directory` or non-existent.
func (aisContext *aistoreContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	// Observability: Record request at START
	recordRequest(aisContext.backend.dirName, "list")

	// Observability: Record latency/response at END
	startTime := time.Now()
	defer func() {
		recordBackendMetrics(aisContext.backend.dirName, "list", startTime, err, 0)
	}()

	var (
		backend     = aisContext.backend
		fullDirPath = backend.prefix + listDirectoryInput.dirPath
		lsmsg       = &apc.LsoMsg{
			Prefix: fullDirPath,
			Props:  strings.Join(apc.GetPropsAll, ","),
		}
	)

	defer func() {
		switch aisContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", aisContext.backend.dirName, listDirectoryInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.listDirectory(%#v) succeeded", aisContext.backend.dirName, listDirectoryInput)
			} else {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", aisContext.backend.dirName, listDirectoryInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.listDirectory(%#v) returning listDirectoryOutput: %#v", aisContext.backend.dirName, listDirectoryInput, listDirectoryOutput)
			} else {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", aisContext.backend.dirName, listDirectoryInput, err)
			}
		}
	}()

	// Set continuation token if provided
	if listDirectoryInput.continuationToken != "" {
		lsmsg.ContinuationToken = listDirectoryInput.continuationToken
	}

	// Set page size if specified
	if listDirectoryInput.maxItems != 0 {
		lsmsg.PageSize = int64(listDirectoryInput.maxItems)
	}

	// List objects (one page)
	var lsoResult *cmn.LsoRes                                                                          // List Objects Result
	lsoResult, err = api.ListObjectsPage(aisContext.baseParams, aisContext.bck, lsmsg, api.ListArgs{}) // List Objects Page
	if err != nil {
		err = fmt.Errorf("[AIStore] listDirectory failed: %v", err)
		return
	}

	// Parse results
	listDirectoryOutput = &listDirectoryOutputStruct{
		subdirectory:          make([]string, 0),
		file:                  make([]listDirectoryOutputFileStruct, 0),
		nextContinuationToken: lsoResult.ContinuationToken,
		isTruncated:           lsoResult.ContinuationToken != "",
	}

	// Process entries
	for _, entry := range lsoResult.Entries {
		// Remove the fullDirPath prefix
		relativeName := strings.TrimPrefix(entry.Name, fullDirPath)

		// Skip if empty (shouldn't happen)
		if relativeName == "" {
			continue
		}

		// Check if this is a subdirectory (contains a slash after the prefix)
		slashIdx := strings.Index(relativeName, "/")
		if slashIdx != -1 {
			// This is a subdirectory or a file in a subdirectory
			subdirName := relativeName[:slashIdx]

			// Add subdirectory if not already present
			found := false
			for _, existing := range listDirectoryOutput.subdirectory {
				if existing == subdirName {
					found = true
					break
				}
			}
			if !found {
				listDirectoryOutput.subdirectory = append(listDirectoryOutput.subdirectory, subdirName)
			}
		} else {
			// This is a file in the current directory
			// Note: entry.Atime is string, parse it or use current time as fallback
			mtime := time.Now()
			if entry.Atime != "" {
				// Atime format is Unix timestamp string (microseconds)
				if atimeInt, err := strconv.ParseInt(entry.Atime, 10, 64); err == nil {
					mtime = time.UnixMicro(atimeInt)
				}
			}
			listDirectoryOutput.file = append(listDirectoryOutput.file, listDirectoryOutputFileStruct{
				basename: relativeName,
				eTag:     entry.Checksum,
				mTime:    mtime,
				size:     uint64(entry.Size),
			})
		}
	}

	return
}

// `readFile` is called to read a range of a `file` at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (aisContext *aistoreContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	// Observability: Record request at START
	recordRequest(aisContext.backend.dirName, "read")

	// Observability: Record latency/response at END
	startTime := time.Now()
	defer func() {
		bytesRead := int64(0)
		if err == nil && readFileOutput != nil {
			bytesRead = int64(len(readFileOutput.buf))
		}
		recordBackendMetrics(aisContext.backend.dirName, "read", startTime, err, bytesRead)
	}()

	var (
		backend      = aisContext.backend
		fullFilePath = backend.prefix + readFileInput.filePath
		rangeBegin   = readFileInput.offsetCacheLine * globals.config.cacheLineSize
		rangeEnd     = rangeBegin + globals.config.cacheLineSize - 1
	)

	defer func() {
		switch aisContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", aisContext.backend.dirName, readFileInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.readFile(%#v) succeeded", aisContext.backend.dirName, readFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", aisContext.backend.dirName, readFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.readFile(%#v) returning readFileOutput: {\"eTag\":\"%s\",len(\"buf\":%v)}", aisContext.backend.dirName, readFileInput, readFileOutput.eTag, len(readFileOutput.buf))
			} else {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", aisContext.backend.dirName, readFileInput, err)
			}
		}
	}()

	// Verify ETag if specified
	if readFileInput.ifMatch != "" {
		var props *cmn.ObjectProps
		props, err = api.HeadObject(aisContext.baseParams, aisContext.bck, fullFilePath, api.HeadArgs{
			Silent: true,
		})
		if err != nil {
			return
		}
		if props.Cksum != nil && props.Cksum.Value() != readFileInput.ifMatch {
			err = errors.New("eTag mismatch")
			return
		}
	}

	// Create buffer and GetArgs
	buf := &bytes.Buffer{}
	getArgs := &api.GetArgs{
		Writer: buf,
		Header: http.Header{},
	}

	// Set range header
	getArgs.Header.Set(cos.HdrRange, fmt.Sprintf("bytes=%d-%d", rangeBegin, rangeEnd))

	// Get the object
	var oah api.ObjAttrs
	oah, err = api.GetObject(aisContext.baseParams, aisContext.bck, fullFilePath, getArgs)
	if err != nil {
		return
	}

	// Build output
	readFileOutput = &readFileOutputStruct{
		eTag: oah.Attrs().Cksum.Value(),
		buf:  buf.Bytes(),
	}

	return
}

// `statDirectory` is called to verify that the specified path refers to a `directory`.
// An error is returned if either the specified path is not a `directory` or non-existent.
func (aisContext *aistoreContextStruct) statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
	// Observability: Record request at START
	recordRequest(aisContext.backend.dirName, "info")

	// Observability: Record latency/response at END
	startTime := time.Now()
	defer func() {
		recordBackendMetrics(aisContext.backend.dirName, "info", startTime, err, 0)
	}()

	var (
		backend     = aisContext.backend
		fullDirPath = backend.prefix + statDirectoryInput.dirPath
		lsmsg       = &apc.LsoMsg{
			Prefix:   fullDirPath,
			PageSize: 1,
			Props:    strings.Join(apc.GetPropsMinimal, ","),
		}
		lsoResult *cmn.LsoRes
	)

	defer func() {
		switch aisContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", aisContext.backend.dirName, statDirectoryInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statDirectory(%#v) succeeded", aisContext.backend.dirName, statDirectoryInput)
			} else {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", aisContext.backend.dirName, statDirectoryInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statDirectory(%#v) returning statDirectoryOutput: %#v", aisContext.backend.dirName, statDirectoryInput, statDirectoryOutput)
			} else {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", aisContext.backend.dirName, statDirectoryInput, err)
			}
		}
	}()

	// List with limit of 1 to check if directory is accessible
	// Note: In object storage, directories are just prefixes and can be empty.
	// We rely on the API error to determine if the bucket/prefix is inaccessible.
	lsoResult, err = api.ListObjectsPage(aisContext.baseParams, aisContext.bck, lsmsg, api.ListArgs{})
	if err == nil {
		if (lsoResult == nil) || (lsoResult.Entries == nil) || (len(lsoResult.Entries) == 0) {
			err = errors.New("missing directory")
			return
		}

		statDirectoryOutput = &statDirectoryOutputStruct{}
	}

	return
}

// `statFile` is called to fetch the `file` metadata at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (aisContext *aistoreContextStruct) statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
	// Observability: Record request at START
	recordRequest(aisContext.backend.dirName, "info")

	// Observability: Record latency/response at END
	startTime := time.Now()
	defer func() {
		bytesReported := int64(0)
		if err == nil && statFileOutput != nil {
			bytesReported = int64(statFileOutput.size)
		}
		recordBackendMetrics(aisContext.backend.dirName, "info", startTime, err, bytesReported)
	}()

	var (
		backend      = aisContext.backend
		fullFilePath = backend.prefix + statFileInput.filePath
	)

	defer func() {
		switch aisContext.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", aisContext.backend.dirName, statFileInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statFile(%#v) succeeded", aisContext.backend.dirName, statFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", aisContext.backend.dirName, statFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statFile(%#v) returning statFileOutput: %#v", aisContext.backend.dirName, statFileInput, statFileOutput)
			} else {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", aisContext.backend.dirName, statFileInput, err)
			}
		}
	}()

	// Head the object
	var props *cmn.ObjectProps
	props, err = api.HeadObject(aisContext.baseParams, aisContext.bck, fullFilePath, api.HeadArgs{
		Silent: true,
	})
	if err != nil {
		return
	}

	// Verify ETag if specified
	if statFileInput.ifMatch != "" {
		if props.Cksum != nil && props.Cksum.Value() != statFileInput.ifMatch {
			err = errors.New("eTag mismatch")
			return
		}
	}

	statFileOutput = &statFileOutputStruct{
		eTag:  props.Cksum.Value(),
		mTime: time.UnixMicro(props.Atime),
		size:  uint64(props.Size),
	}

	return
}
