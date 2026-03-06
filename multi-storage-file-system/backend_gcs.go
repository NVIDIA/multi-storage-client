package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

// `gcsContextStruct` holds the GCS-specific backend details.
type gcsContextStruct struct {
	backend     *backendStruct
	gcsClient   *storage.Client
	retryOption storage.RetryOption
}

// `setupGCSContext` establishes the GCS client context. Once set up, each
// method defined in the `backendConfigIf` interface may be invoked.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupGCSContext() (err error) {
	var (
		backendGCS                          = backend.backendTypeSpecifics.(*backendConfigGCSStruct)
		envError                            error
		gcsClient                           *storage.Client
		gcsClientOptionSlice                []option.ClientOption
		httpClient                          *http.Client
		httpTransport                       *http.Transport
		retryOption                         storage.RetryOption
		storageEmulatorHostEnvAtEntry       string
		storageEmulatorHostEnvAtEntryWasSet bool
		tlsConfig                           *tls.Config
	)

	gcsClientOptionSlice = make([]option.ClientOption, 0)

	if backendGCS.apiKey == "" {
		gcsClientOptionSlice = append(gcsClientOptionSlice, option.WithoutAuthentication())
	} else {
		gcsClientOptionSlice = append(gcsClientOptionSlice, option.WithAPIKey(backendGCS.apiKey))
	}

	if backendGCS.skipTLSCertificateVerify {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: true,
		}

		httpTransport = &http.Transport{
			TLSClientConfig: tlsConfig,
		}

		httpClient = &http.Client{
			Transport: httpTransport,
		}

		gcsClientOptionSlice = append(gcsClientOptionSlice, option.WithHTTPClient(httpClient))
	}

	if backendGCS.endpoint != "" {
		// Ideally, we would just to the following:
		//
		//   gcsClientOptionSlice = append(gcsClientOptionSlice, option.WithEndpoint(backendGCS.endpoint))
		//
		// Alas, at least for fake-gcs-server, the paths we ultimately send lack the proper mangling
		// required. So, instead, we will use the sledgehammer approach and set the "STORAGE_EMULATOR_HOST"
		// environment variable around the call to storage.NewClient(). Fortunately, this code path is
		// single threaded, so this setting will not affect other calls to storage.NewClient().

		storageEmulatorHostEnvAtEntry, storageEmulatorHostEnvAtEntryWasSet = os.LookupEnv("STORAGE_EMULATOR_HOST")
		envError = os.Setenv("STORAGE_EMULATOR_HOST", backendGCS.endpoint)
		if envError != nil {
			globals.logger.Fatalf("[FATAL] os.Setenv(\"STORAGE_EMULATOR_HOST\", backendGCS.endpoint) failed: %v", envError)
		}

		// Unfortunately, the current version of the SDK switches to XML mode (instead of JSON mode)
		// for reads. This is despite the fact that the use of the STORAGE_EMULATOR_HOST was supposed
		// to prevent that. That is not the case.

		gcsClientOptionSlice = append(gcsClientOptionSlice, storage.WithJSONReads())

		// Interestingly, there is a corresponding issue on the write side and, indeed, the use of
		// STORAGE_EMULATOR_HOST was also supposed to implicitly force JSON mode for writes as well.
		// Unfortunately, the exposure of the following is not (yet) available.

		// gcsClientOptionSlice = append(gcsClientOptionSlice, storage.WithJSONWrites())

		// [TODO] Track GCS SDK updates to hopefully more cleanly resolve the above emulator compatibility issues.
	}

	gcsClient, err = storage.NewClient(context.Background(), gcsClientOptionSlice...)
	if backendGCS.endpoint != "" {
		// Now undo the setting of "STORAGE_EMULATOR_HOST" environment variable
		if storageEmulatorHostEnvAtEntryWasSet {
			envError = os.Setenv("STORAGE_EMULATOR_HOST", storageEmulatorHostEnvAtEntry)
			if envError != nil {
				globals.logger.Fatalf("[FATAL] os.Setenv(\"STORAGE_EMULATOR_HOST\", storageEmulatorHostEnvAtEntry) failed: %v", envError)
			}
		} else {
			envError = os.Unsetenv("STORAGE_EMULATOR_HOST")
			if envError != nil {
				globals.logger.Fatalf("[FATAL] os.Unsetenv(\"STORAGE_EMULATOR_HOST\") failed: %v", envError)
			}
		}
	}
	if err != nil {
		err = fmt.Errorf("[GCS] storage.NewClient(context.Background()) failed: %v", err)
		return
	}

	retryOption = storage.WithBackoff(gax.Backoff{
		Initial:    backendGCS.retryBaseDelay,
		Max:        backendGCS.retryMaxDelay,
		Multiplier: backendGCS.retryNextDelayMultiplier,
	})

	backend.context = &gcsContextStruct{
		backend:     backend,
		gcsClient:   gcsClient,
		retryOption: retryOption,
	}

	err = nil
	return
}

// `backendCommon` is called to return a pointer to the context's common `backendStruct`.
func (backend *gcsContextStruct) backendCommon() (backendCommon *backendStruct) {
	backendCommon = backend.backend
	return
}

// `deleteFile` is called to remove a "file" at the specified path.
// If a `subdirectory` or nothing is found at that path, an error will be returned.
func (gcsContext *gcsContextStruct) deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
	var (
		objectHandle *storage.ObjectHandle
	)

	objectHandle = gcsContext.gcsClient.Bucket(gcsContext.backend.bucketContainerName).Object(gcsContext.backend.prefix + deleteFileInput.filePath)
	objectHandle = objectHandle.Retryer(gcsContext.retryOption)

	err = objectHandle.Delete(context.Background())
	if err != nil {
		err = fmt.Errorf("[GCS] objectHandle.Delete() failed: %v", err)
		return
	}

	return
}

// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
// indicates the `directory` has been completely enumerated. The `isTruncated` field will also
// align with this convention.
func (gcsContext *gcsContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	var (
		bucketHandle          *storage.BucketHandle
		nextContinuationToken string
		objectAttrs           *storage.ObjectAttrs
		objectAttrsSlice      []*storage.ObjectAttrs
		objectIterator        *storage.ObjectIterator
		pager                 *iterator.Pager
		query                 *storage.Query
	)

	bucketHandle = gcsContext.gcsClient.Bucket(gcsContext.backend.bucketContainerName)
	bucketHandle = bucketHandle.Retryer(gcsContext.retryOption)

	query = &storage.Query{
		Prefix:    gcsContext.backend.prefix + listDirectoryInput.dirPath,
		Delimiter: "/",
	}

	objectIterator = bucketHandle.Objects(context.Background(), query)

	// [TODO] Revert NextPage() workaround in listDirectory()
	//
	// Currently, the SDK validates the pageSize argument to iterator.NewPager() to
	// be positive. The ideal would be for a specification of ZERO to let the server
	// decide the page size (that it typically limits responses to in any event).
	//
	// One workaround is to instead modify the storage.ObjectIterator as follows:
	//
	//   objectIterator.PageInfo().Token = listDirectoryInput.continuationToken
	//   objectIterator.PageInfo().MaxSize = int(maxItems)
	//
	// After that, the objectIterator itself would, in some future SDK version,
	// support the NextPage() method:
	//
	//   nextContinuationToken, err = objectIterator.NextPage(&objectAttrsSlice)
	//
	// Unfortunately, this is also not (yet) available. As such, we instead must
	// ensure the pageSize value is positive and still call iterator.NewPager()
	// by substituting a reasonable value. We will use 1000.

	if listDirectoryInput.maxItems == 0 {
		pager = iterator.NewPager(objectIterator, 1000, listDirectoryInput.continuationToken)
	} else {
		pager = iterator.NewPager(objectIterator, int(listDirectoryInput.maxItems), listDirectoryInput.continuationToken)
	}

	nextContinuationToken, err = pager.NextPage(&objectAttrsSlice)
	if err != nil {
		err = fmt.Errorf("[GCS] pager.NextPage() failed: %v", err)
		return
	}

	listDirectoryOutput = &listDirectoryOutputStruct{
		subdirectory:          make([]string, 0, len(objectAttrsSlice)),
		file:                  make([]listDirectoryOutputFileStruct, 0, len(objectAttrsSlice)),
		nextContinuationToken: nextContinuationToken,
		isTruncated:           (nextContinuationToken != ""),
	}

	for _, objectAttrs = range objectAttrsSlice {
		if objectAttrs.Prefix == "" {
			listDirectoryOutput.file = append(listDirectoryOutput.file, listDirectoryOutputFileStruct{
				basename: path.Base(objectAttrs.Name),
				eTag:     objectAttrs.Etag,
				mTime:    objectAttrs.Updated,
				size:     uint64(objectAttrs.Size),
			})
		} else {
			listDirectoryOutput.subdirectory = append(listDirectoryOutput.subdirectory, path.Base(objectAttrs.Prefix))
		}
	}

	return
}

// `listObjects` is called to fetch a `page` of the objects. An empty continuationToken or
// empty list of elements (`objects`) indicates the list of `objects` has been completely
// enumerated. The `isTruncated` field will also align with this convention.
func (gcsContext *gcsContextStruct) listObjects(listObjectsInput *listObjectsInputStruct) (listObjectsOutput *listObjectsOutputStruct, err error) {
	var (
		backend               = gcsContext.backend
		bucketHandle          *storage.BucketHandle
		nextContinuationToken string
		objectAttrs           *storage.ObjectAttrs
		objectAttrsSlice      []*storage.ObjectAttrs
		objectIterator        *storage.ObjectIterator
		pager                 *iterator.Pager
		query                 *storage.Query
	)

	bucketHandle = gcsContext.gcsClient.Bucket(gcsContext.backend.bucketContainerName)
	bucketHandle = bucketHandle.Retryer(gcsContext.retryOption)

	query = &storage.Query{
		Prefix: gcsContext.backend.prefix,
	}

	objectIterator = bucketHandle.Objects(context.Background(), query)

	// [TODO] Revert NextPage() workaround in listDirectory()
	//
	// Currently, the SDK validates the pageSize argument to iterator.NewPager() to
	// be positive. The ideal would be for a specification of ZERO to let the server
	// decide the page size (that it typically limits responses to in any event).
	//
	// One workaround is to instead modify the storage.ObjectIterator as follows:
	//
	//   objectIterator.PageInfo().Token = listDirectoryInput.continuationToken
	//   objectIterator.PageInfo().MaxSize = int(maxItems)
	//
	// After that, the objectIterator itself would, in some future SDK version,
	// support the NextPage() method:
	//
	//   nextContinuationToken, err = objectIterator.NextPage(&objectAttrsSlice)
	//
	// Unfortunately, this is also not (yet) available. As such, we instead must
	// ensure the pageSize value is positive and still call iterator.NewPager()
	// by substituting a reasonable value. We will use 1000.

	if listObjectsInput.maxItems == 0 {
		pager = iterator.NewPager(objectIterator, 1000, listObjectsInput.continuationToken)
	} else {
		pager = iterator.NewPager(objectIterator, int(listObjectsInput.maxItems), listObjectsInput.continuationToken)
	}

	nextContinuationToken, err = pager.NextPage(&objectAttrsSlice)
	if err != nil {
		err = fmt.Errorf("[GCS] pager.NextPage() failed: %v", err)
		return
	}

	listObjectsOutput = &listObjectsOutputStruct{
		object:                make([]listObjectsOutputObjectStruct, 0, len(objectAttrsSlice)),
		nextContinuationToken: nextContinuationToken,
		isTruncated:           (nextContinuationToken != ""),
	}

	for _, objectAttrs = range objectAttrsSlice {
		listObjectsOutput.object = append(listObjectsOutput.object, listObjectsOutputObjectStruct{
			path:  strings.TrimPrefix(objectAttrs.Name, backend.prefix),
			eTag:  objectAttrs.Etag,
			mTime: objectAttrs.Updated,
			size:  uint64(objectAttrs.Size),
		})
	}

	return
}

// `readFile` is called to read a range of a `file` at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (gcsContext *gcsContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	var (
		attrs             *storage.ObjectAttrs
		objectHandle      *storage.ObjectHandle
		rangeReader       *storage.Reader
		rangeReaderLength uint64
		rangeReaderOffset uint64
	)

	objectHandle = gcsContext.gcsClient.Bucket(gcsContext.backend.bucketContainerName).Object(gcsContext.backend.prefix + readFileInput.filePath)
	objectHandle = objectHandle.Retryer(gcsContext.retryOption)

	// Note: .IfMatch not directly supported nor does a NewRangeReader() return eTag, so we must do the non-atomic manual ETag comparison check and fetch
	//       Guidance is to switch to use attrs.Generation (an int64 value that monitonically increases with each update). As the .ifMatch field is a
	//       string, an isomorphic map would need to be applied to allow GCS's preference for .Generation to be used be the endpoint-agnostic ETag checks
	//
	// [TODO] Once ETags are actually being used to ensure atomicity, switch GCS to report and if-check attrs.Generation.

	attrs, err = objectHandle.Attrs(context.Background())
	if err != nil {
		err = fmt.Errorf("[GCS] objectHandle.Attrs() failed: %v", err)
		return
	}

	if readFileInput.ifMatch != "" {
		if attrs.Etag != readFileInput.ifMatch {
			err = errors.New("eTag mismatch")
			return
		}
	}

	rangeReaderOffset = readFileInput.offsetCacheLine * globals.config.cacheLineSize
	rangeReaderLength = globals.config.cacheLineSize

	// Note: More non-atomic logic here attempts to avoid reading beyond EOF

	if rangeReaderOffset >= uint64(attrs.Size) {
		err = errors.New("rangeReaderOffset >= uint64(attrs.Size)")
		return
	}

	if (rangeReaderOffset + rangeReaderLength) > uint64(attrs.Size) {
		rangeReaderLength = uint64(attrs.Size) - rangeReaderOffset
	}

	rangeReader, err = objectHandle.NewRangeReader(context.Background(), int64(rangeReaderOffset), int64(rangeReaderLength))
	if err == nil {
		readFileOutput = &readFileOutputStruct{
			eTag: attrs.Etag,
		}

		readFileOutput.buf, err = io.ReadAll(rangeReader)
		if err != nil {
			globals.logger.Fatalf("[FATAL] io.ReadAll(rangeReader) failed: %v", err)
		}

		err = rangeReader.Close()
		if err != nil {
			globals.logger.Fatalf("[FATAL] rangeReader.Close() failed: %v", err)
		}
	}

	return
}

// `statDirectory` is called to verify that the specified path refers to a `directory`.
// An error is returned if either the specified path is not a `directory` or non-existent.
func (gcsContext *gcsContextStruct) statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
	var (
		bucketHandle     *storage.BucketHandle
		objectAttrsSlice []*storage.ObjectAttrs
		objectIterator   *storage.ObjectIterator
		pager            *iterator.Pager
		query            *storage.Query
	)

	bucketHandle = gcsContext.gcsClient.Bucket(gcsContext.backend.bucketContainerName)
	bucketHandle = bucketHandle.Retryer(gcsContext.retryOption)

	query = &storage.Query{
		Prefix: gcsContext.backend.prefix + statDirectoryInput.dirPath,
	}

	objectIterator = bucketHandle.Objects(context.Background(), query)

	pager = iterator.NewPager(objectIterator, int(1), "")

	_, err = pager.NextPage(&objectAttrsSlice)
	if err == nil {
		if len(objectAttrsSlice) == 0 {
			err = errors.New("missing directory")
		} else {
			statDirectoryOutput = &statDirectoryOutputStruct{}
		}
	} else {
		err = errors.New("missing directory")
	}

	return
}

// `statFile` is called to fetch the `file` metadata at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (gcsContext *gcsContextStruct) statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
	var (
		attrs        *storage.ObjectAttrs
		objectHandle *storage.ObjectHandle
	)

	objectHandle = gcsContext.gcsClient.Bucket(gcsContext.backend.bucketContainerName).Object(gcsContext.backend.prefix + statFileInput.filePath)
	objectHandle = objectHandle.Retryer(gcsContext.retryOption)

	attrs, err = objectHandle.Attrs(context.Background())
	if err != nil {
		err = fmt.Errorf("[GCS] objectHandle.Attrs() failed: %v", err)
		return
	}

	statFileOutput = &statFileOutputStruct{
		eTag:  attrs.Etag,
		mTime: attrs.Updated,
		size:  uint64(attrs.Size),
	}

	return
}
