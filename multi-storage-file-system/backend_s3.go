package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// Hard service limits. No MSFS configuration can raise these, so any size
// derived from config has to be reconciled against them before a request is
// issued. S3-compatible endpoints may impose lower ceilings of their own.
const (
	s3MinPartSize      = 5 * 1024 * 1024        // every part but the last
	s3MaxPartSize      = 5 * 1024 * 1024 * 1024 // one UploadPart body
	s3MaxSinglePutSize = 5 * 1024 * 1024 * 1024 // one PutObject body
	s3MaxPartCount     = 10000                  // parts per multipart upload
)

// `s3ContextStruct` holds the S3-specific backend details.
type s3ContextStruct struct {
	backend  *backendStruct
	s3Client *s3.Client
}

type s3WriteStream struct {
	s3Context       *s3ContextStruct
	fullFilePath    string
	uploadID        string
	partSize        uint64
	nextPartNumber  int32
	completedParts  []types.CompletedPart
	completedPartsM sync.Mutex
	err             error
	errMu           sync.Mutex
	wg              sync.WaitGroup
	// jobs feeds a fixed pool of uploaders. queueClosed is guarded by
	// globals.Lock rather than a mutex: every caller of queuePartNumber,
	// complete and abort is a `...Locked` function.
	jobs        chan s3StreamPart
	queueClosed bool
}

type s3StreamPart struct {
	partNumber int32
	data       []byte
}

// `backendCommon` is called to return a pointer to the context's common `backendStruct`.
func (backend *s3ContextStruct) backendCommon() (backendCommon *backendStruct) {
	backendCommon = backend.backend
	return
}

// `setupS3Context` establishes the S3 client context. Once set up, each
// method defined in the `backendConfigIf` interafce may be invoked.
// Note that there is no `destroyContext` counterpart.
func (backend *backendStruct) setupS3Context() (err error) {
	var (
		backendPathParsed *url.URL
		backendS3         = backend.backendTypeSpecifics.(*backendConfigS3Struct)
		configOptions     []func(*config.LoadOptions) error
		s3Config          aws.Config
		s3Endpoint        string
	)

	configOptions = []func(*config.LoadOptions) error{}

	if backendS3.useConfigEnv || backendS3.useCredentialsEnv {
		configOptions = append(configOptions, config.WithSharedConfigProfile(backendS3.configCredentialsProfile))
	}

	if backendS3.useConfigEnv {
		configOptions = append(configOptions, config.WithSharedConfigFiles([]string{backendS3.configFilePath}))
	} else {
		configOptions = append(configOptions, config.WithSharedConfigFiles(nil), config.WithRegion(backendS3.region))
	}

	switch {
	case backendS3.anonymous:
		// Anonymous access: install the sentinel provider so the SDK skips
		// request signing entirely (public / no-auth S3-compatible endpoints).
		configOptions = append(configOptions, config.WithSharedCredentialsFiles(nil), config.WithCredentialsProvider(aws.AnonymousCredentials{}))
	case backendS3.useCredentialsEnv:
		configOptions = append(configOptions, config.WithSharedCredentialsFiles(([]string{backendS3.credentialsFilePath})))
	default:
		configOptions = append(configOptions, config.WithSharedCredentialsFiles(nil), config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     backendS3.accessKeyID,
				SecretAccessKey: backendS3.secretAccessKey,
			}}))
	}

	connectionCount := max(int(globals.config.writeCommitWorkers), int(backend.uploadPartConcurrency), 32)
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConns = connectionCount * 2
		t.MaxIdleConnsPerHost = connectionCount
		t.MaxConnsPerHost = connectionCount * 2
		if backendS3.skipTLSCertificateVerify {
			if t.TLSClientConfig == nil {
				t.TLSClientConfig = &tls.Config{}
			}
			t.TLSClientConfig.InsecureSkipVerify = true
			t.TLSClientConfig.MinVersion = tls.VersionTLS12
		}
	})
	configOptions = append(configOptions,
		config.WithHTTPClient(httpClient),
		config.WithRetryer(func() aws.Retryer {
			return backend
		}),
	)

	s3Config, err = config.LoadDefaultConfig(context.Background(), configOptions...)
	if err != nil {
		err = fmt.Errorf("[S3] config.LoadDefaultConfig() failed: %v", err)
		return
	}

	if backendS3.useConfigEnv {
		if s3Config.BaseEndpoint == nil {
			err = errors.New("s3Config.BaseEndpoint == nil")
			return
		}
		backendPathParsed, err = url.Parse(*s3Config.BaseEndpoint)
		if err != nil {
			err = fmt.Errorf("url.Parse(*s3Config.BaseEndpoint) failed: %v", err)
			return
		}
	} else {
		backendPathParsed, err = url.Parse(backendS3.endpoint)
		if err != nil {
			err = fmt.Errorf("url.Parse(backendS3.endpoint) failed: %v", err)
			return
		}
	}

	if backendS3.virtualHostedStyleRequest {
		backendPathParsed.Host = backend.bucketContainerName + "." + backendPathParsed.Host
		s3Endpoint = backendPathParsed.Scheme + "://" + backendPathParsed.Host + backendPathParsed.Path
	} else {
		s3Endpoint = backendPathParsed.Scheme + "://" + backendPathParsed.Host + backendPathParsed.Path
		backendPathParsed.Path += "/" + backend.bucketContainerName
	}

	if backend.prefix == "" {
		backend.backendPath = backendPathParsed.String() + "/"
	} else {
		backendPathParsed.Path += "/" + backend.prefix
		backend.backendPath = backendPathParsed.String()
	}

	backend.context = &s3ContextStruct{
		backend: backend,
		s3Client: s3.NewFromConfig(s3Config, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
			o.UsePathStyle = !backendS3.virtualHostedStyleRequest
			o.ResponseChecksumValidation = aws.ResponseChecksumValidationWhenRequired
		}),
	}

	return
}

// `IsErrorRetryable` is an aws.Retryer callback that returns whether or not a
// request that fails should be retried. See
// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#AdaptiveMode.IsErrorRetryable.
func (backend *backendStruct) IsErrorRetryable(err error) bool {
	var (
		httpErr           *awshttp.ResponseError
		httpErrStatusCode int
	)

	if err == nil {
		return false
	}

	if !errors.As(err, &httpErr) {
		return true
	}

	httpErrStatusCode = httpErr.HTTPStatusCode()

	switch {
	case httpErrStatusCode < 400:
		return true
	case httpErrStatusCode == http.StatusTooManyRequests:
		return true
	case httpErrStatusCode >= 500:
		return true
	default:
		return false
	}
}

// `MaxAttempts` is an aws.Retryer callback that returns the maximum number of attempts
// (including the initial attempt) to be made for a retryable request.
// See https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#Standard.MaxAttempts.
func (backend *backendStruct) MaxAttempts() int {
	return len(backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay) + 1
}

// `RetryDelay` is an aws.Retryer callback that returns the delay before a previously
// failed request should be retried.
// See https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#Standard.RetryDelay.
func (backend *backendStruct) RetryDelay(attempt int, _ error) (time.Duration, error) {
	if (attempt < 1) || (attempt > len(backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay)) {
		return time.Duration(0), fmt.Errorf("unexpected attempt: %v (should have been in [1:%v])", attempt, len(backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay))
	}

	return backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay[attempt-1], nil
}

// `GetRetryToken` is an aws.Retryer callback that returns a func used to additionally
// apply a retry `cost` for performing a retry of a previously failed request.
// See https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#Standard.GetRetryToken.
func (backend *backendStruct) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return func(error) error {
		return nil
	}, nil
}

// `GetInitialToken` is an aws.Retryer callback that returns a func used to additionally
// apply an initial `cost` for performing a retry of a previously failed request.
// See https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#Standard.GetRetryToken.
// Note that this callback has been deprecated but is provided here to satisfy the
// requirements of a custom aws.Retryer interface.
func (backend *backendStruct) GetInitialToken() (releaseToken func(error) error) {
	return func(error) error {
		return nil
	}
}

// `GetAttemptToken` is an aws.Retryer callback that returns a func used to additionally
// apply a `cost` for performing a retry of a previously failed request.
// See https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws/retry#AdaptiveMode.GetAttemptToken.
func (backend *backendStruct) GetAttemptToken(context.Context) (func(error) error, error) {
	return func(error) error {
		return nil
	}, nil
}

// `deleteFile` is called to remove a "file" at the specified path.
// If a `subdirectory` or nothing is found at that path, an error will be returned.
func (s3Context *s3ContextStruct) deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
	var (
		backend             = s3Context.backend
		fullFilePath        = backend.prefix + deleteFileInput.filePath
		s3DeleteObjectInput *s3.DeleteObjectInput
	)

	s3DeleteObjectInput = &s3.DeleteObjectInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	}
	if deleteFileInput.ifMatch != "" {
		s3DeleteObjectInput.IfMatch = aws.String(deleteFileInput.ifMatch)
	}

	_, err = s3Context.s3Client.DeleteObject(context.Background(), s3DeleteObjectInput)

	return
}

// `listDirectory` is called to fetch a `page` of the `directory` at the specified path.
// An empty continuationToken or empty list of directory elements (`subdirectories` and `files`)
// indicates the `directory` has been completely enumerated. The `isTruncated` field will also
// align with this convention.
func (s3Context *s3ContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	var (
		backend               = s3Context.backend
		fullDirPath           = backend.prefix + listDirectoryInput.dirPath
		s3CommonPrefix        types.CommonPrefix
		s3ListObjectsV2Input  *s3.ListObjectsV2Input
		s3ListObjectsV2Output *s3.ListObjectsV2Output
		s3Object              types.Object
	)

	s3ListObjectsV2Input = &s3.ListObjectsV2Input{
		Bucket:    aws.String(backend.bucketContainerName),
		Prefix:    aws.String(fullDirPath),
		Delimiter: aws.String("/"),
	}
	if listDirectoryInput.continuationToken != "" {
		s3ListObjectsV2Input.ContinuationToken = aws.String(listDirectoryInput.continuationToken)
	}
	if listDirectoryInput.startAfter != "" {
		s3ListObjectsV2Input.StartAfter = aws.String(backend.prefix + listDirectoryInput.startAfter)
	}
	if listDirectoryInput.maxItems != 0 {
		s3ListObjectsV2Input.MaxKeys = aws.Int32(int32(listDirectoryInput.maxItems))
	}

	s3ListObjectsV2Output, err = s3Context.s3Client.ListObjectsV2(context.Background(), s3ListObjectsV2Input)
	if err != nil {
		err = fmt.Errorf("[S3] listDirectory failed: %v", err)
		return
	}

	listDirectoryOutput = &listDirectoryOutputStruct{
		subdirectory: make([]string, 0, len(s3ListObjectsV2Output.CommonPrefixes)),
		file:         make([]listDirectoryOutputFileStruct, 0, len(s3ListObjectsV2Output.Contents)),
	}

	if s3ListObjectsV2Output.NextContinuationToken == nil {
		listDirectoryOutput.nextContinuationToken = ""
	} else {
		listDirectoryOutput.nextContinuationToken = *s3ListObjectsV2Output.NextContinuationToken
	}

	// AWS S3 neglects to set s3ListObjectsV2Output.IsTruncated properly, so we
	// instead compute our listDirectoryOutput.isTruncated value on whether or now
	// listDirectoryOutput.nextContinuationToken is above set to a non-empty string

	listDirectoryOutput.isTruncated = (listDirectoryOutput.nextContinuationToken != "")

	for _, s3CommonPrefix = range s3ListObjectsV2Output.CommonPrefixes {
		listDirectoryOutput.subdirectory = append(listDirectoryOutput.subdirectory, strings.TrimSuffix(strings.TrimPrefix(*s3CommonPrefix.Prefix, fullDirPath), "/"))
	}

	for _, s3Object = range s3ListObjectsV2Output.Contents {
		listDirectoryOutput.file = append(listDirectoryOutput.file, listDirectoryOutputFileStruct{
			basename: strings.TrimPrefix(*s3Object.Key, fullDirPath),
			eTag:     strings.TrimLeft(strings.TrimRight(*s3Object.ETag, "\""), "\""),
			mTime:    *s3Object.LastModified,
			size:     uint64(*s3Object.Size),
		})
	}

	return
}

// `listObjects` is called to fetch a `page` of the objects. An empty continuationToken or
// empty list of elements (`objects`) indicates the list of `objects` has been completely
// enumerated. The `isTruncated` field will also align with this convention.
func (s3Context *s3ContextStruct) listObjects(listObjectsInput *listObjectsInputStruct) (listObjectsOutput *listObjectsOutputStruct, err error) {
	var (
		backend               = s3Context.backend
		s3ListObjectsV2Input  *s3.ListObjectsV2Input
		s3ListObjectsV2Output *s3.ListObjectsV2Output
		s3Object              types.Object
	)

	s3ListObjectsV2Input = &s3.ListObjectsV2Input{
		Bucket: aws.String(backend.bucketContainerName),
		Prefix: aws.String(backend.prefix + listObjectsInput.prefix),
	}
	if listObjectsInput.continuationToken != "" {
		s3ListObjectsV2Input.ContinuationToken = aws.String(listObjectsInput.continuationToken)
	}
	if listObjectsInput.startAfter != "" {
		s3ListObjectsV2Input.StartAfter = aws.String(backend.prefix + listObjectsInput.startAfter)
	}
	if listObjectsInput.maxItems != 0 {
		s3ListObjectsV2Input.MaxKeys = aws.Int32(int32(listObjectsInput.maxItems))
	}

	s3ListObjectsV2Output, err = s3Context.s3Client.ListObjectsV2(context.Background(), s3ListObjectsV2Input)
	if err != nil {
		err = fmt.Errorf("[S3] listObjects failed: %v", err)
		return
	}

	listObjectsOutput = &listObjectsOutputStruct{
		object: make([]listObjectsOutputObjectStruct, 0, len(s3ListObjectsV2Output.Contents)),
	}

	if s3ListObjectsV2Output.NextContinuationToken == nil {
		listObjectsOutput.nextContinuationToken = ""
	} else {
		listObjectsOutput.nextContinuationToken = *s3ListObjectsV2Output.NextContinuationToken
	}

	// AWS S3 neglects to set s3ListObjectsV2Output.IsTruncated properly, so we
	// instead compute our listDirectoryOutput.isTruncated value on whether or now
	// listDirectoryOutput.nextContinuationToken is above set to a non-empty string

	listObjectsOutput.isTruncated = (listObjectsOutput.nextContinuationToken != "")

	for _, s3Object = range s3ListObjectsV2Output.Contents {
		listObjectsOutput.object = append(listObjectsOutput.object, listObjectsOutputObjectStruct{
			path:  strings.TrimPrefix(*s3Object.Key, backend.prefix),
			eTag:  strings.TrimLeft(strings.TrimRight(*s3Object.ETag, "\""), "\""),
			mTime: *s3Object.LastModified,
			size:  uint64(*s3Object.Size),
		})
	}

	return
}

var (
	// `awsAccessKeyIDPattern` matches AWS access key IDs: AKIA (long-term) or
	// ASIA (STS session) prefix, followed by 16 uppercase alphanumeric chars.
	awsAccessKeyIDPattern = regexp.MustCompile(`\b(?:AKIA|ASIA)[A-Z0-9]{16}\b`)
	// `awsSecretAccessKeyPattern` matches AWS secret access keys: exactly 40
	// characters of base64 alphabet. Conservative — may also match other
	// 40-char base64 strings (etags, tokens), which is acceptable at the
	// no-backend config-parse log sites where this heuristic is the fallback.
	awsSecretAccessKeyPattern = regexp.MustCompile(`\b[A-Za-z0-9+/=]{40}\b`)
)

// `redactAWSSecretShapes` redacts AWS-credential-shaped substrings from s. It is
// the heuristic used by the package-level `redactSecrets` wrapper when no backend
// context is available (e.g. config-parse errors logged before backends exist).
func redactAWSSecretShapes(s string) string {
	s = awsAccessKeyIDPattern.ReplaceAllString(s, "***REDACTED-AWS-ACCESS-KEY-ID***")
	s = awsSecretAccessKeyPattern.ReplaceAllString(s, "***REDACTED-AWS-SECRET-ACCESS-KEY***")
	return s
}

// `redactSecrets` redacts this S3 backend's configured credential values, then
// applies the AWS-credential-shape heuristic to also cover credentials sourced
// from the environment or instance metadata (which are not stored in config).
func (s3Context *s3ContextStruct) redactSecrets(s string) string {
	if cfg, ok := s3Context.backend.backendTypeSpecifics.(*backendConfigS3Struct); ok && cfg != nil {
		s = redactValue(s, cfg.secretAccessKey, "***REDACTED-AWS-SECRET-ACCESS-KEY***")
		s = redactValue(s, cfg.accessKeyID, "***REDACTED-AWS-ACCESS-KEY-ID***")
	}
	return redactAWSSecretShapes(s)
}

// `readFile` is called to read a range of a `file` at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (s3Context *s3ContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	var (
		backend           = s3Context.backend
		fullFilePath      = backend.prefix + readFileInput.filePath
		rangeBegin        = readFileInput.offsetCacheLine * globals.config.cacheLineSize
		rangeEnd          = rangeBegin + globals.config.cacheLineSize - 1
		s3GetObjectInput  *s3.GetObjectInput
		s3GetObjectOutput *s3.GetObjectOutput
	)

	s3GetObjectInput = &s3.GetObjectInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", rangeBegin, rangeEnd)),
	}
	if readFileInput.ifMatch != "" {
		s3GetObjectInput.IfMatch = aws.String(readFileInput.ifMatch)
	}

	s3GetObjectOutput, err = s3Context.s3Client.GetObject(context.Background(), s3GetObjectInput)
	if err == nil {
		defer s3GetObjectOutput.Body.Close()
		readFileOutput = &readFileOutputStruct{}
		if s3GetObjectOutput.ETag == nil {
			readFileOutput.eTag = ""
		} else {
			readFileOutput.eTag = *s3GetObjectOutput.ETag
		}
		readFileOutput.buf, err = io.ReadAll(s3GetObjectOutput.Body)
	}

	return
}

// `statDirectory` is called to verify that the specified path refers to a `directory`.
// An error is returned if either the specified path is not a `directory` or non-existent.
func (s3Context *s3ContextStruct) statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
	var (
		backend               = s3Context.backend
		fullDirPath           = backend.prefix + statDirectoryInput.dirPath
		s3ListObjectsV2Input  *s3.ListObjectsV2Input
		s3ListObjectsV2Output *s3.ListObjectsV2Output
	)

	s3ListObjectsV2Input = &s3.ListObjectsV2Input{
		Bucket:  aws.String(backend.bucketContainerName),
		MaxKeys: aws.Int32(1),
		Prefix:  aws.String(fullDirPath),
	}

	s3ListObjectsV2Output, err = s3Context.s3Client.ListObjectsV2(context.Background(), s3ListObjectsV2Input)
	if err == nil {
		if (fullDirPath != "") && ((len(s3ListObjectsV2Output.CommonPrefixes) + len(s3ListObjectsV2Output.Contents)) == 0) {
			err = errors.New("missing directory")
			return
		}

		statDirectoryOutput = &statDirectoryOutputStruct{}
	}

	return
}

// `statFile` is called to fetch the `file` metadata at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (s3Context *s3ContextStruct) statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
	var (
		backend            = s3Context.backend
		fullFilePath       = backend.prefix + statFileInput.filePath
		s3HeadObjectInput  *s3.HeadObjectInput
		s3HeadObjectOutput *s3.HeadObjectOutput
	)

	s3HeadObjectInput = &s3.HeadObjectInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	}
	if statFileInput.ifMatch != "" {
		s3HeadObjectInput.IfMatch = aws.String(statFileInput.ifMatch)
	}

	s3HeadObjectOutput, err = s3Context.s3Client.HeadObject(context.Background(), s3HeadObjectInput)
	if err != nil {
		return
	}

	statFileOutput = &statFileOutputStruct{
		eTag:  strings.TrimLeft(strings.TrimRight(*s3HeadObjectOutput.ETag, "\""), "\""),
		mTime: *s3HeadObjectOutput.LastModified,
		size:  uint64(*s3HeadObjectOutput.ContentLength),
	}

	return
}

// `s3UseSinglePut` decides between one PutObject and a multipart upload.
//
// multipartThreshold is `multipart_cache_line_threshold * cache_line_size`, both
// from MSFS configuration, so it can name a size the service will not accept in
// one request: the default works out to exactly s3MaxSinglePutSize, which makes
// the shipped configuration safe only by coincidence. forceSinglePut and a zero
// threshold carry no size bound at all. Reconcile against the service limit here
// rather than discovering it after the whole body has been staged.
//
// The clamp only ever moves a decision from single PutObject to multipart, so
// objects at or below the limit behave exactly as before.
func s3UseSinglePut(size, multipartThreshold uint64, forceSinglePut bool) (singlePut bool) {
	singlePut = forceSinglePut || multipartThreshold == 0 || size <= multipartThreshold
	return singlePut && size <= s3MaxSinglePutSize
}

func (s3Context *s3ContextStruct) writeFile(writeFileInput *writeFileInputStruct) (writeFileOutput *writeFileOutputStruct, err error) {
	var (
		backend            = s3Context.backend
		fullFilePath       = backend.prefix + writeFileInput.filePath
		multipartThreshold = backend.multiPartCacheLineThreshold * globals.config.cacheLineSize
		s3PutObjectInput   *s3.PutObjectInput
		s3PutObjectOutput  *s3.PutObjectOutput
	)

	// Multipart addresses the body at per-part offsets, so it is the only route
	// for an object above the single-request limit. Every caller supplies this;
	// refuse rather than let the size decision silently depend on it.
	if writeFileInput.readerAt == nil {
		err = fmt.Errorf("%s: writeFileInput.readerAt is required", fullFilePath)
		return
	}

	if s3UseSinglePut(writeFileInput.size, multipartThreshold, writeFileInput.forceSinglePut) {
		s3PutObjectInput = &s3.PutObjectInput{
			Bucket:        aws.String(backend.bucketContainerName),
			Key:           aws.String(fullFilePath),
			Body:          writeFileInput.body,
			ContentLength: aws.Int64(int64(writeFileInput.size)),
		}
		if writeFileInput.ifMatch != "" {
			s3PutObjectInput.IfMatch = aws.String(writeFileInput.ifMatch)
		}

		s3PutObjectOutput, err = s3Context.s3Client.PutObject(context.Background(), s3PutObjectInput)
		if err != nil {
			return
		}

		writeFileOutput = &writeFileOutputStruct{
			size:  writeFileInput.size,
			mTime: time.Now(),
		}
		if s3PutObjectOutput.ETag != nil {
			writeFileOutput.eTag = trimS3ETag(*s3PutObjectOutput.ETag)
		}
		return
	}

	writeFileOutput, err = s3Context.writeFileMultipart(fullFilePath, writeFileInput)
	if err != nil {
		return
	}

	if writeFileOutput.eTag == "" {
		// The upload already committed. This HEAD only recovers the eTag, so its
		// failure must not surface as a failed write.
		s3HeadObjectOutput, headErr := s3Context.s3Client.HeadObject(context.Background(), &s3.HeadObjectInput{
			Bucket: aws.String(backend.bucketContainerName),
			Key:    aws.String(fullFilePath),
		})
		if headErr == nil && s3HeadObjectOutput.ETag != nil {
			writeFileOutput.eTag = trimS3ETag(*s3HeadObjectOutput.ETag)
		}
	}

	return
}

// `s3MultipartLayout` reconciles a configured part size with the service's part
// rules: every part but the last must be at least s3MinPartSize, no part may
// exceed s3MaxPartSize, and an upload may not exceed s3MaxPartCount parts. Both
// bounds are otherwise reachable from configuration alone, and each one fails
// only at CompleteMultipartUpload, after every part has been uploaded.
//
// The part count is capped by growing the part size rather than by reducing
// concurrency, because in this path each part body is an io.SectionReader over
// an already-resident buffer, so a larger part costs no additional memory.
func s3MultipartLayout(size, configuredPartSize uint64) (partSize, partCount uint64, err error) {
	partSize = configuredPartSize
	if partSize < s3MinPartSize {
		partSize = s3MinPartSize
	}

	if smallestAllowed := (size + s3MaxPartCount - 1) / s3MaxPartCount; smallestAllowed > partSize {
		partSize = smallestAllowed
	}

	if partSize > s3MaxPartSize {
		err = fmt.Errorf("size %d requires %d byte parts, above the %d byte part limit",
			size, partSize, uint64(s3MaxPartSize))
		return
	}

	partCount = (size + partSize - 1) / partSize
	if partCount == 0 {
		partCount = 1
	}
	return
}

func (s3Context *s3ContextStruct) writeFileMultipart(fullFilePath string, writeFileInput *writeFileInputStruct) (writeFileOutput *writeFileOutputStruct, err error) {
	var (
		backend          = s3Context.backend
		completedParts   []types.CompletedPart
		createOutput     *s3.CreateMultipartUploadOutput
		partSize         = backend.uploadPartCacheLines * globals.config.cacheLineSize
		partCount        uint64
		uploadID         string
		uploadPartErr    error
		uploadPartErrMu  sync.Mutex
		uploadPartWG     sync.WaitGroup
		uploadPartTokens chan struct{}
	)

	if partSize == 0 {
		partSize = globals.config.cacheLineSize
	}
	partSize, partCount, err = s3MultipartLayout(writeFileInput.size, partSize)
	if err != nil {
		err = fmt.Errorf("%s: %w", fullFilePath, err)
		return
	}
	completedParts = make([]types.CompletedPart, partCount)

	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	}
	createOutput, err = s3Context.s3Client.CreateMultipartUpload(context.Background(), createInput)
	if err != nil {
		return
	}
	uploadID = aws.ToString(createOutput.UploadId)

	defer func() {
		if err != nil && uploadID != "" {
			_, _ = s3Context.s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(backend.bucketContainerName),
				Key:      aws.String(fullFilePath),
				UploadId: aws.String(uploadID),
			})
		}
	}()

	concurrency := backend.uploadPartConcurrency
	if concurrency == 0 {
		concurrency = 1
	}
	uploadPartTokens = make(chan struct{}, concurrency)

	for partIndex := range partCount {
		offset := partIndex * partSize
		length := partSize
		if offset+length > writeFileInput.size {
			length = writeFileInput.size - offset
		}

		uploadPartWG.Add(1)
		uploadPartTokens <- struct{}{}
		go func() {
			defer uploadPartWG.Done()
			defer func() { <-uploadPartTokens }()

			partNumber := int32(partIndex + 1)
			body := io.NewSectionReader(writeFileInput.readerAt, int64(offset), int64(length))
			uploadOutput, uploadErr := s3Context.s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
				Bucket:        aws.String(backend.bucketContainerName),
				Key:           aws.String(fullFilePath),
				UploadId:      aws.String(uploadID),
				PartNumber:    aws.Int32(partNumber),
				Body:          body,
				ContentLength: aws.Int64(int64(length)),
			})
			if uploadErr != nil {
				uploadPartErrMu.Lock()
				if uploadPartErr == nil {
					uploadPartErr = uploadErr
				}
				uploadPartErrMu.Unlock()
				return
			}

			completedParts[partIndex] = types.CompletedPart{
				ETag:       uploadOutput.ETag,
				PartNumber: aws.Int32(partNumber),
			}
		}()
	}

	uploadPartWG.Wait()
	if uploadPartErr != nil {
		err = uploadPartErr
		return
	}

	sort.Slice(completedParts, func(i, j int) bool {
		return aws.ToInt32(completedParts[i].PartNumber) < aws.ToInt32(completedParts[j].PartNumber)
	})

	completeOutput, err := s3Context.s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(backend.bucketContainerName),
		Key:      aws.String(fullFilePath),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return
	}

	writeFileOutput = &writeFileOutputStruct{
		size:  writeFileInput.size,
		mTime: time.Now(),
	}
	if completeOutput.ETag != nil {
		writeFileOutput.eTag = trimS3ETag(*completeOutput.ETag)
	}
	return
}

func trimS3ETag(eTag string) string {
	return strings.TrimLeft(strings.TrimRight(eTag, "\""), "\"")
}

func (s3Context *s3ContextStruct) writeFileOverlay(inode *inodeStruct) (writeFileOutput *writeFileOutputStruct, err error) {
	var (
		backend        = s3Context.backend
		completedParts []types.CompletedPart
		createOutput   *s3.CreateMultipartUploadOutput
		fullFilePath   = backend.prefix + inode.objectPath
		partCount      uint64
		partSize       = backend.uploadPartCacheLines * globals.config.cacheLineSize
		uploadID       string
	)

	if !inode.writeStateActive {
		return nil, errors.New("missing write state")
	}
	if inode.sizeInMemory == 0 {
		return s3Context.writeFile(&writeFileInputStruct{
			filePath: inode.objectPath,
			ifMatch:  inode.eTag,
			body:     bytes.NewReader(nil),
			readerAt: bytes.NewReader(nil),
			size:     0,
		})
	}
	partSize, partCount, err = s3MultipartLayout(inode.sizeInMemory, partSize)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", fullFilePath, err)
	}
	completedParts = make([]types.CompletedPart, 0, partCount)

	// Merged once here rather than per part: segments are appended one per FUSE
	// write and never coalesced, so this list can be long, while the merge of a
	// sequential rewrite collapses to a single range.
	dirtyRanges := inode.writeState.mergedDirtyRanges()

	createInput := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	}
	createOutput, err = s3Context.s3Client.CreateMultipartUpload(context.Background(), createInput)
	if err != nil {
		return nil, err
	}
	uploadID = aws.ToString(createOutput.UploadId)

	defer func() {
		if err != nil && uploadID != "" {
			_, _ = s3Context.s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(backend.bucketContainerName),
				Key:      aws.String(fullFilePath),
				UploadId: aws.String(uploadID),
			})
		}
	}()

	for partIndex := range partCount {
		offset := partIndex * partSize
		length := partSize
		if offset+length > inode.sizeInMemory {
			length = inode.sizeInMemory - offset
		}
		partNumber := int32(partIndex + 1)

		if !inode.writeState.truncateAtOpen && offset+length <= inode.sizeInBackend && !inode.writeState.hasDirtyOverlap(offset, length) {
			copyOutput, copyErr := s3Context.s3Client.UploadPartCopy(context.Background(), &s3.UploadPartCopyInput{
				Bucket:     aws.String(backend.bucketContainerName),
				Key:        aws.String(fullFilePath),
				UploadId:   aws.String(uploadID),
				PartNumber: aws.Int32(partNumber),
				// QueryEscape would emit "+" for a space, which S3 percent-decodes
				// back to "+", and would escape the bucket/key separator.
				CopySource:      aws.String((&url.URL{Path: backend.bucketContainerName + "/" + fullFilePath}).EscapedPath()),
				CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
			})
			if copyErr != nil {
				err = copyErr
				return nil, err
			}
			completedParts = append(completedParts, types.CompletedPart{
				ETag:       copyOutput.CopyPartResult.ETag,
				PartNumber: aws.Int32(partNumber),
			})
			continue
		}

		partData, partErr := s3Context.assembleOverlayPart(inode, offset, length, dirtyRanges)
		if partErr != nil {
			err = partErr
			return nil, err
		}
		uploadOutput, uploadErr := s3Context.s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
			Bucket:        aws.String(backend.bucketContainerName),
			Key:           aws.String(fullFilePath),
			UploadId:      aws.String(uploadID),
			PartNumber:    aws.Int32(partNumber),
			Body:          bytes.NewReader(partData),
			ContentLength: aws.Int64(int64(len(partData))),
		})
		if uploadErr != nil {
			err = uploadErr
			return nil, err
		}
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadOutput.ETag,
			PartNumber: aws.Int32(partNumber),
		})
	}

	completeOutput, err := s3Context.s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(backend.bucketContainerName),
		Key:      aws.String(fullFilePath),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	if err != nil {
		return nil, err
	}

	writeFileOutput = &writeFileOutputStruct{
		size:  inode.sizeInMemory,
		mTime: time.Now(),
	}
	if completeOutput.ETag != nil {
		writeFileOutput.eTag = trimS3ETag(*completeOutput.ETag)
	}
	return writeFileOutput, nil
}

func (s3Context *s3ContextStruct) assembleOverlayPart(inode *inodeStruct, offset, length uint64, dirtyRanges []writeRange) ([]byte, error) {
	partData := make([]byte, length)
	if !inode.writeState.truncateAtOpen && offset < inode.sizeInBackend {
		readLimit := offset + length
		if readLimit > inode.sizeInBackend {
			readLimit = inode.sizeInBackend
		}
		// Bytes the overlay below rewrites in full need not be fetched first. A
		// whole-object rewrite hits this for every part, which is where the read
		// would otherwise cost one part-sized GET per part, all discarded.
		if readLimit > offset && !rangesCover(dirtyRanges, offset, readLimit-offset) {
			baseBytes, err := s3Context.readRange(inode.objectPath, offset, readLimit-offset, inode.eTag)
			if err != nil {
				return nil, err
			}
			copy(partData, baseBytes)
		}
	}
	for _, segment := range inode.writeState.segments {
		overlayBytes(partData, offset, segment)
	}
	return partData, nil
}

func (s3Context *s3ContextStruct) readRange(filePath string, offset, length uint64, ifMatch string) ([]byte, error) {
	if length == 0 {
		return nil, nil
	}
	getInput := &s3.GetObjectInput{
		Bucket: aws.String(s3Context.backend.bucketContainerName),
		Key:    aws.String(s3Context.backend.prefix + filePath),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)),
	}
	if ifMatch != "" {
		getInput.IfMatch = aws.String(ifMatch)
	}
	getOutput, err := s3Context.s3Client.GetObject(context.Background(), getInput)
	if err != nil {
		return nil, err
	}
	defer getOutput.Body.Close()
	return io.ReadAll(getOutput.Body)
}

func (stream *s3WriteStream) init(backend *backendStruct, filePath string) error {
	var (
		concurrency uint64
		createOut   *s3.CreateMultipartUploadOutput
		err         error
		partSize    uint64
		s3Context   *s3ContextStruct
	)

	s3Context, ok := backend.context.(*s3ContextStruct)
	if !ok {
		return errors.New("backend is not S3")
	}

	partSize = backend.uploadPartCacheLines * globals.config.cacheLineSize
	if partSize < s3MinPartSize {
		partSize = s3MinPartSize
	}
	concurrency = backend.uploadPartConcurrency
	if concurrency == 0 {
		concurrency = 1
	}

	fullFilePath := backend.prefix + filePath
	createOut, err = s3Context.s3Client.CreateMultipartUpload(context.Background(), &s3.CreateMultipartUploadInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	})
	if err != nil {
		return err
	}

	stream.s3Context = s3Context
	stream.fullFilePath = fullFilePath
	stream.uploadID = aws.ToString(createOut.UploadId)
	stream.partSize = partSize
	stream.nextPartNumber = 1
	stream.completedParts = nil
	stream.err = nil

	// A goroutine per part made the goroutine count track the part count, which
	// is bounded only by the object size. A fixed pool bounds it at the
	// configured concurrency instead.
	//
	// The queue is sized to the largest part count an upload may legally have, so
	// in practice queuePartNumber never has to wait -- which matters because it
	// runs under globals.Lock, where blocking would stall every mount.
	stream.jobs = make(chan s3StreamPart, s3MaxPartCount)
	stream.queueClosed = false
	for range concurrency {
		stream.wg.Add(1)
		go func() {
			defer stream.wg.Done()
			for job := range stream.jobs {
				stream.uploadPart(job.partNumber, job.data)
			}
		}()
	}
	return nil
}

func (stream *s3WriteStream) queuePart(data []byte) {
	partNumber := stream.nextPartNumber
	stream.nextPartNumber++
	stream.queuePartNumber(partNumber, data)
}

func (stream *s3WriteStream) queuePartNumber(partNumber int32, data []byte) {
	if stream.queueClosed {
		stream.setErr(fmt.Errorf("part %d queued after the multipart stream stopped accepting parts", partNumber))
		return
	}
	select {
	case stream.jobs <- s3StreamPart{partNumber: partNumber, data: data}:
	default:
		// Never wait here. This runs under globals.Lock, so blocking would freeze
		// every mount, and a full queue means the part count already passed
		// s3MaxPartCount, which CompleteMultipartUpload would reject regardless.
		stream.setErr(fmt.Errorf("multipart stream exceeded %d queued parts", uint64(s3MaxPartCount)))
	}
}

func (stream *s3WriteStream) uploadPart(partNumber int32, data []byte) {
	uploadOut, err := stream.s3Context.s3Client.UploadPart(context.Background(), &s3.UploadPartInput{
		Bucket:        aws.String(stream.s3Context.backend.bucketContainerName),
		Key:           aws.String(stream.fullFilePath),
		UploadId:      aws.String(stream.uploadID),
		PartNumber:    aws.Int32(partNumber),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		stream.setErr(err)
		return
	}

	stream.completedPartsM.Lock()
	stream.completedParts = append(stream.completedParts, types.CompletedPart{
		ETag:       uploadOut.ETag,
		PartNumber: aws.Int32(partNumber),
	})
	stream.completedPartsM.Unlock()
}

// drainQueue stops accepting parts and waits for the pool to finish. Idempotent,
// because complete calls abort on failure and abort drains again.
func (stream *s3WriteStream) drainQueue() {
	if !stream.queueClosed {
		stream.queueClosed = true
		close(stream.jobs)
	}
	stream.wg.Wait()
}

func (stream *s3WriteStream) complete(finalPart []byte, size uint64) (*writeFileOutputStruct, error) {
	if len(finalPart) > 0 {
		stream.queuePart(finalPart)
	}
	stream.drainQueue()
	if err := stream.getErr(); err != nil {
		_ = stream.abort()
		return nil, err
	}

	sort.Slice(stream.completedParts, func(i, j int) bool {
		return aws.ToInt32(stream.completedParts[i].PartNumber) < aws.ToInt32(stream.completedParts[j].PartNumber)
	})

	completeOut, err := stream.s3Context.s3Client.CompleteMultipartUpload(context.Background(), &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(stream.s3Context.backend.bucketContainerName),
		Key:      aws.String(stream.fullFilePath),
		UploadId: aws.String(stream.uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: stream.completedParts,
		},
	})
	if err != nil {
		_ = stream.abort()
		return nil, err
	}

	output := &writeFileOutputStruct{
		size:  size,
		mTime: time.Now(),
	}
	if completeOut.ETag != nil {
		output.eTag = trimS3ETag(*completeOut.ETag)
	}
	return output, nil
}

func (stream *s3WriteStream) abort() error {
	stream.drainQueue()
	_, err := stream.s3Context.s3Client.AbortMultipartUpload(context.Background(), &s3.AbortMultipartUploadInput{
		Bucket:   aws.String(stream.s3Context.backend.bucketContainerName),
		Key:      aws.String(stream.fullFilePath),
		UploadId: aws.String(stream.uploadID),
	})
	return err
}

func (stream *s3WriteStream) setErr(err error) {
	stream.errMu.Lock()
	if stream.err == nil {
		stream.err = err
	}
	stream.errMu.Unlock()
}

func (stream *s3WriteStream) getErr() error {
	stream.errMu.Lock()
	defer stream.errMu.Unlock()
	return stream.err
}
