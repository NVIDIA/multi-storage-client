package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// `s3ContextStruct` holds the S3-specific backend details.
type s3ContextStruct struct {
	backend  *backendStruct
	s3Client *s3.Client
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

	if backendS3.useCredentialsEnv {
		configOptions = append(configOptions, config.WithSharedCredentialsFiles(([]string{backendS3.credentialsFilePath})))
	} else {
		configOptions = append(configOptions, config.WithSharedCredentialsFiles(nil), config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     backendS3.accessKeyID,
				SecretAccessKey: backendS3.secretAccessKey,
			}}))
	}

	if backendS3.skipTLSCertificateVerify {
		configOptions = append(configOptions, config.WithHTTPClient(awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			if t.TLSClientConfig == nil {
				t.TLSClientConfig = &tls.Config{}
			}
			t.TLSClientConfig.InsecureSkipVerify = true
			t.TLSClientConfig.MinVersion = tls.VersionTLS12
		})))
	}

	configOptions = append(configOptions, config.WithRetryer(func() aws.Retryer {
		return backend
	}))

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

	if backend.prefix != "" {
		backendPathParsed.Path += "/" + backend.prefix
	}

	backend.backendPath = backendPathParsed.String() + "/"

	backend.context = &s3ContextStruct{
		backend: backend,
		s3Client: s3.NewFromConfig(s3Config, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
			o.UsePathStyle = !backendS3.virtualHostedStyleRequest
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
	// Observability: Record request at START (matches Python line 209)
	recordRequest(s3Context.backend.dirName, "delete")

	// Observability: Record latency/response at END (matches Python lines 235-272)
	startTime := time.Now()
	defer func() {
		recordBackendMetrics(s3Context.backend.dirName, "delete", startTime, err, 0)
	}()

	var (
		backend             = s3Context.backend
		fullFilePath        = backend.prefix + deleteFileInput.filePath
		s3DeleteObjectInput *s3.DeleteObjectInput
		s3HeadObjectInput   *s3.HeadObjectInput
		s3HeadObjectOutput  *s3.HeadObjectOutput
	)

	defer func() {
		switch s3Context.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", s3Context.backend.dirName, deleteFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.deleteFile(%#v) succeeded", s3Context.backend.dirName, deleteFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning err: %v", s3Context.backend.dirName, deleteFileInput, err)
			}
		}
	}()

	// Note: .IfMatch not necessarily supported, so we must (also) do the non-atomic manual ETag comparison check

	s3HeadObjectInput = &s3.HeadObjectInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	}
	if deleteFileInput.ifMatch != "" {
		s3HeadObjectInput.IfMatch = aws.String(deleteFileInput.ifMatch)
	}

	s3HeadObjectOutput, err = s3Context.s3Client.HeadObject(context.Background(), s3HeadObjectInput)
	if err != nil {
		return
	}
	if deleteFileInput.ifMatch != "" {
		if s3HeadObjectOutput.ETag != nil {
			if deleteFileInput.ifMatch != strings.TrimLeft(strings.TrimRight(*s3HeadObjectOutput.ETag, "\""), "\"") {
				err = errors.New("eTag mismatch")
				return
			}
		}
	}

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
// indicates the `directory` has been completely enumerated. An error is returned if either the
// specified path is not a `directory` or non-existent.
func (s3Context *s3ContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
	// Observability: Record request at START (matches Python line 209)
	recordRequest(s3Context.backend.dirName, "list")

	// Observability: Record latency/response at END (matches Python lines 235-272)
	startTime := time.Now()
	defer func() {
		recordBackendMetrics(s3Context.backend.dirName, "list", startTime, err, 0)
	}()

	var (
		backend               = s3Context.backend
		fullDirPath           = backend.prefix + listDirectoryInput.dirPath
		s3CommonPrefix        types.CommonPrefix
		s3ListObjectsV2Input  *s3.ListObjectsV2Input
		s3ListObjectsV2Output *s3.ListObjectsV2Output
		s3Object              types.Object
	)

	defer func() {
		switch s3Context.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", s3Context.backend.dirName, listDirectoryInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.listDirectory(%#v) succeeded", s3Context.backend.dirName, listDirectoryInput)
			} else {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", s3Context.backend.dirName, listDirectoryInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.listDirectory(%#v) returning deleteFileOutput: %#v", s3Context.backend.dirName, listDirectoryInput, listDirectoryOutput)
			} else {
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning err: %v", s3Context.backend.dirName, listDirectoryInput, err)
			}
		}
	}()

	s3ListObjectsV2Input = &s3.ListObjectsV2Input{
		Bucket:    aws.String(backend.bucketContainerName),
		Prefix:    aws.String(fullDirPath),
		Delimiter: aws.String("/"),
	}
	if listDirectoryInput.continuationToken != "" {
		s3ListObjectsV2Input.ContinuationToken = aws.String(listDirectoryInput.continuationToken)
	}
	if listDirectoryInput.maxItems != 0 {
		s3ListObjectsV2Input.MaxKeys = aws.Int32(int32(listDirectoryInput.maxItems))
	}

	s3ListObjectsV2Output, err = s3Context.s3Client.ListObjectsV2(context.Background(), s3ListObjectsV2Input)
	if err == nil {
		listDirectoryOutput = &listDirectoryOutputStruct{
			subdirectory: make([]string, 0, len(s3ListObjectsV2Output.CommonPrefixes)),
			file:         make([]listDirectoryOutputFileStruct, 0, len(s3ListObjectsV2Output.Contents)),
		}

		if s3ListObjectsV2Output.NextContinuationToken == nil {
			listDirectoryOutput.nextContinuationToken = ""
		} else {
			listDirectoryOutput.nextContinuationToken = *s3ListObjectsV2Output.NextContinuationToken
		}

		if s3ListObjectsV2Output.IsTruncated == nil {
			listDirectoryOutput.isTruncated = false
		} else {
			listDirectoryOutput.isTruncated = *s3ListObjectsV2Output.IsTruncated
		}

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
	}

	return
}

// `readFile` is called to read a range of a `file` at the specified path.
// An error is returned if either the specified path is not a `file` or non-existent.
func (s3Context *s3ContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
	// Observability: Record request at START (matches Python functionality)
	recordRequest(s3Context.backend.dirName, "read")

	// Observability: Record latency/response at END (matches Python functionality)
	startTime := time.Now()
	defer func() {
		bytesRead := int64(0)
		if err == nil && readFileOutput != nil {
			bytesRead = int64(len(readFileOutput.buf))
		}
		recordBackendMetrics(s3Context.backend.dirName, "read", startTime, err, bytesRead)
	}()

	var (
		backend            = s3Context.backend
		fullFilePath       = backend.prefix + readFileInput.filePath
		rangeBegin         = readFileInput.offsetCacheLine * globals.config.cacheLineSize
		rangeEnd           = rangeBegin + globals.config.cacheLineSize - 1
		s3GetObjectInput   *s3.GetObjectInput
		s3GetObjectOutput  *s3.GetObjectOutput
		s3HeadObjectInput  *s3.HeadObjectInput
		s3HeadObjectOutput *s3.HeadObjectOutput
	)

	defer func() {
		switch s3Context.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", s3Context.backend.dirName, readFileInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.readFile(%#v) succeeded", s3Context.backend.dirName, readFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", s3Context.backend.dirName, readFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.readFile(%#v) returning deleteFileOutput: {\"eTag\":\"%s\",len(\"buf\":%v)}", s3Context.backend.dirName, readFileInput, readFileOutput.eTag, len(readFileOutput.buf))
			} else {
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning err: %v", s3Context.backend.dirName, readFileInput, err)
			}
		}
	}()

	// Note: .IfMatch not necessarily supported, so we must (also) do the non-atomic manual ETag comparison check

	s3HeadObjectInput = &s3.HeadObjectInput{
		Bucket: aws.String(backend.bucketContainerName),
		Key:    aws.String(fullFilePath),
	}
	if readFileInput.ifMatch != "" {
		s3HeadObjectInput.IfMatch = aws.String(readFileInput.ifMatch)
	}

	s3HeadObjectOutput, err = s3Context.s3Client.HeadObject(context.Background(), s3HeadObjectInput)
	if err != nil {
		return
	}
	if readFileInput.ifMatch != "" {
		if s3HeadObjectOutput.ETag != nil {
			if readFileInput.ifMatch != strings.TrimLeft(strings.TrimRight(*s3HeadObjectOutput.ETag, "\""), "\"") {
				err = errors.New("eTag mismatch")
				return
			}
		}
	}

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
	// Observability: Record request at START (matches Python line 209)
	recordRequest(s3Context.backend.dirName, "info")

	// Observability: Record latency/response at END (matches Python lines 235-272)
	startTime := time.Now()
	defer func() {
		recordBackendMetrics(s3Context.backend.dirName, "info", startTime, err, 0)
	}()

	var (
		backend               = s3Context.backend
		fullDirPath           = backend.prefix + statDirectoryInput.dirPath
		s3ListObjectsV2Input  *s3.ListObjectsV2Input
		s3ListObjectsV2Output *s3.ListObjectsV2Output
	)

	defer func() {
		switch s3Context.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", s3Context.backend.dirName, statDirectoryInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statDirectory(%#v) succeeded", s3Context.backend.dirName, statDirectoryInput)
			} else {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", s3Context.backend.dirName, statDirectoryInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statDirectory(%#v) returning deleteFileOutput: %#v", s3Context.backend.dirName, statDirectoryInput, statDirectoryOutput)
			} else {
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning err: %v", s3Context.backend.dirName, statDirectoryInput, err)
			}
		}
	}()

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
	// Observability: Record request at START (matches Python line 209)
	recordRequest(s3Context.backend.dirName, "info")

	// Observability: Record latency/response at END (matches Python lines 235-272)
	startTime := time.Now()
	defer func() {
		bytesReported := int64(0)
		if err == nil && statFileOutput != nil {
			bytesReported = int64(statFileOutput.size)
		}
		recordBackendMetrics(s3Context.backend.dirName, "info", startTime, err, bytesReported)
	}()

	var (
		backend            = s3Context.backend
		fullFilePath       = backend.prefix + statFileInput.filePath
		s3HeadObjectInput  *s3.HeadObjectInput
		s3HeadObjectOutput *s3.HeadObjectOutput
	)

	defer func() {
		switch s3Context.backend.traceLevel {
		case 0:
			// Trace nothing
		case 1:
			if err != nil {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", s3Context.backend.dirName, statFileInput, err)
			}
		case 2:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statFile(%#v) succeeded", s3Context.backend.dirName, statFileInput)
			} else {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", s3Context.backend.dirName, statFileInput, err)
			}
		default:
			if err == nil {
				globals.logger.Printf("[INFO] %s.statFile(%#v) returning deleteFileOutput: %#v", s3Context.backend.dirName, statFileInput, statFileOutput)
			} else {
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning err: %v", s3Context.backend.dirName, statFileInput, err)
			}
		}
	}()

	// Note: .IfMatch not necessarily supported, so we must (also) do the non-atomic manual ETag comparison check

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
	if statFileInput.ifMatch != "" {
		if s3HeadObjectOutput.ETag != nil {
			if statFileInput.ifMatch != strings.TrimLeft(strings.TrimRight(*s3HeadObjectOutput.ETag, "\""), "\"") {
				err = errors.New("eTag mismatch")
				return
			}
		}
	}

	statFileOutput = &statFileOutputStruct{
		eTag:  strings.TrimLeft(strings.TrimRight(*s3HeadObjectOutput.ETag, "\""), "\""),
		mTime: *s3HeadObjectOutput.LastModified,
		size:  uint64(*s3HeadObjectOutput.ContentLength),
	}

	return
}
