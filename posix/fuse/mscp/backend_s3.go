package main

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type s3ContextStruct struct {
	backend  *backendStruct
	s3Client *s3.Client
}

func (backend *backendStruct) setupS3Context() (err error) {
	var (
		backendS3     = backend.backendTypeSpecifics.(*backendConfigS3Struct)
		s3Config      aws.Config
		s3Endpoint    string
		configOptions []func(*config.LoadOptions) error
	)

	if backendS3.allowHTTP {
		backend.backendPath = "http://"
	} else {
		backend.backendPath = "https://"
	}

	if backendS3.virtualHostedStyleRequest {
		backend.backendPath += backend.bucketContainerName + "."
	}

	backend.backendPath += backendS3.endpoint + "/"

	if !backendS3.virtualHostedStyleRequest {
		backend.backendPath += backend.bucketContainerName + "/"
	}

	backend.backendPath += backend.prefix

	// Build config options
	configOptions = []func(*config.LoadOptions) error{
		config.WithCredentialsProvider(credentials.StaticCredentialsProvider{
			Value: aws.Credentials{
				AccessKeyID:     backendS3.accessKeyID,
				SecretAccessKey: backendS3.secretAccessKey,
			},
		}),
		config.WithRegion(backendS3.region),
	}

	// Add custom retryer
	configOptions = append(configOptions, config.WithRetryer(func() aws.Retryer {
		return backend
	}))

	// Add custom HTTP client if TLS certificate verification should be skipped
	if backendS3.skipTLSCertificateVerify {
		customHTTPClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
			if t.TLSClientConfig == nil {
				t.TLSClientConfig = &tls.Config{}
			}
			t.TLSClientConfig.InsecureSkipVerify = true
			t.TLSClientConfig.MinVersion = tls.VersionTLS12
		})
		configOptions = append(configOptions, config.WithHTTPClient(customHTTPClient))
	}

	s3Config, err = config.LoadDefaultConfig(context.Background(), configOptions...)
	if err != nil {
		err = fmt.Errorf("[S3] config.LoadDefaultConfig() failed: %v", err)
		return
	}

	if backendS3.allowHTTP {
		s3Endpoint = "http://" + backendS3.endpoint
	} else {
		s3Endpoint = "https://" + backendS3.endpoint
	}

	backend.context = &s3ContextStruct{
		backend: backend,
		s3Client: s3.NewFromConfig(s3Config, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(s3Endpoint)
			o.UsePathStyle = !backendS3.virtualHostedStyleRequest
		}),
	}

	return
}

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

func (backend *backendStruct) MaxAttempts() int {
	return len(backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay) + 1
}

func (backend *backendStruct) RetryDelay(attempt int, _ error) (time.Duration, error) {
	if (attempt < 1) || (attempt > len(backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay)) {
		return time.Duration(0), fmt.Errorf("unexpected attempt: %v (should have been in [1:%v])", attempt, len(backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay))
	}

	return backend.backendTypeSpecifics.(*backendConfigS3Struct).retryDelay[attempt-1], nil
}

func (backend *backendStruct) GetRetryToken(ctx context.Context, opErr error) (releaseToken func(error) error, err error) {
	return func(error) error {
		return nil
	}, nil
}

func (backend *backendStruct) GetInitialToken() (releaseToken func(error) error) {
	return func(error) error {
		return nil
	}
}

func (backend *backendStruct) GetAttemptToken(context.Context) (func(error) error, error) {
	return func(error) error {
		return nil
	}, nil
}

func (s3Context *s3ContextStruct) deleteFile(deleteFileInput *deleteFileInputStruct) (deleteFileOutput *deleteFileOutputStruct, err error) {
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
				globals.logger.Printf("[WARN] %s.deleteFile(%#v) returning errno: %v", s3Context.backend.dirName, deleteFileInput, err)
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

func (s3Context *s3ContextStruct) listDirectory(listDirectoryInput *listDirectoryInputStruct) (listDirectoryOutput *listDirectoryOutputStruct, err error) {
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
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning errno: %v", s3Context.backend.dirName, listDirectoryInput, err)
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
				globals.logger.Printf("[WARN] %s.listDirectory(%#v) returning errno: %v", s3Context.backend.dirName, listDirectoryInput, err)
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

func (s3Context *s3ContextStruct) readFile(readFileInput *readFileInputStruct) (readFileOutput *readFileOutputStruct, err error) {
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
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning errno: %v", s3Context.backend.dirName, readFileInput, err)
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
				globals.logger.Printf("[WARN] %s.readFile(%#v) returning errno: %v", s3Context.backend.dirName, readFileInput, err)
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

func (s3Context *s3ContextStruct) statDirectory(statDirectoryInput *statDirectoryInputStruct) (statDirectoryOutput *statDirectoryOutputStruct, err error) {
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
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning errno: %v", s3Context.backend.dirName, statDirectoryInput, err)
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
				globals.logger.Printf("[WARN] %s.statDirectory(%#v) returning errno: %v", s3Context.backend.dirName, statDirectoryInput, err)
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

func (s3Context *s3ContextStruct) statFile(statFileInput *statFileInputStruct) (statFileOutput *statFileOutputStruct, err error) {
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
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning errno: %v", s3Context.backend.dirName, statFileInput, err)
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
				globals.logger.Printf("[WARN] %s.statFile(%#v) returning errno: %v", s3Context.backend.dirName, statFileInput, err)
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
