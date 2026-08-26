package main

import (
	"testing"
)

// The upload decision derives from MSFS configuration alone, so it can name a
// size the service will not accept. These cases pin the reconciliation.
func TestS3UseSinglePut(t *testing.T) {
	const (
		defaultThreshold = 512 * 10 * 1024 * 1024 // multipart_cache_line_threshold 512 x 10 MiB cache line
		raisedThreshold  = 512 * 20 * 1024 * 1024 // the same 512 against a 20 MiB cache line
	)

	if defaultThreshold != uint64(s3MaxSinglePutSize) {
		t.Fatalf("the default threshold is %d, expected it to coincide with the %d byte PutObject limit",
			uint64(defaultThreshold), uint64(s3MaxSinglePutSize))
	}

	for _, testCase := range []struct {
		name           string
		size           uint64
		threshold      uint64
		forceSinglePut bool
		singlePut      bool
	}{
		{name: "below threshold", size: 1 << 20, threshold: defaultThreshold, singlePut: true},
		{name: "at threshold and at the service limit", size: defaultThreshold, threshold: defaultThreshold, singlePut: true},
		{name: "above threshold", size: defaultThreshold + 1, threshold: defaultThreshold, singlePut: false},

		// Each of these reached PutObject with an oversized body before the clamp.
		{name: "raised threshold above the service limit", size: 7 << 30, threshold: raisedThreshold, singlePut: false},
		{name: "multipart disabled by a zero threshold", size: 7 << 30, threshold: 0, singlePut: false},
		{name: "forceSinglePut above the service limit", size: 7 << 30, threshold: defaultThreshold, forceSinglePut: true, singlePut: false},

		// The clamp must not disturb anything at or below the limit.
		{name: "forceSinglePut below the service limit", size: 64 << 20, threshold: defaultThreshold, forceSinglePut: true, singlePut: true},
		{name: "zero threshold below the service limit", size: 64 << 20, threshold: 0, singlePut: true},
		{name: "exactly at the service limit with multipart disabled", size: s3MaxSinglePutSize, threshold: 0, singlePut: true},
		{name: "one byte over the service limit with multipart disabled", size: s3MaxSinglePutSize + 1, threshold: 0, singlePut: false},
		{name: "empty object", size: 0, threshold: defaultThreshold, singlePut: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			singlePut := s3UseSinglePut(testCase.size, testCase.threshold, testCase.forceSinglePut)
			if singlePut != testCase.singlePut {
				t.Fatalf("s3UseSinglePut(%d, %d, %v) = %v, expected %v",
					testCase.size, testCase.threshold, testCase.forceSinglePut, singlePut, testCase.singlePut)
			}
		})
	}
}

// Both the part floor and the part-count ceiling are reachable from
// configuration, and each one fails only at CompleteMultipartUpload, after every
// part has already been uploaded.
func TestS3MultipartLayout(t *testing.T) {
	for _, testCase := range []struct {
		name          string
		size          uint64
		configured    uint64
		wantPartSize  uint64
		wantPartCount uint64
		wantErr       bool
	}{
		{
			name: "configured part size is honored", size: 100 << 20, configured: 10 << 20,
			wantPartSize: 10 << 20, wantPartCount: 10,
		},
		{
			// A cache_line_size below 5 MiB produced undersized parts and
			// EntityTooSmall after the whole upload.
			name: "part size raised to the service minimum", size: 100 << 20, configured: 256 << 10,
			wantPartSize: s3MinPartSize, wantPartCount: 20,
		},
		{
			// 100 GiB at 10 MiB parts is 10,240 parts, past the 10,000 ceiling.
			// The replacement part size must round up, or the count creeps back
			// over the ceiling by one.
			name: "part size raised to respect the part ceiling", size: 100 << 30, configured: 10 << 20,
			wantPartSize: ((100 << 30) + s3MaxPartCount - 1) / s3MaxPartCount, wantPartCount: s3MaxPartCount,
		},
		{
			name: "part count never exceeds the ceiling for a huge object", size: 4 << 40, configured: 10 << 20,
			wantPartSize: ((4 << 40) + s3MaxPartCount - 1) / s3MaxPartCount, wantPartCount: s3MaxPartCount,
		},
		{
			// Above 5 TiB no legal part size exists.
			name: "object beyond the maximum size is refused", size: uint64(s3MaxPartSize)*s3MaxPartCount + 1,
			configured: 10 << 20, wantErr: true,
		},
		{
			name: "empty object still yields one part", size: 0, configured: 10 << 20,
			wantPartSize: 10 << 20, wantPartCount: 1,
		},
		{
			name: "a partial trailing part is counted", size: (10 << 20) + 1, configured: 10 << 20,
			wantPartSize: 10 << 20, wantPartCount: 2,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			partSize, partCount, err := s3MultipartLayout(testCase.size, testCase.configured)
			if testCase.wantErr {
				if err == nil {
					t.Fatalf("s3MultipartLayout(%d, %d) unexpectedly succeeded with partSize %d, partCount %d",
						testCase.size, testCase.configured, partSize, partCount)
				}
				return
			}
			if err != nil {
				t.Fatalf("s3MultipartLayout(%d, %d) unexpectedly failed: %v", testCase.size, testCase.configured, err)
			}
			if partSize != testCase.wantPartSize {
				t.Fatalf("s3MultipartLayout(%d, %d) partSize = %d, expected %d",
					testCase.size, testCase.configured, partSize, testCase.wantPartSize)
			}
			if partCount != testCase.wantPartCount {
				t.Fatalf("s3MultipartLayout(%d, %d) partCount = %d, expected %d",
					testCase.size, testCase.configured, partCount, testCase.wantPartCount)
			}
			if partSize < s3MinPartSize {
				t.Fatalf("partSize %d is below the %d byte service minimum", partSize, uint64(s3MinPartSize))
			}
			if partCount > s3MaxPartCount {
				t.Fatalf("partCount %d exceeds the %d part ceiling", partCount, uint64(s3MaxPartCount))
			}
		})
	}
}
