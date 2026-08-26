// SPDX-FileCopyrightText: Copyright (c) 2025 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"os"
	"testing"
)

const defaultMultipartUploadThreshold = uint64(67108864) // 64 MiB
const defaultWriteDeferralMaxBytes = uint64(1073741824)  // 1 GiB
const defaultWriteCommitWorkers = uint64(32)
const defaultWriteCommitQueueDepth = uint64(256)

// TestMultipartUploadThresholdConfigDefaults verifies the deferred-multipart
// knobs default correctly when omitted from the config.
func TestMultipartUploadThresholdConfigDefaults(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: s3,
    bucket_container_name: test,
    backend_type: S3,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	backend, ok := globals.backendsToMount["s3"]
	if !ok {
		t.Fatalf("expected backend \"s3\" in backendsToMount")
	}
	if backend.multipartUploadThreshold != defaultMultipartUploadThreshold {
		t.Fatalf("multipartUploadThreshold default = %d, expected %d", backend.multipartUploadThreshold, defaultMultipartUploadThreshold)
	}
	if globals.config.writeDeferralMaxBytes != defaultWriteDeferralMaxBytes {
		t.Fatalf("writeDeferralMaxBytes default = %d, expected %d", globals.config.writeDeferralMaxBytes, defaultWriteDeferralMaxBytes)
	}
	if globals.config.writeCommitWorkers != defaultWriteCommitWorkers {
		t.Fatalf("writeCommitWorkers default = %d, expected %d", globals.config.writeCommitWorkers, defaultWriteCommitWorkers)
	}
	if globals.config.writeCommitQueueDepth != defaultWriteCommitQueueDepth {
		t.Fatalf("writeCommitQueueDepth default = %d, expected %d", globals.config.writeCommitQueueDepth, defaultWriteCommitQueueDepth)
	}
	if globals.config.writeCachePromotion {
		t.Fatal("writeCachePromotion default = true, expected false")
	}
}

// TestMultipartUploadThresholdConfigExplicit verifies that explicit values for
// the deferred-multipart knobs are parsed.
func TestMultipartUploadThresholdConfigExplicit(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
write_deferral_max_bytes: 2147483648
write_commit_workers: 8
write_commit_queue_depth: 64
write_cache_promotion: true
backends: [
  {
    dir_name: s3,
    bucket_container_name: test,
    backend_type: S3,
    multipart_upload_threshold_bytes: 33554432,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	backend, ok := globals.backendsToMount["s3"]
	if !ok {
		t.Fatalf("expected backend \"s3\" in backendsToMount")
	}
	if backend.multipartUploadThreshold != uint64(33554432) {
		t.Fatalf("multipartUploadThreshold = %d, expected 33554432", backend.multipartUploadThreshold)
	}
	if globals.config.writeDeferralMaxBytes != uint64(2147483648) {
		t.Fatalf("writeDeferralMaxBytes = %d, expected 2147483648", globals.config.writeDeferralMaxBytes)
	}
	if globals.config.writeCommitWorkers != 8 {
		t.Fatalf("writeCommitWorkers = %d, expected 8", globals.config.writeCommitWorkers)
	}
	if globals.config.writeCommitQueueDepth != 64 {
		t.Fatalf("writeCommitQueueDepth = %d, expected 64", globals.config.writeCommitQueueDepth)
	}
	if !globals.config.writeCachePromotion {
		t.Fatal("writeCachePromotion = false, expected true")
	}
}

func TestWriteCommitConfigRejectsZero(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))
	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
write_commit_workers: 0
write_commit_queue_depth: 0
backends: [
  {
    dir_name: ram,
    bucket_container_name: ignored,
    backend_type: RAM,
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}
	if err = checkConfigFile(); err == nil {
		t.Fatal("checkConfigFile() unexpectedly accepted zero write commit limits")
	}
}

// TestMultipartUploadThresholdReloadImmutable verifies that the per-backend
// threshold cannot be changed via a SIGHUP-style config reload.
func TestMultipartUploadThresholdReloadImmutable(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: s3,
    bucket_container_name: test,
    backend_type: S3,
    multipart_upload_threshold_bytes: 33554432,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	activateBackendsToMountForTest()

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: s3,
    bucket_container_name: test,
    backend_type: S3,
    multipart_upload_threshold_bytes: 16777216,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly allowed changing multipart_upload_threshold_bytes via reload")
	}
}

// TestShouldPromoteDeferred verifies the deferred-to-multipart promotion
// decision by per-object threshold and by the global deferral budget.
func TestShouldPromoteDeferred(t *testing.T) {
	savedConfig := globals.config
	savedDeferred := globals.deferredWriteBytes
	defer func() {
		globals.config = savedConfig
		globals.deferredWriteBytes = savedDeferred
	}()

	globals.config = &configStruct{writeDeferralMaxBytes: 0}
	globals.deferredWriteBytes = 0
	backend := &backendStruct{multipartUploadThreshold: 64 << 20}
	inode := &inodeStruct{}

	inode.sizeInMemory = 1 << 20
	if inode.shouldPromoteDeferredLocked(backend) {
		t.Fatalf("1 MiB < 64 MiB threshold should not promote")
	}

	inode.sizeInMemory = 64 << 20
	if !inode.shouldPromoteDeferredLocked(backend) {
		t.Fatalf("64 MiB >= 64 MiB threshold should promote")
	}

	// threshold 0 promotes immediately (deferral disabled).
	zeroBackend := &backendStruct{multipartUploadThreshold: 0}
	inode.sizeInMemory = 1
	if !inode.shouldPromoteDeferredLocked(zeroBackend) {
		t.Fatalf("threshold 0 should promote on first byte")
	}

	// Global deferral budget forces promotion below the per-object threshold.
	globals.config = &configStruct{writeDeferralMaxBytes: 10 << 20}
	globals.deferredWriteBytes = 9 << 20
	inode = &inodeStruct{}
	inode.sizeInMemory = 2 << 20 // projected 11 MiB > 10 MiB budget
	if !inode.shouldPromoteDeferredLocked(backend) {
		t.Fatalf("exceeding global deferral budget should promote")
	}

	// A budget of 0 disables the global cap.
	globals.config = &configStruct{writeDeferralMaxBytes: 0}
	globals.deferredWriteBytes = 1 << 30
	inode = &inodeStruct{}
	inode.sizeInMemory = 1 << 20
	if inode.shouldPromoteDeferredLocked(backend) {
		t.Fatalf("global budget 0 should not force promotion below per-object threshold")
	}
}

// TestDeferredWriteAccounting verifies the global deferred-byte counter is kept
// in sync and that clearing is idempotent.
func TestDeferredWriteAccounting(t *testing.T) {
	savedDeferred := globals.deferredWriteBytes
	defer func() { globals.deferredWriteBytes = savedDeferred }()

	globals.deferredWriteBytes = 0
	inode := &inodeStruct{}

	inode.setDeferredBytesLocked(100)
	if globals.deferredWriteBytes != 100 || inode.writeState.deferredBytesCounted != 100 {
		t.Fatalf("after set(100): global=%d counted=%d", globals.deferredWriteBytes, inode.writeState.deferredBytesCounted)
	}

	inode.setDeferredBytesLocked(250)
	if globals.deferredWriteBytes != 250 || inode.writeState.deferredBytesCounted != 250 {
		t.Fatalf("after set(250): global=%d counted=%d", globals.deferredWriteBytes, inode.writeState.deferredBytesCounted)
	}

	inode.clearDeferredAccountingLocked()
	if globals.deferredWriteBytes != 0 || inode.writeState.deferredBytesCounted != 0 {
		t.Fatalf("after clear: global=%d counted=%d", globals.deferredWriteBytes, inode.writeState.deferredBytesCounted)
	}

	inode.clearDeferredAccountingLocked()
	if globals.deferredWriteBytes != 0 {
		t.Fatalf("clear should be idempotent: global=%d", globals.deferredWriteBytes)
	}
}
