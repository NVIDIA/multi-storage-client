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
	"strings"
	"testing"
)

func activateBackendsToMountForTest() {
	for dirName, backend := range globals.backendsToMount {
		delete(globals.backendsToMount, dirName)
		globals.config.backends[dirName] = backend
	}
}

// TestObservabilityConfigParsing verifies that observability config is parsed correctly
// as an add-on to existing MSFS config without breaking anything.
func TestObservabilityConfigParsing(t *testing.T) {
	var (
		err error
	)

	// Use the existing dev config which has opentelemetry section
	initGlobals(testOsArgs("msc_config_dev.yaml"))

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	// Verify observability config was parsed
	if globals.config.observability == nil {
		t.Fatal("Expected observability config to be parsed from opentelemetry section, got nil")
	}

	obs := globals.config.observability

	// Verify metrics exporter
	if obs.metricsExporter == nil {
		t.Fatal("Expected metrics exporter to be configured, got nil")
	}
	if obs.metricsExporter.Type != "otlp" {
		t.Errorf("Expected exporter type 'otlp', got '%s'", obs.metricsExporter.Type)
	}
	if endpoint, ok := obs.metricsExporter.Options["endpoint"].(string); !ok || endpoint != "otel-collector:4318" {
		t.Errorf("Expected endpoint 'otel-collector:4318', got '%v'", obs.metricsExporter.Options["endpoint"])
	}

	// Verify metrics reader options
	if obs.metricsReaderOptions == nil {
		t.Fatal("Expected metrics reader options to be configured, got nil")
	}
	if obs.metricsReaderOptions.CollectIntervalMillis != 1000 {
		t.Errorf("Expected collect_interval_millis=1000, got %d", obs.metricsReaderOptions.CollectIntervalMillis)
	}
	if obs.metricsReaderOptions.ExportIntervalMillis != 60000 {
		t.Errorf("Expected export_interval_millis=60000, got %d", obs.metricsReaderOptions.ExportIntervalMillis)
	}

	// Verify metrics attributes (should have 5 providers)
	if len(obs.metricsAttributes) != 5 {
		t.Errorf("Expected 5 attribute providers, got %d", len(obs.metricsAttributes))
	}
	expectedTypes := []string{"static", "host", "process", "environment_variables", "msc_config"}
	for i, expected := range expectedTypes {
		if i >= len(obs.metricsAttributes) {
			t.Errorf("Missing attribute provider at index %d (expected '%s')", i, expected)
			continue
		}
		if obs.metricsAttributes[i].Type != expected {
			t.Errorf("Attribute provider %d: expected type '%s', got '%s'", i, expected, obs.metricsAttributes[i].Type)
		}
	}
}

func TestInternalGoodJSONConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
	{
		"msfs_version": 1,
		"backends": [
			{
				"dir_name": "ram",
				"bucket_container_name": "ignored",
				"backend_type": "RAM"
			},
			{
				"dir_name": "s3",
				"bucket_container_name": "test",
				"backend_type": "S3",
				"S3": {
					"region": "us-east-1",
					"endpoint": "http://minio:9000",
					"access_key_id": "minioadmin",
					"secret_access_key": "minioadmin"
				}
			}
		]
	}
	`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}
}

func TestInternalBadJSONConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
	{
		"msfs_version": 1,
		"backends": [
			{
				"dir_name": "ram",
				"backend_type": "RAM"
			},
			{
				"dir_name": "s3",
				"bucket_container_name": "test",
				"backend_type": "S3",
				"S3": {
					"region": "us-east-1",
					"endpoint": "http://minio:9000",
					"access_key_id": "minioadmin",
					"secret_access_key": "minioadmin"
				}
			}
		]
	}
	`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestInternalGoodYAMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    bucket_container_name: ignored,
    backend_type: RAM,
  },
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
}

func TestInternalBadYAMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    backend_type: RAM,
  },
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
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestInternalGoodYMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    bucket_container_name: ignored,
    backend_type: RAM,
  },
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
}

func TestInternalBadYMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    backend_type: RAM,
  },
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
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestExternalGoodJSONConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
	{
		"profiles": {
			"s3": {
				"storage_provider": {
					"type": "s3",
					"options": {
						"base_path": "test",
						"endpoint_url": "http://minio:9000",
						"region_name": "us-east-1"
					}
				},
				"credentials_provider": {
					"type": "S3Credentials",
					"options": {
						"access_key": "minioadmin",
						"secret_key": "minioadmin"
					}
				}
			}
		}
	}
	`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}
}

func TestExternalBadJSONConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
	{
		"profiles": {
			"s3": {
				"storage_provider": {
					"type": "s3",
					"options": {
						"endpoint_url": "http://minio:9000",
						"region_name": "us-east-1"
					}
				},
				"credentials_provider": {
					"type": "S3Credentials",
					"options": {
						"access_key": "minioadmin",
						"secret_key": "minioadmin"
					}
				}
			}
		}
	}
	`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestExternalGoodYAMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
profiles:
  s3:
    storage_provider:
      type: s3
      options:
        base_path: test
        endpoint_url: "http://minio:9000"
        region_name: us-east-1
    credentials_provider:
      type: S3Credentials
      options:
        access_key: minioadmin
        secret_key: minioadmin
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}
}

func TestExternalBadYAMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
profiles:
  s3:
    storage_provider:
      type: s3
      options:
        endpoint_url: "http://minio:9000"
        region_name: us-east-1
    credentials_provider:
      type: S3Credentials
      options:
        access_key: minioadmin
        secret_key: minioadmin
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestExternalGoodYMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
profiles:
  s3:
    storage_provider:
      type: s3
      options:
        base_path: test
        endpoint_url: "http://minio:9000"
        region_name: us-east-1
    credentials_provider:
      type: S3Credentials
      options:
        access_key: minioadmin
        secret_key: minioadmin
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}
}

func TestExternalBadYMLConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
profiles:
  s3:
    storage_provider:
      type: s3
      options:
        endpoint_url: "http://minio:9000"
        region_name: us-east-1
    credentials_provider:
      type: S3Credentials
      options:
        access_key: minioadmin
        secret_key: minioadmin
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestBadOtherSuffixConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".other"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestBadNoSuffixConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[""]))

	err = os.WriteFile(globals.configFilePath, []byte(`
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

func TestConfigFileGoodConfigFileUpdate(t *testing.T) {
	var (
		err   error
		limit uint64
		ok    bool
		start uint64
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram1,
    bucket_container_name: ignored,
    backend_type: RAM,
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

	initFS()
	defer drainFS()

	processToMountList()

	start, limit = globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
	if (limit - start) != 3 {
		t.Fatalf("globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber) should have returned [i:i+3) (\".\", \"..\", \"ram1\")")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, ".")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \".\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "..")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"..\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "ram1")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"ram1\") returned !ok")
	}
	start, limit = globals.physChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
	if (limit - start) != 0 {
		t.Fatalf("globals.physChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber) should have returned [i:i)")
	}

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram1,
    bucket_container_name: ignored,
    backend_type: RAM,
  },
  {
    dir_name: ram2,
    bucket_container_name: ignored,
    backend_type: RAM,
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

	processToUnmountList()

	processToMountList()

	start, limit = globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
	if (limit - start) != 4 {
		t.Fatalf("globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber) should have returned [i:i+4) (\".\", \"..\", \"ram1\", \"ram2\")")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, ".")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \".\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "..")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"..\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "ram1")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"ram1\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "ram2")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"ram2\") returned !ok")
	}
	start, limit = globals.physChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
	if (limit - start) != 0 {
		t.Fatalf("globals.physChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber) should have returned [i:i)")
	}

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram2,
    bucket_container_name: ignored,
    backend_type: RAM,
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

	processToUnmountList()

	processToMountList()

	start, limit = globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
	if (limit - start) != 3 {
		t.Fatalf("globals.virtChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber) should have returned [i:i+3) (\".\", \"..\", \"ram1\")")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, ".")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \".\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "..")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"..\") returned !ok")
	}
	_, ok = globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, "ram2")
	if !ok {
		t.Fatalf("globals.virtChildDirEntryMap.getByBasename(FUSERootDirInodeNumber, \"ram2\") returned !ok")
	}
	start, limit = globals.physChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber)
	if (limit - start) != 0 {
		t.Fatalf("globals.physChildDirEntryMap.getIndexRange(FUSERootDirInodeNumber) should have returned [i:i)")
	}
}

// TestManifestGenBackendReference verifies that an AIStore backend's
// manifest_gen_backend resolves to another configured backend.
func TestManifestGenBackendReference(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    prefix: "p/",
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
      manifest_gen_backend: s3src,
    },
  },
  {
    dir_name: s3src,
    bucket_container_name: test,
    prefix: "p/",
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

	// On first load, checkConfigFile() moves backends into globals.backendsToMount.
	aisBackend, ok := globals.backendsToMount["ais"]
	if !ok {
		t.Fatalf("expected backend \"ais\" to be configured")
	}
	aisCfg, ok := aisBackend.backendTypeSpecifics.(*backendConfigAIStoreStruct)
	if !ok {
		t.Fatalf("expected backend \"ais\" backendTypeSpecifics to be *backendConfigAIStoreStruct")
	}
	if aisCfg.manifestGenBackendName != "s3src" {
		t.Errorf("expected manifestGenBackendName \"s3src\", got %q", aisCfg.manifestGenBackendName)
	}
	if aisCfg.manifestGenBackend == nil {
		t.Fatalf("expected manifest_gen_backend to resolve, got nil")
	}
	if aisCfg.manifestGenBackend.dirName != "s3src" {
		t.Errorf("expected resolved manifest_gen_backend dirName \"s3src\", got %q", aisCfg.manifestGenBackend.dirName)
	}
	if aisCfg.manifestGenBackend.backendType != "S3" {
		t.Errorf("expected resolved manifest_gen_backend backendType \"S3\", got %q", aisCfg.manifestGenBackend.backendType)
	}
	activateBackendsToMountForTest()

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    prefix: "p/",
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
    },
  },
  {
    dir_name: s3src,
    bucket_container_name: test,
    prefix: "p/",
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
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly allowed changing manifest_gen_backend via reload")
	}
}

// TestManifestGenBackendMissing verifies that referencing a non-existent backend
// via manifest_gen_backend is rejected.
func TestManifestGenBackendMissing(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
      manifest_gen_backend: does_not_exist,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded with a dangling manifest_gen_backend reference")
	}
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
      manifest_gen_backend: s3src,
    },
  },
  {
    dir_name: s3src,
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

	activateBackendsToMountForTest()

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
      manifest_gen_backend: does_not_exist,
    },
  },
  {
    dir_name: s3src,
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
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly allowed a missing manifest_gen_backend via reload")
	}
}

// TestManifestGenBackendSelfReference verifies that manifest_gen_backend cannot
// point back to the same AIStore backend.
func TestManifestGenBackendSelfReference(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
      manifest_gen_backend: ais,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded with a self-referential manifest_gen_backend")
	}
}

// TestManifestGenBackendRejectsAIStoreTarget verifies that manifest_gen_backend
// can only delegate listing to a non-AIStore backend.
func TestManifestGenBackendRejectsAIStoreTarget(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ais,
    bucket_container_name: test,
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.1:51080",
      provider: s3,
      manifest_gen_backend: ais_src,
    },
  },
  {
    dir_name: ais_src,
    bucket_container_name: test,
    backend_type: AIStore,
    AIStore: {
      endpoint: "http://10.0.0.2:51080",
      provider: s3,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded with an AIStore manifest_gen_backend target")
	}
}

func TestConfigFileBadConfigFileUpdate(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    bucket_container_name: ignored1,
    backend_type: RAM,
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

	initFS()
	defer drainFS()

	processToMountList()

	err = os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: ram,
    bucket_container_name: ignored2,
    backend_type: RAM,
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	err = checkConfigFile()
	if err == nil {
		t.Fatalf("checkConfigFile() unexpectedly succeeded")
	}
}

// --- cache_storage resolution (three-way selector + deprecated aliases) ------

func TestResolveCacheStorage(t *testing.T) {
	cases := []struct {
		name string
		cfg  map[string]interface{}
		want string
	}{
		{"default", map[string]interface{}{}, cacheStorageMappedFile},
		{"explicit_ram", map[string]interface{}{"cache_storage": "ram"}, cacheStorageRAM},
		{"explicit_mapped_file", map[string]interface{}{"cache_storage": "mapped-file"}, cacheStorageMappedFile},
		{"explicit_per_inode_file", map[string]interface{}{"cache_storage": "per-inode-file"}, cacheStoragePerInodeFile},
		{"alias_mapped_cache_true", map[string]interface{}{"mapped_cache": true}, cacheStorageMappedFile},
		{"alias_mapped_cache_false", map[string]interface{}{"mapped_cache": false}, cacheStorageRAM},
		{"alias_cache_backend_disk", map[string]interface{}{"cache_backend": "disk"}, cacheStoragePerInodeFile},
		{"alias_cache_backend_memory_defaults_mapped_file", map[string]interface{}{"cache_backend": "memory"}, cacheStorageMappedFile},
		{"alias_cache_backend_memory_with_mapped_cache_false", map[string]interface{}{"cache_backend": "memory", "mapped_cache": false}, cacheStorageRAM},
		{"cache_storage_wins_over_aliases", map[string]interface{}{"cache_storage": "per-inode-file", "mapped_cache": true, "cache_backend": "memory"}, cacheStoragePerInodeFile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := resolveCacheStorage(tc.cfg)
			if err != nil {
				t.Fatalf("resolveCacheStorage(%v) returned error: %v", tc.cfg, err)
			}
			if got != tc.want {
				t.Fatalf("resolveCacheStorage(%v) = %q, want %q", tc.cfg, got, tc.want)
			}
		})
	}
}

func TestResolveCacheStorage_Invalid(t *testing.T) {
	cases := map[string]map[string]interface{}{
		"bad_cache_storage": {"cache_storage": "bogus"},
		"bad_cache_backend": {"cache_backend": "bogus"},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := resolveCacheStorage(cfg); err == nil {
				t.Fatalf("resolveCacheStorage(%v) expected error, got nil", cfg)
			}
		})
	}
}

// TestS3AnonymousBackend verifies an S3 backend can be configured for anonymous
// (unsigned) access with no access_key_id / secret_access_key.
func TestS3AnonymousBackend(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: s3pub,
    bucket_container_name: public-bucket,
    prefix: "p/",
    backend_type: S3,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      anonymous: true,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	if err = checkConfigFile(); err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed for anonymous S3: %v", err)
	}

	backend, ok := globals.backendsToMount["s3pub"]
	if !ok {
		t.Fatalf("expected backend \"s3pub\" to be configured")
	}
	s3cfg, ok := backend.backendTypeSpecifics.(*backendConfigS3Struct)
	if !ok {
		t.Fatalf("expected backend \"s3pub\" backendTypeSpecifics to be *backendConfigS3Struct")
	}
	if !s3cfg.anonymous {
		t.Errorf("expected anonymous=true")
	}
	if s3cfg.accessKeyID != "" || s3cfg.secretAccessKey != "" {
		t.Errorf("expected empty access keys for anonymous S3, got accessKeyID=%q secretAccessKey=%q", s3cfg.accessKeyID, s3cfg.secretAccessKey)
	}
}

// TestS3AnonymousClearsUseCredentialsEnv verifies that anonymous wins over
// use_credentials_env: the flag is normalized to false so setupS3Context does
// not load a shared credentials profile.
func TestS3AnonymousClearsUseCredentialsEnv(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: s3pub,
    bucket_container_name: public-bucket,
    backend_type: S3,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      anonymous: true,
      use_credentials_env: true,
    },
  },
]
`), 0o600)
	if err != nil {
		t.Fatalf("os.WriteFile() failed: %v", err)
	}

	if err = checkConfigFile(); err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed: %v", err)
	}

	s3cfg := globals.backendsToMount["s3pub"].backendTypeSpecifics.(*backendConfigS3Struct)
	if !s3cfg.anonymous {
		t.Errorf("expected anonymous=true")
	}
	if s3cfg.useCredentialsEnv {
		t.Errorf("expected useCredentialsEnv cleared when anonymous, got true")
	}
}

// TestDuplicateManifestPathRejected verifies that two backends sharing the same
// manifest_path are rejected: generateManifest does a RemoveAll on the output
// path, so sharing it would clobber one backend's generated manifest.
func TestDuplicateManifestPathRejected(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: a,
    bucket_container_name: test,
    prefix: "pa/",
    readonly: true,
    manifest_path: "/tmp/msfs-test-manifest-shared",
    backend_type: S3,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
  {
    dir_name: b,
    bucket_container_name: test,
    prefix: "pb/",
    readonly: true,
    manifest_path: "/tmp/msfs-test-manifest-shared",
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

	if err = checkConfigFile(); err == nil {
		t.Fatalf("checkConfigFile() unexpectedly allowed duplicate manifest_path")
	} else if !strings.Contains(err.Error(), "manifest_path") {
		t.Fatalf("error should mention manifest_path, got: %v", err)
	}
}

// TestDistinctManifestPathsAccepted verifies that backends with distinct
// manifest_path values are accepted.
func TestDistinctManifestPathsAccepted(t *testing.T) {
	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err := os.WriteFile(globals.configFilePath, []byte(`
msfs_version: 1
backends: [
  {
    dir_name: a,
    bucket_container_name: test,
    prefix: "pa/",
    readonly: true,
    manifest_path: "/tmp/msfs-test-manifest-a",
    backend_type: S3,
    S3: {
      region: us-east-1,
      endpoint: "http://minio:9000",
      access_key_id: minioadmin,
      secret_access_key: minioadmin,
    },
  },
  {
    dir_name: b,
    bucket_container_name: test,
    prefix: "pb/",
    readonly: true,
    manifest_path: "/tmp/msfs-test-manifest-b",
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

	if err = checkConfigFile(); err != nil {
		t.Fatalf("checkConfigFile() unexpectedly failed for distinct manifest_path: %v", err)
	}
}
