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

// TestObservabilityConfigParsing verifies that observability config is parsed correctly
// as an add-on to existing MSCP config without breaking anything.
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
	t.Logf("✓ Exporter: type=%s, endpoint=%v", obs.metricsExporter.Type, obs.metricsExporter.Options["endpoint"])

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
	t.Logf("✓ Reader: collect=%dms, export=%dms", obs.metricsReaderOptions.CollectIntervalMillis, obs.metricsReaderOptions.ExportIntervalMillis)

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
	t.Logf("✓ Attributes: %d providers configured (%v)", len(obs.metricsAttributes), expectedTypes)
}

func TestInternalGoodJSONConfig(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".json"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
	{
		"mscp_version": 1,
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
					"access_key_id": "minioadmin",
					"secret_access_key": "minioadmin",
					"region": "us-east-1",
					"endpoint": "minio:9000",
					"allow_http": true
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
		"mscp_version": 1,
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
					"access_key_id": "minioadmin",
					"secret_access_key": "minioadmin",
					"region": "us-east-1",
					"endpoint": "minio:9000",
					"allow_http": true
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
mscp_version: 1
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
	  access_key_id: minioadmin,
	  secret_access_key: minioadmin,
	  region: us-east-1,
	  endpoint: "minio:9000",
	  allow_http: true,
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
mscp_version: 1
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
	  access_key_id: minioadmin,
	  secret_access_key: minioadmin,
	  region: us-east-1,
	  endpoint: "minio:9000",
	  allow_http: true,
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
mscp_version: 1
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
	  access_key_id: minioadmin,
	  secret_access_key: minioadmin,
	  region: us-east-1,
	  endpoint: "minio:9000",
	  allow_http: true,
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
mscp_version: 1
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
	  access_key_id: minioadmin,
	  secret_access_key: minioadmin,
	  region: us-east-1,
	  endpoint: "minio:9000",
	  allow_http: true,
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
		err error
		ok  bool
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
mscp_version: 1
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

	if globals.inode.virtChildDirMap.Len() != 3 {
		t.Fatalf("globals.inode.virtChildDirMap.Len() should have been 3 (\".\", \"..\", \"ram1\")")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey(".")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\".\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("..")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"..\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("ram1")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"ram1\") returned !ok")
	}

	err = os.WriteFile(globals.configFilePath, []byte(`
mscp_version: 1
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

	if globals.inode.virtChildDirMap.Len() != 4 {
		t.Fatalf("globals.inode.virtChildDirMap.Len() should have been 3 (\".\", \"..\", \"ram1\", \"ram2\")")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey(".")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\".\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("..")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"..\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("ram1")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"ram1\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("ram2")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"ram2\") returned !ok")
	}

	err = os.WriteFile(globals.configFilePath, []byte(`
mscp_version: 1
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

	if globals.inode.virtChildDirMap.Len() != 3 {
		t.Fatalf("globals.inode.virtChildDirMap.Len() should have been 3 (\".\", \"..\", \"ram1\", \"ram2\")")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey(".")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\".\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("..")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"..\") returned !ok")
	}
	_, ok = globals.inode.virtChildDirMap.GetByKey("ram2")
	if !ok {
		t.Fatalf("globals.inode.virtChildDirMap.GetByKey(\"ram2\") returned !ok")
	}
}

func TestConfigFileBadConfigFileUpdate(t *testing.T) {
	var (
		err error
	)

	initGlobals(testOsArgs(testGlobals.testConfigFilePathMap[".yaml"]))

	err = os.WriteFile(globals.configFilePath, []byte(`
mscp_version: 1
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
mscp_version: 1
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
