package main

import (
	"os"
	"testing"
)

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
