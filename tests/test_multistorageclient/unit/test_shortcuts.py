# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import copy
import mmap
import os
import tempfile
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Type

import numpy as np
import pytest

import multistorageclient as msc
from multistorageclient.client import StorageClient
from multistorageclient.file import ObjectFile
from multistorageclient.providers.manifest_metadata import (
    DEFAULT_MANIFEST_BASE_DIR,
)
from multistorageclient.types import MSC_PROTOCOL, ObjectMetadata, SourceVersionCheckMode
from test_multistorageclient.unit.utils import config, tempdatastore

MB = 1024 * 1024


def test_resolve_storage_client(file_storage_config):
    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client(f"{MSC_PROTOCOL}fake/bucket/testfile.bin")

    with pytest.raises(ValueError):
        storage_client, _ = msc.resolve_storage_client("http://fake/bucket/testfile.bin")

    # Verify the three ways to access local filesystem are the same
    sc1, _ = msc.resolve_storage_client("/usr/local/fake/bucket/testfile.bin")
    sc2, _ = msc.resolve_storage_client("file:///usr/local/fake/bucket/testfile.bin")
    sc3, _ = msc.resolve_storage_client("msc://default/usr/local/fake/bucket/testfile.bin")
    assert sc1 == sc2 == sc3
    assert sc1.profile == sc2.profile == sc3.profile == "default"

    # verify that path works with multiple slashes
    _, p1 = msc.resolve_storage_client("/etc/")
    _, p2 = msc.resolve_storage_client("//etc/")
    _, p3 = msc.resolve_storage_client("/////etc/")
    assert p2 == "//etc"
    assert p1 == p3 == "/etc"

    # verify that path works with special characters
    _, p4 = msc.resolve_storage_client("msc://default/tmp/data-#!-_.*'()&$@=;/:+,?\\{{}}%`]<>~|#.bin")
    assert p4 == "tmp/data-#!-_.*'()&$@=;/:+,?\\{{}}%`]<>~|#.bin"

    _, p5 = msc.resolve_storage_client("file:///tmp/data-#!-_.*'()&$@=;/:+,?\\{{}}%`]<>~|#.bin")
    assert p5 == "/tmp/data-#!-_.*'()&$@=;/:+,?\\{{}}%`]<>~|#.bin"

    _, p6 = msc.resolve_storage_client("msc:/default/etc/resolv.conf")
    assert p6 == "etc/resolv.conf"

    _, p7 = msc.resolve_storage_client("./workspace/datasets")
    assert p7 == os.path.realpath("workspace/datasets")

    # Multithreading test to verify the storage_client instance is the same
    def storage_client_thread(number: int) -> tuple[StorageClient, str]:
        tempdir = tempfile.mkdtemp()
        storage_client, path = msc.resolve_storage_client(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin")
        return storage_client, path

    num_threads = 32
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        results = list(executor.map(storage_client_thread, range(num_threads)))
        assert len(results) > 0
        storage_client, _ = results[0]
        for i in range(1, num_threads):
            assert results[i][0] is storage_client, "All threads should return the same StorageClient instance"


def test_resolve_storage_client_with_trailing_question_mark(file_storage_config):
    """Test that URLs with trailing '?' are handled correctly."""

    # Test file URL with trailing "?" - should be treated as part of the filename
    _, path = msc.resolve_storage_client("msc://default/tmp/data/file1.tx?")
    assert path == "tmp/data/file1.tx?"

    # Test POSIX path with trailing "?" - should be treated as part of the filename
    _, path = msc.resolve_storage_client("/tmp/data/file1.tx?")
    assert path == "/tmp/data/file1.tx?"


def test_glob_with_posix_path(file_storage_config):
    for filepath in msc.glob("/etc/**/*.conf"):
        assert filepath.startswith("msc://") is False

    for filepath in msc.glob("msc://default/etc/**/*.conf"):
        assert filepath.startswith("msc://")

    # Test that glob works with multiple slashes which is common in POSIX paths.
    assert msc.glob("/etc/*") == msc.glob("//etc/*")
    assert msc.glob("/etc/*") == msc.glob("/////etc/*")


def test_open_url(file_storage_config):
    body = b"A" * 64 * MB
    tempdir = tempfile.mkdtemp()

    fp = msc.open(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin", "wb")
    fp.write(body)
    fp.close()

    fp = msc.open(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin", "rb")
    content = fp.read()
    fp.close()
    assert body == content

    results = msc.glob(f"{MSC_PROTOCOL}default{tempdir}/*.bin")
    assert len(results) == 1
    assert results[0] == f"{MSC_PROTOCOL}default{tempdir}/testfile.bin"


def test_download_file(file_storage_config):
    body = b"A" * 64 * MB
    tempdir = tempfile.mkdtemp()

    # Write to a test file
    remote_file_path = f"{MSC_PROTOCOL}default{tempdir}/testfile.bin"
    fp = msc.open(remote_file_path, "wb")
    fp.write(body)
    fp.close()

    assert msc.is_file(url=remote_file_path)

    local_tempdir = tempfile.mkdtemp()
    local_file_path = f"{local_tempdir}/testfile.bin"
    msc.download_file(url=remote_file_path, local_path=local_file_path)

    fp = msc.open(f"{MSC_PROTOCOL}default{local_file_path}", "rb")
    content = fp.read()
    fp.close()
    assert body == content

    results = msc.glob(f"{MSC_PROTOCOL}default{local_tempdir}/*.bin")
    assert len(results) == 1
    assert results[0] == f"{MSC_PROTOCOL}default{local_tempdir}/testfile.bin"


def test_list(file_storage_config):
    # Create test file
    body = b"A" * 64 * MB
    tempdir = tempfile.mkdtemp()

    fp = msc.open(f"{MSC_PROTOCOL}default{tempdir}/testfile.bin", "wb")
    fp.write(body)
    fp.close()

    # Test listing without glob pattern
    results = list(msc.list(f"{MSC_PROTOCOL}default{tempdir}"))
    assert len(results) == 1
    assert "testfile.bin" in results[0].key

    # Test listing with POSIX file path
    results = list(msc.list(tempdir))
    assert len(results) == 1
    assert "testfile.bin" in results[0].key
    assert f"{MSC_PROTOCOL}default" not in results[0].key


def test_write(file_storage_config):
    tempdir = tempfile.mkdtemp()
    filepath = os.path.join(tempdir, "testfile.bin")

    # Test writing bytes
    body = b"A" * 64 * MB
    msc.write(f"{MSC_PROTOCOL}default{filepath}", body)

    # Verify content was written correctly
    with msc.open(f"{MSC_PROTOCOL}default{filepath}", "rb") as fp:
        content = fp.read()
        assert body == content


def test_delete(file_storage_config):
    tempdir = tempfile.mkdtemp()
    filepath = os.path.join(tempdir, "testfile.bin")

    # Create test file
    body = b"A" * 64 * MB
    with msc.open(f"{MSC_PROTOCOL}default{filepath}", "wb") as fp:
        fp.write(body)

    # Verify file exists
    with msc.open(f"{MSC_PROTOCOL}default{filepath}", "rb") as fp:
        assert fp.read() == body

    # Delete file
    msc.delete(f"{MSC_PROTOCOL}default{filepath}")

    # Verify file is deleted
    with pytest.raises(FileNotFoundError):
        with msc.open(f"{MSC_PROTOCOL}default{filepath}", "rb") as fp:
            fp.read()


def test_is_empty(file_storage_config):
    assert msc.is_empty("/usr/bin") is False
    assert msc.is_empty("/tmp/dir/not/exist")

    with tempfile.TemporaryDirectory() as tempdir:
        filepath = os.path.join(tempdir, "testfile.bin")
        with msc.open(filepath, "wb") as fp:
            fp.write(b"TEST")

        assert msc.is_empty(f"{MSC_PROTOCOL}default{tempdir}") is False


def verify_shortcuts(profile: str, prefix: str):
    prefix = f"{prefix}/data"
    body = b"A" * (64 * MB)

    # open files
    for i in range(10):
        with msc.open(f"msc://{profile}/{prefix}/folder/data-{i}.bin", "wb") as fp:
            fp.write(body)

    msc.commit_metadata(f"msc://{profile}")
    # glob
    assert len(msc.glob(f"msc://{profile}/{prefix}/**/*.bin")) == 10

    # upload
    fp = tempfile.NamedTemporaryFile(mode="wb", delete=False)
    fp.write(body)
    fp.close()
    msc.upload_file(f"msc://{profile}/{prefix}/folder/data-11.bin", fp.name)

    msc.commit_metadata(f"msc://{profile}")

    file_list = msc.glob(f"msc://{profile}/{prefix}/**/*.bin")
    assert len(file_list) == 11

    for file_url in file_list:
        assert msc.is_file(file_url)

    # info
    obj_metadata = msc.info(f"msc://{profile}/{prefix}/folder/data-11.bin")
    assert isinstance(obj_metadata, ObjectMetadata)

    # download
    filepath = os.path.join(tempfile.gettempdir(), "data-11.bin")
    msc.download_file(f"msc://{profile}/{prefix}/folder/data-11.bin", filepath)
    assert os.path.exists(filepath)

    # numpy
    arr = np.array([1, 2, 3, 4, 5], dtype=np.int32)
    msc.numpy.save(f"msc://{profile}/{prefix}/folder/arr-01.npy", arr)
    msc.commit_metadata(f"msc://{profile}")

    assert msc.numpy.load(f"msc://{profile}/{prefix}/folder/arr-01.npy").all() == arr.all()
    assert (
        msc.numpy.memmap(f"msc://{profile}/{prefix}/folder/arr-01.npy", dtype=np.int32, shape=(5,)).all() == arr.all()
    )

    # mmap
    with msc.open(f"msc://{profile}/{prefix}/folder/data-2.bin") as fp:
        with mmap.mmap(fp.fileno(), length=0, access=mmap.ACCESS_READ) as mm:
            content = mm[:]
            assert content == body

    # open file without cache
    with msc.open(f"msc://{profile}/{prefix}/folder/data-2.bin", disable_read_cache=True) as fp:
        if isinstance(fp, ObjectFile):
            assert fp._cache_manager is None

    # delete files
    msc.delete(f"msc://{profile}/{prefix}", recursive=True)
    for file_url in file_list:
        assert not msc.is_file(file_url)

    # Ensure all directories gone too
    assert len(list(msc.list(f"msc://{profile}/{prefix}/", include_directories=True))) == 0


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_shortcuts(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        profile_config = temp_data_store.profile_config_dict()
        profile_config["caching_enabled"] = True

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
                "cache": {
                    "eviction_policy": {
                        "policy": "fifo",
                    }
                },
            }
        )

        verify_shortcuts(profile="test", prefix="files")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_msc_shortcuts_with_s3_manifest(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        data_with_manifest_profile_config_dict = copy.deepcopy(temp_data_store.profile_config_dict()) | {
            "metadata_provider": {
                "type": "manifest",
                "options": {
                    "manifest_path": DEFAULT_MANIFEST_BASE_DIR,
                    "writable": True,
                },
            },
            "caching_enabled": True,
        }
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": data_with_manifest_profile_config_dict,
                },
                "cache": {
                    "eviction_policy": {
                        "policy": "fifo",
                    }
                },
            }
        )

        verify_shortcuts(profile="test", prefix="files")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
    ],
)
def test_msc_shortcuts_with_empty_base_path(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        profile_dict = temp_data_store.profile_config_dict()
        profile_dict["storage_provider"]["options"]["base_path"] = ""
        profile_dict["caching_enabled"] = True
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": profile_dict,
                },
                "cache": {
                    "eviction_policy": {
                        "policy": "fifo",
                    }
                },
            }
        )

        verify_shortcuts(profile="test", prefix=f"{temp_data_store._bucket_name}/files")


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_glob_include_prefix(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        profile_name = "test_glob_include_prefix"

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    profile_name: temp_data_store.profile_config_dict(),
                }
            }
        )

        body = b"A" * 64 * MB
        sub_prefix = os.path.basename(tempfile.mkdtemp())

        # Write to a test file
        remote_file_path = f"{MSC_PROTOCOL}{profile_name}/{sub_prefix}/testfile.bin"
        with msc.open(remote_file_path, "wb") as fp:
            fp.write(body)

        # NOTE: The URL here does not include the base_path, but profile name and sub-prefix
        results = msc.glob(f"{MSC_PROTOCOL}{profile_name}/{sub_prefix}/**/*.bin")
        assert len(results) == 1

        with msc.open(results[0], "rb") as fp:
            assert fp.read(10) == b"A" * 10


def test_download_and_sync_files(file_storage_config):
    body = b"A" * 4 * MB
    tempdir = tempfile.mkdtemp()

    file_names = ["dir1/testfile1.bin", "dir1/testfile2.bin", "dir2/testfile3.bin"]

    # Write three test files
    for file_name in file_names:
        remote_file_path = f"{MSC_PROTOCOL}default{tempdir}/{file_name}"
        with msc.open(remote_file_path, "wb") as fp:
            fp.write(body)
        assert msc.is_file(url=remote_file_path)

    # Sync to a different destination directory
    sync_dest_tempdir = tempfile.mkdtemp()
    msc.sync(source_url=f"{MSC_PROTOCOL}default{tempdir}/", target_url=f"{MSC_PROTOCOL}default{sync_dest_tempdir}/")

    expected_synced_files = [f"{MSC_PROTOCOL}default{sync_dest_tempdir}/{file_name}" for file_name in file_names]

    for synced_file in expected_synced_files:
        with msc.open(synced_file, "rb") as fp:
            synced_content = fp.read()
        assert synced_content == body

    # Test, by syncing again and verify the data hasn't changed?
    msc.sync(source_url=f"{MSC_PROTOCOL}default{tempdir}/", target_url=f"{MSC_PROTOCOL}default{sync_dest_tempdir}/")


def test_explicit_path_translation(file_storage_config_with_path_mapping):
    """Test path translations defined in the MSC configuration."""

    # Test Case 1: Basic file path translation with direct match to profile
    client, path = msc.resolve_storage_client("/lustrefs/a/b/file.txt")
    assert client.profile == "file-a-b"
    assert path == "/file.txt"

    # Test Case 2: Path translation for parent directory
    client, path = msc.resolve_storage_client("/lustrefs/a/file.txt")
    assert client.profile == "file-a"
    assert path == "/file.txt"

    # Test Case 3: Nested path where longer prefix should take precedence
    client, path = msc.resolve_storage_client("/lustrefs/a/b/c/nested/file.txt")
    assert client.profile == "file-a-b"
    assert path == "/c/nested/file.txt"

    # Test Case 4: S3 path translation
    client, path = msc.resolve_storage_client("s3://bucket1/prefix/file.txt")
    assert client.profile == "s3-bucket1"
    assert path == "prefix/file.txt"

    # Test Case 5: S3 nested path where longer prefix should take precedence
    client, path = msc.resolve_storage_client("s3://bucket1/a/b/prefix/file.txt")
    assert client.profile == "s3-bucket1-a-b"
    assert path == "prefix/file.txt"

    # Test Case 6: S3 path translation with existing s3 url pointing to a different storage provider profile
    client, path = msc.resolve_storage_client("s3://old-bucket-123/prefix/file.txt")
    assert client.profile == "azure-new-bucket-456"
    assert path == "prefix/file.txt"


def test_implicit_profiles_without_msc_config():
    """Test the implicit profiles feature for non-MSC URLs."""
    # Setup environment variables for implicit profiles to create storage providers successfully
    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    os.environ["AIS_ENDPOINT"] = "http://localhost:12345"
    os.environ["GOOGLE_CLOUD_PROJECT_ID"] = "GCS_PROJECT_ID_123"

    try:
        # Test S3 implicit profile
        client, path = msc.resolve_storage_client("s3://test-bucket/path/to/object")
        assert client.profile == "_s3-test-bucket"
        assert path == "path/to/object"

        # Verify the same client is returned for the same implicit profile
        client2, _ = msc.resolve_storage_client("s3://test-bucket/another/path")
        assert client is client2  # Same instance

        # Test AIS implicit profile
        client, path = msc.resolve_storage_client("ais://test-bucket/path/to/object")
        assert client.profile == "_ais-test-bucket"
        assert path == "path/to/object"

        # Test file implicit profile
        client, path = msc.resolve_storage_client("file:///path/to/file")
        assert client.profile == "default"
        assert path == "/path/to/file"

        # Test posix implicit profile
        client, path = msc.resolve_storage_client("/path/to/file")
        assert client.profile == "default"
        assert path == "/path/to/file"

        # Comment out for now because GCS requires credentials json to be present locally
        # client, path = msc.resolve_storage_client("gs://test-bucket/path/to/object")
        # assert client.profile == "_gs-test-bucket"
        # assert path == "path/to/object"

        # Test error cases
        with pytest.raises(ValueError):
            msc.resolve_storage_client("s3:///missing-bucket/path")

        with pytest.raises(ValueError):
            msc.resolve_storage_client("unknown://test-bucket/path")
    finally:
        # Clean up environment variables
        for env_var in ["AWS_ENDPOINT_URL", "AIS_ENDPOINT", "GOOGLE_CLOUD_PROJECT_ID"]:
            if env_var in os.environ:
                del os.environ[env_var]


def test_implicit_profiles_with_msc_config(file_storage_config_with_path_mapping):
    """Test the behavior of paths that don't match any translation in the MSC config.

    This test ensures that:
    1. File paths that don't match any translation use the default profile
    2. Object storage URLs that don't match any translation use implicit profiles
    """
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    os.environ["AWS_ENDPOINT_URL"] = "http://localhost:9000"
    try:
        # Test Case 1: File Path that doesn't match any translation - should use default
        client, path = msc.resolve_storage_client("/tmp/some/path/file.txt")
        assert client.profile == "default"
        assert path == "/tmp/some/path/file.txt"

        # Test Case 2: s3 Path that doesn't match any translation - should use implicit profile
        client, path = msc.resolve_storage_client("s3://bucket2/prefix/file.txt")
        assert client.profile == "_s3-bucket2"
        assert path == "prefix/file.txt"
    finally:
        # Clean up environment variables
        for env_var in ["AWS_ENDPOINT_URL"]:
            if env_var in os.environ:
                del os.environ[env_var]


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_open_with_source_version_check(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]) -> None:
    """Test the open method with different source version check modes."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        profile_config = temp_data_store.profile_config_dict()
        profile_config["caching_enabled"] = True

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
                "cache": {
                    "eviction_policy": {
                        "policy": "no_eviction",
                    }
                },
            }
        )

        test_uuid = str(uuid.uuid4())
        key = f"test-source-version-check-{test_uuid}"
        content1 = b"test content for cache"
        content2 = b"modified content for cache"

        try:
            # Write initial content
            with msc.open(f"{MSC_PROTOCOL}test/{key}", "wb") as f:
                f.write(content1)

            # Read with ENABLE mode - should get updated content
            with msc.open(f"{MSC_PROTOCOL}test/{key}", "rb", check_source_version=SourceVersionCheckMode.ENABLE) as f:
                assert f.read() == content1

            # Modify file content
            with msc.open(f"{MSC_PROTOCOL}test/{key}", "wb") as f:
                f.write(content2)

            # Read with DISABLE mode - should get old content from cache
            with msc.open(f"{MSC_PROTOCOL}test/{key}", "rb", check_source_version=SourceVersionCheckMode.DISABLE) as f:
                assert f.read() == content1  # Should read from cache, not see the modification

            # Read with ENABLE mode - should get new content
            with msc.open(f"{MSC_PROTOCOL}test/{key}", "rb", check_source_version=SourceVersionCheckMode.ENABLE) as f:
                assert f.read() == content2  # Should update cache

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{key}")
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[[tempdatastore.TemporaryAWSS3Bucket]],
)
def test_open_with_cache_config(temp_data_store_type: Type[tempdatastore.TemporaryDataStore]) -> None:
    """Test the open method with different cache configurations."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()
    with temp_data_store_type() as temp_data_store:
        # Test with cache enabled and use_etag=True
        profile_config = temp_data_store.profile_config_dict()
        profile_config["caching_enabled"] = True

        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
                "cache": {
                    "use_etag": True,
                    "eviction_policy": {
                        "policy": "no_eviction",
                    },
                },
            }
        )

        body = b"A" * 64 * MB
        test_uuid = str(uuid.uuid4())
        filepath = f"testfile-{test_uuid}.bin"

        try:
            # Write and read with INHERIT mode (should use etag)
            with msc.open(f"{MSC_PROTOCOL}test/{filepath}", "wb") as fp:
                fp.write(body)

            with msc.open(f"{MSC_PROTOCOL}test/{filepath}", "rb") as fp:
                assert fp.read() == body

            # Test with cache enabled and use_etag=False
            profile_config = temp_data_store.profile_config_dict()
            profile_config["caching_enabled"] = True

            config.setup_msc_config(
                config_dict={
                    "profiles": {
                        "test": temp_data_store.profile_config_dict(),
                    },
                    "cache": {
                        "use_etag": False,
                        "eviction_policy": {
                            "policy": "no_eviction",
                        },
                    },
                }
            )

            # Write and read with INHERIT mode (should not use etag)
            with msc.open(f"{MSC_PROTOCOL}test/{filepath}", "wb") as fp:
                fp.write(body)

            with msc.open(f"{MSC_PROTOCOL}test/{filepath}", "rb") as fp:
                assert fp.read() == body

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{filepath}")
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_write_with_attributes(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    """Test msc.write with attributes functionality."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for msc.write attributes"
        test_uuid = str(uuid.uuid4())
        file_path = f"test-write-attributes-{test_uuid}.txt"

        # Test attributes for msc.write
        test_attributes = {
            "upload_method": "msc.write",
            "version": "1.0",
            "author": "test_user",
        }

        try:
            # Test msc.write with attributes
            msc.write(f"{MSC_PROTOCOL}test/{file_path}", test_content, attributes=test_attributes)

            # Verify content was written correctly
            with msc.open(f"{MSC_PROTOCOL}test/{file_path}", "rb") as fp:
                content = fp.read()
                assert content == test_content

            # Get the storage client to check metadata
            metadata = msc.info(f"{MSC_PROTOCOL}test/{file_path}")

            # For storage providers that support metadata, verify attributes are present
            if hasattr(temp_data_store, "_bucket_name"):  # S3-like storage
                assert metadata is not None
                assert metadata.metadata is not None

                # Verify all attributes are present with msc_ prefix
                for key, value in test_attributes.items():
                    assert key in metadata.metadata, f"Expected attribute '{key}' not found"
                    assert metadata.metadata[key] == value, f"Attribute '{key}' has incorrect value"

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{file_path}")
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_upload_file_with_attributes(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    """Test msc.upload_file with attributes functionality."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for msc.upload_file attributes"
        test_uuid = str(uuid.uuid4())
        file_path = f"test-upload-attributes-{test_uuid}.txt"

        # Test attributes for msc.upload_file
        test_attributes = {
            "upload_method": "msc.upload_file",
            "test_id": test_uuid,
            "file_type": "test_data",
        }

        # Create a temporary local file
        temp_file = tempfile.NamedTemporaryFile(delete=False)
        try:
            temp_file.write(test_content)
            temp_file.flush()

            # Test msc.upload_file with attributes
            msc.upload_file(f"{MSC_PROTOCOL}test/{file_path}", temp_file.name, attributes=test_attributes)

            # Verify content was uploaded correctly
            with msc.open(f"{MSC_PROTOCOL}test/{file_path}", "rb") as fp:
                content = fp.read()
                assert content == test_content

            # Get the storage client to check metadata
            metadata = msc.info(f"{MSC_PROTOCOL}test/{file_path}")

            # For storage providers that support metadata, verify attributes are present
            if hasattr(temp_data_store, "_bucket_name"):  # S3-like storage
                assert metadata is not None
                assert metadata.metadata is not None

                # Verify all attributes are present with msc_ prefix
                for key, value in test_attributes.items():
                    assert key in metadata.metadata, f"Expected attribute '{key}' not found"
                    assert metadata.metadata[key] == value, f"Attribute '{key}' has incorrect value"

        finally:
            # Clean up
            os.unlink(temp_file.name)
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{file_path}")
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_open_with_attributes(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    """Test msc.open with attributes functionality."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for msc.open attributes"
        test_uuid = str(uuid.uuid4())
        file_path = f"test-open-attributes-{test_uuid}.txt"

        # Test attributes for msc.open
        test_attributes = {
            "open_method": "msc.open",
            "mode": "wb",
            "test_case": "context_manager",
        }

        try:
            # Test msc.open with attributes using context manager
            with msc.open(f"{MSC_PROTOCOL}test/{file_path}", "wb", attributes=test_attributes) as f:
                f.write(test_content)

            # Verify content was written correctly
            with msc.open(f"{MSC_PROTOCOL}test/{file_path}", "rb") as fp:
                content = fp.read()
                assert content == test_content

            # Get the storage client to check metadata
            metadata = msc.info(f"{MSC_PROTOCOL}test/{file_path}")

            # For storage providers that support metadata, verify attributes are present
            if hasattr(temp_data_store, "_bucket_name"):  # S3-like storage
                assert metadata is not None
                assert metadata.metadata is not None

                # Verify all attributes are present with msc_ prefix
                for key, value in test_attributes.items():
                    assert key in metadata.metadata, f"Expected attribute '{key}' not found"
                    assert metadata.metadata[key] == value, f"Attribute '{key}' has incorrect value"

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{file_path}")
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_shortcuts_attributes_validation(temp_data_store_type: type[tempdatastore.TemporaryDataStore]) -> None:
    """Test attributes validation in shortcut functions."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for validation"
        test_uuid = str(uuid.uuid4())

        # Test key length validation (max 32 characters)
        long_key_attributes = {"a" * 33: "value"}  # 33 chars, should fail

        with pytest.raises(RuntimeError, match="Failed to PUT object.*exceeds maximum length of 32 characters"):
            msc.write(f"{MSC_PROTOCOL}test/test-long-key-{test_uuid}.txt", test_content, attributes=long_key_attributes)

        # Test value length validation (max 128 characters)
        long_value_attributes = {"key": "x" * 129}  # 129 chars, should fail

        with pytest.raises(RuntimeError, match="Failed to PUT object.*exceeds maximum length of 128 characters"):
            msc.write(
                f"{MSC_PROTOCOL}test/test-long-value-{test_uuid}.txt", test_content, attributes=long_value_attributes
            )


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_list_with_attribute_filter_expression(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
) -> None:
    """Test msc.list with attribute_filter_expression functionality."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for attribute filtering"
        test_uuid = str(uuid.uuid4())
        base_path = f"test-list-filters-{test_uuid}"

        # Create test files with different attributes
        test_files = [
            {
                "name": "model_v1.bin",
                "attributes": {"model_name": "gpt", "version": "1.0", "author": "alice", "priority": "10"},
            },
            {
                "name": "model_v2.bin",
                "attributes": {"model_name": "gpt", "version": "2.0", "author": "bob", "priority": "5"},
            },
            {
                "name": "data_v1.bin",
                "attributes": {"model_name": "bert", "version": "1.0", "author": "alice", "priority": "15"},
            },
            {
                "name": "data_v2.bin",
                "attributes": {"model_name": "bert", "version": "1.5", "author": "charlie", "priority": "8"},
            },
            {
                "name": "config.txt",
                "attributes": {"type": "config", "version": "0.5", "author": "admin", "priority": "20"},
            },
        ]

        try:
            # Create test files with attributes
            for test_file in test_files:
                file_path = f"{MSC_PROTOCOL}test/{base_path}/{test_file['name']}"
                msc.write(file_path, test_content, attributes=test_file["attributes"])

            # Test equality filter
            results = list(
                msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression='model_name = "gpt"')
            )
            assert len(results) == 2
            result_names = [os.path.basename(r.key) for r in results]
            assert "model_v1.bin" in result_names
            assert "model_v2.bin" in result_names

            # Test inequality filter
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression='author != "alice"'))
            assert len(results) == 3
            result_names = [os.path.basename(r.key) for r in results]
            assert "model_v2.bin" in result_names
            assert "data_v2.bin" in result_names
            assert "config.txt" in result_names

            # Test numeric comparison (greater than)
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression='priority > "10"'))
            assert len(results) == 2
            result_names = [os.path.basename(r.key) for r in results]
            assert "data_v1.bin" in result_names  # priority: 15
            assert "config.txt" in result_names  # priority: 20

            # Test numeric comparison (less than or equal)
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression='priority <= "8"'))
            assert len(results) == 2
            result_names = [os.path.basename(r.key) for r in results]
            assert "model_v2.bin" in result_names  # priority: 5
            assert "data_v2.bin" in result_names  # priority: 8

            # Test string comparison (version ordering)
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression='version >= "1.0"'))
            assert len(results) == 4  # All except config.txt with version "0.5"
            result_names = [os.path.basename(r.key) for r in results]
            assert "config.txt" not in result_names

            # Test multiple filters (AND logic)
            results = list(
                msc.list(
                    f"{MSC_PROTOCOL}test/{base_path}",
                    attribute_filter_expression='(model_name = "bert" AND author != "charlie")',
                )
            )
            assert len(results) == 1
            result_names = [os.path.basename(r.key) for r in results]
            assert "data_v1.bin" in result_names  # bert model by alice, not charlie

            # Test filter with no matches
            results = list(
                msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression='model_name = "nonexistent"')
            )
            assert len(results) == 0

            # Test empty filter (should return all files)
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}", attribute_filter_expression=""))
            assert len(results) == 5

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{base_path}", recursive=True)
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_list_show_attributes(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
) -> None:
    """Test msc.list with show_attributes functionality."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for show_attributes"
        test_uuid = str(uuid.uuid4())
        base_path = f"test-list-show-attributes-{test_uuid}"

        # Create test files with different attributes
        test_files = [
            {
                "name": "model_v1.bin",
                "attributes": {"model_name": "gpt", "version": "1.0", "author": "alice", "priority": "10"},
            },
            {
                "name": "model_v2.bin",
                "attributes": {"model_name": "gpt", "version": "2.0", "author": "bob", "priority": "5"},
            },
        ]

        try:
            # Create test files with attributes
            for test_file in test_files:
                file_path = f"{MSC_PROTOCOL}test/{base_path}/{test_file['name']}"
                msc.write(file_path, test_content, attributes=test_file["attributes"])

            # Test list without show attribute flag
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}"))
            assert len(results) == 2
            for object_metadata in results:
                assert object_metadata.metadata is None

            # Test list with show attribute flag = True
            results = list(msc.list(f"{MSC_PROTOCOL}test/{base_path}", show_attributes=True))
            assert len(results) == 2
            for object_metadata in results:
                assert object_metadata.metadata is not None
                assert "model_name" in object_metadata.metadata
                assert "version" in object_metadata.metadata
                assert "author" in object_metadata.metadata
                assert "priority" in object_metadata.metadata

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{base_path}", recursive=True)
            except Exception:
                pass


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_msc_glob_with_attribute_filter_expression(
    temp_data_store_type: type[tempdatastore.TemporaryDataStore],
) -> None:
    """Test msc.glob with attribute_filter_expression functionality."""
    # Clear the instance cache to ensure that the config is not reused from the previous test
    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_content = b"test content for glob attribute filtering"
        test_uuid = str(uuid.uuid4())
        base_path = f"test-glob-filters-{test_uuid}"

        # Create test files with different attributes in various subdirectories
        test_files = [
            {
                "path": f"{base_path}/models/gpt/model_v1.bin",
                "attributes": {"model_name": "gpt", "version": "1.0", "environment": "prod", "size": "large"},
            },
            {
                "path": f"{base_path}/models/gpt/model_v2.bin",
                "attributes": {"model_name": "gpt", "version": "2.0", "environment": "dev", "size": "small"},
            },
            {
                "path": f"{base_path}/models/bert/data_v1.bin",
                "attributes": {"model_name": "bert", "version": "1.0", "environment": "prod", "size": "medium"},
            },
            {
                "path": f"{base_path}/data/training/dataset.bin",
                "attributes": {"type": "dataset", "version": "1.5", "environment": "test", "size": "large"},
            },
            {
                "path": f"{base_path}/config/settings.txt",
                "attributes": {"type": "config", "version": "0.5", "environment": "prod", "size": "small"},
            },
            {
                "path": f"{base_path}/temp/cache.tmp",
                "attributes": {"type": "temp", "version": "1.0", "environment": "dev", "size": "medium"},
            },
        ]

        try:
            # Create test files with attributes
            for test_file in test_files:
                file_path = f"{MSC_PROTOCOL}test/{test_file['path']}"
                msc.write(file_path, test_content, attributes=test_file["attributes"])

            # Test glob with equality filter - find all gpt models
            results = msc.glob(
                f"{MSC_PROTOCOL}test/{base_path}/**/*.bin", attribute_filter_expression='model_name = "gpt"'
            )
            assert len(results) == 2
            assert all("gpt" in path for path in results)

            # Test glob with inequality filter - find all non-prod environments
            results = msc.glob(
                f"{MSC_PROTOCOL}test/{base_path}/**/*", attribute_filter_expression='environment != "prod"'
            )
            assert len(results) == 3  # dev + test + dev files
            result_basenames = [os.path.basename(path) for path in results]
            assert "model_v2.bin" in result_basenames  # dev
            assert "dataset.bin" in result_basenames  # test
            assert "cache.tmp" in result_basenames  # dev

            # Test glob with string comparison - versions >= 1.0
            results = msc.glob(f"{MSC_PROTOCOL}test/{base_path}/**/*", attribute_filter_expression='version >= "1.0"')
            assert len(results) == 5  # All except settings.txt (version 0.5)
            result_basenames = [os.path.basename(path) for path in results]
            assert "settings.txt" not in result_basenames

            # Test glob with multiple filters (AND logic) - large files in prod
            results = msc.glob(
                f"{MSC_PROTOCOL}test/{base_path}/**/*",
                attribute_filter_expression='(size = "large" AND environment = "prod")',
            )
            assert len(results) == 1
            assert "model_v1.bin" in results[0]

            # Test glob pattern specificity with filters - only .bin files that are small
            results = msc.glob(f"{MSC_PROTOCOL}test/{base_path}/**/*.bin", attribute_filter_expression='size = "small"')
            assert len(results) == 1
            assert "model_v2.bin" in results[0]

            # Test glob with nested path pattern and filters
            results = msc.glob(
                f"{MSC_PROTOCOL}test/{base_path}/models/**/*", attribute_filter_expression='version = "1.0"'
            )
            assert len(results) == 2
            result_basenames = [os.path.basename(path) for path in results]
            assert "model_v1.bin" in result_basenames
            assert "data_v1.bin" in result_basenames

            # Test glob with filters that return no results
            results = msc.glob(
                f"{MSC_PROTOCOL}test/{base_path}/**/*", attribute_filter_expression='model_name = "nonexistent"'
            )
            assert len(results) == 0

            # Test glob with empty filters (should return all matching pattern)
            results = msc.glob(f"{MSC_PROTOCOL}test/{base_path}/**/*.bin", attribute_filter_expression="")
            assert len(results) == 4  # All .bin files

            # Test complex glob pattern with attribute filters
            results = msc.glob(
                f"{MSC_PROTOCOL}test/{base_path}/**/model_*.bin", attribute_filter_expression='environment != "test"'
            )
            assert len(results) == 2  # model_v1.bin (prod) and model_v2.bin (dev)

        finally:
            # Clean up
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{base_path}", recursive=True)
            except Exception:
                pass
