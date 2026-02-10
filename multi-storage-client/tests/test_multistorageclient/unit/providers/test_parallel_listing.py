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

"""
Integration tests for parallel recursive listing across all storage providers.

Verifies that list_objects_recursive (parallel, heap-based) produces
identical results to list_objects (sequential) for every provider that
declares supports_parallel_listing = True, and that unsupported providers
fall back to sequential listing gracefully.

The heap algorithm itself is unit-tested in test_base_storage_provider.py
via MockParallelListingProvider (no I/O, deterministic prefix trees).
"""

from unittest.mock import MagicMock, patch

import pytest

from test_multistorageclient.unit.utils import tempdatastore

# ---------------------------------------------------------------------------
# Provider builder helpers
# ---------------------------------------------------------------------------


def _build_s3_provider(temp_bucket, *, enable_rust: bool):
    from multistorageclient.providers.s3 import S3StorageProvider, StaticS3CredentialsProvider

    config = temp_bucket.profile_config_dict()
    storage_options = config["storage_provider"]["options"]
    creds_options = config["credentials_provider"]["options"]

    kwargs = {
        "endpoint_url": storage_options["endpoint_url"],
        "base_path": storage_options["base_path"],
        "credentials_provider": StaticS3CredentialsProvider(
            access_key=creds_options["access_key"],
            secret_key=creds_options["secret_key"],
        ),
    }
    if enable_rust:
        kwargs["rust_client"] = storage_options.get("rust_client", {})
    return S3StorageProvider(**kwargs)


def _build_gcs_provider(temp_bucket):
    from multistorageclient.providers.gcs import GoogleStorageProvider

    config = temp_bucket.profile_config_dict()
    storage_options = config["storage_provider"]["options"]

    return GoogleStorageProvider(
        project_id=storage_options["project_id"],
        endpoint_url=storage_options["endpoint_url"],
        base_path=storage_options["base_path"],
    )


def _build_azure_provider(temp_bucket):
    from multistorageclient.providers.azure import AzureBlobStorageProvider, StaticAzureCredentialsProvider

    config = temp_bucket.profile_config_dict()
    storage_options = config["storage_provider"]["options"]
    creds_options = config["credentials_provider"]["options"]

    return AzureBlobStorageProvider(
        endpoint_url=storage_options["endpoint_url"],
        base_path=storage_options["base_path"],
        credentials_provider=StaticAzureCredentialsProvider(
            connection=creds_options["connection"],
        ),
    )


def _build_posix_provider(temp_dir):
    from multistorageclient.providers.posix_file import PosixFileStorageProvider

    config = temp_dir.profile_config_dict()
    storage_options = config["storage_provider"]["options"]

    return PosixFileStorageProvider(base_path=storage_options["base_path"])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def s3_provider():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=False) as bucket:
        yield _build_s3_provider(bucket, enable_rust=False)


@pytest.fixture
def s3_provider_rust():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        yield _build_s3_provider(bucket, enable_rust=True)


@pytest.fixture
def gcs_provider():
    with tempdatastore.TemporaryGoogleCloudStorageBucket() as bucket:
        yield _build_gcs_provider(bucket)


@pytest.fixture
def azure_provider():
    with tempdatastore.TemporaryAzureBlobStorageContainer() as bucket:
        yield _build_azure_provider(bucket)


@pytest.fixture
def posix_provider():
    with tempdatastore.TemporaryPOSIXDirectory() as tmp:
        yield _build_posix_provider(tmp)


@pytest.fixture
def oci_provider():
    """Lightweight OCI provider with mocked client (no emulator available)."""
    from multistorageclient.providers.oci import OracleStorageProvider

    with patch.object(OracleStorageProvider, "_create_oci_client", return_value=MagicMock()):
        yield OracleStorageProvider(namespace="test-ns", base_path="bucket")


PARALLEL_PROVIDERS = ["s3_provider", "s3_provider_rust", "gcs_provider", "azure_provider"]

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


def _create_test_files(provider, structure: dict[str, list[str]]) -> list[str]:
    all_keys = []
    for prefix, files in structure.items():
        for filename in files:
            key = f"{prefix}/{filename}" if prefix else filename
            provider.put_object(key, b"test content")
            all_keys.append(key)
    return sorted(all_keys)


def _deep_wide_structure() -> dict[str, list[str]]:
    structure = {"": ["file1.txt", "file2.txt"]}
    for i in range(1, 4):
        structure[f"prefix1/sub{i}"] = [f"file{j:03d}.txt" for j in range(100)]
    return structure


# Each case is chosen to exercise a distinct listing topology:
#
#   flat               – no prefixes, pure leaf
#   mixed_interleaving – objects at root interleaved with sub-prefixes
#   deeply_nested      – multi-level, multi-branch (subsumes single-chain "deep")
#   wide_many_prefixes – 20 sibling prefixes (tests parallelism & heap ordering)
#   deep_wide_hybrid   – large volume: 302 files across depth + width
#   path_prefix        – listing scoped to a sub-path
#   special_characters – spaces, dashes, underscores in keys
#   very_deep_nesting  – 10 levels deep (extreme prefix chain)
#   small_window       – constrained max_workers/look_ahead
STRUCTURE_CASES = [
    {
        "name": "flat",
        "structure": {"": ["file1.txt", "file2.txt", "file3.txt"]},
        "path": "",
        "assert_all": True,
    },
    {
        "name": "mixed_interleaving",
        "structure": {"": ["00-readme.txt", "z-final.txt"], "a": ["file1.txt"], "b": ["file2.txt"]},
        "path": "",
        "expected_order": ["00-readme.txt", "a/file1.txt", "b/file2.txt", "z-final.txt"],
        "assert_all": True,
    },
    {
        "name": "deeply_nested",
        "structure": {
            "a/b/c": ["file1.txt"],
            "a/b": ["file2.txt"],
            "a": ["file3.txt"],
            "x/y/z": ["file4.txt"],
        },
        "path": "",
    },
    {
        "name": "wide_many_prefixes",
        "structure": {f"prefix_{i:03d}": ["file.txt"] for i in range(20)},
        "path": "",
        "max_workers": 4,
        "look_ahead": 2,
    },
    {
        "name": "deep_wide_hybrid",
        "structure": _deep_wide_structure(),
        "path": "",
        "max_workers": 4,
        "expected_len": 302,
    },
    {
        "name": "path_prefix",
        "structure": {
            "data/train": ["file1.txt", "file2.txt"],
            "data/test": ["file3.txt"],
            "models": ["model.bin"],
        },
        "path": "data/",
        "assert_prefix": "data/",
    },
    {
        "name": "special_characters",
        "structure": {"data": ["file with spaces.txt", "file-with-dashes.txt", "file_with_underscores.txt"]},
        "path": "",
    },
    {
        "name": "very_deep_nesting",
        "structure": {"/".join(f"level{i}" for i in range(10)): ["file.txt"]},
        "path": "",
        "expected_len": 1,
    },
    {
        "name": "small_window",
        "structure": {f"prefix_{i:02d}": ["file.txt"] for i in range(10)},
        "path": "",
        "max_workers": 2,
        "look_ahead": 1,
    },
]

# ---------------------------------------------------------------------------
# Tests: supports_parallel_listing property
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "provider_fixture, expected",
    [
        ("s3_provider", True),
        ("s3_provider_rust", True),
        ("gcs_provider", True),
        ("azure_provider", True),
        ("oci_provider", True),
        ("posix_provider", False),
    ],
    ids=["s3", "s3_rust", "gcs", "azure", "oci", "posix"],
)
def test_supports_parallel_listing(request, provider_fixture, expected):
    provider = request.getfixturevalue(provider_fixture)
    assert provider.supports_parallel_listing is expected


# ---------------------------------------------------------------------------
# Tests: sequential vs parallel equivalence
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider_fixture", PARALLEL_PROVIDERS)
@pytest.mark.parametrize("case", STRUCTURE_CASES, ids=[c["name"] for c in STRUCTURE_CASES])
def test_recursive_matches_sequential(request, provider_fixture, case):
    provider = request.getfixturevalue(provider_fixture)
    created_keys = _create_test_files(provider, case["structure"])
    path = case.get("path", "")

    sequential = [obj.key for obj in provider.list_objects(path=path)]

    parallel_kwargs = {"path": path}
    if case.get("max_workers") is not None:
        parallel_kwargs["max_workers"] = case["max_workers"]
    if case.get("look_ahead") is not None:
        parallel_kwargs["look_ahead"] = case["look_ahead"]
    parallel = [obj.key for obj in provider.list_objects_recursive(**parallel_kwargs)]

    assert sequential == parallel
    if case.get("assert_all"):
        assert sorted(sequential) == created_keys
    if case.get("expected_order"):
        assert parallel == case["expected_order"]
    if case.get("expected_len") is not None:
        assert len(parallel) == case["expected_len"]
    if case.get("assert_prefix"):
        assert all(key.startswith(case["assert_prefix"]) for key in parallel)


# ---------------------------------------------------------------------------
# Tests: metadata fidelity
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider_fixture", PARALLEL_PROVIDERS)
def test_metadata_fields_match(request, provider_fixture):
    provider = request.getfixturevalue(provider_fixture)
    _create_test_files(provider, {"data": ["file1.txt"]})

    sequential = list(provider.list_objects(path=""))
    parallel = list(provider.list_objects_recursive(path=""))

    assert len(sequential) == 1
    assert len(parallel) == 1

    seq_obj = sequential[0]
    par_obj = parallel[0]

    assert seq_obj.key == par_obj.key
    assert seq_obj.content_length == par_obj.content_length
    assert seq_obj.type == par_obj.type
    seq_etag = seq_obj.etag.strip('"') if seq_obj.etag else None
    par_etag = par_obj.etag.strip('"') if par_obj.etag else None
    assert seq_etag == par_etag


# ---------------------------------------------------------------------------
# Tests: start_after / end_at filtering
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("provider_fixture", PARALLEL_PROVIDERS)
def test_start_after_end_at_filtering(request, provider_fixture):
    provider = request.getfixturevalue(provider_fixture)
    _create_test_files(
        provider,
        {
            "a": ["file.txt"],
            "b": ["file.txt"],
            "c": ["file.txt"],
        },
    )

    results = [
        obj.key for obj in provider.list_objects_recursive(path="", start_after="a/file.txt", end_at="b/file.txt")
    ]
    assert results == ["b/file.txt"]


def test_invalid_start_after_end_at_raises(s3_provider):
    with pytest.raises(ValueError, match="start_after.*must be before end_at"):
        list(s3_provider.list_objects_recursive(start_after="z", end_at="a"))


# ---------------------------------------------------------------------------
# Tests: sequential fallback for non-parallel providers
# ---------------------------------------------------------------------------


def test_sequential_fallback(posix_provider):
    """Non-parallel providers bypass the heap algorithm and fall back to _list_objects directly."""
    _create_test_files(posix_provider, {"a": ["f1.txt"], "b": ["f2.txt"]})

    with patch.object(type(posix_provider), "_shallow_list", wraps=posix_provider._shallow_list) as spy:
        list(posix_provider.list_objects_recursive(path=""))

    spy.assert_not_called()
