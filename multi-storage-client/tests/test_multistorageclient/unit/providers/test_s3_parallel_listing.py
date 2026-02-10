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
Unit tests for parallel listing in S3StorageProvider.
"""

import pytest

from test_multistorageclient.unit.utils import tempdatastore


def _create_test_files(provider, structure: dict[str, list[str]]) -> list[str]:
    all_keys = []
    for prefix, files in structure.items():
        for filename in files:
            key = f"{prefix}/{filename}" if prefix else filename
            provider.put_object(key, b"test content")
            all_keys.append(key)
    return sorted(all_keys)


def _build_provider(temp_s3_bucket, enable_rust: bool):
    from multistorageclient.providers.s3 import S3StorageProvider, StaticS3CredentialsProvider

    config = temp_s3_bucket.profile_config_dict()
    storage_options = config["storage_provider"]["options"]
    creds_options = config["credentials_provider"]["options"]

    credentials_provider = StaticS3CredentialsProvider(
        access_key=creds_options["access_key"],
        secret_key=creds_options["secret_key"],
    )

    kwargs = {
        "endpoint_url": storage_options["endpoint_url"],
        "base_path": storage_options["base_path"],
        "credentials_provider": credentials_provider,
    }
    if enable_rust:
        kwargs["rust_client"] = storage_options.get("rust_client", {})
    return S3StorageProvider(**kwargs)


@pytest.fixture
def s3_provider():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=False) as bucket:
        yield _build_provider(bucket, enable_rust=False)


@pytest.fixture
def s3_provider_rust():
    with tempdatastore.TemporaryAWSS3Bucket(enable_rust_client=True) as bucket:
        yield _build_provider(bucket, enable_rust=True)


def _deep_wide_structure() -> dict[str, list[str]]:
    structure = {"": ["file1.txt", "file2.txt"]}
    for i in range(1, 4):
        structure[f"prefix1/sub{i}"] = [f"file{j:03d}.txt" for j in range(100)]
    return structure


STRUCTURE_CASES = [
    {
        "name": "flat",
        "structure": {"": ["file1.txt", "file2.txt", "file3.txt"]},
        "path": "",
        "assert_all": True,
    },
    {
        "name": "wide",
        "structure": {
            "a": ["file1.txt", "file2.txt"],
            "b": ["file3.txt", "file4.txt"],
            "c": ["file5.txt", "file6.txt"],
        },
        "path": "",
        "max_workers": 4,
    },
    {
        "name": "deep",
        "structure": {"a/b/c": ["file1.txt", "file2.txt", "file3.txt"]},
        "path": "",
    },
    {
        "name": "mixed_interleaving",
        "structure": {"": ["00-readme.txt", "z-final.txt"], "a": ["file1.txt"], "b": ["file2.txt"]},
        "path": "",
        "expected_order": ["00-readme.txt", "a/file1.txt", "b/file2.txt", "z-final.txt"],
        "assert_all": True,
    },
    {
        "name": "deep_wide_hybrid",
        "structure": _deep_wide_structure(),
        "path": "",
        "max_workers": 4,
        "expected_len": 302,
    },
    {
        "name": "hierarchical",
        "structure": {
            "data": ["file1.txt", "file2.txt"],
            "models": ["model1.bin", "model2.bin"],
            "logs": ["log1.txt"],
        },
        "path": "",
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
        "name": "mixed_top_level",
        "structure": {
            "": ["README.md", "config.yaml"],
            "data": ["file1.txt", "file2.txt"],
            "models": ["model.bin"],
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
        "name": "single_prefix",
        "structure": {"only_prefix": ["file1.txt", "file2.txt"]},
        "path": "",
    },
    {
        "name": "very_deep_nesting",
        "structure": {"/".join(f"level{i}" for i in range(10)): ["file.txt"]},
        "path": "",
        "expected_len": 1,
    },
    {
        "name": "special_characters",
        "structure": {"data": ["file with spaces.txt", "file-with-dashes.txt", "file_with_underscores.txt"]},
        "path": "",
    },
    {
        "name": "small_window",
        "structure": {f"prefix_{i:02d}": ["file.txt"] for i in range(10)},
        "path": "",
        "max_workers": 2,
        "look_ahead": 1,
    },
]


@pytest.mark.parametrize("provider_fixture", ["s3_provider", "s3_provider_rust"])
@pytest.mark.parametrize("case", STRUCTURE_CASES, ids=[case["name"] for case in STRUCTURE_CASES])
def test_structures_match_sequential(request, provider_fixture, case):
    provider = request.getfixturevalue(provider_fixture)
    created_keys = _create_test_files(provider, case["structure"])
    path = case.get("path", "")
    max_workers = case.get("max_workers")
    look_ahead = case.get("look_ahead")

    sequential = [obj.key for obj in provider.list_objects(path=path)]
    parallel_kwargs = {"path": path}
    if max_workers is not None:
        parallel_kwargs["max_workers"] = max_workers
    if look_ahead is not None:
        parallel_kwargs["look_ahead"] = look_ahead
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


@pytest.mark.parametrize("provider_fixture", ["s3_provider", "s3_provider_rust"])
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


def test_rust_client_enabled(s3_provider_rust):
    assert s3_provider_rust._rust_client is not None


class TestS3ParallelListingEdgeCases:
    """Edge cases that don't fit the parametrized sequential-vs-parallel pattern."""

    def test_invalid_start_after_end_at_raises(self, s3_provider):
        with pytest.raises(ValueError, match="start_after.*must be before end_at"):
            list(s3_provider.list_objects_recursive(start_after="z", end_at="a"))

    def test_flat_bucket_start_end_filters(self, s3_provider):
        s3_provider.put_object("a.txt", b"content")
        s3_provider.put_object("b.txt", b"content")
        s3_provider.put_object("c.txt", b"content")

        results = [obj.key for obj in s3_provider.list_objects_recursive(path="", start_after="a.txt", end_at="b.txt")]

        assert results == ["b.txt"]
