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

from datetime import datetime, timezone

import pytest

from multistorageclient.providers.manifest_formats import (
    JsonlManifestFormatHandler,
    ManifestFormat,
    ParquetManifestFormatHandler,
    get_format_handler,
)
from multistorageclient.types import ObjectMetadata

try:
    import pyarrow  # noqa: F401

    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False


def test_jsonl_format_round_trip():
    format_handler = JsonlManifestFormatHandler()
    assert format_handler.get_file_suffix() == ".jsonl"

    test_metadata = [
        ObjectMetadata(
            key="file1.txt",
            content_length=100,
            last_modified=datetime.now(timezone.utc),
            metadata={"tag": "value1"},
        ),
        ObjectMetadata(
            key="file2.txt",
            content_length=200,
            last_modified=datetime.now(timezone.utc),
            metadata={"tag": "value2"},
        ),
    ]

    content = format_handler.write_part(test_metadata)
    assert isinstance(content, bytes)

    result_metadata = format_handler.read_part(content)
    assert len(result_metadata) == 2
    assert result_metadata[0].key == "file1.txt"
    assert result_metadata[0].content_length == 100
    assert result_metadata[0].metadata == {"tag": "value1"}
    assert result_metadata[1].key == "file2.txt"
    assert result_metadata[1].content_length == 200
    assert result_metadata[1].metadata == {"tag": "value2"}


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not installed")
def test_parquet_format_round_trip():
    format_handler = ParquetManifestFormatHandler()
    assert format_handler.get_file_suffix() == ".parquet"

    test_metadata = [
        ObjectMetadata(
            key="file1.txt",
            content_length=100,
            last_modified=datetime.now(timezone.utc),
            metadata={"tag": "value1"},
        ),
        ObjectMetadata(
            key="file2.txt",
            content_length=200,
            last_modified=datetime.now(timezone.utc),
            metadata={"tag": "value2"},
        ),
    ]

    content = format_handler.write_part(test_metadata)
    assert isinstance(content, bytes)

    result_metadata = format_handler.read_part(content)
    assert len(result_metadata) == 2
    assert result_metadata[0].key == "file1.txt"
    assert result_metadata[0].content_length == 100
    assert result_metadata[0].metadata == {"tag": "value1"}
    assert result_metadata[1].key == "file2.txt"
    assert result_metadata[1].content_length == 200
    assert result_metadata[1].metadata == {"tag": "value2"}


def test_get_format_handler_jsonl():
    handler = get_format_handler(ManifestFormat.JSONL)
    assert isinstance(handler, JsonlManifestFormatHandler)
    handler = get_format_handler("jsonl")
    assert isinstance(handler, JsonlManifestFormatHandler)


@pytest.mark.skipif(not PYARROW_AVAILABLE, reason="PyArrow not installed")
def test_get_format_handler_parquet():
    handler = get_format_handler(ManifestFormat.PARQUET)
    assert isinstance(handler, ParquetManifestFormatHandler)
    handler = get_format_handler("parquet")
    assert isinstance(handler, ParquetManifestFormatHandler)


def test_get_format_handler_unsupported():
    with pytest.raises(ValueError, match="Unsupported manifest format"):
        get_format_handler("unsupported")
