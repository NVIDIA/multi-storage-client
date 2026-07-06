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

import subprocess
import sys
import textwrap
from unittest.mock import patch

import pytest


def test_missing_numpy_dependency():
    # Mock the import system to raise ImportError when numpy is imported
    with patch.dict(sys.modules, {"numpy": None}):
        with pytest.raises(ImportError):
            import numpy  # noqa

        # This should not raise an error due to lazy import
        import multistorageclient as msc  # noqa


def test_virtual_manifest_provider_import_is_lazy_without_optional_dependencies() -> None:
    script = textwrap.dedent(
        """
        import builtins

        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name.split(".", 1)[0] in {"pyarrow", "requests"}:
                raise ImportError(f"blocked optional dependency: {name}")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = blocked_import

        import multistorageclient
        from multistorageclient.providers import ManifestStorageProvider, PosixFileStorageProvider
        from multistorageclient.manifest import virtual_manifest_v2_schema

        assert ManifestStorageProvider.__name__ == "ManifestStorageProvider"
        assert PosixFileStorageProvider.__name__ == "PosixFileStorageProvider"
        try:
            virtual_manifest_v2_schema()
        except ImportError as exc:
            assert "virtual-manifest" in str(exc)
        else:
            raise AssertionError("schema construction must require PyArrow")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr


def test_http_manifest_reader_reports_missing_requests_dependency_only_when_used() -> None:
    script = textwrap.dedent(
        """
        import builtins

        real_import = builtins.__import__

        def blocked_import(name, *args, **kwargs):
            if name.split(".", 1)[0] == "requests":
                raise ImportError(f"blocked optional dependency: {name}")
            return real_import(name, *args, **kwargs)

        builtins.__import__ = blocked_import

        from multistorageclient.manifest.http import HTTPServiceRangeReader
        from multistorageclient.types import Range

        reader = HTTPServiceRangeReader(
            base_url="https://service.example.test/v1",
            binding_identity="https://service.example.test/v1",
            allowed_path_prefixes=("render",),
            allowed_query_parameters=(),
        )
        reader.validate("render/file.bin", ())
        try:
            reader.read("render/file.bin", (), Range(0, 1), total_size=1)
        except ImportError as exc:
            assert "requests" in str(exc).lower()
            assert "virtual-manifest" in str(exc)
        else:
            raise AssertionError("HTTP reads must require requests")
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
