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

import sys
from importlib.metadata import PackageNotFoundError
from unittest.mock import patch

import pytest


def test_missing_numpy_dependency():
    # Mock the import system to raise ImportError when numpy is imported
    with patch.dict(sys.modules, {"numpy": None}):
        with pytest.raises(ImportError):
            import numpy  # noqa

        # This should not raise an error due to lazy import
        import multistorageclient as msc  # noqa


def test_unknown_attribute_raises_attribute_error():
    import multistorageclient as msc

    with pytest.raises(AttributeError, match="has no attribute"):
        msc.__getattr__("not_a_contrib_module")


@pytest.mark.parametrize(
    ("attribute", "missing", "extra"),
    [
        ("zarr", "zarr", "zarr"),
        # multistorageclient.xarray imports zarr, so a missing zarr must point at the zarr extra.
        ("xarray", "zarr", "zarr"),
        ("hydra", "omegaconf", "hydra-core"),
        ("async_fs", "fsspec", "fsspec"),
    ],
)
def test_missing_contrib_dependency_names_extra(attribute, missing, extra, monkeypatch):
    import multistorageclient as msc

    # Pin the version so the zarr cases exercise the install hint rather than the 3.14 message,
    # and pin the reported Zarr version so they don't take the Zarr 3 branch either.
    monkeypatch.setattr(sys, "version_info", (3, 13, 0))
    error = ModuleNotFoundError(f"No module named '{missing}'", name=missing)
    with (
        patch("multistorageclient.version", return_value="2.18.7"),
        patch("multistorageclient.importlib.import_module", side_effect=error),
        pytest.raises(ImportError) as excinfo,
    ):
        msc.__getattr__(attribute)

    message = str(excinfo.value)
    assert f"multistorageclient.{attribute}" in message
    assert f"multi-storage-client[{extra}]" in message
    assert excinfo.value.__cause__ is error


def test_missing_zarr_on_python_314_explains_the_gap(monkeypatch):
    import multistorageclient as msc

    monkeypatch.setattr(sys, "version_info", (3, 14, 0))
    error = ModuleNotFoundError("No module named 'zarr'", name="zarr")
    with (
        patch("multistorageclient.importlib.import_module", side_effect=error),
        pytest.raises(ImportError) as excinfo,
    ):
        msc.__getattr__("zarr")

    message = str(excinfo.value)
    assert "Python 3.14" in message
    # The extra is empty on 3.14, so telling people to install it would be bad advice.
    assert "multi-storage-client[zarr]" not in message


def test_missing_zarr_on_python_314_ignores_the_proximate_failure(monkeypatch):
    # multistorageclient.contrib.zarr imports numpy before zarr, so on 3.14 the import fails on
    # numpy first. The 3.14 explanation must not depend on which module happened to be missing.
    import multistorageclient as msc

    monkeypatch.setattr(sys, "version_info", (3, 14, 0))
    error = ModuleNotFoundError("No module named 'numpy'", name="numpy")
    with (
        patch("multistorageclient.importlib.import_module", side_effect=error),
        pytest.raises(ImportError) as excinfo,
    ):
        msc.__getattr__("zarr")

    message = str(excinfo.value)
    assert "Python 3.14" in message
    # Pointing at the numpy extra would send the user round a loop: installing numpy just moves
    # the failure to the zarr import, which can never succeed on 3.14.
    assert "multi-storage-client[numpy]" not in message


@pytest.mark.parametrize("attribute", ["zarr", "xarray"])
def test_zarr_3_is_reported_as_unsupported(attribute, monkeypatch):
    # Zarr 3 removed zarr.storage.BaseStore. A missing *name* in an existing module raises plain
    # ImportError, not ModuleNotFoundError, so the handler has to catch the base class.
    import multistorageclient as msc

    monkeypatch.setattr(sys, "version_info", (3, 13, 0))
    error = ImportError("cannot import name 'BaseStore' from 'zarr.storage'", name="zarr.storage")
    with (
        patch("multistorageclient.version", return_value="3.1.0"),
        patch("multistorageclient.importlib.import_module", side_effect=error),
        pytest.raises(ImportError) as excinfo,
    ):
        msc.__getattr__(attribute)

    message = str(excinfo.value)
    assert f"multistorageclient.{attribute}" in message
    assert "Zarr 3.1.0 is installed" in message
    assert excinfo.value.__cause__ is error


def test_zarr_absent_is_not_reported_as_a_version_problem(monkeypatch):
    # importlib.metadata raises PackageNotFoundError when zarr isn't installed at all; that must
    # fall through to the plain "install the extra" hint.
    import multistorageclient as msc

    monkeypatch.setattr(sys, "version_info", (3, 13, 0))
    error = ModuleNotFoundError("No module named 'zarr'", name="zarr")
    with (
        patch("multistorageclient.version", side_effect=PackageNotFoundError("zarr")),
        patch("multistorageclient.importlib.import_module", side_effect=error),
        pytest.raises(ImportError) as excinfo,
    ):
        msc.__getattr__("zarr")

    assert "multi-storage-client[zarr]" in str(excinfo.value)


def test_unrecognized_missing_module_is_not_masked():
    import multistorageclient as msc

    error = ModuleNotFoundError("No module named 'some_internal_typo'", name="some_internal_typo")
    with (
        # Pin the reported Zarr version so a usable Zarr, not the Zarr 3 branch, is under test.
        patch("multistorageclient.version", return_value="2.18.7"),
        patch("multistorageclient.importlib.import_module", side_effect=error),
        pytest.raises(ModuleNotFoundError) as excinfo,
    ):
        msc.__getattr__("zarr")

    assert excinfo.value is error
