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

import os
import tempfile

import pytest
import zarr

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL


@pytest.fixture
def sample_zarr_data():
    """Fixture to generate some sample Zarr data."""
    temp_dir = tempfile.TemporaryDirectory()

    store_path = os.path.join(temp_dir.name, "test_zarr.zarr")
    root = zarr.open(store_path, mode="w")
    assert isinstance(root, zarr.Group)

    array1 = root.create_dataset("array1", shape=(100, 100), dtype="int32")
    array2 = root.create_dataset("array2", shape=(50, 50), dtype="float64")
    array1[:] = 1
    array2[:] = 2.0

    # Add the _ARRAY_DIMENSIONS attribute for xarray compatibility
    array1.attrs["_ARRAY_DIMENSIONS"] = ["dim_0_100", "dim_1_100"]
    array2.attrs["_ARRAY_DIMENSIONS"] = ["dim_0_50", "dim_1_50"]

    zarr.consolidate_metadata(root.store)

    yield store_path
    temp_dir.cleanup()


def test_xarray_open_zarr(sample_zarr_data, file_storage_config):
    zarr_paths = [sample_zarr_data, f"{MSC_PROTOCOL}default{sample_zarr_data}/"]
    for path in zarr_paths:
        if path.startswith(MSC_PROTOCOL):
            xr_dataset = msc.xarray.open_zarr(path, msc_max_workers=4)
        else:
            xr_dataset = msc.xarray.open_zarr(path)
        assert "array1" in xr_dataset
        assert "array2" in xr_dataset
        array1 = xr_dataset["array1"]
        array2 = xr_dataset["array2"]
        assert array1.shape == (100, 100)
        assert array2.shape == (50, 50)
        assert (array1[:] == 1).all(), "array1 should contain all 1s"
        assert (array2[:] == 2.0).all(), "array2 should contain all 2.0s"
