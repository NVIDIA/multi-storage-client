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

import tempfile
import uuid

import pytest

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL
from test_multistorageclient.unit.utils import config, tempdatastore


@pytest.fixture
def sample_data():
    import numpy as np

    return np.array([1, 2, 3, 4, 5], dtype=np.int32)


def test_numpy_memmap(file_storage_config_with_cache, sample_data):
    import numpy as np

    with tempfile.NamedTemporaryFile(delete=True, mode="wb") as temp:
        sample_data.tofile(temp.name)  # save as raw binary

        # test file path
        result = msc.numpy.memmap(temp.name, dtype=np.int32, mode="r", shape=(5,))
        assert np.array_equal(result, sample_data)

        # test msc-prefixed path
        result = msc.numpy.memmap(f"{MSC_PROTOCOL}default{temp.name}", dtype=np.int32, mode="r", shape=(5,))
        assert np.array_equal(result, sample_data)

        # test MultiStoragePath
        result = msc.numpy.memmap(msc.Path(temp.name), dtype=np.int32, mode="r", shape=(5,))
        assert np.array_equal(result, sample_data)

        # test file object
        with open(temp.name) as fp:
            result = msc.numpy.memmap(fp, dtype=np.int32, mode="r", shape=(5,))
            assert np.array_equal(result, sample_data)

        # test default mode
        result = msc.numpy.memmap(temp.name, dtype=np.int32, shape=(5,))
        assert np.array_equal(result, sample_data)

        # test incorrect argument
        with pytest.raises(TypeError):
            _ = msc.numpy.memmap(filename=temp.name, dtype=np.int32, mode="r", shape=(5,))

        # mismatch mode should fail: default mode of memmap function is r+
        with pytest.raises(PermissionError):
            with open(temp.name, mode="r") as fp:
                result = msc.numpy.memmap(fp, dtype=np.int32, shape=(5,))


def test_numpy_load(file_storage_config_with_cache, sample_data):
    import numpy as np

    with tempfile.NamedTemporaryFile(delete=True, mode="wb", suffix=".npy") as temp:
        np.save(temp.name, sample_data)  # save as .npy file

        # test file path
        result = msc.numpy.load(temp.name, allow_pickle=True, mmap_mode="r")
        assert np.array_equal(result, sample_data)

        # test msc-prefixed path
        result = msc.numpy.load(f"{MSC_PROTOCOL}default{temp.name}", allow_pickle=True, mmap_mode="r")
        assert np.array_equal(result, sample_data)

        # test MultiStoragePath
        result = msc.numpy.load(msc.Path(temp.name), allow_pickle=True, mmap_mode="r")
        assert np.array_equal(result, sample_data)

        # test file object
        with open(temp.name, "rb") as fp:
            with pytest.raises(ValueError):
                _ = msc.numpy.load(fp, allow_pickle=True, mmap_mode="r")  # memmap mode is not supported for file handle

            result = msc.numpy.load(fp, allow_pickle=True)
            assert np.array_equal(result, sample_data)


def test_numpy_save(file_storage_config_with_cache, sample_data):
    import numpy as np

    with tempfile.NamedTemporaryFile(delete=True, mode="wb", suffix=".npy") as temp:
        # Test file path
        msc.numpy.save(temp.name, sample_data)

        result = np.load(temp.name)
        assert np.array_equal(result, sample_data)

        # Test msc-prefixed path
        msc_path = f"{MSC_PROTOCOL}default{temp.name}"
        msc.numpy.save(msc_path, sample_data)

        result = np.load(temp.name)
        assert np.array_equal(result, sample_data)

        # Test MultiStoragePath
        msc.numpy.save(msc.Path(temp.name), sample_data)

        result = np.load(temp.name)
        assert np.array_equal(result, sample_data)

        # test file object
        with open(temp.name, "wb") as fp:
            msc.numpy.save(fp, sample_data)

            result = np.load(temp.name)
            assert np.array_equal(result, sample_data)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_numpy_save_with_attributes(
    file_storage_config_with_cache, temp_data_store_type: type[tempdatastore.TemporaryDataStore], sample_data
):
    """Test numpy.save with attributes functionality."""
    import numpy as np

    msc.shortcuts._STORAGE_CLIENT_CACHE.clear()

    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                },
            }
        )

        test_uuid = str(uuid.uuid4())
        file_path = f"test-numpy-attributes-{test_uuid}.npy"

        test_attributes = {
            "method": "numpy.save",
            "version": "1.0",
            "test_id": test_uuid,
        }

        try:
            # Test save with attributes using file path as first positional arg
            msc.numpy.save(f"{MSC_PROTOCOL}test/{file_path}", sample_data, attributes=test_attributes)

            # Verify content was written correctly
            result = msc.numpy.load(f"{MSC_PROTOCOL}test/{file_path}")
            assert np.array_equal(result, sample_data)

            # Verify attributes for storage providers that support metadata
            if hasattr(temp_data_store, "_bucket_name"):
                metadata = msc.info(f"{MSC_PROTOCOL}test/{file_path}")
                assert metadata is not None
                assert metadata.metadata is not None

                for key, value in test_attributes.items():
                    assert key in metadata.metadata, f"Expected attribute '{key}' not found"
                    assert metadata.metadata[key] == value, f"Attribute '{key}' has incorrect value"

            # Test save with attributes using MultiStoragePath
            file_path2 = f"test-numpy-attributes-path-{test_uuid}.npy"
            msc.numpy.save(msc.Path(f"{MSC_PROTOCOL}test/{file_path2}"), sample_data, attributes=test_attributes)

            result = msc.numpy.load(msc.Path(f"{MSC_PROTOCOL}test/{file_path2}"))
            assert np.array_equal(result, sample_data)

            # Test save with attributes using file kwarg
            file_path3 = f"test-numpy-attributes-kwarg-{test_uuid}.npy"
            msc.numpy.save(file=f"{MSC_PROTOCOL}test/{file_path3}", arr=sample_data, attributes=test_attributes)

            result = msc.numpy.load(f"{MSC_PROTOCOL}test/{file_path3}")
            assert np.array_equal(result, sample_data)

        finally:
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{file_path}")
                msc.delete(f"{MSC_PROTOCOL}test/{file_path2}")
                msc.delete(f"{MSC_PROTOCOL}test/{file_path3}")
            except Exception:
                pass
