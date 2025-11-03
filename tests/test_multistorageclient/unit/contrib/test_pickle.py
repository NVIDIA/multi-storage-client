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
import pickle
import tempfile
import uuid

import pytest

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL
from test_multistorageclient.unit.utils import config, tempdatastore


@pytest.fixture
def sample_data():
    return {"key": "value", "number": 42}


@pytest.fixture
def pickle_file_path(sample_data):
    with tempfile.NamedTemporaryFile(delete=False, mode="wb") as temp:
        pickle.dump(sample_data, temp)
    yield temp.name
    os.unlink(temp.name)


def test_pickle_load(pickle_file_path, sample_data):
    # test load with file path
    result = msc.pickle.load(pickle_file_path)
    assert result == sample_data

    # test load with msc-prefixed file path
    msc_path = f"{MSC_PROTOCOL}default{pickle_file_path}"
    result = msc.pickle.load(msc_path)
    assert result == sample_data

    # test load with MultiStoragePath
    result = msc.pickle.load(msc.Path(pickle_file_path))
    assert result == sample_data

    # test load with file object
    with open(pickle_file_path, "rb") as f:
        result = msc.pickle.load(f)
    assert result == sample_data


def test_pickle_dump(sample_data):
    with tempfile.NamedTemporaryFile(delete=True) as temp:
        msc_path = f"{MSC_PROTOCOL}default{temp.name}"

        # Test dump with msc-prefixed file path
        msc.pickle.dump(sample_data, msc_path)
        result = msc.pickle.load(msc_path)
        assert result == sample_data

        # Test dump with normal file path
        msc.pickle.dump(sample_data, temp.name)
        result = msc.pickle.load(temp.name)
        assert result == sample_data

        # test dump with MultiStoragePath
        msc.pickle.dump(sample_data, msc.Path(temp.name))
        result = msc.pickle.load(msc.Path(temp.name))
        assert result == sample_data

        # Test dump with msc.open (file-like object)
        with pytest.raises(NotImplementedError):
            msc.pickle.dump(sample_data, msc.open(msc_path, "wb"))


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_pickle_dump_with_attributes(temp_data_store_type: type[tempdatastore.TemporaryDataStore], sample_data):
    """Test pickle.dump with attributes functionality."""
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
        file_path = f"test-pickle-attributes-{test_uuid}.pkl"

        test_attributes = {
            "method": "pickle.dump",
            "version": "1.0",
            "test_id": test_uuid,
        }

        try:
            # Test dump with attributes using msc-prefixed path
            msc.pickle.dump(sample_data, f"{MSC_PROTOCOL}test/{file_path}", attributes=test_attributes)

            # Verify content was written correctly
            result = msc.pickle.load(f"{MSC_PROTOCOL}test/{file_path}")
            assert result == sample_data

            # Verify attributes for storage providers that support metadata
            if hasattr(temp_data_store, "_bucket_name"):
                metadata = msc.info(f"{MSC_PROTOCOL}test/{file_path}")
                assert metadata is not None
                assert metadata.metadata is not None

                for key, value in test_attributes.items():
                    assert key in metadata.metadata, f"Expected attribute '{key}' not found"
                    assert metadata.metadata[key] == value, f"Attribute '{key}' has incorrect value"

            # Test dump with attributes using MultiStoragePath
            file_path2 = f"test-pickle-attributes-path-{test_uuid}.pkl"
            msc.pickle.dump(sample_data, msc.Path(f"{MSC_PROTOCOL}test/{file_path2}"), attributes=test_attributes)

            result = msc.pickle.load(msc.Path(f"{MSC_PROTOCOL}test/{file_path2}"))
            assert result == sample_data

        finally:
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{file_path}")
                msc.delete(f"{MSC_PROTOCOL}test/{file_path2}")
            except Exception:
                pass
