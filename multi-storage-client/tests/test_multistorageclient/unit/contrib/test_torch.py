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

import uuid

import pytest
import torch
import torch.distributed.checkpoint as dcp

import multistorageclient as msc
from multistorageclient.types import MSC_PROTOCOL
from test_multistorageclient.unit.utils import config, tempdatastore


@pytest.fixture
def sample_data(tmp_path):
    # Create a small tensor
    tensor = torch.tensor([1, 2, 3, 4])
    # Create a filepath
    filepath = tmp_path / "test.pt"
    # Save the tensor to the file
    msc.torch.save(tensor, filepath)
    return filepath, tensor


def test_torch_load_with_filepath(sample_data):
    filepath, expected_tensor = sample_data

    result = msc.torch.load(str(filepath))
    assert torch.equal(result, expected_tensor)


def test_torch_load_with_msc_prefix(sample_data):
    filepath, expected_tensor = sample_data

    result = msc.torch.load(f"{MSC_PROTOCOL}default{filepath}")
    assert torch.equal(result, expected_tensor)


def test_torch_save_with_msc_path(sample_data):
    filepath, expected_tensor = sample_data

    msc.torch.save(expected_tensor, msc.Path(filepath))
    result = torch.load(filepath)
    assert torch.equal(result, expected_tensor)


def test_torch_load_with_msc_path(sample_data):
    filepath, expected_tensor = sample_data

    result = msc.torch.load(msc.Path(filepath))
    assert torch.equal(result, expected_tensor)


class SimpleModel(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = torch.nn.Linear(10, 2)

    def forward(self, x):
        return self.linear(x)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryAzureBlobStorageContainer],
        [tempdatastore.TemporaryGoogleCloudStorageBucket],
        [tempdatastore.TemporarySwiftStackBucket],
    ],
)
def test_filesystem_reader_writer(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                }
            }
        )

        # Save model checkpoint
        model = SimpleModel()
        state_dict = {"model": model}

        writer = msc.torch.MultiStorageFileSystemWriter("msc://test/checkpoint/1")
        dcp.save(  # type: ignore[reportPrivateImportUsage]
            state_dict=state_dict,
            storage_writer=writer,
        )

        # Load model checkpoint
        loaded_model = SimpleModel()
        loaded_state_dict = {"model": loaded_model}

        reader = msc.torch.MultiStorageFileSystemReader("msc://test/checkpoint/1", thread_count=2)
        dcp.load(  # type: ignore[reportPrivateImportUsage]
            state_dict=loaded_state_dict,
            storage_reader=reader,
        )

        # Compare the state dictionaries
        assert "model" in loaded_state_dict and "model" in state_dict

        # Get the state_dict from both models to compare parameters
        original_state_dict = state_dict["model"].state_dict()
        loaded_state_dict_params = loaded_state_dict["model"].state_dict()

        # Verify both state dictionaries have the same keys
        assert set(original_state_dict.keys()) == set(loaded_state_dict_params.keys())

        # Compare each parameter tensor
        for param_name in original_state_dict:
            assert torch.equal(original_state_dict[param_name], loaded_state_dict_params[param_name]), (
                f"Parameter {param_name} does not match"
            )


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_filesystem_basic_operations(temp_data_store_type):
    with temp_data_store_type() as temp_data_store:
        config.setup_msc_config(
            config_dict={
                "profiles": {
                    "test": temp_data_store.profile_config_dict(),
                }
            }
        )

        fs = msc.torch.MultiStorageFileSystem()

        # Test concat_path
        path = "msc://test/path"
        suffix = "file.txt"
        concat_path = fs.concat_path(path, suffix)
        assert str(concat_path) == "msc://test/path/file.txt"

        # Test init_path
        init_path = fs.init_path(path)
        assert str(init_path) == path

        # Test create_stream for writing
        test_file = "msc://test/new_directory/test.txt"
        with fs.create_stream(test_file, "wb") as stream:
            stream.write(b"test content")

        # Test exists for file
        assert fs.exists(test_file)

        # Test exists for directory
        test_dir = "msc://test/new_directory"
        assert fs.exists(test_dir)

        # Test create_stream for reading
        with fs.create_stream(test_file, "rb") as stream:
            content = stream.read()
            assert content == b"test content"

        # Test ls
        listing = fs.ls("msc://test/new_directory")
        assert len(listing) == 1
        assert listing[0].endswith("test.txt")

        # Test rename
        new_file_path = "msc://test/new_directory/renamed.txt"
        fs.rename(test_file, new_file_path)
        assert not fs.exists(test_file)
        assert fs.exists(new_file_path)

        # Test rm_file
        fs.rm_file(new_file_path)
        assert not fs.exists(new_file_path)


@pytest.mark.parametrize(
    argnames=["temp_data_store_type"],
    argvalues=[
        [tempdatastore.TemporaryAWSS3Bucket],
        [tempdatastore.TemporaryPOSIXDirectory],
    ],
)
def test_torch_save_with_attributes(temp_data_store_type: type[tempdatastore.TemporaryDataStore]):
    """Test torch.save with attributes functionality."""
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
        file_path = f"test-torch-attributes-{test_uuid}.pt"
        tensor = torch.tensor([1, 2, 3, 4])

        test_attributes = {
            "method": "torch.save",
            "version": "1.0",
            "test_id": test_uuid,
        }

        try:
            # Test save with attributes using msc-prefixed path
            msc.torch.save(tensor, f"{MSC_PROTOCOL}test/{file_path}", attributes=test_attributes)

            # Verify content was written correctly
            result = msc.torch.load(f"{MSC_PROTOCOL}test/{file_path}")
            assert torch.equal(result, tensor)

            # Verify attributes for storage providers that support metadata
            if hasattr(temp_data_store, "_bucket_name"):
                metadata = msc.info(f"{MSC_PROTOCOL}test/{file_path}")
                assert metadata is not None
                assert metadata.metadata is not None

                for key, value in test_attributes.items():
                    assert key in metadata.metadata, f"Expected attribute '{key}' not found"
                    assert metadata.metadata[key] == value, f"Attribute '{key}' has incorrect value"

            # Test save with attributes using MultiStoragePath
            file_path2 = f"test-torch-attributes-path-{test_uuid}.pt"
            msc.torch.save(tensor, msc.Path(f"{MSC_PROTOCOL}test/{file_path2}"), attributes=test_attributes)

            result = msc.torch.load(msc.Path(f"{MSC_PROTOCOL}test/{file_path2}"))
            assert torch.equal(result, tensor)

        finally:
            try:
                msc.delete(f"{MSC_PROTOCOL}test/{file_path}")
                msc.delete(f"{MSC_PROTOCOL}test/{file_path2}")
            except Exception:
                pass
