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

import functools
import os
import random
import uuid

import pytest

import multistorageclient.telemetry as telemetry
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient import StorageClient, StorageClientConfig
from multistorageclient.constants import MEMORY_LOAD_LIMIT


@pytest.fixture(scope="session")
def storage_client():
    with tempdatastore.TemporaryAWSS3Bucket() as temp_data_store:
        profile = "data"
        config_dict = {
            "profiles": {profile: temp_data_store.profile_config_dict()},
            "opentelemetry": {
                "metrics": {
                    "attributes": [
                        {"type": "static", "options": {"attributes": {"cluster": "local"}}},
                        {"type": "host", "options": {"attributes": {"node": "name"}}},
                        {"type": "process", "options": {"attributes": {"process": "pid"}}},
                    ],
                    "reader": {
                        "options": {
                            "collect_interval_millis": 10,
                            "collect_timeout_millis": 100,
                            "export_interval_millis": 1000,
                            "export_timeout_millis": 500,
                        }
                    },
                    "exporter": {
                        "type": "otlp",
                        "options": {
                            "endpoint": "http://127.0.0.1:8080/otlp/v1/metrics",
                            "headers": {"X-Scope-OrgID": "multi-storage-client"},
                        },
                    },
                },
            },
        }
        storage_client = StorageClient(
            config=StorageClientConfig.from_dict(
                config_dict=config_dict,
                profile=profile,
                telemetry_provider=functools.partial(telemetry.init, mode=telemetry.TelemetryMode.LOCAL),
            )
        )
        yield storage_client


@pytest.mark.usefixtures("storage_client")
def test_storage_client(storage_client: StorageClient) -> None:
    file_extension = ".txt"
    # Add a random string to the file path below so concurrent tests don't conflict.
    file_path_fragments = [f"{uuid.uuid4()}-prefix", f"suffix{file_extension}"]
    file_path = os.path.join(*file_path_fragments)
    file_body_bytes = b"\x00" * int(random.uniform(1, MEMORY_LOAD_LIMIT))
    file_copy_path_fragments = ["copy", *file_path_fragments]
    file_copy_path = os.path.join(*file_copy_path_fragments)

    # Check the file doesn't exist.
    try:
        storage_client.read(path=file_path)
    except Exception:
        pass
    try:
        storage_client.info(path=file_path)
    except Exception:
        pass
    try:
        storage_client.copy(src_path=file_path, dest_path=file_copy_path)
    except Exception:
        pass
    try:
        storage_client.delete(path=file_path)
    except Exception:
        pass

    # Write a file.
    storage_client.write(path=file_path, body=file_body_bytes)

    # Read the file.
    storage_client.read(path=file_path)

    # Read the file metadata.
    storage_client.info(path=file_path)

    # List the prefix directory.
    list(storage_client.list(prefix=os.path.join(*file_path_fragments[:1])))

    # Copy the file.
    storage_client.copy(src_path=file_path, dest_path=file_copy_path)

    # Read the file copy.
    storage_client.read(path=file_copy_path)

    # Read the file copy metadata.
    storage_client.info(path=file_copy_path)

    # Delete the file and its copy.
    for path in [file_path, file_copy_path]:
        storage_client.delete(path=path)
