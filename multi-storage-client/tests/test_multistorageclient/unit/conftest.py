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

# pytest fixtures.
#
# https://docs.pytest.org/en/stable/reference/fixtures.html#conftest-py-sharing-fixtures-across-multiple-files

import os
import tempfile
import uuid

import pytest

CONFIG_DIR = tempfile.gettempdir()

CONFIG_YAML = """
profiles:
  default:
    storage_provider:
      type: file
      options:
        base_path: /
"""

CONFIG_YAML_WITH_CACHE = """
profiles:
  default:
    storage_provider:
      type: file
      options:
        base_path: /
cache:
  eviction_policy:
    policy: no_eviction
"""

CONFIG_YAML_WITH_PATH_MAPPING = """
profiles:
  default:
    storage_provider:
      type: file
      options:
        base_path: /
  file-a:
    storage_provider:
      type: file
      options:
        base_path: /lustrefs/a/
  file-a-b:
    storage_provider:
      type: file
      options:
        base_path: /lustrefs/a/b/
  s3-bucket1:
    storage_provider:
      type: s3
      options:
        base_path: bucket1
  s3-bucket1-a-b:
    storage_provider:
      type: s3
      options:
        base_path: bucket1/a/b
  azure-new-bucket-456:
    storage_provider:
      type: azure
      options:
        base_path: new-bucket-456
        endpoint_url: https://new-bucket-456.blob.core.windows.net
path_mapping:
  /lustrefs/a/b/: msc://file-a-b/
  /lustrefs/a/: msc://file-a/
  s3://bucket1/: msc://s3-bucket1/
  s3://bucket1/a/b/: msc://s3-bucket1-a-b/
  s3://old-bucket-123/: msc://azure-new-bucket-456/
"""


def setup_config_file(config_json):
    config_filename = os.path.join(CONFIG_DIR, f"msc_config-{uuid.uuid4().hex}.yaml")
    with open(config_filename, "w") as fp:
        fp.write(config_json)

    os.environ["MSC_CONFIG"] = config_filename
    return config_filename


def delete_config_file(config_filename):
    os.unlink(config_filename)


@pytest.fixture
def file_storage_config():
    config_filename = setup_config_file(CONFIG_YAML)
    yield config_filename
    delete_config_file(config_filename)


@pytest.fixture
def file_storage_config_with_cache():
    config_filename = setup_config_file(CONFIG_YAML_WITH_CACHE)
    yield config_filename
    delete_config_file(config_filename)


@pytest.fixture
def file_storage_config_with_path_mapping():
    config_filename = setup_config_file(CONFIG_YAML_WITH_PATH_MAPPING)
    yield config_filename
    delete_config_file(config_filename)


@pytest.fixture(autouse=True, scope="function")
def reset_globals():
    # Reset the instance cache before each test.
    from multistorageclient import shortcuts

    with shortcuts._STORAGE_CLIENT_CACHE_LOCK:
        shortcuts._STORAGE_CLIENT_CACHE.clear()

    # Preserve MSC environment variables for testing
    msc_num_processes = os.environ.get("MSC_NUM_PROCESSES")
    msc_num_threads_per_process = os.environ.get("MSC_NUM_THREADS_PER_PROCESS")
    msc_max_workers = os.environ.get("MSC_MAX_WORKERS")

    # Reset the environment variables before each test.
    os.environ.clear()

    # Restore MSC environment variables
    if msc_num_processes is not None:
        os.environ["MSC_NUM_PROCESSES"] = msc_num_processes
    if msc_num_threads_per_process is not None:
        os.environ["MSC_NUM_THREADS_PER_PROCESS"] = msc_num_threads_per_process
    if msc_max_workers is not None:
        os.environ["MSC_MAX_WORKERS"] = msc_max_workers

    yield
