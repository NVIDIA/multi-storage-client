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
import hashlib
import os

import pytest

import multistorageclient
import multistorageclient.telemetry as telemetry
import test_multistorageclient.unit.utils.tempdatastore as tempdatastore
from multistorageclient.telemetry.attributes.base import collect_attributes
from multistorageclient.telemetry.attributes.environment_variables import EnvironmentVariablesAttributesProvider
from multistorageclient.telemetry.attributes.host import HostAttributesProvider
from multistorageclient.telemetry.attributes.msc_config import MSCConfigAttributesProvider
from multistorageclient.telemetry.attributes.process import ProcessAttributesProvider
from multistorageclient.telemetry.attributes.static import StaticAttributesProvider
from multistorageclient.telemetry.attributes.thread import ThreadAttributesProvider
from test_multistorageclient.unit.utils.telemetry.metrics.export import InMemoryMetricExporter


def test_environment_variables_attributes_provider():
    environment_variable_key, environment_variable_value = list(os.environ.items())[0]
    attribute_key = "attribute"
    attributes = EnvironmentVariablesAttributesProvider(
        attributes={attribute_key: environment_variable_key}
    ).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] == environment_variable_value


def test_host_attributes_provider():
    attribute_key = "host"
    attributes = HostAttributesProvider(attributes={attribute_key: "name"}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] is not None

    with pytest.raises(ValueError):
        HostAttributesProvider(attributes={"unsupported": "unsupported"})


def test_msc_config_attributes_provider():
    client_credential_value = "secret"
    client_credential_value_hash_algorithm = "sha3-256"
    client_credential_value_hash = hashlib.new(client_credential_value_hash_algorithm)
    client_credential_value_hash.update(client_credential_value.encode())
    attribute_key = "credentials"
    hashed_attribute_key = "credentials_hash"
    hashed_attribute_value = client_credential_value_hash.hexdigest()
    config_dict_path = "opentelemetry.metrics.exporter.options.auth.client_credential"
    attributes = MSCConfigAttributesProvider(
        attributes={
            attribute_key: {"expression": config_dict_path},
            hashed_attribute_key: {
                "expression": f"hash('{client_credential_value_hash_algorithm}', {config_dict_path})"
            },
        },
        config_dict={
            "opentelemetry": {
                "metrics": {"exporter": {"options": {"auth": {"client_credential": client_credential_value}}}}
            }
        },
    ).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] == client_credential_value
    assert hashed_attribute_key in attributes
    assert attributes[hashed_attribute_key] == hashed_attribute_value


def test_process_attributes_provider():
    attribute_key = "process"
    attributes = ProcessAttributesProvider(attributes={attribute_key: "pid"}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] is not None

    with pytest.raises(ValueError):
        ProcessAttributesProvider(attributes={"unsupported": "unsupported"})


def test_static_attributes_provider():
    attribute_key = "attribute"
    attribute_value = True
    attributes = StaticAttributesProvider(attributes={attribute_key: attribute_value}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] == attribute_value


def test_thread_attributes_provider():
    attribute_key = "thread"
    attributes = ThreadAttributesProvider(attributes={attribute_key: "native_id"}).attributes()
    assert attributes is not None
    assert attribute_key in attributes
    assert attributes[attribute_key] is not None

    with pytest.raises(ValueError):
        ThreadAttributesProvider(attributes={"unsupported": "unsupported"})


def test_collect_attributes():
    attribute_key_x = "x"
    attribute_key_y = "y"
    attribute_value_x = 1
    attribute_value_y = 1
    attribute_value_y_overlay = 2
    attributes_providers = [
        StaticAttributesProvider(attributes={attribute_key_x: attribute_value_x, attribute_key_y: attribute_value_y}),
        StaticAttributesProvider(attributes={attribute_key_y: attribute_value_y_overlay}),
    ]
    attributes = collect_attributes(attributes_providers=attributes_providers)
    assert attributes is not None
    assert attribute_key_x in attributes
    assert attributes[attribute_key_x] == attribute_value_x
    assert attribute_key_y in attributes
    assert attributes[attribute_key_y] == attribute_value_y_overlay


def test_storage_client_with_attributes_providers():
    with tempdatastore.TemporaryPOSIXDirectory() as temp_data_store:
        profile = "data"
        storage_client = multistorageclient.StorageClient(
            config=multistorageclient.StorageClientConfig.from_dict(
                config_dict={
                    "profiles": {
                        profile: temp_data_store.profile_config_dict(),
                    },
                    "opentelemetry": {
                        "metrics": {
                            "attributes": [
                                {
                                    "type": "environment_variables",
                                    "options": {"attributes": {"environment_variable": list(os.environ.keys())[0]}},
                                },
                                {"type": "host", "options": {"attributes": {"host": "name"}}},
                                {
                                    "type": "msc_config",
                                    "options": {
                                        "attributes": {
                                            "msc_config": {"expression": "opentelemetry.metrics.exporter.type"}
                                        }
                                    },
                                },
                                {"type": "process", "options": {"attributes": {"process": "pid"}}},
                                {"type": "static", "options": {"attributes": {"static": "static"}}},
                                {"type": "thread", "options": {"attributes": {"thread": "native_id"}}},
                            ],
                            "exporter": {"type": telemetry._fully_qualified_name(InMemoryMetricExporter)},
                        }
                    },
                },
                telemetry_provider=functools.partial(telemetry.init, mode=telemetry.TelemetryMode.LOCAL),
            )
        )

        storage_client.list()
