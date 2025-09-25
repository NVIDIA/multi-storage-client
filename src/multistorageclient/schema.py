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

from collections import Counter
from typing import Any

from jsonschema import validate

EXTENSION_SCHEMA = {
    "type": "object",
    "properties": {
        "type": {"type": "string"},
        "options": {
            "type": "object",
        },
    },
    "required": ["type"],
}

OTEL_SCHEMA = {
    "type": "object",
    "properties": {
        "metrics": {
            "type": "object",
            "properties": {
                "attributes": {"type": "array", "items": EXTENSION_SCHEMA},
                "reader": {
                    "type": "object",
                    "properties": {
                        "options": {
                            "type": "object",
                        },
                    },
                },
                "exporter": EXTENSION_SCHEMA,
            },
        },
        "traces": {
            "type": "object",
            "properties": {
                "exporter": {
                    "type": "object",
                    "properties": {
                        "type": {
                            "type": "string",
                            "enum": ["otlp", "console"],
                        },
                        "options": {
                            "type": "object",
                            "properties": {
                                "endpoint": {"type": "string", "format": "uri"},
                            },
                            "required": ["endpoint"],
                        },
                    },
                    "required": ["type"],
                }
            },
        },
    },
    "additionalProperties": False,
}

CACHE_SCHEMA = {
    "type": "object",
    "properties": {
        "size": {
            "type": "string",
            "pattern": "(?i)^[0-9]+[MGT]$",  # Accepts size with M, G suffix
        },
        "size_mb": {"type": "integer"},
        "location": {"type": "string"},
        "use_etag": {"type": "boolean"},
        "check_source_version": {"type": "boolean"},
        "cache_line_size": {
            "type": "string",
            "pattern": "(?i)^[0-9]+[MGT]$",  # Accepts size with M, G suffix
        },
        "eviction_policy": {
            "type": "object",
            "properties": {
                "policy": {
                    "type": "string",
                    "enum": ["lru", "fifo", "random", "no_eviction", "LRU", "FIFO", "RANDOM", "NO_EVICTION"],
                    "default": "lru",
                },
                "refresh_interval": {"type": "integer", "minimum": 300},
            },
        },
        "cache_backend": {  # Optional: If not specified, default cache backend will be used
            "type": "object",
            "properties": {"cache_path": {"type": "string"}},
        },
    },
    "additionalProperties": False,
}


PROFILE_SCHEMA = {
    "type": "object",
    "additionalProperties": {
        "type": "object",
        "properties": {
            "storage_provider": {
                "type": "object",
                "properties": {
                    "type": {
                        "type": "string",
                        "enum": ["ais", "azure", "file", "gcs", "gcs_s3", "oci", "s3", "s8k", "huggingface"],
                    },
                    "options": {
                        "type": "object",
                        "properties": {
                            "base_path": {"type": "string", "minLength": 0},
                            "rust_client": {"type": "object"},
                        },
                        "required": ["base_path"],
                    },
                },
                "required": ["type", "options"],
            },
            "replicas": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "replica_profile": {"type": "string"},
                        "read_priority": {"type": "integer", "minimum": 1},
                    },
                    "required": ["replica_profile", "read_priority"],
                },
                "uniqueItems": True,
            },
            "credentials_provider": EXTENSION_SCHEMA,
            "metadata_provider": EXTENSION_SCHEMA,
            "provider_bundle": EXTENSION_SCHEMA,
            "autocommit": {
                "type": "object",
                "properties": {
                    "interval_minutes": {"type": "number", "minimum": 0},
                    "at_exit": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
            "comment": {"type": "string"},
            "caching_enabled": {"type": "boolean", "default": False},
        },
        "oneOf": [
            {
                "required": ["storage_provider"],
            },
            {
                "required": ["provider_bundle"],
            },
        ],
    },
    "propertyNames": {
        "pattern": "^[^_].*$",  # Profile names must not start with an underscore to prevent collision with implicit profiles
    },
}

# Schema for the path_mapping section
PATH_MAPPING_SCHEMA = {
    "type": "object",
    "additionalProperties": {"type": "string", "pattern": "^msc://[^/]+/$"},
    "propertyNames": {"pattern": "^(/|[a-z][a-z0-9+.-]*://)[^/].*/$"},
}

# Schema for the posix section (FUSE mount configuration)
POSIX_SCHEMA = {
    "type": "object",
    "properties": {
        "mountname": {
            "type": "string",
            "description": "FUSE mount name that shows up in commands like mount(1) and df(1)",
        },
        "mountpoint": {
            "type": "string",
            "description": "File system path where the FUSE mount will be created",
            "pattern": "^/([^/\0]+/)*[^/\0]*$",  # Valid absolute path: starts with /, no null chars, no double slashes
            "default": "/mnt",
        },
        "allow_other": {
            "type": "boolean",
            "description": "Allows users other than the one launching the tool to see the mountpoint",
            "default": False,
        },
        "auto_sighup_interval": {
            "type": "integer",
            "minimum": 0,
            "description": "Time in seconds between config re-reads (defaults to 0 requiring an actual SIGHUP)",
            "default": 0,
        },
    },
    "additionalProperties": False,
}

CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "profiles": PROFILE_SCHEMA,
        "cache": CACHE_SCHEMA,
        "opentelemetry": OTEL_SCHEMA,
        "path_mapping": PATH_MAPPING_SCHEMA,
        "posix": POSIX_SCHEMA,
        "additionalProperties": False,
    },
    "required": ["profiles"],
}

BENCHMARK_SCHEMA = {
    "type": "object",
    "properties": {
        "processes": {"type": "array", "items": {"type": "integer"}},
        "threads": {"type": "array", "items": {"type": "integer"}},
        "test_object_sizes": {"type": "object", "additionalProperties": {"type": "integer"}},
    },
    "additionalProperties": False,
}


def validate_config(config_dict: dict[str, Any]) -> None:
    try:
        validate(instance=config_dict, schema=CONFIG_SCHEMA)
    except Exception as e:
        raise RuntimeError("Failed to validate the config file", e)

    # Custom validation: ensure replica_profile uniqueness within each profile's replicas
    profiles = config_dict.get("profiles", {})
    for profile_name, profile in profiles.items():
        replicas = profile.get("replicas", [])
        replica_profiles = [r.get("replica_profile") for r in replicas]
        counter = Counter(replica_profiles)
        duplicates = [rp for rp, count in counter.items() if count > 1]
        if duplicates:
            raise ValueError(f"Duplicate replica entry for profile '{duplicates[0]}'")
