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

import pytest

from multistorageclient.config import STORAGE_PROVIDER_MAPPING
from multistorageclient.schema import validate_config


def test_validate_profiles():
    # Invalid: incorrect type
    with pytest.raises(RuntimeError):
        validate_config({"profiles": "incorrect type"})

    # Invalid: missing storage_provider
    with pytest.raises(RuntimeError):
        validate_config({"profiles": {"default": {}}})

    # Invalid: storage_provider and provider_bundle cannot exist at the same time
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": {
                        "storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}},
                        "provider_bundle": {"type": "module.MyProviderBundle", "options": {}},
                    }
                }
            }
        )

    # Valid configurations
    for provider in STORAGE_PROVIDER_MAPPING.keys():
        validate_config(
            {
                "profiles": {
                    "default": {"storage_provider": {"type": provider, "options": {"base_path": "bucket/prefix"}}}
                }
            }
        )

    # Valid configurations for s3 and swiftstack with Rust client options
    for provider in ("s3", "s8k"):
        validate_config(
            {
                "profiles": {
                    "default": {
                        "storage_provider": {
                            "type": provider,
                            "options": {
                                "base_path": "bucket/prefix",
                                "endpoint_url": "http://localhost:9000",
                                "rust_client": {
                                    "allow_http": True,
                                },
                            },
                        }
                    }
                }
            }
        )


def test_validate_cache():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Invalid: incorrect properties
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "cache": {
                    "my_prop1": False,
                    "my_prop2": "x",
                },
            }
        )

    # Valid configurations
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "cache": {"eviction_policy": {"policy": "no_eviction"}},
        }
    )

    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "cache": {
                "size": "50M",
                "use_etag": True,
                "location": "/path/to/cache",
                "eviction_policy": {
                    "policy": "FIFO",
                    "refresh_interval": 300,
                },
            },
        }
    )

    # Test invalid eviction policy format
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "cache": {
                    "eviction_policy": "no_eviction"  # String format is no longer supported
                },
            }
        )


def test_validate_caching_enabled():
    """Test that caching_enabled field is properly validated in profiles."""
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Valid: caching_enabled as boolean
    validate_config(
        {
            "profiles": {
                "default": {**default_storage_provider, "caching_enabled": True},
            }
        }
    )

    validate_config(
        {
            "profiles": {
                "default": {**default_storage_provider, "caching_enabled": False},
            }
        }
    )

    # Invalid: caching_enabled as string
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": {**default_storage_provider, "caching_enabled": "true"},
                }
            }
        )

    # Invalid: caching_enabled as integer
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": {**default_storage_provider, "caching_enabled": 1},
                }
            }
        )


def test_validate_opentelemetry():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Invalid: incorrect properties
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "opentelemetry": {"logs": {"exporter": {"type": "console"}}},
            }
        )

    # Valid configurations
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "opentelemetry": {"metrics": {"exporter": {"type": "console"}}},
        }
    )


def test_validate_posix():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Valid: minimal posix configuration
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "posix": {
                "mountname": "msc-fuse-mount",
            },
        }
    )

    # Valid: full posix configuration
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "posix": {
                "mountname": "msc-fuse-mount",
                "mountpoint": "/mnt/msc",
                "allow_other": True,
                "auto_sighup_interval": 300,
            },
        }
    )

    # Valid: posix with default values
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "posix": {
                "mountname": "test-mount",
                "mountpoint": "/mnt",
                "allow_other": False,
                "auto_sighup_interval": 0,
            },
        }
    )

    # Valid: posix without mountname (optional field)
    validate_config(
        {
            "profiles": {
                "default": default_storage_provider,
            },
            "posix": {
                "mountpoint": "/mnt/msc",
                "allow_other": True,
            },
        }
    )

    # Valid: posix with various valid mountpoint paths
    for valid_path in ["/", "/mnt", "/tmp/msc", "/home/user/mounts", "/var/lib/msc"]:
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "mountpoint": valid_path,
                },
            }
        )

    # Invalid: incorrect type for mountname
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": 123,  # Should be string
                },
            }
        )

    # Invalid: incorrect type for allow_other
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "allow_other": "true",  # Should be boolean
                },
            }
        )

    # Invalid: negative auto_sighup_interval
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "auto_sighup_interval": -1,  # Should be >= 0
                },
            }
        )

    # Invalid: relative mountpoint path
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "mountpoint": "relative/path",  # Should be absolute path
                },
            }
        )

    # Invalid: mountpoint with double slashes
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "mountpoint": "/mnt//msc",  # Double slashes not allowed
                },
            }
        )

    # Invalid: mountpoint with null character
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "mountpoint": "/mnt\0msc",  # Null character not allowed
                },
            }
        )

    # Invalid: unknown property
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "posix": {
                    "mountname": "test-mount",
                    "unknown_property": "value",  # Not allowed
                },
            }
        )
