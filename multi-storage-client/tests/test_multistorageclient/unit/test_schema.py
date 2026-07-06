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
    for provider in STORAGE_PROVIDER_MAPPING.keys() - {"manifest"}:
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


@pytest.mark.parametrize(
    "profile_name",
    [".range-cache-user", ".cache_refresh.lock", "project.v2+preview@west"],
)
def test_validate_profiles_accepts_legal_names_that_resemble_cache_internals(profile_name: str) -> None:
    """Cache bookkeeping only reserves names that collide with actual cache paths."""
    validate_config(
        {"profiles": {profile_name: {"storage_provider": {"type": "file", "options": {"base_path": "/objects"}}}}}
    )


@pytest.mark.parametrize("profile_name", [".tmp-user", ".tmp-legacy-profile", ".tmp-"])
def test_validate_profiles_rejects_names_reserved_for_legacy_temp_downloads(profile_name: str) -> None:
    """Schema validation rejects names that collide with legacy temporary-download directories."""
    with pytest.raises(RuntimeError, match="Failed to validate"):
        validate_config(
            {"profiles": {profile_name: {"storage_provider": {"type": "file", "options": {"base_path": "/objects"}}}}}
        )


@pytest.mark.parametrize(
    "profile_name",
    [".msc-cache-internal", "nested/profile", r"nested\\profile", "nested/./..", ".", ".."],
)
def test_validate_profiles_rejects_names_that_alias_cache_paths(profile_name: str) -> None:
    """Configured profile keys must be safe single components outside the exact internal root."""
    with pytest.raises(RuntimeError, match="Failed to validate"):
        validate_config(
            {"profiles": {profile_name: {"storage_provider": {"type": "file", "options": {"base_path": "/objects"}}}}}
        )


def _manifest_schema_config() -> dict:
    return {
        "profiles": {
            "manifest-store": {"storage_provider": {"type": "file", "options": {"base_path": "/manifests"}}},
            "objects": {"storage_provider": {"type": "file", "options": {"base_path": "/objects"}}},
            "virtual": {
                "storage_provider": {
                    "type": "manifest",
                    "options": {
                        "manifest_storage_profile": "manifest-store",
                        "manifest_path": "datasets/catalog.parquet",
                        "max_workers": 8,
                        "source_profiles": {"objects": {"profile": "objects", "binding_revision": "objects-r1"}},
                        "services": {
                            "renderer": {
                                "type": "http",
                                "options": {
                                    "base_url": "https://renderer.example/v1/",
                                    "binding_revision": "renderer-r1",
                                    "allowed_path_prefixes": ["render/"],
                                    "allowed_query_parameters": ["frame", "variant"],
                                    "headers": {"Authorization": "Bearer ${RENDERER_TOKEN}"},
                                    "connect_timeout_seconds": 2,
                                    "read_timeout_seconds": 10,
                                    "verify_tls": True,
                                    "allow_insecure_http": False,
                                },
                            }
                        },
                    },
                },
                "caching_enabled": True,
                "comment": "read-only virtual files",
            },
        }
    }


def test_validate_manifest_profile() -> None:
    validate_config(_manifest_schema_config())


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("manifest_path", "/absolute/catalog.parquet"),
        ("manifest_path", "./catalog.parquet"),
        ("manifest_path", "../catalog.parquet"),
        ("manifest_path", "datasets/./catalog.parquet"),
        ("manifest_path", "datasets//catalog.parquet"),
        ("manifest_path", "datasets\\catalog.parquet"),
        ("manifest_path", "catalog.parquet\n"),
        ("manifest_path", "datasets/\x00catalog.parquet"),
        ("manifest_path", "datasets/catalog\t.parquet"),
        ("manifest_path", "datasets/catalog.json"),
        ("max_workers", 0),
        ("rust_client", {"allow_http": True}),
    ],
)
def test_validate_manifest_profile_rejects_invalid_options(option: str, value: object) -> None:
    config = _manifest_schema_config()
    config["profiles"]["virtual"]["storage_provider"]["options"][option] = value

    with pytest.raises(RuntimeError):
        validate_config(config)


@pytest.mark.parametrize("manifest_path", ["catalog.parquet", "datasets/2026/catalog.parquet", "日本語/data.parquet"])
def test_validate_manifest_profile_accepts_normalized_relative_parquet_paths(manifest_path: str) -> None:
    config = _manifest_schema_config()
    config["profiles"]["virtual"]["storage_provider"]["options"]["manifest_path"] = manifest_path

    validate_config(config)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("credentials_provider", {"type": "S3Credentials"}),
        ("metadata_provider", {"type": "manifest"}),
        ("replicas", []),
        ("storage_provider_profiles", ["objects"]),
        ("provider_bundle", {"type": "module.ProviderBundle"}),
        ("autocommit", {"at_exit": True}),
    ],
)
def test_validate_manifest_profile_rejects_logical_routing_and_mutation_fields(field: str, value: object) -> None:
    config = _manifest_schema_config()
    config["profiles"]["virtual"][field] = value

    with pytest.raises(RuntimeError):
        validate_config(config)


@pytest.mark.parametrize(
    "binding",
    [
        {"profile": "objects"},
        {"binding_revision": "objects-r1"},
        {"profile": "objects", "binding_revision": ""},
        {"profile": "objects", "binding_revision": "objects-r1", "secret": "not-allowed"},
    ],
)
def test_validate_manifest_source_binding_requires_closed_revisioned_shape(binding: dict) -> None:
    config = _manifest_schema_config()
    config["profiles"]["virtual"]["storage_provider"]["options"]["source_profiles"] = {"objects": binding}

    with pytest.raises(RuntimeError):
        validate_config(config)


@pytest.mark.parametrize(
    "required_field",
    ["base_url", "binding_revision", "allowed_path_prefixes", "allowed_query_parameters"],
)
def test_validate_manifest_http_service_requires_each_transport_contract_field(required_field: str) -> None:
    config = _manifest_schema_config()
    options = config["profiles"]["virtual"]["storage_provider"]["options"]["services"]["renderer"]["options"]
    del options[required_field]

    with pytest.raises(RuntimeError):
        validate_config(config)


def test_validate_manifest_http_service_requires_a_nonempty_path_allowlist() -> None:
    config = _manifest_schema_config()
    options = config["profiles"]["virtual"]["storage_provider"]["options"]["services"]["renderer"]["options"]
    options["allowed_path_prefixes"] = []

    with pytest.raises(RuntimeError):
        validate_config(config)


@pytest.mark.parametrize("binding_kind", ["source_profiles", "services"])
@pytest.mark.parametrize(
    "alias",
    ["", ".", "..", "nested/alias", " leading", "trailing ", "objects\n", "objects\t", "objects\x00"],
)
def test_validate_manifest_binding_aliases_are_normalized_identifiers(binding_kind: str, alias: str) -> None:
    config = _manifest_schema_config()
    options = config["profiles"]["virtual"]["storage_provider"]["options"]
    binding = options[binding_kind].pop("objects" if binding_kind == "source_profiles" else "renderer")
    options[binding_kind][alias] = binding

    with pytest.raises(RuntimeError):
        validate_config(config)


@pytest.mark.parametrize("alias", ["objects", "objects-primary", "objects.v2", "objects_2"])
def test_validate_manifest_binding_aliases_accept_stable_identifier_characters(alias: str) -> None:
    config = _manifest_schema_config()
    options = config["profiles"]["virtual"]["storage_provider"]["options"]
    binding = options["source_profiles"].pop("objects")
    options["source_profiles"][alias] = binding

    validate_config(config)


def test_validate_storage_provider_profiles():
    metadata_provider = {"metadata_provider": {"type": "module.MyMetadataProvider"}}

    # Valid: storage_provider_profiles with metadata_provider
    validate_config(
        {
            "profiles": {
                "composite": {
                    "storage_provider_profiles": ["loc1", "loc2"],
                    **metadata_provider,
                }
            }
        }
    )

    # Invalid: storage_provider_profiles without metadata_provider
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "composite": {
                        "storage_provider_profiles": ["loc1", "loc2"],
                    }
                }
            }
        )

    # Invalid: storage_provider_profiles with empty array
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "composite": {
                        "storage_provider_profiles": [],
                        **metadata_provider,
                    }
                }
            }
        )

    # Invalid: storage_provider_profiles with storage_provider (conflict)
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "composite": {
                        "storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}},
                        "storage_provider_profiles": ["loc1", "loc2"],
                        **metadata_provider,
                    }
                }
            }
        )

    # Invalid: storage_provider_profiles with provider_bundle (conflict)
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "composite": {
                        "provider_bundle": {"type": "module.MyProviderBundle"},
                        "storage_provider_profiles": ["loc1", "loc2"],
                        **metadata_provider,
                    }
                }
            }
        )

    # Invalid: storage_provider_profiles with non-string items
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "composite": {
                        "storage_provider_profiles": ["loc1", 123],
                        **metadata_provider,
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
                "check_source_version": True,
                "location": "/path/to/cache",
                "eviction_policy": {
                    "policy": "FIFO",
                    "refresh_interval": 1,
                },
            },
        }
    )

    with pytest.raises(ValueError, match="cache.use_etag is no longer supported.*cache.check_source_version"):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "cache": {
                    "size": "50M",
                    "use_etag": True,
                    "location": "/path/to/cache",
                },
            }
        )

    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "cache": {
                    "eviction_policy": {
                        "policy": "FIFO",
                        "refresh_interval": 0,
                    }
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


def test_validate_include():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    # Valid: include with array of strings
    validate_config(
        {
            "include": [
                "/common_config/telemetry_config.yaml",
                "/common_config/shared_profiles.yaml",
                "./local_overrides.yaml",
            ],
            "profiles": {
                "default": default_storage_provider,
            },
        }
    )

    # Valid: include with single file
    validate_config(
        {
            "include": ["/path/to/config.yaml"],
            "profiles": {
                "default": default_storage_provider,
            },
        }
    )

    # Valid: empty include array
    validate_config(
        {
            "include": [],
            "profiles": {
                "default": default_storage_provider,
            },
        }
    )

    # Invalid: include is not an array
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "include": "/single/path.yaml",  # Should be array
                "profiles": {
                    "default": default_storage_provider,
                },
            }
        )

    # Invalid: include array contains non-string
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "include": ["/valid/path.yaml", 123, "/another/path.yaml"],  # 123 is not a string
                "profiles": {
                    "default": default_storage_provider,
                },
            }
        )

    # Invalid: include array contains object instead of string
    with pytest.raises(RuntimeError):
        validate_config(
            {
                "include": [{"path": "/config.yaml"}],  # Should be string, not object
                "profiles": {
                    "default": default_storage_provider,
                },
            }
        )


def test_validate_unknown_top_level_key():
    default_storage_provider = {"storage_provider": {"type": "s3", "options": {"base_path": "bucket/prefix"}}}

    with pytest.raises(RuntimeError):
        validate_config(
            {
                "profiles": {
                    "default": default_storage_provider,
                },
                "typo": True,
            }
        )
