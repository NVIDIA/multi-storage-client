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

import copy
import json
import logging
import os
import tempfile
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any, Iterable, Optional, Union
from urllib.parse import urlparse

import yaml

from .cache import DEFAULT_CACHE_LINE_SIZE, DEFAULT_CACHE_SIZE, CacheManager
from .caching.cache_config import CacheConfig, EvictionPolicyConfig
from .providers.manifest_metadata import ManifestMetadataProvider
from .rclone import read_rclone_config
from .schema import validate_config
from .telemetry import Telemetry
from .telemetry import init as telemetry_init
from .types import (
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_BACKOFF_MULTIPLIER,
    DEFAULT_RETRY_DELAY,
    MSC_PROTOCOL,
    AutoCommitConfig,
    CredentialsProvider,
    MetadataProvider,
    ProviderBundle,
    ProviderBundleV2,
    Replica,
    RetryConfig,
    StorageBackend,
    StorageProvider,
    StorageProviderConfig,
)
from .utils import expand_env_vars, import_class, merge_dictionaries_no_overwrite

# Constants related to implicit profiles
SUPPORTED_IMPLICIT_PROFILE_PROTOCOLS = ("s3", "gs", "ais", "file")
PROTOCOL_TO_PROVIDER_TYPE_MAPPING = {
    "s3": "s3",
    "gs": "gcs",
    "ais": "ais",
    "file": "file",
}


# Template for creating implicit profile configurations
def create_implicit_profile_config(profile_name: str, protocol: str, base_path: str) -> dict:
    """
    Create a configuration dictionary for an implicit profile.

    :param profile_name: The name of the profile (e.g., "_s3-bucket1")
    :param protocol: The storage protocol (e.g., "s3", "gs", "ais")
    :param base_path: The base path (e.g., bucket name) for the storage provider

    :return: A configuration dictionary for the implicit profile
    """
    provider_type = PROTOCOL_TO_PROVIDER_TYPE_MAPPING[protocol]
    return {
        "profiles": {profile_name: {"storage_provider": {"type": provider_type, "options": {"base_path": base_path}}}}
    }


DEFAULT_POSIX_PROFILE_NAME = "default"
DEFAULT_POSIX_PROFILE = create_implicit_profile_config(DEFAULT_POSIX_PROFILE_NAME, "file", "/")

STORAGE_PROVIDER_MAPPING = {
    "file": "PosixFileStorageProvider",
    "s3": "S3StorageProvider",
    "gcs": "GoogleStorageProvider",
    "oci": "OracleStorageProvider",
    "azure": "AzureBlobStorageProvider",
    "ais": "AIStoreStorageProvider",
    "ais_s3": "AIStoreS3StorageProvider",
    "s8k": "S8KStorageProvider",
    "gcs_s3": "GoogleS3StorageProvider",
    "huggingface": "HuggingFaceStorageProvider",
}

CREDENTIALS_PROVIDER_MAPPING = {
    "S3Credentials": "StaticS3CredentialsProvider",
    "AzureCredentials": "StaticAzureCredentialsProvider",
    "AISCredentials": "StaticAISCredentialProvider",
    "GoogleIdentityPoolCredentialsProvider": "GoogleIdentityPoolCredentialsProvider",
    "GoogleServiceAccountCredentialsProvider": "GoogleServiceAccountCredentialsProvider",
    "HuggingFaceCredentials": "HuggingFaceCredentialsProvider",
    "FileBasedCredentials": "FileBasedCredentialsProvider",
}


def _resolve_include_path(include_path: str, parent_config_path: str) -> str:
    """
    Resolve include path (absolute or relative to parent config file).

    :param include_path: Path from include keyword (can be absolute or relative)
    :param parent_config_path: Absolute path of the config file containing the include
    :return: Absolute, normalized path to the included config file
    """
    if os.path.isabs(include_path):
        return os.path.abspath(include_path)
    else:
        parent_dir = os.path.dirname(parent_config_path)
        return os.path.abspath(os.path.join(parent_dir, include_path))


def _merge_profiles(
    base_profiles: dict[str, Any],
    new_profiles: dict[str, Any],
    base_path: str,
    new_path: str,
) -> dict[str, Any]:
    """
    Merge profiles from two config files with conflict detection.

    Profiles with the same name are allowed if their definitions are identical (idempotent).
    If a profile name exists in both configs with different definitions, an error is raised.

    NOTE: This function performs SHALLOW merging - each profile is treated as a complete unit.
    We cannot use merge_dictionaries_no_overwrite() here because it would recursively merge
    profile fields, which could lead to unintended behavior. For example:

    base_config:
        profiles:
            my-profile:
                storage_provider:
                    type: s3
                    options:
                        base_path: bucket

    new_config:
        profiles:
            my-profile:
                credentials_provider:
                    type: S3Credentials,
                    options:
                        access_key: foo
                        secret_key: bar

    If we used merge_dictionaries_no_overwrite(), the result would be a profile with BOTH storage_provider
    and credentials_provider, which might not be the intended configuration.

    :param base_profiles: Profiles from the base config
    :param new_profiles: Profiles from the new config to merge
    :param base_path: Path to the base config file (for error messages)
    :param new_path: Path to the new config file (for error messages)
    :return: Merged profiles dictionary
    :raises ValueError: If any profile name exists in both configs with different definitions
    """
    result = base_profiles.copy()
    conflicting_profiles = []

    for profile_name, new_profile_def in new_profiles.items():
        if profile_name in result:
            if result[profile_name] != new_profile_def:
                conflicting_profiles.append(profile_name)
        else:
            result[profile_name] = new_profile_def

    if conflicting_profiles:
        conflicts_list = ", ".join(f"'{p}'" for p in sorted(conflicting_profiles))
        raise ValueError(f"Profile conflict: {conflicts_list} defined differently in {base_path} and {new_path}")

    return result


def _merge_configs(
    base_config: dict[str, Any],
    new_config: dict[str, Any],
    base_path: str,
    new_path: str,
) -> dict[str, Any]:
    """
    Merge two config dictionaries with field-specific strategies.

    Different config fields have different merge strategies:
    - profiles: Shallow merge with idempotent check (uses _merge_profiles)
    - path_mapping: Flat dict merge with idempotent check
    - experimental_features: Flat dict merge with idempotent check
    - cache, opentelemetry, posix: Global configs, idempotent if identical, error if different

    :param base_config: Base configuration dictionary
    :param new_config: New configuration to merge
    :param base_path: Path to base config file (for error messages)
    :param new_path: Path to new config file (for error messages)
    :return: Merged configuration dictionary
    :raises ValueError: If conflicts are detected
    """
    result = {}

    all_keys = set(base_config.keys()) | set(new_config.keys())

    for key in all_keys:
        base_value = base_config.get(key)
        new_value = new_config.get(key)

        # Key only in base
        if key not in new_config:
            result[key] = base_value
            continue

        # Key only in new
        if key not in base_config:
            result[key] = new_value
            continue

        # Key in both - need to merge or detect conflict
        if key == "profiles":
            result["profiles"] = _merge_profiles(base_value or {}, new_value or {}, base_path, new_path)

        elif key in ("path_mapping", "experimental_features"):
            merged, conflicts = merge_dictionaries_no_overwrite(
                (base_value or {}).copy(), new_value or {}, allow_idempotent=True
            )
            if conflicts:
                conflicts_list = ", ".join(f"'{k}'" for k in sorted(conflicts))
                raise ValueError(
                    f"Config merge conflict: {conflicts_list} have different values in {base_path} and {new_path}"
                )
            result[key] = merged

        elif key in ("cache", "opentelemetry", "posix"):
            if base_value != new_value:
                raise ValueError(f"'{key}' defined differently in {base_path} and {new_path}")
            result[key] = base_value

        elif key == "include":
            # 'include' is processed by _load_and_merge_includes, not part of final config
            pass

        else:
            # This should never happen and all top level fields must have explicit handling above
            raise ValueError(f"Unknown field '{key}' in config file.")

    return result


def _load_and_merge_includes(
    main_config_path: str,
    main_config_dict: dict[str, Any],
) -> dict[str, Any]:
    """
    Load and merge included config files.

    Processes the 'include' directive in the main config, loading and merging all
    specified config files. Only supports one level of includes - included files
    cannot themselves have 'include' directives.

    :param main_config_path: Absolute path to the main config file
    :param main_config_dict: Dictionary loaded from the main config file
    :return: Merged configuration dictionary (without 'include' field)
    :raises ValueError: If include file not found, malformed, or contains nested includes
    """
    include_paths = main_config_dict.get("include", [])

    if not include_paths:
        return {k: v for k, v in main_config_dict.items() if k != "include"}

    merged_config = {k: v for k, v in main_config_dict.items() if k != "include"}

    for include_path in include_paths:
        resolved_path = _resolve_include_path(include_path, main_config_path)

        if not os.path.exists(resolved_path):
            raise ValueError(f"Included config file not found: {resolved_path} (from {main_config_path})")

        try:
            with open(resolved_path) as f:
                if resolved_path.endswith(".json"):
                    included_config = json.load(f)
                else:
                    included_config = yaml.safe_load(f)
        except Exception as e:
            raise ValueError(f"Failed to load included config {resolved_path}: {e}")

        validate_config(included_config)
        if "include" in included_config:
            raise ValueError(f"Nested includes not allowed: {resolved_path} contains 'include' directive")

        merged_config = _merge_configs(merged_config, included_config, main_config_path, resolved_path)

    return merged_config


def _find_config_file_paths() -> tuple[str]:
    """
    Get configuration file search paths.

    Returns paths in order of precedence:

    1. User-specific config (${XDG_CONFIG_HOME}/msc/, ${HOME}/, ${HOME}/.config/msc/)
    2. System-wide configs (${XDG_CONFIG_DIRS}/msc/, /etc/xdg, /etc/)
    """
    paths = []

    # 1. User-specific configuration directory
    xdg_config_home = os.getenv("XDG_CONFIG_HOME")

    if xdg_config_home:
        paths.extend(
            [
                os.path.join(xdg_config_home, "msc", "config.yaml"),
                os.path.join(xdg_config_home, "msc", "config.json"),
            ]
        )

    user_home = os.getenv("HOME")

    if user_home:
        paths.extend(
            [
                os.path.join(user_home, ".msc_config.yaml"),
                os.path.join(user_home, ".msc_config.json"),
                os.path.join(user_home, ".config", "msc", "config.yaml"),
                os.path.join(user_home, ".config", "msc", "config.json"),
            ]
        )

    # 2. System-wide configuration directories
    xdg_config_dirs = os.getenv("XDG_CONFIG_DIRS")
    if not xdg_config_dirs:
        xdg_config_dirs = "/etc/xdg"

    for config_dir in xdg_config_dirs.split(":"):
        config_dir = config_dir.strip()
        if config_dir:
            paths.extend(
                [
                    os.path.join(config_dir, "msc", "config.yaml"),
                    os.path.join(config_dir, "msc", "config.json"),
                ]
            )

    paths.extend(
        [
            "/etc/msc_config.yaml",
            "/etc/msc_config.json",
        ]
    )

    return tuple(paths)


PACKAGE_NAME = "multistorageclient"

logger = logging.getLogger(__name__)


class ImmutableDict(dict):
    """
    Immutable dictionary that raises an error when attempting to modify it.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Recursively freeze nested structures
        for key, value in list(super().items()):
            if isinstance(value, dict) and not isinstance(value, ImmutableDict):
                super().__setitem__(key, ImmutableDict(value))
            elif isinstance(value, list):
                super().__setitem__(key, self._freeze_list(value))

    @staticmethod
    def _freeze_list(lst):
        """
        Convert list to tuple, freezing nested dicts recursively.
        """
        frozen = []
        for item in lst:
            if isinstance(item, dict):
                frozen.append(ImmutableDict(item))
            elif isinstance(item, list):
                frozen.append(ImmutableDict._freeze_list(item))
            else:
                frozen.append(item)
        return tuple(frozen)

    def __deepcopy__(self, memo):
        """
        Return a regular mutable dict when deepcopy is called.
        """
        return copy.deepcopy(dict(self), memo)

    def __reduce__(self):
        """
        Support for pickle serialization.
        """
        return (self.__class__, (dict(self),))

    def _copy_value(self, value):
        """
        Convert frozen structures back to mutable equivalents.
        """
        if isinstance(value, ImmutableDict):
            return {k: self._copy_value(v) for k, v in value.items()}
        elif isinstance(value, tuple):
            # Check if it was originally a list (frozen by _freeze_list)
            return [self._copy_value(item) for item in value]
        else:
            return value

    def __getitem__(self, key):
        """
        Return a mutable copy of the value.
        """
        value = super().__getitem__(key)
        return self._copy_value(value)

    def get(self, key, default=None):
        """
        Return a mutable copy of the value.
        """
        return self[key] if key in self else default

    def __setitem__(self, key, value):
        raise TypeError("ImmutableDict is immutable")

    def __delitem__(self, key):
        raise TypeError("ImmutableDict is immutable")

    def clear(self):
        raise TypeError("ImmutableDict is immutable")

    def pop(self, *args):
        raise TypeError("ImmutableDict is immutable")

    def popitem(self):
        raise TypeError("ImmutableDict is immutable")

    def setdefault(self, key, default=None):
        raise TypeError("ImmutableDict is immutable")

    def update(self, *args, **kwargs):
        raise TypeError("ImmutableDict is immutable")


class SimpleProviderBundle(ProviderBundle):
    def __init__(
        self,
        storage_provider_config: StorageProviderConfig,
        credentials_provider: Optional[CredentialsProvider] = None,
        metadata_provider: Optional[MetadataProvider] = None,
        replicas: Optional[list[Replica]] = None,
    ):
        if replicas is None:
            replicas = []

        self._storage_provider_config = storage_provider_config
        self._credentials_provider = credentials_provider
        self._metadata_provider = metadata_provider
        self._replicas = replicas

    @property
    def storage_provider_config(self) -> StorageProviderConfig:
        return self._storage_provider_config

    @property
    def credentials_provider(self) -> Optional[CredentialsProvider]:
        return self._credentials_provider

    @property
    def metadata_provider(self) -> Optional[MetadataProvider]:
        return self._metadata_provider

    @property
    def replicas(self) -> list[Replica]:
        return self._replicas


class SimpleProviderBundleV2(ProviderBundleV2):
    def __init__(
        self, storage_backends: dict[str, StorageBackend], metadata_provider: Optional[MetadataProvider] = None
    ):
        self._storage_backends = storage_backends
        self._metadata_provider = metadata_provider

    @staticmethod
    def from_v1_bundle(profile_name: str, v1_bundle: ProviderBundle) -> "SimpleProviderBundleV2":
        backend = StorageBackend(
            storage_provider_config=v1_bundle.storage_provider_config,
            credentials_provider=v1_bundle.credentials_provider,
            replicas=v1_bundle.replicas,
        )

        return SimpleProviderBundleV2(
            storage_backends={profile_name: backend},
            metadata_provider=v1_bundle.metadata_provider,
        )

    @property
    def storage_backends(self) -> dict[str, StorageBackend]:
        return self._storage_backends

    @property
    def metadata_provider(self) -> Optional[MetadataProvider]:
        return self._metadata_provider


DEFAULT_CACHE_REFRESH_INTERVAL = 300


class StorageClientConfigLoader:
    _provider_bundle: ProviderBundleV2
    _resolved_config_dict: dict[str, Any]
    _profiles: dict[str, Any]
    _profile: str
    _profile_dict: dict[str, Any]
    _opentelemetry_dict: Optional[dict[str, Any]]
    _telemetry_provider: Optional[Callable[[], Telemetry]]
    _cache_dict: Optional[dict[str, Any]]

    def __init__(
        self,
        config_dict: dict[str, Any],
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        provider_bundle: Optional[Union[ProviderBundle, ProviderBundleV2]] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> None:
        """
        Initializes a :py:class:`StorageClientConfigLoader` to create a
        StorageClientConfig. Components are built using the ``config_dict`` and
        profile, but a pre-built provider_bundle takes precedence.

        :param config_dict: Dictionary of configuration options.
        :param profile: Name of profile in ``config_dict`` to use to build configuration.
        :param provider_bundle: Optional pre-built :py:class:`multistorageclient.types.ProviderBundle` or :py:class:`multistorageclient.types.ProviderBundleV2`, takes precedence over ``config_dict``.
        :param telemetry_provider: A function that provides a telemetry instance. The function must be defined at the top level of a module to work with pickling.
        """
        # Interpolates all environment variables into actual values.
        config_dict = expand_env_vars(config_dict)
        self._resolved_config_dict = ImmutableDict(config_dict)

        self._profiles = config_dict.get("profiles", {})

        if DEFAULT_POSIX_PROFILE_NAME not in self._profiles:
            # Assign the default POSIX profile
            self._profiles[DEFAULT_POSIX_PROFILE_NAME] = DEFAULT_POSIX_PROFILE["profiles"][DEFAULT_POSIX_PROFILE_NAME]
        else:
            # Cannot override default POSIX profile
            storage_provider_type = (
                self._profiles[DEFAULT_POSIX_PROFILE_NAME].get("storage_provider", {}).get("type", None)
            )
            if storage_provider_type != "file":
                raise ValueError(
                    f'Cannot override "{DEFAULT_POSIX_PROFILE_NAME}" profile with storage provider type '
                    f'"{storage_provider_type}"; expected "file".'
                )

        profile_dict = self._profiles.get(profile)

        if not profile_dict:
            raise ValueError(f"Profile {profile} not found; available profiles: {list(self._profiles.keys())}")

        self._profile = profile
        self._profile_dict = ImmutableDict(profile_dict)

        self._opentelemetry_dict = config_dict.get("opentelemetry", None)
        # Multiprocessing unpickles during the Python interpreter's bootstrap phase for new processes.
        # New processes (e.g. multiprocessing manager server) can't be created during this phase.
        #
        # Pass thunks everywhere instead for lazy telemetry initialization.
        self._telemetry_provider = telemetry_provider or telemetry_init

        self._cache_dict = config_dict.get("cache", None)
        self._experimental_features = config_dict.get("experimental_features", None)

        self._provider_bundle = self._build_provider_bundle(provider_bundle)
        self._inject_profiles_from_bundle()

    def _build_storage_provider(
        self,
        storage_provider_name: str,
        storage_options: Optional[dict[str, Any]] = None,
        credentials_provider: Optional[CredentialsProvider] = None,
    ) -> StorageProvider:
        if storage_options is None:
            storage_options = {}
        if storage_provider_name not in STORAGE_PROVIDER_MAPPING:
            raise ValueError(
                f"Storage provider {storage_provider_name} is not supported. "
                f"Supported providers are: {list(STORAGE_PROVIDER_MAPPING.keys())}"
            )
        if credentials_provider:
            storage_options["credentials_provider"] = credentials_provider
        if self._resolved_config_dict is not None:
            # Make a deep copy to drop any external references which may be mutated or cause infinite recursion.
            storage_options["config_dict"] = copy.deepcopy(self._resolved_config_dict)
        if self._telemetry_provider is not None:
            storage_options["telemetry_provider"] = self._telemetry_provider
        class_name = STORAGE_PROVIDER_MAPPING[storage_provider_name]
        module_name = ".providers"
        cls = import_class(class_name, module_name, PACKAGE_NAME)
        return cls(**storage_options)

    def _build_storage_provider_from_profile(self, storage_provider_profile: str):
        storage_profile_dict = self._profiles.get(storage_provider_profile)
        if not storage_profile_dict:
            raise ValueError(
                f"Profile '{storage_provider_profile}' referenced by storage_provider_profile does not exist."
            )

        # Check if metadata provider is configured for this profile
        # NOTE: The storage profile for manifests does not support metadata provider (at the moment).
        local_metadata_provider_dict = storage_profile_dict.get("metadata_provider", None)
        if local_metadata_provider_dict:
            raise ValueError(
                f"Profile '{storage_provider_profile}' cannot have a metadata provider when used for manifests"
            )

        # Initialize CredentialsProvider
        local_creds_provider_dict = storage_profile_dict.get("credentials_provider", None)
        local_creds_provider = self._build_credentials_provider(credentials_provider_dict=local_creds_provider_dict)

        # Initialize StorageProvider
        local_storage_provider_dict = storage_profile_dict.get("storage_provider", None)
        if local_storage_provider_dict:
            local_name = local_storage_provider_dict["type"]
            local_storage_options = local_storage_provider_dict.get("options", {})
        else:
            raise ValueError(f"Missing storage_provider in the config for profile {storage_provider_profile}.")

        storage_provider = self._build_storage_provider(local_name, local_storage_options, local_creds_provider)
        return storage_provider

    def _build_credentials_provider(
        self,
        credentials_provider_dict: Optional[dict[str, Any]],
        storage_options: Optional[dict[str, Any]] = None,
    ) -> Optional[CredentialsProvider]:
        """
        Initializes the CredentialsProvider based on the provided dictionary.

        Args:
            credentials_provider_dict: Dictionary containing credentials provider configuration
            storage_options: Storage provider options required by some credentials providers to scope the credentials.
        """
        if not credentials_provider_dict:
            return None

        if credentials_provider_dict["type"] not in CREDENTIALS_PROVIDER_MAPPING:
            # Fully qualified class path case
            class_type = credentials_provider_dict["type"]
            module_name, class_name = class_type.rsplit(".", 1)
            cls = import_class(class_name, module_name)
        else:
            # Mapped class name case
            class_name = CREDENTIALS_PROVIDER_MAPPING[credentials_provider_dict["type"]]
            module_name = ".providers"
            cls = import_class(class_name, module_name, PACKAGE_NAME)

        # Propagate storage provider options to credentials provider since they may be
        # required by some credentials providers to scope the credentials.
        import inspect

        init_params = list(inspect.signature(cls.__init__).parameters)[1:]  # skip 'self'
        options = credentials_provider_dict.get("options", {})
        if storage_options:
            for storage_provider_option in storage_options.keys():
                if storage_provider_option in init_params and storage_provider_option not in options:
                    options[storage_provider_option] = storage_options[storage_provider_option]

        return cls(**options)

    def _build_provider_bundle_from_config(self, profile_dict: dict[str, Any]) -> ProviderBundle:
        # Initialize StorageProvider
        storage_provider_dict = profile_dict.get("storage_provider", None)
        if storage_provider_dict:
            storage_provider_name = storage_provider_dict["type"]
            storage_options = storage_provider_dict.get("options", {})
        else:
            raise ValueError("Missing storage_provider in the config.")

        # Initialize CredentialsProvider
        # It is prudent to assume that in some cases, the credentials provider
        # will provide credentials scoped to specific base_path.
        # So we need to pass the storage_options to the credentials provider.
        credentials_provider_dict = profile_dict.get("credentials_provider", None)
        credentials_provider = self._build_credentials_provider(
            credentials_provider_dict=credentials_provider_dict,
            storage_options=storage_options,
        )

        # Initialize MetadataProvider
        metadata_provider_dict = profile_dict.get("metadata_provider", None)
        metadata_provider = None
        if metadata_provider_dict:
            if metadata_provider_dict["type"] == "manifest":
                metadata_options = metadata_provider_dict.get("options", {})
                # If MetadataProvider has a reference to a different storage provider profile
                storage_provider_profile = metadata_options.pop("storage_provider_profile", None)
                if storage_provider_profile:
                    storage_provider = self._build_storage_provider_from_profile(storage_provider_profile)
                else:
                    storage_provider = self._build_storage_provider(
                        storage_provider_name, storage_options, credentials_provider
                    )

                metadata_provider = ManifestMetadataProvider(storage_provider, **metadata_options)
            else:
                class_type = metadata_provider_dict["type"]
                if "." not in class_type:
                    raise ValueError(
                        f"Expected a fully qualified class name (e.g., 'module.ClassName'); got '{class_type}'."
                    )
                module_name, class_name = class_type.rsplit(".", 1)
                cls = import_class(class_name, module_name)
                options = metadata_provider_dict.get("options", {})
                metadata_provider = cls(**options)

        # Build replicas if configured
        replicas_config = profile_dict.get("replicas", [])
        replicas = []
        if replicas_config:
            for replica_dict in replicas_config:
                replicas.append(
                    Replica(
                        replica_profile=replica_dict["replica_profile"],
                        read_priority=replica_dict["read_priority"],
                    )
                )

            # Sort replicas by read_priority
            replicas.sort(key=lambda r: r.read_priority)

        return SimpleProviderBundle(
            storage_provider_config=StorageProviderConfig(storage_provider_name, storage_options),
            credentials_provider=credentials_provider,
            metadata_provider=metadata_provider,
            replicas=replicas,
        )

    def _build_provider_bundle_from_extension(self, provider_bundle_dict: dict[str, Any]) -> ProviderBundle:
        class_type = provider_bundle_dict["type"]
        module_name, class_name = class_type.rsplit(".", 1)
        cls = import_class(class_name, module_name)
        options = provider_bundle_dict.get("options", {})
        return cls(**options)

    def _build_provider_bundle(
        self, provider_bundle: Optional[Union[ProviderBundle, ProviderBundleV2]]
    ) -> ProviderBundleV2:
        if provider_bundle:
            bundle = provider_bundle
        else:
            provider_bundle_dict = self._profile_dict.get("provider_bundle", None)
            if provider_bundle_dict:
                bundle = self._build_provider_bundle_from_extension(provider_bundle_dict)
            else:
                bundle = self._build_provider_bundle_from_config(self._profile_dict)

        if isinstance(bundle, ProviderBundle) and not isinstance(bundle, ProviderBundleV2):
            bundle = SimpleProviderBundleV2.from_v1_bundle(self._profile, bundle)

        return bundle

    def _inject_profiles_from_bundle(self) -> None:
        if len(self._provider_bundle.storage_backends) > 1:
            profiles = copy.deepcopy(self._profiles)

            for child_name, backend in self._provider_bundle.storage_backends.items():
                child_config = {
                    "storage_provider": {
                        "type": backend.storage_provider_config.type,
                        "options": backend.storage_provider_config.options,
                    }
                }

                if child_name in self._profiles:
                    # Profile already exists - check if it matches what we would inject
                    existing = self._profiles[child_name]
                    if existing.get("storage_provider") == child_config["storage_provider"]:
                        continue
                    else:
                        raise ValueError(
                            f"Profile '{child_name}' already exists in configuration with different settings."
                        )

                profiles[child_name] = child_config

            resolved_config_dict = copy.deepcopy(self._resolved_config_dict)
            resolved_config_dict["profiles"] = profiles

            self._profiles = ImmutableDict(profiles)
            self._resolved_config_dict = ImmutableDict(resolved_config_dict)

    def _build_retry_config(self) -> RetryConfig:
        """Build retry config from profile dict."""
        retry_config_dict = self._profile_dict.get("retry", None)
        if retry_config_dict:
            attempts = retry_config_dict.get("attempts", DEFAULT_RETRY_ATTEMPTS)
            delay = retry_config_dict.get("delay", DEFAULT_RETRY_DELAY)
            backoff_multiplier = retry_config_dict.get("backoff_multiplier", DEFAULT_RETRY_BACKOFF_MULTIPLIER)
            return RetryConfig(attempts=attempts, delay=delay, backoff_multiplier=backoff_multiplier)
        else:
            return RetryConfig(
                attempts=DEFAULT_RETRY_ATTEMPTS,
                delay=DEFAULT_RETRY_DELAY,
                backoff_multiplier=DEFAULT_RETRY_BACKOFF_MULTIPLIER,
            )

    def _build_autocommit_config(self) -> AutoCommitConfig:
        """Build autocommit config from profile dict."""
        autocommit_dict = self._profile_dict.get("autocommit", None)
        if autocommit_dict:
            interval_minutes = autocommit_dict.get("interval_minutes", None)
            at_exit = autocommit_dict.get("at_exit", False)
            return AutoCommitConfig(interval_minutes=interval_minutes, at_exit=at_exit)
        return AutoCommitConfig()

    def _verify_cache_config(self, cache_dict: dict[str, Any]) -> None:
        if "size_mb" in cache_dict:
            raise ValueError(
                "The 'size_mb' property is no longer supported. \n"
                "Please use 'size' with a unit suffix (M, G, T) instead of size_mb.\n"
                "Example configuration:\n"
                "cache:\n"
                "  size: 500G               # Optional: If not specified, default cache size (10GB) will be used\n"
                "  use_etag: true           # Optional: If not specified, default cache use_etag (true) will be used\n"
                "  location: /tmp/msc_cache # Optional: If not specified, default cache location (system temporary directory + '/msc_cache') will be used\n"
                "  eviction_policy:         # Optional: The eviction policy to use\n"
                "    policy: fifo           # Optional: The eviction policy to use, default is 'fifo'\n"
                "    refresh_interval: 300  # Optional: If not specified, default cache refresh interval (300 seconds) will be used\n"
            )

    def _validate_replicas(self, replicas: list[Replica]) -> None:
        """
        Validates that replica profiles do not have their own replicas configuration.

        This prevents circular references where a replica profile could reference
        another profile that also has replicas, creating an infinite loop.

        :param replicas: The list of Replica objects to validate
        :raises ValueError: If any replica profile has its own replicas configuration
        """
        for replica in replicas:
            replica_profile_name = replica.replica_profile

            # Check that replica profile is not the same as the current profile
            if replica_profile_name == self._profile:
                raise ValueError(
                    f"Replica profile {replica_profile_name} cannot be the same as the profile {self._profile}."
                )

            # Check if the replica profile exists in the configuration
            if replica_profile_name not in self._profiles:
                raise ValueError(f"Replica profile '{replica_profile_name}' not found in configuration")

            # Get the replica profile configuration
            replica_profile_dict = self._profiles[replica_profile_name]

            # Check if the replica profile has its own replicas configuration
            if "replicas" in replica_profile_dict and replica_profile_dict["replicas"]:
                raise ValueError(
                    f"Invalid replica configuration: profile '{replica_profile_name}' has its own replicas. "
                    f"This creates a circular reference which is not allowed."
                )

    # todo: remove once experimental features are stable
    def _validate_experimental_features(self, eviction_policy: EvictionPolicyConfig) -> None:
        """
        Validate that experimental features are enabled when used.

        :param eviction_policy: The eviction policy configuration to validate
        :raises ValueError: If experimental features are used without being enabled
        """
        # Validate MRU eviction policy
        if eviction_policy.policy.upper() == "MRU":
            if not (self._experimental_features and self._experimental_features.get("cache_mru_eviction")):
                raise ValueError(
                    "MRU eviction policy is experimental and not enabled.\n"
                    "Enable it by adding to config:\n"
                    "  experimental_features:\n"
                    "    cache_mru_eviction: true"
                )

        # Validate purge_factor
        if eviction_policy.purge_factor != 0:
            if eviction_policy.purge_factor < 0 or eviction_policy.purge_factor > 100:
                raise ValueError("purge_factor must be between 0 and 100")

            if not (self._experimental_features and self._experimental_features.get("cache_purge_factor")):
                raise ValueError(
                    "purge_factor is experimental and not enabled.\n"
                    "Enable it by adding to config:\n"
                    "  experimental_features:\n"
                    "    cache_purge_factor: true"
                )

    def build_config(self) -> "StorageClientConfig":
        bundle = self._provider_bundle
        backends = bundle.storage_backends

        if len(backends) > 1:
            if not bundle.metadata_provider:
                raise ValueError(
                    f"Multi-backend configuration for profile '{self._profile}' requires metadata_provider "
                    "for routing between storage locations."
                )

            child_profile_names = list(backends.keys())
            retry_config = self._build_retry_config()
            autocommit_config = self._build_autocommit_config()

            # config for Composite StorageClient
            config = StorageClientConfig(
                profile=self._profile,
                storage_provider=None,
                credentials_provider=None,
                storage_provider_profiles=child_profile_names,
                metadata_provider=bundle.metadata_provider,
                cache_config=None,
                cache_manager=None,
                retry_config=retry_config,
                telemetry_provider=self._telemetry_provider,
                replicas=[],
                autocommit_config=autocommit_config,
            )

            config._config_dict = self._resolved_config_dict
            return config

        # Single-backend (len == 1)
        _, backend = list(backends.items())[0]
        storage_provider = self._build_storage_provider(
            backend.storage_provider_config.type,
            backend.storage_provider_config.options,
            backend.credentials_provider,
        )
        credentials_provider = backend.credentials_provider
        metadata_provider = bundle.metadata_provider
        replicas = backend.replicas

        # Validate replicas to prevent circular references
        if replicas:
            self._validate_replicas(replicas)

        cache_config: Optional[CacheConfig] = None
        cache_manager: Optional[CacheManager] = None

        # Check if caching is enabled for this profile
        caching_enabled = self._profile_dict.get("caching_enabled", False)

        if self._cache_dict is not None and caching_enabled:
            tempdir = tempfile.gettempdir()
            default_location = os.path.join(tempdir, "msc_cache")
            location = self._cache_dict.get("location", default_location)

            # Check if cache_backend.cache_path is defined
            cache_backend = self._cache_dict.get("cache_backend", {})
            cache_backend_path = cache_backend.get("cache_path") if cache_backend else None

            # Warn if both location and cache_backend.cache_path are defined
            if cache_backend_path and self._cache_dict.get("location") is not None:
                logger.warning(
                    f"Both 'location' and 'cache_backend.cache_path' are defined in cache config. "
                    f"Using 'location' ({location}) and ignoring 'cache_backend.cache_path' ({cache_backend_path})."
                )
            elif cache_backend_path:
                # Use cache_backend.cache_path only if location is not explicitly defined
                location = cache_backend_path

            # Resolve the effective flag while preserving explicit ``False``.
            if "check_source_version" in self._cache_dict:
                check_source_version = self._cache_dict["check_source_version"]
            else:
                check_source_version = self._cache_dict.get("use_etag", True)

            # Warn if both keys are specified – the new one wins.
            if "check_source_version" in self._cache_dict and "use_etag" in self._cache_dict:
                logger.warning(
                    "Both 'check_source_version' and 'use_etag' are defined in cache config. "
                    "Using 'check_source_version' and ignoring 'use_etag'."
                )

            if not Path(location).is_absolute():
                raise ValueError(f"Cache location must be an absolute path: {location}")

            # Initialize cache_dict with default values
            cache_dict = self._cache_dict

            # Verify cache config
            self._verify_cache_config(cache_dict)

            # Initialize eviction policy
            if "eviction_policy" in cache_dict:
                policy = cache_dict["eviction_policy"]["policy"].lower()
                purge_factor = cache_dict["eviction_policy"].get("purge_factor", 0)
                eviction_policy = EvictionPolicyConfig(
                    policy=policy,
                    refresh_interval=cache_dict["eviction_policy"].get(
                        "refresh_interval", DEFAULT_CACHE_REFRESH_INTERVAL
                    ),
                    purge_factor=purge_factor,
                )
            else:
                eviction_policy = EvictionPolicyConfig(policy="fifo", refresh_interval=DEFAULT_CACHE_REFRESH_INTERVAL)

            # todo: remove once experimental features are stable
            # Validate experimental features before creating cache_config
            self._validate_experimental_features(eviction_policy)

            # Create cache config from the standardized format
            cache_config = CacheConfig(
                size=cache_dict.get("size", DEFAULT_CACHE_SIZE),
                location=cache_dict.get("location", location),
                check_source_version=check_source_version,
                eviction_policy=eviction_policy,
                cache_line_size=cache_dict.get("cache_line_size", DEFAULT_CACHE_LINE_SIZE),
            )

            cache_manager = CacheManager(profile=self._profile, cache_config=cache_config)
        elif self._cache_dict is not None and not caching_enabled:
            logger.debug(f"Caching is disabled for profile '{self._profile}'")
        elif self._cache_dict is None and caching_enabled:
            logger.warning(f"Caching is enabled for profile '{self._profile}' but no cache configuration is provided")

        retry_config = self._build_retry_config()
        autocommit_config = self._build_autocommit_config()

        config = StorageClientConfig(
            profile=self._profile,
            storage_provider=storage_provider,
            credentials_provider=credentials_provider,
            storage_provider_profiles=None,
            metadata_provider=metadata_provider,
            cache_config=cache_config,
            cache_manager=cache_manager,
            retry_config=retry_config,
            telemetry_provider=self._telemetry_provider,
            replicas=replicas,
            autocommit_config=autocommit_config,
        )

        config._config_dict = self._resolved_config_dict
        return config


class PathMapping:
    """
    Class to handle path mappings defined in the MSC configuration.

    Path mappings create a nested structure of protocol -> bucket -> [(prefix, profile)]
    where entries are sorted by prefix length (longest first) for optimal matching.
    Longer paths take precedence when matching.
    """

    def __init__(self):
        """Initialize an empty PathMapping."""
        self._mapping = defaultdict(lambda: defaultdict(list))

    @classmethod
    def from_config(cls, config_dict: Optional[dict[str, Any]] = None) -> "PathMapping":
        """
        Create a PathMapping instance from configuration dictionary.

        :param config_dict: Configuration dictionary, if None the config will be loaded
        :return: A PathMapping instance with processed mappings
        """
        if config_dict is None:
            # Import locally to avoid circular imports
            from multistorageclient.config import StorageClientConfig

            config_dict, _ = StorageClientConfig.read_msc_config()

        if not config_dict:
            return cls()

        instance = cls()
        instance._load_mapping(config_dict)
        return instance

    def _load_mapping(self, config_dict: dict[str, Any]) -> None:
        """
        Load path mapping from a configuration dictionary.

        :param config_dict: Configuration dictionary containing path mapping
        """
        # Get the path_mapping section
        path_mapping = config_dict.get("path_mapping", {})
        if path_mapping is None:
            return

        # Process each mapping
        for source_path, dest_path in path_mapping.items():
            # Validate format
            if not source_path.endswith("/"):
                continue
            if not dest_path.startswith(MSC_PROTOCOL):
                continue
            if not dest_path.endswith("/"):
                continue

            # Extract the destination profile
            pr_dest = urlparse(dest_path)
            dest_profile = pr_dest.netloc

            # Parse the source path
            pr = urlparse(source_path)
            protocol = pr.scheme.lower() if pr.scheme else "file"

            if protocol == "file" or source_path.startswith("/"):
                # For file or absolute paths, use the whole path as the prefix
                # and leave bucket empty
                bucket = ""
                prefix = source_path if source_path.startswith("/") else pr.path
            else:
                # For object storage, extract bucket and prefix
                bucket = pr.netloc
                prefix = pr.path
                if prefix.startswith("/"):
                    prefix = prefix[1:]

            # Add the mapping to the nested dict
            self._mapping[protocol][bucket].append((prefix, dest_profile))

        # Sort each bucket's prefixes by length (longest first) for optimal matching
        for protocol, buckets in self._mapping.items():
            for bucket, prefixes in buckets.items():
                self._mapping[protocol][bucket] = sorted(prefixes, key=lambda x: len(x[0]), reverse=True)

    def find_mapping(self, url: str) -> Optional[tuple[str, str]]:
        """
        Find the best matching mapping for the given URL.

        :param url: URL to find matching mapping for
        :return: Tuple of (profile_name, translated_path) if a match is found, None otherwise
        """
        # Parse the URL
        pr = urlparse(url)
        protocol = pr.scheme.lower() if pr.scheme else "file"

        # For file paths or absolute paths
        if protocol == "file" or url.startswith("/"):
            path = url if url.startswith("/") else pr.path

            possible_mapping = self._mapping[protocol][""] if protocol in self._mapping else []

            # Check each prefix (already sorted by length, longest first)
            for prefix, profile in possible_mapping:
                if path.startswith(prefix):
                    # Calculate the relative path
                    rel_path = path[len(prefix) :]
                    if not rel_path.startswith("/"):
                        rel_path = "/" + rel_path
                    return profile, rel_path

            return None

        # For object storage
        bucket = pr.netloc
        path = pr.path
        if path.startswith("/"):
            path = path[1:]

        # Check bucket-specific mapping
        possible_mapping = (
            self._mapping[protocol][bucket] if (protocol in self._mapping and bucket in self._mapping[protocol]) else []
        )

        # Check each prefix (already sorted by length, longest first)
        for prefix, profile in possible_mapping:
            # matching prefix
            if path.startswith(prefix):
                rel_path = path[len(prefix) :]
                # Remove leading slash if present
                if rel_path.startswith("/"):
                    rel_path = rel_path[1:]

                return profile, rel_path

        return None


class StorageClientConfig:
    """
    Configuration class for the :py:class:`multistorageclient.StorageClient`.
    """

    profile: str
    storage_provider: Optional[StorageProvider]
    credentials_provider: Optional[CredentialsProvider]
    storage_provider_profiles: Optional[list[str]]
    metadata_provider: Optional[MetadataProvider]
    cache_config: Optional[CacheConfig]
    cache_manager: Optional[CacheManager]
    retry_config: Optional[RetryConfig]
    telemetry_provider: Optional[Callable[[], Telemetry]]
    replicas: list[Replica]
    autocommit_config: Optional[AutoCommitConfig]

    _config_dict: Optional[dict[str, Any]]

    def __init__(
        self,
        profile: str,
        storage_provider: Optional[StorageProvider] = None,
        credentials_provider: Optional[CredentialsProvider] = None,
        storage_provider_profiles: Optional[list[str]] = None,
        metadata_provider: Optional[MetadataProvider] = None,
        cache_config: Optional[CacheConfig] = None,
        cache_manager: Optional[CacheManager] = None,
        retry_config: Optional[RetryConfig] = None,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
        replicas: Optional[list[Replica]] = None,
        autocommit_config: Optional[AutoCommitConfig] = None,
    ):
        # exactly one of storage_provider or storage_provider_profiles must be set
        if storage_provider and storage_provider_profiles:
            raise ValueError(
                "Cannot specify both storage_provider and storage_provider_profiles. "
                "Use storage_provider for SingleStorageClient or storage_provider_profiles for CompositeStorageClient."
            )
        if not storage_provider and not storage_provider_profiles:
            raise ValueError("Must specify either storage_provider or storage_provider_profiles.")

        if replicas is None:
            replicas = []
        self.profile = profile
        self.storage_provider = storage_provider
        self.credentials_provider = credentials_provider
        self.storage_provider_profiles = storage_provider_profiles
        self.metadata_provider = metadata_provider
        self.cache_config = cache_config
        self.cache_manager = cache_manager
        self.retry_config = retry_config
        self.telemetry_provider = telemetry_provider
        self.replicas = replicas
        self.autocommit_config = autocommit_config

    @staticmethod
    def from_json(
        config_json: str,
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from a JSON string.

        :param config_json: Configuration JSON string.
        :param profile: Profile to use.
        :param telemetry_provider: A function that provides a telemetry instance. The function must be defined at the top level of a module to work with pickling.
        """
        config_dict = json.loads(config_json)
        return StorageClientConfig.from_dict(
            config_dict=config_dict, profile=profile, telemetry_provider=telemetry_provider
        )

    @staticmethod
    def from_yaml(
        config_yaml: str,
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from a YAML string.

        :param config_yaml: Configuration YAML string.
        :param profile: Profile to use.
        :param telemetry_provider: A function that provides a telemetry instance. The function must be defined at the top level of a module to work with pickling.
        """
        config_dict = yaml.safe_load(config_yaml)
        return StorageClientConfig.from_dict(
            config_dict=config_dict, profile=profile, telemetry_provider=telemetry_provider
        )

    @staticmethod
    def from_dict(
        config_dict: dict[str, Any],
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        skip_validation: bool = False,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from a Python dictionary.

        :param config_dict: Configuration Python dictionary.
        :param profile: Profile to use.
        :param skip_validation: Skip configuration schema validation.
        :param telemetry_provider: A function that provides a telemetry instance. The function must be defined at the top level of a module to work with pickling.
        """
        # Validate the config file with predefined JSON schema
        if not skip_validation:
            validate_config(config_dict)

        # Load config
        loader = StorageClientConfigLoader(
            config_dict=config_dict, profile=profile, telemetry_provider=telemetry_provider
        )
        config = loader.build_config()

        return config

    @staticmethod
    def from_file(
        config_file_paths: Optional[Iterable[str]] = None,
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from the first file found.

        :param config_file_paths: Configuration file search paths. If omitted, the default search paths are used (see :py:meth:`StorageClientConfig.read_msc_config`).
        :param profile: Profile to use.
        :param telemetry_provider: A function that provides a telemetry instance. The function must be defined at the top level of a module to work with pickling.
        """
        msc_config_dict, msc_config_file = StorageClientConfig.read_msc_config(config_file_paths=config_file_paths)
        # Parse rclone config file.
        rclone_config_dict, rclone_config_file = read_rclone_config()

        # Merge config files.
        merged_config, conflicted_keys = merge_dictionaries_no_overwrite(msc_config_dict, rclone_config_dict)
        if conflicted_keys:
            raise ValueError(
                f'Conflicting keys found in configuration files "{msc_config_file}" and "{rclone_config_file}: {conflicted_keys}'
            )
        merged_profiles = merged_config.get("profiles", {})

        # Check if profile is in merged_profiles
        if profile in merged_profiles:
            return StorageClientConfig.from_dict(
                config_dict=merged_config, profile=profile, telemetry_provider=telemetry_provider
            )
        else:
            # Check if profile is the default profile or an implicit profile
            if profile == DEFAULT_POSIX_PROFILE_NAME:
                implicit_profile_config = DEFAULT_POSIX_PROFILE
            elif profile.startswith("_"):
                # Handle implicit profiles
                parts = profile[1:].split("-", 1)
                if len(parts) == 2:
                    protocol, bucket = parts
                    # Verify it's a supported protocol
                    if protocol not in SUPPORTED_IMPLICIT_PROFILE_PROTOCOLS:
                        raise ValueError(f'Unsupported protocol in implicit profile: "{protocol}"')
                    implicit_profile_config = create_implicit_profile_config(
                        profile_name=profile, protocol=protocol, base_path=bucket
                    )
                else:
                    raise ValueError(f'Invalid implicit profile format: "{profile}"')
            else:
                raise ValueError(
                    f'Profile "{profile}" not found in configuration files. Configuration was checked in '
                    f"{msc_config_file or 'MSC config (not found)'} and {rclone_config_file or 'Rclone config (not found)'}. "
                    f"Please verify that the profile exists and that configuration files are correctly located."
                )
            # merge the implicit profile config into the merged config so the cache & observability config can be inherited
            if "profiles" not in merged_config:
                merged_config["profiles"] = implicit_profile_config["profiles"]
            else:
                merged_config["profiles"][profile] = implicit_profile_config["profiles"][profile]
            # the config is already validated while reading, skip the validation for implicit profiles which start profile with "_"
            return StorageClientConfig.from_dict(
                config_dict=merged_config, profile=profile, skip_validation=True, telemetry_provider=telemetry_provider
            )

    @staticmethod
    def from_provider_bundle(
        config_dict: dict[str, Any],
        provider_bundle: Union[ProviderBundle, ProviderBundleV2],
        telemetry_provider: Optional[Callable[[], Telemetry]] = None,
    ) -> "StorageClientConfig":
        loader = StorageClientConfigLoader(
            config_dict=config_dict, provider_bundle=provider_bundle, telemetry_provider=telemetry_provider
        )
        config = loader.build_config()
        config._config_dict = None  # Explicitly mark as None to avoid confusing pickling errors
        return config

    @staticmethod
    def read_msc_config(
        config_file_paths: Optional[Iterable[str]] = None,
    ) -> tuple[Optional[dict[str, Any]], Optional[str]]:
        """Get the MSC configuration dictionary and the path of the first file found.

        If no config paths are specified, configs are searched in the following order:

        1. ``MSC_CONFIG`` environment variable (highest precedence)
        2. Standard search paths (user-specified config and system-wide config)

        :param config_file_paths: Configuration file search paths. If omitted, the default search paths are used.
        :return: Tuple of ``(config_dict, config_file_path)``. ``config_dict`` is the MSC configuration
                 dictionary or empty dict if no config was found. ``config_file_path`` is the absolute
                 path of the config file used, or ``None`` if no config file was found.
        """
        config_dict: dict[str, Any] = {}
        config_file_path: Optional[str] = None

        config_file_paths = list(config_file_paths or [])

        # Add default paths if none provided.
        if len(config_file_paths) == 0:
            # Environment variable.
            msc_config_env = os.getenv("MSC_CONFIG", None)
            if msc_config_env is not None:
                config_file_paths.append(msc_config_env)

            # Standard search paths.
            config_file_paths.extend(_find_config_file_paths())

        # Normalize + absolutize paths.
        config_file_paths = [os.path.abspath(path) for path in config_file_paths]

        # Log plan.
        logger.debug(f"Searching MSC config file paths: {config_file_paths}")

        # Load config.
        for path in config_file_paths:
            if os.path.exists(path):
                try:
                    with open(path) as f:
                        if path.endswith(".json"):
                            config_dict = json.load(f)
                        else:
                            config_dict = yaml.safe_load(f)
                        config_file_path = path
                    # Use the first config file.
                    break
                except Exception as e:
                    raise ValueError(f"malformed MSC config file: {path}, exception: {e}")

        # Log result.
        if config_file_path is None:
            logger.debug("No MSC config files found in any of the search locations.")
        else:
            logger.debug(f"Using MSC config file: {config_file_path}")

        if config_dict:
            validate_config(config_dict)

            if "include" in config_dict and config_file_path:
                config_dict = _load_and_merge_includes(config_file_path, config_dict)

        return config_dict, config_file_path

    @staticmethod
    def read_path_mapping() -> PathMapping:
        """
        Get the path mapping defined in the MSC configuration.

        Path mappings create a nested structure of protocol -> bucket -> [(prefix, profile)]
        where entries are sorted by prefix length (longest first) for optimal matching.
        Longer paths take precedence when matching.

        :return: A PathMapping instance with translation mappings
        """
        try:
            return PathMapping.from_config()
        except Exception:
            # Log the error but continue - this shouldn't stop the application from working
            logger.error("Failed to load path_mapping from MSC config")
            return PathMapping()

    def __getstate__(self) -> dict[str, Any]:
        state = self.__dict__.copy()
        if not state.get("_config_dict"):
            raise ValueError("StorageClientConfig is not serializable")
        del state["credentials_provider"]
        del state["storage_provider"]
        del state["metadata_provider"]
        del state["cache_manager"]
        del state["replicas"]
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        # Presence checked by __getstate__.
        config_dict = state["_config_dict"]
        loader = StorageClientConfigLoader(
            config_dict=config_dict,
            profile=state["profile"],
            telemetry_provider=state["telemetry_provider"],
        )
        new_config = loader.build_config()
        self.profile = new_config.profile
        self.storage_provider = new_config.storage_provider
        self.credentials_provider = new_config.credentials_provider
        self.storage_provider_profiles = new_config.storage_provider_profiles
        self.metadata_provider = new_config.metadata_provider
        self.cache_config = new_config.cache_config
        self.cache_manager = new_config.cache_manager
        self.retry_config = new_config.retry_config
        self.telemetry_provider = new_config.telemetry_provider
        self._config_dict = config_dict
        self.replicas = new_config.replicas
        self.autocommit_config = new_config.autocommit_config
