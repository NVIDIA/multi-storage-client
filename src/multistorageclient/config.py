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

import json
import logging
import os
import tempfile
from collections import defaultdict
from collections.abc import Sequence
from multiprocessing import current_process
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urlparse

import opentelemetry.metrics as api_metrics
import yaml

from .cache import DEFAULT_CACHE_LINE_SIZE, DEFAULT_CACHE_SIZE, CacheManager
from .caching.cache_config import CacheConfig, EvictionPolicyConfig
from .instrumentation import setup_opentelemetry
from .providers.manifest_metadata import ManifestMetadataProvider
from .rclone import read_rclone_config
from .schema import validate_config
from .telemetry import Telemetry, TelemetryMode
from .telemetry import init as telemetry_init
from .telemetry.attributes.base import AttributesProvider
from .types import (
    DEFAULT_RETRY_ATTEMPTS,
    DEFAULT_RETRY_DELAY,
    MSC_PROTOCOL,
    AutoCommitConfig,
    CredentialsProvider,
    MetadataProvider,
    ProviderBundle,
    Replica,
    RetryConfig,
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

_TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING = {
    "environment_variables": "multistorageclient.telemetry.attributes.environment_variables.EnvironmentVariablesAttributesProvider",
    "host": "multistorageclient.telemetry.attributes.host.HostAttributesProvider",
    "msc_config": "multistorageclient.telemetry.attributes.msc_config.MSCConfigAttributesProvider",
    "process": "multistorageclient.telemetry.attributes.process.ProcessAttributesProvider",
    "static": "multistorageclient.telemetry.attributes.static.StaticAttributesProvider",
    "thread": "multistorageclient.telemetry.attributes.thread.ThreadAttributesProvider",
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
    "s8k": "S8KStorageProvider",
    "gcs_s3": "GoogleS3StorageProvider",
    "huggingface": "HuggingFaceStorageProvider",
}

CREDENTIALS_PROVIDER_MAPPING = {
    "S3Credentials": "StaticS3CredentialsProvider",
    "AzureCredentials": "StaticAzureCredentialsProvider",
    "AISCredentials": "StaticAISCredentialProvider",
    "GoogleIdentityPoolCredentialsProvider": "GoogleIdentityPoolCredentialsProvider",
    "HuggingFaceCredentials": "HuggingFaceCredentialsProvider",
}


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


DEFAULT_CACHE_REFRESH_INTERVAL = 300


class StorageClientConfigLoader:
    _provider_bundle: Optional[ProviderBundle]
    _profiles: dict[str, Any]
    _profile: str
    _profile_dict: dict[str, Any]
    _opentelemetry_dict: Optional[dict[str, Any]]
    _metric_gauges: Optional[dict[Telemetry.GaugeName, api_metrics._Gauge]]
    _metric_counters: Optional[dict[Telemetry.CounterName, api_metrics.Counter]]
    _metric_attributes_providers: Optional[Sequence[AttributesProvider]]
    _cache_dict: Optional[dict[str, Any]]

    def __init__(
        self,
        config_dict: dict[str, Any],
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        provider_bundle: Optional[ProviderBundle] = None,
        telemetry: Optional[Telemetry] = None,
        metric_gauges: Optional[dict[Telemetry.GaugeName, api_metrics._Gauge]] = None,
        metric_counters: Optional[dict[Telemetry.CounterName, api_metrics.Counter]] = None,
        metric_attributes_providers: Optional[Sequence[AttributesProvider]] = None,
    ) -> None:
        """
        Initializes a :py:class:`StorageClientConfigLoader` to create a
        StorageClientConfig. Components are built using the ``config_dict`` and
        profile, but a pre-built provider_bundle takes precedence.

        :param config_dict: Dictionary of configuration options.
        :param profile: Name of profile in ``config_dict`` to use to build configuration.
        :param provider_bundle: Optional pre-built :py:class:`multistorageclient.types.ProviderBundle`, takes precedence over ``config_dict``.
        :param telemetry: Optional telemetry instance to use, takes precedence over ``metric_gauges``, ``metric_counters``, and ``metric_attributes_providers``.
        :param metric_gauges: Optional metric gauges to use.
        :param metric_counters: Optional metric counters to use.
        :param metric_attributes_providers: Optional metric attributes providers to use.
        """
        # ProviderBundle takes precedence
        self._provider_bundle = provider_bundle

        # Interpolates all environment variables into actual values.
        config_dict = expand_env_vars(config_dict)

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
        self._profile_dict = profile_dict

        self._opentelemetry_dict = config_dict.get("opentelemetry", None)

        self._metric_gauges = metric_gauges
        self._metric_counters = metric_counters
        self._metric_attributes_providers = metric_attributes_providers
        if self._opentelemetry_dict is not None:
            # Try to create a telemetry instance only if no telemetry instance or OpenTelemetry instruments are provided (e.g. when unpickling).
            #
            # Multiprocessing unpickles during the Python interpreter's bootstrap phase for new processes.
            # New processes (e.g. multiprocessing manager server) can't be created during this phase.
            if telemetry is None and not any(
                (self._metric_gauges, self._metric_counters, self._metric_attributes_providers)
            ):
                try:
                    # Try to create a telemetry instance with the default mode heuristic.
                    telemetry = telemetry_init()
                except (AssertionError, OSError, RuntimeError, ValueError):
                    try:
                        if current_process().daemon:
                            # Daemons can't have child processes.
                            #
                            # Try to create a telemetry instance in local mode.
                            #
                            # ⚠️ This may cause CPU contention if the current process is compute-intensive
                            # and a high collect and/or export frequency is used due to global interpreter lock (GIL).
                            telemetry = telemetry_init(mode=TelemetryMode.LOCAL)
                        else:
                            # Try to create a telemetry instance in server mode.
                            telemetry = telemetry_init(mode=TelemetryMode.SERVER)
                    except (AssertionError, OSError, RuntimeError, ValueError):
                        # Don't throw on telemetry init failures.
                        logger.error("Failed to automatically create telemetry instance!", exc_info=True)

            if "metrics" in self._opentelemetry_dict:
                if telemetry is not None:
                    self._metric_gauges = {}
                    for name in Telemetry.GaugeName:
                        gauge = telemetry.gauge(config=self._opentelemetry_dict["metrics"], name=name)
                        if gauge is not None:
                            self._metric_gauges[name] = gauge
                    self._metric_counters = {}
                    for name in Telemetry.CounterName:
                        counter = telemetry.counter(config=self._opentelemetry_dict["metrics"], name=name)
                        if counter is not None:
                            self._metric_counters[name] = counter

                    if "attributes" in self._opentelemetry_dict["metrics"]:
                        attributes_providers: list[AttributesProvider] = []
                        attributes_provider_configs: list[dict[str, Any]] = self._opentelemetry_dict["metrics"][
                            "attributes"
                        ]
                        for config in attributes_provider_configs:
                            attributes_provider_type: str = config["type"]
                            attributes_provider_fully_qualified_name = _TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING.get(
                                attributes_provider_type, attributes_provider_type
                            )
                            attributes_provider_module_name, attributes_provider_class_name = (
                                attributes_provider_fully_qualified_name.rsplit(".", 1)
                            )
                            cls = import_class(attributes_provider_class_name, attributes_provider_module_name)
                            attributes_provider_options = config.get("options", {})
                            if (
                                attributes_provider_fully_qualified_name
                                == _TELEMETRY_ATTRIBUTES_PROVIDER_MAPPING["msc_config"]
                            ):
                                attributes_provider_options["config_dict"] = config_dict
                            attributes_provider: AttributesProvider = cls(**attributes_provider_options)
                            attributes_providers.append(attributes_provider)
                        self._metric_attributes_providers = tuple(attributes_providers)
                elif not any([metric_gauges, metric_counters, metric_attributes_providers]):
                    # TODO: Remove "beta" from the log once legacy metrics are removed.
                    logger.error("No telemetry instance! Disabling beta metrics.")

        self._cache_dict = config_dict.get("cache", None)

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
        if self._metric_gauges is not None:
            storage_options["metric_gauges"] = self._metric_gauges
        if self._metric_counters is not None:
            storage_options["metric_counters"] = self._metric_counters
        if self._metric_attributes_providers is not None:
            storage_options["metric_attributes_providers"] = self._metric_attributes_providers
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

    def _build_provider_bundle(self) -> ProviderBundle:
        if self._provider_bundle:
            return self._provider_bundle  # Return if previously provided.

        # Load 3rd party extension
        provider_bundle_dict = self._profile_dict.get("provider_bundle", None)
        if provider_bundle_dict:
            return self._build_provider_bundle_from_extension(provider_bundle_dict)

        return self._build_provider_bundle_from_config(self._profile_dict)

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

    def build_config(self) -> "StorageClientConfig":
        bundle = self._build_provider_bundle()

        # Validate replicas to prevent circular references
        self._validate_replicas(bundle.replicas)

        storage_provider = self._build_storage_provider(
            bundle.storage_provider_config.type,
            bundle.storage_provider_config.options,
            bundle.credentials_provider,
        )

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
                eviction_policy = EvictionPolicyConfig(
                    policy=policy,
                    refresh_interval=cache_dict["eviction_policy"].get(
                        "refresh_interval", DEFAULT_CACHE_REFRESH_INTERVAL
                    ),
                )
            else:
                eviction_policy = EvictionPolicyConfig(policy="fifo", refresh_interval=DEFAULT_CACHE_REFRESH_INTERVAL)

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

        # retry options
        retry_config_dict = self._profile_dict.get("retry", None)
        if retry_config_dict:
            attempts = retry_config_dict.get("attempts", DEFAULT_RETRY_ATTEMPTS)
            delay = retry_config_dict.get("delay", DEFAULT_RETRY_DELAY)
            retry_config = RetryConfig(attempts=attempts, delay=delay)
        else:
            retry_config = RetryConfig(attempts=DEFAULT_RETRY_ATTEMPTS, delay=DEFAULT_RETRY_DELAY)

        # autocommit options
        autocommit_config = AutoCommitConfig()
        autocommit_dict = self._profile_dict.get("autocommit", None)
        if autocommit_dict:
            interval_minutes = autocommit_dict.get("interval_minutes", None)
            at_exit = autocommit_dict.get("at_exit", False)
            autocommit_config = AutoCommitConfig(interval_minutes=interval_minutes, at_exit=at_exit)

        # set up OpenTelemetry providers once per process
        #
        # TODO: Legacy, need to remove.
        if self._opentelemetry_dict:
            setup_opentelemetry(self._opentelemetry_dict)

        return StorageClientConfig(
            profile=self._profile,
            storage_provider=storage_provider,
            credentials_provider=bundle.credentials_provider,
            metadata_provider=bundle.metadata_provider,
            cache_config=cache_config,
            cache_manager=cache_manager,
            retry_config=retry_config,
            metric_gauges=self._metric_gauges,
            metric_counters=self._metric_counters,
            metric_attributes_providers=self._metric_attributes_providers,
            replicas=bundle.replicas,
            autocommit_config=autocommit_config,
        )


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
    storage_provider: StorageProvider
    credentials_provider: Optional[CredentialsProvider]
    metadata_provider: Optional[MetadataProvider]
    cache_config: Optional[CacheConfig]
    cache_manager: Optional[CacheManager]
    retry_config: Optional[RetryConfig]
    metric_gauges: Optional[dict[Telemetry.GaugeName, api_metrics._Gauge]]
    metric_counters: Optional[dict[Telemetry.CounterName, api_metrics.Counter]]
    metric_attributes_providers: Optional[Sequence[AttributesProvider]]
    replicas: list[Replica]
    autocommit_config: Optional[AutoCommitConfig]

    _config_dict: Optional[dict[str, Any]]

    def __init__(
        self,
        profile: str,
        storage_provider: StorageProvider,
        credentials_provider: Optional[CredentialsProvider] = None,
        metadata_provider: Optional[MetadataProvider] = None,
        cache_config: Optional[CacheConfig] = None,
        cache_manager: Optional[CacheManager] = None,
        retry_config: Optional[RetryConfig] = None,
        metric_gauges: Optional[dict[Telemetry.GaugeName, api_metrics._Gauge]] = None,
        metric_counters: Optional[dict[Telemetry.CounterName, api_metrics.Counter]] = None,
        metric_attributes_providers: Optional[Sequence[AttributesProvider]] = None,
        replicas: Optional[list[Replica]] = None,
        autocommit_config: Optional[AutoCommitConfig] = None,
    ):
        if replicas is None:
            replicas = []
        self.profile = profile
        self.storage_provider = storage_provider
        self.credentials_provider = credentials_provider
        self.metadata_provider = metadata_provider
        self.cache_config = cache_config
        self.cache_manager = cache_manager
        self.retry_config = retry_config
        self.metric_gauges = metric_gauges
        self.metric_counters = metric_counters
        self.metric_attributes_providers = metric_attributes_providers
        self.replicas = replicas
        self.autocommit_config = autocommit_config

    @staticmethod
    def from_json(
        config_json: str, profile: str = DEFAULT_POSIX_PROFILE_NAME, telemetry: Optional[Telemetry] = None
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from a JSON string.

        :param config_json: Configuration JSON string.
        :param profile: Profile to use.
        :param telemetry: Telemetry instance to use.
        """
        config_dict = json.loads(config_json)
        return StorageClientConfig.from_dict(config_dict=config_dict, profile=profile, telemetry=telemetry)

    @staticmethod
    def from_yaml(
        config_yaml: str, profile: str = DEFAULT_POSIX_PROFILE_NAME, telemetry: Optional[Telemetry] = None
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from a YAML string.

        :param config_yaml: Configuration YAML string.
        :param profile: Profile to use.
        :param telemetry: Telemetry instance to use.
        """
        config_dict = yaml.safe_load(config_yaml)
        return StorageClientConfig.from_dict(config_dict=config_dict, profile=profile, telemetry=telemetry)

    @staticmethod
    def from_dict(
        config_dict: dict[str, Any],
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        skip_validation: bool = False,
        telemetry: Optional[Telemetry] = None,
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from a Python dictionary.

        :param config_dict: Configuration Python dictionary.
        :param profile: Profile to use.
        :param skip_validation: Skip configuration schema validation.
        :param telemetry: Telemetry instance to use.
        """
        # Validate the config file with predefined JSON schema
        if not skip_validation:
            validate_config(config_dict)

        # Load config
        loader = StorageClientConfigLoader(config_dict=config_dict, profile=profile, telemetry=telemetry)
        config = loader.build_config()
        config._config_dict = config_dict

        return config

    @staticmethod
    def from_file(
        config_file_paths: Optional[Iterable[str]] = None,
        profile: str = DEFAULT_POSIX_PROFILE_NAME,
        telemetry: Optional[Telemetry] = None,
    ) -> "StorageClientConfig":
        """
        Load a storage client configuration from the first file found.

        :param config_file_paths: Configuration file search paths. If omitted, the default search paths are used (see :py:meth:`StorageClientConfig.read_msc_config`).
        :param profile: Profile to use.
        :param telemetry: Telemetry instance to use.
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
            return StorageClientConfig.from_dict(config_dict=merged_config, profile=profile, telemetry=telemetry)
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
                config_dict=merged_config, profile=profile, skip_validation=True, telemetry=telemetry
            )

    @staticmethod
    def from_provider_bundle(
        config_dict: dict[str, Any], provider_bundle: ProviderBundle, telemetry: Optional[Telemetry] = None
    ) -> "StorageClientConfig":
        loader = StorageClientConfigLoader(
            config_dict=config_dict, provider_bundle=provider_bundle, telemetry=telemetry
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
            logger.info(f"Using MSC config file: {config_file_path}")

        if config_dict:
            validate_config(config_dict)
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
            metric_gauges=state["metric_gauges"],
            metric_counters=state["metric_counters"],
            metric_attributes_providers=state["metric_attributes_providers"],
        )
        new_config = loader.build_config()
        self.profile = new_config.profile
        self.storage_provider = new_config.storage_provider
        self.credentials_provider = new_config.credentials_provider
        self.metadata_provider = new_config.metadata_provider
        self.cache_config = new_config.cache_config
        self.cache_manager = new_config.cache_manager
        self.retry_config = new_config.retry_config
        self.metric_gauges = new_config.metric_gauges
        self.metric_counters = new_config.metric_counters
        self.metric_attributes_providers = new_config.metric_attributes_providers
        self._config_dict = config_dict
        self.replicas = new_config.replicas
        self.autocommit_config = new_config.autocommit_config
