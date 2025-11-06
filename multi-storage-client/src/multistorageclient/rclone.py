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

import configparser
import os
from functools import cache
from pathlib import Path
from typing import Any, Optional

from .utils import find_executable_path

DEFAULT_RCLONE_CONFIG_FILE_SEARCH_PATHS = (
    "/etc/rclone.conf",
    os.path.join(os.getenv("HOME", ""), ".config", "rclone", "rclone.conf"),
    os.path.join(os.getenv("HOME", ""), ".rclone.conf"),
)


def _get_rclone_config_path() -> Optional[Path]:
    """
    Attempt to locate rclone.conf in several standard locations:
      1. The same directory as the `rclone` executable (if found in PATH).
      2. XDG_CONFIG_HOME/rclone/rclone.conf (if XDG_CONFIG_HOME is set).
      3. The default search paths in DEFAULT_RCLONE_CONFIG_FILE_SEARCH_PATHS.

    :return: Path to the located rclone.conf, or None if not found
    """
    # First, check if rclone executable is in PATH
    rclone_exe_path = find_executable_path("rclone")
    if rclone_exe_path is not None and rclone_exe_path.is_file():
        rclone_config_path = rclone_exe_path.with_name("rclone.conf")
        if rclone_config_path.is_file():
            return rclone_config_path

    # Second, check XDG_CONFIG_HOME
    xdg_config_home = os.environ.get("XDG_CONFIG_HOME")
    if xdg_config_home and Path(xdg_config_home).is_dir():
        rclone_config_path = Path(xdg_config_home) / "rclone" / "rclone.conf"
        if rclone_config_path.is_file():
            return rclone_config_path

    # Third, check default search locations
    for filename in DEFAULT_RCLONE_CONFIG_FILE_SEARCH_PATHS:
        filepath = Path(filename)
        if filepath.is_file():
            return filepath

    return None


def _set_if_exists(
    section: configparser.SectionProxy, target_dict: dict[str, Any], msc_key: str, rclone_key: str
) -> None:
    """
    Checks for a specific rclone key in a config section; if found, sets its value with MSC key in the target dict.

    rclone and MSC might use different keys for the same field. Therefore, we need to ensure

    :param section: The config section (configparser.SectionProxy) from which to read
    :param target_dict: The dictionary to update
    :param msc_key: The MSC key
    :param rclone_key: The rclone key to look for in the config section
    :return: None
    """
    if rclone_key in section:
        target_dict[msc_key] = section[rclone_key]


def _parse_s3_storage_provider_config(section: configparser.SectionProxy) -> tuple[dict, dict]:
    """
    Parse S3 related keys from a config section.

    :param section: The configparser.SectionProxy representing one remote (section) in rclone config
    :return: A tuple of:
        - storage_provider_options (dict): Includes region_name, endpoint_url, etc.
        - credentials_provider (dict): Includes type (if applicable) and an 'options' dict
    """
    storage_provider_options: dict[str, Any] = {}
    _set_if_exists(section, storage_provider_options, "region_name", "region")
    _set_if_exists(section, storage_provider_options, "endpoint_url", "endpoint")
    _set_if_exists(section, storage_provider_options, "base_path", "base_path")
    _set_if_exists(section, storage_provider_options, "request_checksum_calculation", "request_checksum_calculation")
    _set_if_exists(section, storage_provider_options, "response_checksum_validation", "response_checksum_validation")

    credentials_provider_options: dict[str, Any] = {}
    _set_if_exists(section, credentials_provider_options, "access_key", "access_key_id")
    _set_if_exists(section, credentials_provider_options, "secret_key", "secret_access_key")
    _set_if_exists(section, credentials_provider_options, "session_token", "session_token")

    credentials_provider: dict[str, Any] = {}
    credentials_provider["options"] = credentials_provider_options

    # If there's at least an access_key, we can consider this S3Credentials
    if "access_key" in credentials_provider_options:
        credentials_provider["type"] = "S3Credentials"

    return storage_provider_options, credentials_provider


def _parse_azure_storage_provider_config(section: configparser.SectionProxy) -> tuple[dict, dict]:
    """
    Parse Azure related keys from a config section.

    :param section: The configparser.SectionProxy representing one remote (section) in rclone config
    :return: A tuple of:
        - storage_provider_options (dict): Includes storage specific provider options, etc.
        - credentials_provider (dict): Includes type (if applicable) and an 'options' dict
    """
    storage_provider_options: dict[str, Any] = {}
    _set_if_exists(section, storage_provider_options, "endpoint_url", "endpoint")
    _set_if_exists(section, storage_provider_options, "base_path", "base_path")

    credentials_provider_options: dict[str, Any] = {}
    _set_if_exists(section, credentials_provider_options, "connection", "connection")

    credentials_provider: dict[str, Any] = {"options": credentials_provider_options}

    # If there's a connection string, we assume static Azure credentials are being used
    if "connection" in credentials_provider_options:
        credentials_provider["type"] = "AzureCredentials"

    return storage_provider_options, credentials_provider


def _parse_gcs_storage_provider_config(section: configparser.SectionProxy) -> tuple[dict, dict]:
    """
    Parse Google Cloud Storage related keys from a config section.

    :param section: The configparser.SectionProxy representing one remote (section) in rclone config
    :return: A tuple of:
        - storage_provider_options (dict): Includes storage specific provider options, etc.
        - credentials_provider (dict): Includes type (if applicable) and an 'options' dict
    """
    storage_provider_options: dict[str, Any] = {}
    # rclone uses 'project_number' for GCS.
    _set_if_exists(section, storage_provider_options, "project_id", "project_number")
    _set_if_exists(section, storage_provider_options, "endpoint_url", "endpoint")
    _set_if_exists(section, storage_provider_options, "base_path", "base_path")

    return storage_provider_options, {}


def _parse_oci_storage_provider_config(section: configparser.SectionProxy) -> tuple[dict, dict]:
    """
    Parse Oracle Cloud Infrastructure Object Storage related keys from a config section.

    :param section: The configparser.SectionProxy representing one remote (section) in rclone config
    :return: A tuple of:
        - storage_provider_options (dict): Includes storage specific provider options, etc.
        - credentials_provider (dict): Includes type (if applicable) and an 'options' dict
    """
    storage_provider_options: dict[str, Any] = {}
    _set_if_exists(section, storage_provider_options, "namespace", "namespace")
    _set_if_exists(section, storage_provider_options, "base_path", "base_path")

    return storage_provider_options, {}


def _parse_ais_storage_provider_config(section: configparser.SectionProxy) -> tuple[dict, dict]:
    """
    Parse AIStore related keys from a config section.

    :param section: The configparser.SectionProxy representing one remote (section) in rclone config
    :return: A tuple of:
        - storage_provider_options (dict): Includes storage specific provider options, etc.
        - credentials_provider (dict): Includes type (if applicable) and an 'options' dict
    """
    storage_provider_options: dict[str, Any] = {}
    _set_if_exists(section, storage_provider_options, "endpoint", "endpoint")
    _set_if_exists(section, storage_provider_options, "base_path", "base_path")

    return storage_provider_options, {}


def _parse_config_section(section: configparser.SectionProxy) -> dict[str, Any]:
    """
    Parses a config section to create a dictionary with 'storage_provider' and 'credentials_provider'.

    :param section: A configparser.SectionProxy representing a single remote's configuration
    :return: A dictionary of the form:
        {
          "storage_provider": {
            "type": <storage_type>,
            "options": {...}
          },
          "credentials_provider": {...}
        }
    """
    storage_type = section.get("type", "").lower()

    storage_provider_options = {}
    credentials_provider = {}

    # First, parse storage provider specific options.
    #
    # To infer the storage provider, use both:
    #   - MSC configuration storage type key (e.g. azure)
    #   - rclone default storage type key (e.g. azureblob)
    #
    # Then, convert to storage type to MSC configuration storage key (e.g. azure).
    if storage_type == "s3" or storage_type == "s8k":
        storage_provider_options, credentials_provider = _parse_s3_storage_provider_config(section)
    elif storage_type == "azure" or storage_type == "azureblob":
        storage_provider_options, credentials_provider = _parse_azure_storage_provider_config(section)
        storage_type = "azure"
    elif storage_type == "gcs" or storage_type == "google cloud storage":
        storage_provider_options, credentials_provider = _parse_gcs_storage_provider_config(section)
        storage_type = "gcs"
    elif storage_type == "oci" or storage_type == "oracleobjectstorage":
        storage_provider_options, credentials_provider = _parse_oci_storage_provider_config(section)
        storage_type = "oci"
    elif storage_type == "ais":
        storage_provider_options, credentials_provider = _parse_ais_storage_provider_config(section)
    elif storage_type in ("file"):
        # Gather all generic config keys for all other supported storage providers.
        storage_provider_options = {k: v for k, v in section.items()}
    else:
        return {}

    # Set default base_path to make it compatible with rclone config
    storage_provider_options["base_path"] = storage_provider_options.get("base_path", "")

    storage_provider: dict[str, Any] = {
        "type": storage_type,
        "options": storage_provider_options,
    }

    config = {}
    if storage_provider:
        config["storage_provider"] = storage_provider
    if credentials_provider:
        config["credentials_provider"] = credentials_provider

    return config


def _parse_from_config_parser(config: configparser.ConfigParser) -> dict[str, Any]:
    """
    Parse a ConfigParser object containing one or more rclone sections (remotes).

    :param config: A configparser.ConfigParser object with zero or more sections
    :return: A dictionary of the form:
        {
            "profiles": {
                "<section_name>": {
                    "name": <section_name>,
                    "storage_provider": {...},
                    "credentials_provider": {...}
                },
                ...
            }
        }
    """
    config_entries = {}
    for section in config.sections():
        config_entry = _parse_config_section(config[section])
        if not config_entry:
            continue
        config_entry["name"] = section
        config_entries[section] = config_entry

    return {"profiles": config_entries}


@cache
def read_rclone_config() -> tuple[Optional[dict[str, Any]], Optional[str]]:
    """
    High-level utility to locate an rclone.conf file, parse it, and return its representation.

    :return: A tuple of (parsed_config, config_path):
        - parsed_config: the representation of configuration, or None if config not found
        - config_path: The absolute path to the rclone.conf file, or None if not found
    """
    config_path = _get_rclone_config_path()
    if config_path is None:
        return None, config_path

    config = configparser.ConfigParser()
    config.read(config_path)

    return _parse_from_config_parser(config), os.path.abspath(config_path)
