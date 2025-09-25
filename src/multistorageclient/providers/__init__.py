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

import importlib
import logging
from typing import Any

from .manifest_metadata import ManifestMetadataProvider
from .posix_file import PosixFileStorageProvider

# Dictionary to hold lazy imported classes
_imports: dict[str, Any] = {}

logger = logging.getLogger(__name__)


def __getattr__(name: str) -> Any:
    """Lazily import attributes when accessed."""
    if name in _imports:
        return _imports[name]

    # Map class names to their respective modules
    module_map = {
        # Azure
        "AzureBlobStorageProvider": ".azure",
        "StaticAzureCredentialsProvider": ".azure",
        # GCS
        "GoogleStorageProvider": ".gcs",
        "GoogleIdentityPoolCredentialsProvider": ".gcs",
        "GoogleS3StorageProvider": ".gcs_s3",
        # Oracle
        "OracleStorageProvider": ".oci",
        # S3
        "S3StorageProvider": ".s3",
        "StaticS3CredentialsProvider": ".s3",
        # S8K
        "S8KStorageProvider": ".s8k",
        # AIS
        "AIStoreStorageProvider": ".ais",
        "StaticAISCredentialProvider": ".ais",
        # HuggingFace
        "HuggingFaceStorageProvider": ".huggingface",
        "HuggingFaceCredentialsProvider": ".huggingface",
    }

    if name in module_map:
        module_name = module_map[name]
        try:
            module = importlib.import_module(module_name, package=__package__)
            obj = getattr(module, name)
            _imports[name] = obj
            return obj
        except ModuleNotFoundError:
            # Map modules to their pip package requirements
            package_map = {
                ".azure": "azure-storage-blob",
                ".gcs": "google-cloud-storage",
                ".gcs-s3": "boto3",
                ".oci": "oci",
                ".s3": "boto3",
                ".s8k": "boto3",
                ".ais": "aistore",
                ".huggingface": "huggingface",
            }

            required_package = package_map.get(module_name, module_name.lstrip("."))
            provider_name = {
                "azure-storage-blob": "Azure Blob Storage",
                "google-cloud-storage": "Google Cloud Storage",
                "oci": "Oracle Cloud Infrastructure",
                "boto3": "Amazon S3 or other S3-compatible storage",
                "aistore": "NVIDIA AIStore",
                "huggingface": "HuggingFace Hub",
            }.get(required_package, required_package)

            # Write a helpful message to stderr
            logger.error(
                "\n".join(
                    [
                        "",
                        f"Accessing {provider_name} requires additional dependencies.",
                        "To use this storage provider, please install the optional dependency:",
                        "",
                        f"    pip install multi-storage-client[{required_package}]",
                        "",
                    ]
                )
            )
            # Re-raise the original exception
            raise

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    "ManifestMetadataProvider",
    "PosixFileStorageProvider",
]
