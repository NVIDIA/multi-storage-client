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

import sys

if sys.version_info >= (3, 10):
    """MCP prompt definitions for Multi-Storage Client help and guidance."""

    from .server import mcp

    @mcp.prompt("msc_help")
    def msc_help() -> str:
        """
        Provides general information and help on how to configure and use Multi-Storage Client.

        :return: Help text for MSC configuration and usage
        """
        return """
# Multi-Storage Client (MSC) Help

Multi-Storage Client is a unified Python client for multiple object storage backends including:
- Amazon S3 and S3-compatible storage
- Google Cloud Storage  
- Microsoft Azure Blob Storage
- Oracle Cloud Infrastructure Object Storage
- AIStore distributed storage
- Local filesystem
- Hugging Face Hub
- And more...

## Available MCP Tools

### msc_list
Lists files and directories from storage locations.
- **url**: Storage URL (e.g., 'msc://profile/path/', 's3://bucket/prefix/')
- **include_directories**: Include directories in results (default: false)
- **limit**: Maximum number of objects to return
- **attribute_filter_expression**: Filter results by attributes

Example: "List all files in my S3 bucket under the data/ prefix"

### msc_info  
Gets detailed metadata about a specific file or directory.
- **url**: Full path to the object (e.g., 'msc://profile/path/file.txt')

Example: "Get information about the model.pkl file in my storage"

## Configuration

MSC uses configuration files (YAML or JSON) typically located at:
- ~/.msc_config.yaml
- ~/.msc_config.json
- Or specified via MSC_CONFIG environment variable

Configuration includes:
- **profiles**: Storage backend configurations
- **credentials**: Authentication settings (auto-discovered from environment)
- **path_mapping**: URL routing rules
- **caching**: Performance optimization settings

## URL Formats

- **msc://profile/path**: Uses named profile from configuration
- **s3://bucket/key**: Direct S3 access (uses implicit profile)
- **gs://bucket/key**: Direct GCS access
- **file:///local/path**: Local filesystem access

## Resources

Use the msc://config resource to view your current configuration.
Use the msc://providers resource to see all supported storage types.
Use the msc://version resource to check the MSC version.

For more information, visit: https://nvidia.github.io/multi-storage-client/
"""
