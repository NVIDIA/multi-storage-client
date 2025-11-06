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

from .s3 import S3StorageProvider

PROVIDER = "gcs_s3"


class GoogleS3StorageProvider(S3StorageProvider):
    """
    A concrete implementation of the :py:class:`multistorageclient.types.StorageProvider` for interacting with GCS via its S3 interface.
    """

    def __init__(self, *args, **kwargs):
        kwargs["request_checksum_calculation"] = "when_required"
        kwargs["response_checksum_validation"] = "when_required"

        super().__init__(*args, **kwargs)

        # override the provider name from "s3"
        self._provider_name = PROVIDER
