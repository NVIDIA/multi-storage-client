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

# Default limit (512MB) for loading files into memory
MEMORY_LOAD_LIMIT = 512 * 1024 * 1024

# Default timeout values (in seconds) for storage provider connections
DEFAULT_CONNECT_TIMEOUT = 60
DEFAULT_READ_TIMEOUT = 60
DEFAULT_MAX_POOL_CONNECTIONS = 128

# Default host and port for the MSC Explorer application
DEFAULT_EXPLORER_HOST = "127.0.0.1"
DEFAULT_EXPLORER_PORT = 8888

# Default batch size for sync operations
DEFAULT_SYNC_BATCH_SIZE = 32
