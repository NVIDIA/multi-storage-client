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

"""
Hydra MSC Plugin Auto-Discovery Wrapper.

This module enables automatic discovery of MSC Hydra plugins by Hydra's
plugin discovery system. It imports the actual plugin implementations from
multistorageclient.contrib.hydra and exposes them in the hydra_plugins namespace
for automatic registration.

Only registers plugins if hydra dependencies are available and can be imported successfully.
"""

try:
    import hydra.core.plugins  # noqa: F401 - Used to check if Hydra is available

    from multistorageclient.contrib.hydra import MSCConfigSource, MSCSearchPathPlugin

    __all__ = ["MSCConfigSource", "MSCSearchPathPlugin"]
except ImportError:
    __all__ = []
