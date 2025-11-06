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

import asyncio
from typing import Any


def run_async_rust_client_method(rust_client: Any, method_name: str, *args, **kwargs) -> Any:
    """
    Run a method of the rust client asynchronously.

    :param rust_client: The rust client instance.
    :param method_name: The name of the method to call.
    :param args: Positional arguments for the method.
    :param kwargs: Keyword arguments for the method.

    :return: The result of the method call.
    """

    if not hasattr(rust_client, method_name):
        raise AttributeError(f"MSC Rust client has no method '{method_name}'")

    async def _run_method():
        method = getattr(rust_client, method_name)
        return await method(*args, **kwargs)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(_run_method())
    else:
        return loop.run_until_complete(_run_method())
