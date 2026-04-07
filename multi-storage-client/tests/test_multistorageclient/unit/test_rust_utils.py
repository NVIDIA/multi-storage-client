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
import sys
import types

import pytest


@pytest.mark.asyncio
async def test_run_async_rust_client_method_inside_running_loop(monkeypatch: pytest.MonkeyPatch):
    stub_rust_module = types.SimpleNamespace(RustRetryConfig=type("RustRetryConfig", (), {}))
    monkeypatch.setitem(sys.modules, "multistorageclient_rust", stub_rust_module)
    monkeypatch.delitem(sys.modules, "multistorageclient.rust_utils", raising=False)

    run_async_rust_client_method = importlib.import_module("multistorageclient.rust_utils").run_async_rust_client_method

    class DummyRustClient:
        async def download(self, key: str, destination: str) -> tuple[str, str]:
            return key, destination

    result = run_async_rust_client_method(DummyRustClient(), "download", "file.txt", "/tmp/file.txt")

    assert result == ("file.txt", "/tmp/file.txt")
