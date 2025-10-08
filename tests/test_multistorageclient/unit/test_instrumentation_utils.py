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
from unittest.mock import MagicMock, patch

import pytest

from multistorageclient.instrumentation import utils as instrument_utils


@pytest.fixture
def patch_observability_not_available():
    """Patch to simulate environment without optional observability dependencies"""
    import multistorageclient.instrumentation.utils as instrument_utils

    with (
        patch.dict(
            "sys.modules",
            {"opentelemetry.sdk.metrics": None, "datasketches": None},
        ),
        patch("multistorageclient.instrumentation.HAS_OBSERVABILITY_DEPS", False),
    ):
        # Force reload the module to pick up the patched imports
        importlib.reload(instrument_utils)
        yield
        # Reload again to restore original state
        importlib.reload(instrument_utils)


@pytest.fixture
def reset_observability_state():
    """Reset the module to its original state"""
    import multistorageclient.instrumentation.utils as instrument_utils

    importlib.reload(instrument_utils)
    yield


def test_generic_tracer_when_observability_not_available(patch_observability_not_available):
    """Test that _generic_tracer still works when optional observability deps are not available"""

    def test_func():
        return "test"

    decorated = instrument_utils._generic_tracer(test_func, "TestClass")
    # Should still work with basic tracing
    assert decorated() == "test"


def test_file_tracer_when_observability_not_available(patch_observability_not_available):
    """Test that file_tracer still works when optional observability deps are not available"""

    # Create a mock managed file instance
    mock_file = MagicMock()
    mock_file._trace_span = None

    def test_func(self):
        return "test"

    decorated = instrument_utils.file_tracer(test_func)
    # Should still work with basic tracing
    assert decorated(mock_file) == "test"


def test_instrumented_when_observability_not_available(patch_observability_not_available):
    """Test that instrumented decorator still works when optional observability deps are not available"""

    @instrument_utils.instrumented
    class TestClass:
        def test_method(self):
            return "test"

    obj = TestClass()
    # Method should still work with basic tracing
    assert obj.test_method() == "test"


def test_generic_tracer_when_observability_available(reset_observability_state):
    """Test that _generic_tracer works with advanced tracing when optional observability deps are available"""

    def test_func():
        return "test"

    decorated = instrument_utils._generic_tracer(test_func, "TestClass")
    # Should be wrapped
    assert hasattr(decorated, "__wrapped__")
    assert decorated.__wrapped__ == test_func  # pyright: ignore
    assert decorated() == "test"


def test_file_tracer_when_observability_available(reset_observability_state):
    """Test that file_tracer works with advanced tracing when optional observability deps are available"""

    # Create a mock managed file instance
    mock_file = MagicMock()
    mock_file._trace_span = None

    def test_func(self):
        return "test"

    decorated = instrument_utils.file_tracer(test_func)
    # Should be wrapped
    assert hasattr(decorated, "__wrapped__")
    assert decorated.__wrapped__ == test_func  # pyright: ignore
    assert decorated(mock_file) == "test"


def test_instrumented_when_observability_available(reset_observability_state):
    """Test that instrumented decorator works with advanced tracing when optional observability deps are available"""

    @instrument_utils.instrumented
    class TestClass:
        def test_method(self):
            return "test"

    obj = TestClass()
    # Method should be wrapped but still work
    assert obj.test_method() == "test"
