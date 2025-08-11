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
from pathlib import Path

import hydra
import pytest
import yaml
from omegaconf import DictConfig

from test_multistorageclient.unit.utils import tempdatastore


def test_hydra_simple_msc_loading(monkeypatch):
    with tempdatastore.TemporaryPOSIXDirectory() as temp_store:
        # Setup MSC config file and environment
        msc_config_file = Path(temp_store._directory.name) / "msc_config.yaml"
        with open(msc_config_file, "w") as f:
            yaml.dump({"profiles": {"test": temp_store.profile_config_dict()}}, f)
        monkeypatch.setenv("MSC_CONFIG", str(msc_config_file))

        # Create test config file
        config_dir = Path(temp_store._directory.name) / "configs"
        config_dir.mkdir(parents=True, exist_ok=True)
        with open(config_dir / "app.yaml", "w") as f:
            yaml.dump({"app_name": "test_app", "version": "1.0.0"}, f)

        # Disable Hydra's output directory and logging for tests (minimal args)
        original_argv = sys.argv
        sys.argv = [
            "test_app",
            "hydra.output_subdir=null",  # Prevents .hydra subdirectory
            "hydra.run.dir=.",  # Use current dir (no outputs/)
            "hydra/job_logging=none",
        ]  # Disable log file creation

        try:
            test_passed = False

            @hydra.main(version_base="1.3", config_path="msc://test/configs", config_name="app")
            def app(cfg: DictConfig) -> None:
                nonlocal test_passed
                # Verify config was loaded correctly from MSC
                assert cfg.app_name == "test_app"
                assert cfg.version == "1.0.0"
                test_passed = True

            app()
            assert test_passed, "Hydra app did not run or assertions failed"
        finally:
            sys.argv = original_argv


def test_hydra_error_handling_invalid_profile():
    """Test that hydra exits on invalid MSC profiles."""
    original_argv = sys.argv
    sys.argv = [
        "test_app",
        "hydra.output_subdir=null",  # Prevents .hydra subdirectory
        "hydra.run.dir=.",  # Use current dir (no outputs/)
        "hydra/job_logging=none",
    ]  # Disable log file creation

    try:

        @hydra.main(version_base="1.3", config_path="msc://nonexistent/configs", config_name="config")
        def failing_app(cfg: DictConfig) -> None:
            pass  # Should never reach here

        with pytest.raises(SystemExit):
            failing_app()
    finally:
        sys.argv = original_argv
