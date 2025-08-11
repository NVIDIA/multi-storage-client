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

from pathlib import Path

import pytest
import yaml
from hydra._internal.config_search_path_impl import ConfigSearchPathImpl
from hydra.test_utils.config_source_common_tests import ConfigSourceTestSuite

from multistorageclient.contrib.hydra import MSCConfigSource, MSCSearchPathPlugin
from test_multistorageclient.unit.utils import tempdatastore


class TestMSCSearchPathPlugin:
    """Test cases for MSCSearchPathPlugin."""

    def test_fix_mangled_msc_urls_from_cli(self):
        """
        Test that the plugin correctly fixes mangled MSC URLs from CLI arguments.

        When users specify msc:// URLs via CLI, Hydra's path normalization can mangle them.
        For example: --config-path="msc://dev/configs" becomes "/current/dir/msc:/dev/configs"
        """
        plugin = MSCSearchPathPlugin()
        search_path = ConfigSearchPathImpl()

        # Test key mangled URL patterns
        search_path.append("mangled1", "/home/user/msc:/dev/configs")  # Typical CLI mangling
        search_path.append("mangled2", "msc:/missing-slash")  # Missing second slash
        search_path.append("normal", "file://normal/path")  # Should remain unchanged

        plugin.manipulate_search_path(search_path)

        # Verify fixes
        paths = [elem.path for elem in search_path.get_path()]
        assert "msc://dev/configs" in paths, "Should fix CLI mangling"
        assert "msc://missing-slash" in paths, "Should fix missing slash"
        assert "file://normal/path" in paths, "Should preserve normal paths"

    def test_add_universal_msc_source_when_missing(self):
        """
        Test that the plugin adds universal MSC source when no MSC sources exist.

        This covers both empty search paths and non-empty paths without MSC sources.
        """
        plugin = MSCSearchPathPlugin()
        search_path = ConfigSearchPathImpl()

        # Test with non-empty search path containing no MSC sources
        search_path.append("local", "file://./configs")

        plugin.manipulate_search_path(search_path)

        # Verify universal MSC source was added
        elements = search_path.get_path()
        msc_elements = [elem for elem in elements if elem.path and elem.path.startswith("msc://")]

        assert len(msc_elements) == 1, "Should have added exactly one MSC source"
        assert msc_elements[0].provider == "msc-universal"
        assert msc_elements[0].path == "msc://"

    def test_no_duplicate_msc_source_when_already_exists(self):
        """
        Test that the plugin doesn't add duplicate MSC sources when one already exists.
        """
        plugin = MSCSearchPathPlugin()
        search_path = ConfigSearchPathImpl()

        # Add search path with existing MSC source
        search_path.append("existing_msc", "msc://dev/configs")

        plugin.manipulate_search_path(search_path)

        # Verify no additional MSC source was added - should still have exactly one
        msc_elements = [elem for elem in search_path.get_path() if elem.path and elem.path.startswith("msc://")]
        assert len(msc_elements) == 1, "Should not add duplicate MSC source"
        assert msc_elements[0].path == "msc://dev/configs", "Should preserve original MSC source"


@pytest.fixture
def compliance_setup(monkeypatch):
    """
    Create MSC setup with config structure following Hydra's official pattern.

    Uses TemporaryPOSIXDirectory for cleaner infrastructure management
    and leverages MSC's established test patterns.
    """
    with tempdatastore.TemporaryPOSIXDirectory() as temp_store:
        # Get the base directory from TemporaryDataStore
        base_config = temp_store.profile_config_dict()
        base_path = Path(base_config["storage_provider"]["options"]["base_path"])
        config_dir = base_path / "configs"
        config_dir.mkdir()

        # Complete config structure based on Hydra's official example
        # This matches exactly what ConfigSourceTestSuite expects to test
        configs_with_headers = {
            # Basic configs (no package headers)
            "primary_config.yaml": {"content": {"primary": True}, "header": None},
            "config_without_group.yaml": {"content": {"group": False}, "header": None},
            "config_with_unicode.yaml": {"content": {"group": "数据库"}, "header": None},
            "config_with_defaults_list.yaml": {
                "content": {"defaults": [{"dataset": "imagenet"}], "key": "value"},
                "header": None,
            },
            # Primary config with package header
            "primary_config_with_non_global_package.yaml": {"content": {"primary": True}, "header": "foo"},
            # Dataset configs (for overlap testing - dataset.yaml exists alongside dataset/ group)
            "dataset.yaml": {"content": {"dataset_yaml": True}, "header": None},
            "dataset/imagenet.yaml": {"content": {"name": "imagenet", "path": "/datasets/imagenet"}, "header": None},
            "dataset/cifar10.yaml": {"content": {"name": "cifar10", "path": "/datasets/cifar10"}, "header": None},
            # Optimizer configs
            "optimizer/adam.yaml": {"content": {"lr": 0.001, "optimizer": "adam"}, "header": None},
            "optimizer/nesterov.yaml": {"content": {"lr": 0.01, "momentum": 0.9}, "header": None},
            # Nested level configs
            "level1/level2/nested1.yaml": {"content": {"l1_l2_n1": True}, "header": None},
            "level1/level2/nested2.yaml": {"content": {"l1_l2_n2": True}, "header": None},
            # Configs with defaults list and package headers
            "configs_with_defaults_list/global_package.yaml": {
                "content": {"defaults": [{"foo": "bar"}], "x": 10},
                "header": "_global_",
            },
            "configs_with_defaults_list/group_package.yaml": {
                "content": {"defaults": [{"foo": "bar"}], "x": 10},
                "header": "_group_",
            },
            # Package test configs (test all package directive types)
            "package_test/none.yaml": {"content": {"foo": "bar"}, "header": None},
            "package_test/explicit.yaml": {"content": {"foo": "bar"}, "header": "a.b"},
            "package_test/global.yaml": {"content": {"foo": "bar"}, "header": "_global_"},
            "package_test/group.yaml": {"content": {"foo": "bar"}, "header": "_group_"},
            "package_test/group_name.yaml": {"content": {"foo": "bar"}, "header": "foo._group_._name_"},
            "package_test/name.yaml": {"content": {"foo": "bar"}, "header": "_name_"},
        }

        # Create all config files
        for config_path, config_data in configs_with_headers.items():
            file_path = config_dir / config_path
            file_path.parent.mkdir(parents=True, exist_ok=True)

            # Build file content with package header if needed
            content = ""
            if config_data["header"]:
                content = f"# @package {config_data['header']}\n"
            content += yaml.dump(config_data["content"])

            file_path.write_text(content)

        # Use TemporaryDataStore's profile config and modify base_path to point to configs subdirectory
        profile_config = temp_store.profile_config_dict()
        profile_config["storage_provider"]["options"]["base_path"] = str(config_dir)

        msc_config = {"profiles": {"compliance_profile": profile_config}}

        # Set up MSC config file
        msc_config_dir = base_path / ".msc"
        msc_config_dir.mkdir()
        msc_config_file = msc_config_dir / "config.yaml"
        with open(msc_config_file, "w") as f:
            yaml.dump(msc_config, f)

        monkeypatch.setenv("MSC_CONFIG", str(msc_config_file))
        yield {"profile_name": "compliance_profile"}


@pytest.mark.parametrize("type_, path", [(MSCConfigSource, "msc://compliance_profile")])
class TestMSCConfigSourceCompliance(ConfigSourceTestSuite):
    """
    Test MSCConfigSource compliance with Hydra's ConfigSource interface.

    This class inherits from Hydra's ConfigSourceTestSuite which runs standardized
    tests to ensure our MSCConfigSource properly implements the ConfigSource interface.
    """

    def skip_overlap_config_path_name(self) -> bool:
        """
        MSC storage doesn't support having both a config file and config group
        with the same name accessible simultaneously (e.g., dataset.yaml and dataset/).
        The directory takes precedence in MSC's implementation.
        """
        return True

    @pytest.fixture(autouse=True)
    def setup_compliance(self, compliance_setup):
        """Auto-use the compliance setup for all tests in this class."""
        pass
