# SPDX-FileCopyrightText: Copyright (c) 2024 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
# SPDX-License-Identifier: Apache-2.0

from multistorageclient import StorageClientConfig


def test_implicit_profile_reuses_matching_explicit_profile_options(monkeypatch) -> None:
    msc_config = {
        "profiles": {
            "s3-my-bucket": {
                "storage_provider": {
                    "type": "s3",
                    "options": {
                        "base_path": "my-bucket",
                        "endpoint_url": "https://my-endpoint.s3.com",
                        "region_name": "us-west-2",
                    },
                }
            }
        }
    }

    monkeypatch.setattr(
        StorageClientConfig,
        "read_msc_config",
        staticmethod(lambda config_file_paths=None: (msc_config, "/tmp/msc.yaml")),
    )
    monkeypatch.setattr("multistorageclient.config.read_rclone_config", lambda: ({}, None))

    captured = {}

    def fake_from_dict(config_dict, profile="__filesystem__", skip_validation=False, telemetry_provider=None):
        captured["config_dict"] = config_dict
        captured["profile"] = profile
        captured["skip_validation"] = skip_validation
        return profile

    monkeypatch.setattr(StorageClientConfig, "from_dict", staticmethod(fake_from_dict))

    result = StorageClientConfig.from_file(profile="_s3-my-bucket")

    assert result == "_s3-my-bucket"
    assert captured["profile"] == "_s3-my-bucket"
    assert captured["skip_validation"] is True
    implicit_profile = captured["config_dict"]["profiles"]["_s3-my-bucket"]
    assert implicit_profile["storage_provider"]["options"]["endpoint_url"] == "https://my-endpoint.s3.com"
    assert implicit_profile["storage_provider"]["options"]["region_name"] == "us-west-2"
