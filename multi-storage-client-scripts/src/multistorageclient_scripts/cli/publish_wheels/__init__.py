# SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
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

import argparse
import logging
import pathlib
import pprint
import tempfile
from dataclasses import dataclass
from typing import Final

import githubkit
import packaging.utils
import pyartifactory
import pyartifactory.exception
import pydantic

import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions

logger = logging.getLogger(__name__)


class Phase(argparse_extensions.ArgumentEnum):
    """
    Phase to exit after.
    """

    #: Check inputs.
    check = 0
    #: Publish.
    publish = 1


@dataclass(frozen=True, kw_only=True)
class Arguments(argparse_extensions.Arguments):
    """
    Command arguments.
    """

    #: GitHub token.
    github_token: Final[str]
    #: Artifactory username.
    artifactory_username: Final[str]
    #: Artifactory password.
    artifactory_password: Final[str]
    #: Kitmaker token.
    # kitmaker_token: Final[str]
    #: Git tag.
    git_tag: Final[str]
    #: Phase to exit after.
    phase: Final[Phase]


# TODO: Add `color` and `suggest_on_error` once we're on Python 3.14+.
PARSER = cli.SUBPARSERS.add_parser(
    name="publish-wheels",
    help="Publish the wheels for a release.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    allow_abbrev=False,
)


argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="github_token")(
    help="GitHub token. For local runs, log in with the GitHub CLI and pass `$(gh auth token)`."
)
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="artifactory_username")(
    help="Artifactory username."
)
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="artifactory_password")(
    help="Artifactory password."
)
# argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="kitmaker_token")(
#     help="Kitmaker token."
# )
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="git_tag")(
    help="Git tag."
)
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="phase")(
    help="Phase to exit after."
)


def func(arguments: Arguments) -> argparse_extensions.CommandFunction.ExitCode:
    # ----------------------------------------------------------------------------------------------------
    #
    # Collect inputs.
    #
    # ----------------------------------------------------------------------------------------------------

    github_client = githubkit.GitHub(auth=githubkit.TokenAuthStrategy(token=arguments.github_token))

    get_release_by_tag_response = github_client.rest.repos.get_release_by_tag(
        owner="NVIDIA",
        repo="multi-storage-client",
        tag=arguments.git_tag,
    )
    logger.info(
        "\n".join(
            [
                f"Found existing GitHub release for {arguments.git_tag}:",
                pprint.pformat(get_release_by_tag_response.parsed_data.model_dump()),
            ]
        )
    )

    release_wheels = sorted(
        [
            release_asset
            for release_asset in get_release_by_tag_response.parsed_data.assets
            if release_asset.name.endswith(".whl")
        ],
        key=lambda release_asset: release_asset.name,
    )

    # ----------------------------------------------------------------------------------------------------
    #
    # Check inputs.
    #
    # ----------------------------------------------------------------------------------------------------

    logger.info(
        "\n".join(
            [
                "Release wheels:",
                pprint.pformat([release_wheel.model_dump() for release_wheel in release_wheels]),
            ]
        )
    )

    for release_wheel in release_wheels:
        packaging.utils.parse_wheel_filename(release_wheel.name)

    if arguments.phase == Phase.check:
        return 0

    # ----------------------------------------------------------------------------------------------------
    #
    # Publish.
    #
    # Make this re-entrant.
    #
    # ----------------------------------------------------------------------------------------------------

    # Only publish for non-draft releases.
    if get_release_by_tag_response.parsed_data.draft:
        logger.info(f"Existing GitHub release for {arguments.git_tag} is a draft. Nothing to do.")
        return 0

    if len(release_wheels) == 0:
        logger.info("No release wheels to mirror. Nothing to do.")
        return 0

    artifactory_client = pyartifactory.Artifactory(
        url="https://urm.nvidia.com/artifactory",
        auth=(arguments.artifactory_username, pydantic.SecretStr(arguments.artifactory_password)),
        api_version=2,
        timeout=60,
    )

    for release_wheel in release_wheels:
        artifactory_artifact_path = pathlib.Path(
            "sw-ngc-data-platform-pypi", "multi-storage-client", arguments.git_tag, release_wheel.name
        )
        logger.info(
            f"Using Artifactory artifact path {str(artifactory_artifact_path)} for release wheel {release_wheel.name}."
        )

        try:
            get_artifact_response = artifactory_client.artifacts.info(artifact_path=artifactory_artifact_path)
            logger.info(
                "\n".join(
                    [
                        f"Found existing Artifactory artifact for {release_wheel.name}:",
                        pprint.pformat(get_artifact_response.model_dump()),
                    ]
                )
            )

            logger.info(
                f"Existing Artifactory artifact for release wheel {release_wheel.name}. Skipping release wheel."
            )
            continue
        except pyartifactory.exception.ArtifactNotFoundError:
            with tempfile.NamedTemporaryFile(mode="wb") as release_wheel_file:
                get_release_asset_response = github_client.rest.repos.get_release_asset(
                    owner="NVIDIA",
                    repo="multi-storage-client",
                    asset_id=release_wheel.id,
                    headers={"Accept": "application/octet-stream"},
                    stream=True,
                )
                for chunk in get_release_asset_response.iter_bytes(chunk_size=1_000_000):
                    release_wheel_file.write(chunk)
                release_wheel_file.flush()
                logger.info(f"Downloaded release wheel {release_wheel.name} to {release_wheel_file.name}.")

                _, version, _, tags = packaging.utils.parse_wheel_filename(release_wheel.name)
                interpreters = {tag.interpreter for tag in tags}
                deploy_artifact_response = artifactory_client.artifacts.deploy(
                    local_file_location=release_wheel_file.name,
                    artifact_path=artifactory_artifact_path,
                    # Hardcode "arch=any" + "os=any" temporarily to unblock publishing.
                    #
                    # This is just for Kitmaker K1 bookkeeping since it doesn't extract wheel metadata.
                    properties={
                        "component_name": ["multi_storage_client"],
                        "version": [str(version)],
                        "python-tag": [next(iter(interpreters))],
                        "arch": ["any"],
                        "os": ["any"],
                        "changelist": [get_release_by_tag_response.parsed_data.html_url],
                        "branch": ["release"],
                        "release_approver": ["svc-nv-msc"],
                        "release_status": ["ready"],
                    },
                )
                logger.info(
                    "\n".join(
                        [
                            f"Created Artifactory artifact for {release_wheel.name}:",
                            pprint.pformat(deploy_artifact_response.model_dump()),
                        ]
                    )
                )

    if arguments.phase == Phase.publish:
        return 0

    return 0


argparse_extensions.set_command_function(parser=PARSER, arguments_type=Arguments, func=func)
