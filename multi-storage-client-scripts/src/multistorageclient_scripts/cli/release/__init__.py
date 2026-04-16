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
import importlib.metadata
import logging
import pathlib
import pprint
from dataclasses import dataclass
from typing import Final

import dulwich.repo
import githubkit
import githubkit.exception
import githubkit.response
import githubkit.versions.latest.models
import packaging.utils

import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions

logger = logging.getLogger(__name__)


class Phase(argparse_extensions.ArgumentEnum):
    """
    Release phase to exit after.
    """

    #: Check release inputs.
    check = 0
    #: Publish release.
    publish = 1


@dataclass(frozen=True, kw_only=True)
class Arguments(argparse_extensions.Arguments):
    """
    Command arguments.
    """

    #: GitHub token.
    github_token: Final[str]
    #: Release phase to exit after.
    phase: Final[Phase]


# TODO: Add `color` and `suggest_on_error` once we're on Python 3.14+.
PARSER = cli.PARSER.add_subparsers().add_parser(
    name="release",
    help="Release helper.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    allow_abbrev=False,
)


argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="github_token")(
    help="GitHub token. For local runs, log in with the GitHub CLI and pass `$(gh auth token)`."
)
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="phase")(
    help="Release phase to exit after."
)


def func(arguments: Arguments) -> argparse_extensions.CommandFunction.ExitCode:
    # ----------------------------------------------------------------------------------------------------
    #
    # Collect release inputs.
    #
    # ----------------------------------------------------------------------------------------------------

    git_repository = dulwich.repo.Repo(root="..")
    git_commit_revision = git_repository.head().decode()

    multi_storage_client_version = importlib.metadata.version("multi-storage-client")

    release_notes = pathlib.Path(f"../.release_notes/{multi_storage_client_version}.md").resolve()

    multi_storage_client_artifacts = pathlib.Path("../multi-storage-client/dist").resolve()
    multi_storage_client_wheels = [*multi_storage_client_artifacts.glob(f"*{multi_storage_client_version}*.whl")]

    multi_storage_file_system_artifacts = pathlib.Path("../multi-storage-file-system/build").resolve()
    multi_storage_file_system_debs = [
        *multi_storage_file_system_artifacts.glob(f"debian/archives/*{multi_storage_client_version}*.deb")
    ]
    multi_storage_file_system_rpms = [
        *multi_storage_file_system_artifacts.glob(f"rpm/RPMS/*/*{multi_storage_client_version}*.rpm")
    ]
    multi_storage_file_system_tars = [
        *multi_storage_file_system_artifacts.glob(f"tar/*{multi_storage_client_version}*.tar.*")
    ]
    multi_storage_file_system_zips = [
        *multi_storage_file_system_artifacts.glob(f"zip/*{multi_storage_client_version}*.zip")
    ]

    release_assets = [
        # TODO: Attach wheels to the GitHub release once wheel mirroring is migrated to Kitmaker K2.
        # *multi_storage_client_wheels,
        *multi_storage_file_system_debs,
        *multi_storage_file_system_rpms,
        *multi_storage_file_system_tars,
        *multi_storage_file_system_zips,
    ]

    # ----------------------------------------------------------------------------------------------------
    #
    # Check release inputs.
    #
    # ----------------------------------------------------------------------------------------------------

    logger.info(f"Git commit revision: {git_commit_revision}")

    logger.info(f"multi-storage-client version: {multi_storage_client_version}")

    if not release_notes.exists():
        raise FileNotFoundError(f"Missing release notes at {str(release_notes)}!")
    if len(release_notes.read_text().strip()) == 0:
        raise ValueError(f"Empty release notes at {str(release_notes)}!")

    logger.info(
        "\n".join(
            [
                "Release notes:",
                pprint.pformat(
                    {
                        "body": release_notes.read_text(),
                        "path": release_notes,
                    }
                ),
            ]
        )
    )

    if len(multi_storage_client_wheels) == 0:
        raise FileNotFoundError(f"Missing multi-storage-client wheels in {str(multi_storage_client_artifacts)}!")
    if len(multi_storage_file_system_debs) == 0:
        raise FileNotFoundError(
            f"Missing multi-storage-file-system debs in {str(multi_storage_file_system_artifacts)}!"
        )
    if len(multi_storage_file_system_rpms) == 0:
        raise FileNotFoundError(
            f"Missing multi-storage-file-system rpms in {str(multi_storage_file_system_artifacts)}!"
        )
    if len(multi_storage_file_system_tars) == 0:
        raise FileNotFoundError(
            f"Missing multi-storage-file-system tars in {str(multi_storage_file_system_artifacts)}!"
        )
    if len(multi_storage_file_system_zips) == 0:
        raise FileNotFoundError(
            f"Missing multi-storage-file-system zips in {str(multi_storage_file_system_artifacts)}!"
        )

    for multi_storage_client_wheel in multi_storage_client_wheels:
        name, version, _, _ = packaging.utils.parse_wheel_filename(str(multi_storage_client_wheel.name))
        if name != "multi-storage-client":
            raise ValueError(f"Mismatched multi-storage-client wheel name at {str(multi_storage_client_wheel)}.")
        if str(version) != multi_storage_client_version:
            raise ValueError(f"Mismatched multi-storage-client wheel version at {str(multi_storage_client_wheel)}.")

    logger.info(
        "\n".join(
            [
                "Release assets:",
                pprint.pformat(
                    [
                        {
                            "name": release_asset.name,
                            "path": release_asset,
                        }
                        for release_asset in release_assets
                    ]
                ),
            ]
        )
    )

    if len(release_assets) != len({release_asset.name for release_asset in release_assets}):
        raise ValueError("Multiple release assets have the same name!")

    if arguments.phase == Phase.check:
        return 0

    # ----------------------------------------------------------------------------------------------------
    #
    # Publish release.
    #
    # Make this as close to transactional as possible.
    #
    # ----------------------------------------------------------------------------------------------------

    github_client = githubkit.GitHub(auth=githubkit.TokenAuthStrategy(token=arguments.github_token))

    try:
        # Delete existing draft release + tag (cancel concurrent transaction).
        get_release_by_tag_response: githubkit.response.Response[githubkit.versions.latest.models.Release] = (
            github_client.rest.repos.get_release_by_tag(
                owner="NVIDIA",
                repo="multi-storage-client",
                tag=multi_storage_client_version,
            )
        )

        logger.info(
            "\n".join(
                [
                    f"Found existing GitHub release for {multi_storage_client_version}:",
                    pprint.pformat(get_release_by_tag_response.parsed_data.model_dump()),
                ]
            )
        )

        if not get_release_by_tag_response.parsed_data.draft:
            logger.info(f"Existing GitHub release for {multi_storage_client_version} is not a draft. Nothing to do.")
            return 0

        github_client.rest.repos.delete_release(
            owner="NVIDIA",
            repo="multi-storage-client",
            release_id=get_release_by_tag_response.parsed_data.id,
        )
        logger.info(f"Deleted existing draft GitHub release for {multi_storage_client_version}.")

        github_client.rest.git.delete_ref(
            owner="NVIDIA",
            repo="multi-storage-client",
            ref=f"refs/tags/{get_release_by_tag_response.parsed_data.tag_name}",
        )
        logger.info(f"Deleted existing Git tag for {multi_storage_client_version}.")
    except githubkit.exception.RequestFailed as e:
        if e.response.status_code != 404:
            raise

    # Create draft release (create transaction).
    create_release_response = github_client.rest.repos.create_release(
        owner="NVIDIA",
        repo="multi-storage-client",
        tag_name=multi_storage_client_version,
        # GitHub ignores this if the tag already exists instead of returning an error.
        # There isn't an atomic compare-and-swap either.
        target_commitish=git_commit_revision,
        name=multi_storage_client_version,
        body=release_notes.read_text(),
        draft=True,
    )
    logger.info(
        "\n".join(
            [
                f"Created draft GitHub release for {multi_storage_client_version}:",
                pprint.pformat(create_release_response.parsed_data.model_dump()),
            ]
        )
    )

    # Upload draft release assets.
    for release_asset in release_assets:
        with release_asset.open(mode="rb") as release_asset_stream:
            upload_release_asset_response = github_client.rest.repos.upload_release_asset(
                owner="NVIDIA",
                repo="multi-storage-client",
                release_id=create_release_response.parsed_data.id,
                name=release_asset.name,
                data=release_asset_stream,
            )
            logger.info(
                "\n".join(
                    [
                        "Uploaded draft release asset:",
                        pprint.pformat(upload_release_asset_response.parsed_data.model_dump()),
                    ]
                )
            )

    # Undraft release (commit transaction).
    github_client.rest.repos.update_release(
        owner="NVIDIA",
        repo="multi-storage-client",
        release_id=create_release_response.parsed_data.id,
        draft=False,
    )

    if arguments.phase == Phase.publish:
        return 0

    return 0


argparse_extensions.set_command_function(parser=PARSER, arguments_type=Arguments, func=func)
