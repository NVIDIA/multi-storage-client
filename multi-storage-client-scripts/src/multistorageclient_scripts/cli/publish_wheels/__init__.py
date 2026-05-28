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
import pprint
from dataclasses import dataclass
from typing import Final

import githubkit
import httpx
import httpx_retries
import packaging.utils

import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions
import multistorageclient_scripts.utils.httpx.auth
from multistorageclient_scripts.utils.wait import wait

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
    #: Kitmaker Portal token.
    kitmaker_portal_token: Final[str]
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
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="kitmaker_portal_token")(
    help="Kitmaker Portal token."
)
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

    # https://kitmaker.gitlab-master-pages.nvidia.com/kitmaker-docs/users/portal-api/wheel-release-api
    # https://kitmaker-portal.nvidia.com/api/v0/docs
    kitmaker_client = httpx.Client(
        auth=multistorageclient_scripts.utils.httpx.auth.BearerAuth(token=arguments.kitmaker_portal_token),
        base_url="https://kitmaker-portal.nvidia.com/api/",
        follow_redirects=True,
        transport=httpx_retries.RetryTransport(retry=httpx_retries.Retry(backoff_factor=2)),
        # Kitmaker's TLS certificate uses an NVIDIA CA instead of ones in common CA bundles on most systems.
        verify=False,
    )

    def kitmaker_create_release(upload: bool) -> httpx.Response:
        """
        Create a Kitmaker release.

        :param upload: Whether to upload wheels or do a dry run.
        """
        return kitmaker_client.post(
            url="v0/projects/397/releases",
            json={
                "project_name": "multi-storage-client",
                "payload": [
                    {
                        "pic": "svc-nv-msc@nvidia.com",
                        "job_type": "wheel-release-job",
                        "url": release_wheel.browser_download_url,
                        "upload": upload,
                    }
                    for release_wheel in release_wheels
                ],
            },
        ).raise_for_status()

    def kitmaker_get_release_status(release_uuid: str) -> httpx.Response:
        """
        Get a Kitmaker release's status.

        :param release_uuid: Release UUID.
        """
        return kitmaker_client.get(url=f"v0/status/{release_uuid}").raise_for_status()

    def poll_kitmaker_release_status(release_uuid: str) -> httpx.Response:
        kitmaker_get_release_status_response = kitmaker_get_release_status(release_uuid=release_uuid)
        kitmaker_get_release_status_response_json = kitmaker_get_release_status_response.json()
        logger.info(
            f"Kitmaker release {release_uuid} is in {kitmaker_get_release_status_response_json['status']} status."
        )
        return kitmaker_get_release_status_response

    kitmaker_release_success_statuses = {"completed"}
    kitmaker_release_failure_statuses = {"failed"}
    kitmaker_release_end_statuses = kitmaker_release_success_statuses | kitmaker_release_failure_statuses

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

    if get_release_by_tag_response.parsed_data.draft or len(release_wheels) == 0:
        # Draft GitHub releases are private. We haven't worked with Kitmaker to set up access yet.
        logger.info(
            f"Skipping Kitmaker dry-run release since the existing GitHub release for {arguments.git_tag} is a draft or has no wheels."
        )
    else:
        # Create a Kitmaker dry-run release to reduce the probability of partial batch publishing failures.
        try:
            kitmaker_create_release_response = kitmaker_create_release(upload=False)
        except httpx.HTTPStatusError as e:
            logger.error(
                "\n".join(
                    [
                        "Failed to create a Kitmaker dry-run release:",
                        pprint.pformat(e.response.json()),
                    ]
                )
            )
            raise
        kitmaker_create_release_response_json = kitmaker_create_release_response.json()
        logger.info(
            "\n".join(
                [
                    "Created a Kitmaker dry-run release:",
                    pprint.pformat(kitmaker_create_release_response_json),
                ]
            )
        )

        try:
            logger.info(
                f"Waiting for Kitmaker dry-run release {kitmaker_create_release_response_json['release_uuid']}..."
            )
            kitmaker_get_release_status_response = wait(
                waitable=lambda: poll_kitmaker_release_status(
                    release_uuid=kitmaker_create_release_response_json["release_uuid"]
                ),
                should_wait=lambda kitmaker_get_release_status_response: (
                    kitmaker_get_release_status_response.json()["status"] not in kitmaker_release_end_statuses
                ),
                # 5 seconds * 120 attempts = 600 seconds (10 minutes).
                attempt_interval_seconds=5,
                max_attempts=120,
            )
            kitmaker_get_release_status_response_json = kitmaker_get_release_status_response.json()
            logger.info(
                "\n".join(
                    [
                        f"Kitmaker dry-run release {kitmaker_create_release_response_json['release_uuid']} ended in {kitmaker_get_release_status_response_json['status']} status:",
                        pprint.pformat(kitmaker_get_release_status_response_json),
                    ]
                )
            )

            if kitmaker_get_release_status_response_json["status"] not in kitmaker_release_success_statuses:
                raise RuntimeError(
                    f"Kitmaker dry-run release {kitmaker_create_release_response_json['release_uuid']} failed with status {kitmaker_get_release_status_response_json['status']}!"
                )
        except AssertionError:
            logger.error(
                f"Timed out waiting for Kitmaker dry-run release {kitmaker_create_release_response_json['release_uuid']}!"
            )
            raise

    if arguments.phase == Phase.check:
        return 0

    # ----------------------------------------------------------------------------------------------------
    #
    # Publish.
    #
    # ----------------------------------------------------------------------------------------------------

    # Only publish for non-draft releases.
    if get_release_by_tag_response.parsed_data.draft:
        logger.info(f"Existing GitHub release for {arguments.git_tag} is a draft. Nothing to do.")
        return 0

    if len(release_wheels) == 0:
        logger.info("No release wheels to mirror. Nothing to do.")
        return 0

    try:
        kitmaker_create_release_response = kitmaker_create_release(upload=True)
    except httpx.HTTPStatusError as e:
        logger.error(
            "\n".join(
                [
                    "Failed to create a Kitmaker release:",
                    pprint.pformat(e.response.json()),
                ]
            )
        )
        raise
    kitmaker_create_release_response_json = kitmaker_create_release_response.json()
    logger.info(
        "\n".join(
            [
                "Created a Kitmaker release:",
                pprint.pformat(kitmaker_create_release_response_json),
            ]
        )
    )

    try:
        logger.info(f"Waiting for Kitmaker release {kitmaker_create_release_response_json['release_uuid']}...")
        kitmaker_get_release_status_response = wait(
            waitable=lambda: poll_kitmaker_release_status(
                release_uuid=kitmaker_create_release_response_json["release_uuid"]
            ),
            should_wait=lambda kitmaker_get_release_status_response: (
                kitmaker_get_release_status_response.json()["status"] not in kitmaker_release_end_statuses
            ),
            # 5 seconds * 120 attempts = 600 seconds (10 minutes).
            attempt_interval_seconds=5,
            max_attempts=120,
        )
        kitmaker_get_release_status_response_json = kitmaker_get_release_status_response.json()
        logger.info(
            "\n".join(
                [
                    f"Kitmaker release {kitmaker_create_release_response_json['release_uuid']} ended in {kitmaker_get_release_status_response_json['status']} status:",
                    pprint.pformat(kitmaker_get_release_status_response_json),
                ]
            )
        )

        if kitmaker_get_release_status_response_json["status"] not in kitmaker_release_success_statuses:
            raise RuntimeError(
                f"Kitmaker release {kitmaker_create_release_response_json['release_uuid']} failed with status {kitmaker_get_release_status_response_json['status']}!"
            )
    except AssertionError:
        logger.error(f"Timed out waiting for Kitmaker release {kitmaker_create_release_response_json['release_uuid']}!")
        raise

    if arguments.phase == Phase.publish:
        return 0

    return 0


argparse_extensions.set_command_function(parser=PARSER, arguments_type=Arguments, func=func)
