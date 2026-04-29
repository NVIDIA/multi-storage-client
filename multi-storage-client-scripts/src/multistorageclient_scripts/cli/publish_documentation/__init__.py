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
import os
import pprint
import signal
import sys
from dataclasses import dataclass
from types import FrameType
from typing import Final, Optional

import githubkit
import githubkit.versions.latest.models
import urllib3

import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions
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

    #: GitHub artifact ID.
    github_artifact_id: Final[int]
    #: GitHub token.
    github_token: Final[str]
    #: Git tag of the GitHub release.
    git_tag: Final[str]
    #: Release phase to exit after.
    phase: Final[Phase]


# TODO: Add `color` and `suggest_on_error` once we're on Python 3.14+.
PARSER = cli.SUBPARSERS.add_parser(
    name="publish-documentation",
    help="Publish the documentation for a release.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    allow_abbrev=False,
)


argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="github_artifact_id")(
    help="GitHub artifact ID."
)
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="github_token")(
    help="GitHub token. For local runs, log in with the GitHub CLI and pass `$(gh auth token)`."
)
argparse_extensions.add_argument_partial(parser=PARSER, arguments_type=Arguments, argument_key="git_tag")(
    help="Git tag of the GitHub release."
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

    get_latest_release_response = github_client.rest.repos.get_latest_release(
        owner="NVIDIA", repo="multi-storage-client"
    )

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

    multi_storage_client_docs_archives = [
        release_asset
        for release_asset in get_release_by_tag_response.parsed_data.assets
        if release_asset.name == "multi-storage-client-docs.zip"
    ]

    # ----------------------------------------------------------------------------------------------------
    #
    # Check inputs.
    #
    # ----------------------------------------------------------------------------------------------------

    logger.info(
        "\n".join(
            [
                "Release documentation assets:",
                pprint.pformat(
                    [
                        multi_storage_client_docs_archive.model_dump()
                        for multi_storage_client_docs_archive in multi_storage_client_docs_archives
                    ]
                ),
            ]
        )
    )

    if len(multi_storage_client_docs_archives) != 1:
        raise ValueError("Expected 1 release documentation asset!")

    if arguments.phase == Phase.check:
        return 0

    # ----------------------------------------------------------------------------------------------------
    #
    # Publish.
    #
    # ----------------------------------------------------------------------------------------------------

    # GitHub Pages deployments require a GitHub Actions OIDC token.
    if "GITHUB_ACTIONS" not in os.environ:
        logger.info("GitHub Pages deployments must be done in GitHub Actions. Nothing to do.")
        return 0

    # Only publish for latest releases.
    if get_release_by_tag_response.parsed_data.draft:
        logger.info(f"Existing GitHub release for {arguments.git_tag} is a draft. Nothing to do.")
        return 0
    if get_release_by_tag_response.parsed_data.prerelease:
        logger.info(f"Existing GitHub release for {arguments.git_tag} is a pre-release. Nothing to do.")
        return 0
    if get_latest_release_response.parsed_data.id != get_release_by_tag_response.parsed_data.id:
        logger.info(f"Existing GitHub release for {arguments.git_tag} is not the latest release. Nothing to do.")
        return 0

    # https://docs.github.com/en/actions/reference/security/oidc#methods-for-requesting-the-oidc-token
    # https://docs.github.com/en/actions/how-tos/secure-your-work/security-harden-deployments/oidc-in-cloud-providers#using-custom-actions
    get_oidc_token_response = urllib3.request(
        method="GET",
        url=os.environ["ACTIONS_ID_TOKEN_REQUEST_URL"],
        headers={"Authorization": f"Bearer {os.environ['ACTIONS_ID_TOKEN_REQUEST_TOKEN']}"},
        timeout=30,
        retries=urllib3.Retry(),
    )
    if get_oidc_token_response.status != 200:
        raise RuntimeError(
            "Failed to get GitHub Actions OIDC token!",
            get_oidc_token_response.status,
            get_oidc_token_response.data.decode(),
        )
    oidc_token = get_oidc_token_response.json().get("value")
    if oidc_token is None:
        raise ValueError("GitHub Actions OIDC token is missing from response!")
    if not isinstance(oidc_token, str):
        raise ValueError("GitHub Actions OIDC token is not a string!")

    # create_pages_deployment_response = github_client.rest.repos.create_pages_deployment(
    #     owner="NVIDIA",
    #     repo="multi-storage-client",
    #     # TODO: Upload the documentation archive release asset as a GitHub Actions artifact in this script.
    #     #
    #     # GitHub's artifact upload API isn't documented so we have to rely on `actions/upload-pages-artifact` for now.
    #     #
    #     # https://github.com/actions/upload-artifact/issues/180#issuecomment-1086306269
    #     artifact_id=arguments.github_artifact_id,
    #     environment="GitHub Pages",
    #     # This must be a Git commit revision.
    #     #
    #     # https://github.com/orgs/community/discussions/170184
    #     # https://github.com/actions/deploy-pages/issues/383
    #     #
    #     # publish-release uses a Git commit revision.
    #     pages_build_version=get_release_by_tag_response.parsed_data.target_commitish,
    #     oidc_token=oidc_token,
    # )
    create_pages_deployment_response = github_client.request(
        method="POST",
        url="/repos/NVIDIA/multi-storage-client/pages/deployments",
        headers={
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2026-03-10",
        },
        json={
            # The `artifact_id` parameter is broken on `github_client.rest.repos.create_pages_deployment()`
            #
            # This is because GitHub's API model has type `number` which is turned into a Python `float`.
            #
            # When serialized, `.0` is appended to the artifact ID which causes the GitHub Pages deployment to get stuck in `deployment_queued`.
            #
            # https://github.com/yanyongyu/githubkit/issues/300
            "artifact_id": arguments.github_artifact_id,
            "environment": "GitHub Pages",
            "pages_build_version": get_release_by_tag_response.parsed_data.target_commitish,
            "oidc_token": oidc_token,
        },
        response_model=githubkit.versions.latest.models.PageDeployment,
    )
    logger.info(
        "\n".join(
            [
                "Created a GitHub Pages deployment:",
                pprint.pformat(create_pages_deployment_response.parsed_data.model_dump()),
            ]
        )
    )

    def cancel_github_pages_deployment() -> None:
        github_client.rest.repos.cancel_pages_deployment(
            owner="NVIDIA",
            repo="multi-storage-client",
            pages_deployment_id=create_pages_deployment_response.parsed_data.id,
        )
        logger.info(f"Canceled GitHub Pages deployment {create_pages_deployment_response.parsed_data.id}.")

    def cancel_github_pages_deployment_signal_handler(signal_number: int, stack_frame: Optional[FrameType]) -> None:
        cancel_github_pages_deployment()
        sys.exit(128 + signal_number)

    signal.signal(signalnum=signal.SIGINT, handler=cancel_github_pages_deployment_signal_handler)
    signal.signal(signalnum=signal.SIGTERM, handler=cancel_github_pages_deployment_signal_handler)

    def poll_github_pages_deployment_status():
        get_pages_deployment_response = github_client.rest.repos.get_pages_deployment(
            owner="NVIDIA",
            repo="multi-storage-client",
            pages_deployment_id=create_pages_deployment_response.parsed_data.id,
        )
        # https://github.com/yanyongyu/githubkit/issues/299
        logger.info(
            f"GitHub Pages deployment {create_pages_deployment_response.parsed_data.id} is in {get_pages_deployment_response.json().get('status')} status."
        )
        return get_pages_deployment_response

    github_pages_deployment_success_statuses = {"succeed"}
    github_pages_deployment_failure_statuses = {
        "deployment_cancelled",
        "deployment_failed",
        "deployment_content_failed",
        "deployment_lost",
    }
    github_pages_deployment_end_statuses = (
        github_pages_deployment_success_statuses | github_pages_deployment_failure_statuses
    )

    try:
        logger.info(f"Waiting for GitHub Pages deployment {create_pages_deployment_response.parsed_data.id}...")
        get_pages_deployment_response = wait(
            waitable=poll_github_pages_deployment_status,
            should_wait=lambda get_pages_deployment_response: (
                get_pages_deployment_response.json().get("status") not in github_pages_deployment_end_statuses
            ),
            # 5 seconds * 120 attempts = 600 seconds (10 minutes).
            attempt_interval_seconds=5,
            max_attempts=120,
        )
        logger.info(
            f"GitHub Pages deployment {create_pages_deployment_response.parsed_data.id} ended in {get_pages_deployment_response.json().get('status')} status."
        )

        if get_pages_deployment_response.json().get("status") not in github_pages_deployment_success_statuses:
            raise RuntimeError(
                f"GitHub Pages deployment {create_pages_deployment_response.parsed_data.id} failed with status {get_pages_deployment_response.json().get('status')}!"
            )
    except AssertionError:
        logger.error(
            f"Timed out waiting for GitHub Pages deployment {create_pages_deployment_response.parsed_data.id}!"
        )
        cancel_github_pages_deployment()
        raise

    if arguments.phase == Phase.publish:
        return 0

    return 0


argparse_extensions.set_command_function(parser=PARSER, arguments_type=Arguments, func=func)
