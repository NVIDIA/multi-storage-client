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
from dataclasses import dataclass

import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions


@dataclass(frozen=True, kw_only=True)
class Arguments(argparse_extensions.Arguments):
    """
    Command arguments.
    """

    pass


# TODO: Add `color` and `suggest_on_error` once we're on Python 3.14+.
PARSER = argparse.ArgumentParser(
    description="Internal helper scripts for things too painful to do with Bash.",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    allow_abbrev=False,
)
SUBPARSERS = PARSER.add_subparsers()


def func(arguments: Arguments) -> int:
    PARSER.print_help()
    return 0


argparse_extensions.set_command_function(parser=PARSER, arguments_type=Arguments, func=func)
