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


import logging

# Imports have side effects (registers commands).
import multistorageclient_scripts.cli as cli
import multistorageclient_scripts.cli.publish_release  # noqa: F401
import multistorageclient_scripts.utils.argparse_extensions as argparse_extensions

logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)


def main():
    argparse_extensions.run_cli(parser=cli.PARSER)


if __name__ == "__main__":
    main()
