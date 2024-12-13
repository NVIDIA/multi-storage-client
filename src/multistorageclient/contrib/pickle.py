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

import pickle as _pickle
from typing import IO, Any, Callable, Iterable, Optional, Union

from ..shortcuts import open as msc_open
from ..types import MSC_PROTOCOL


def load(file: Union[str, IO[bytes]],
         *,
         fix_imports: bool = True,
         encoding: str = "ASCII",
         errors: str = "strict",
         buffers: Optional[Iterable[Any]] = None,
         ) -> Any:
    """
    Adapt pickle.load.

    This function aims to provide additional flexibility for callers to load from files in the following ways:
    - multistorageclient.pickle.load(multistorageclient.open(file_path_w_storageclient_protocol, "rb"))
    - multistorageclient.pickle.load(file_path_w_storageclient_protocol)

    User can also use native pickle function to achieve the same goal:
    - pickle.load(multistorageclient.open(file_path_w_storageclient_protocol, "rb"))

    User, however, cannot directly pass the file object as the msc-prefixed file path cannot be used by native open()
    i.e. multistorageclient.pickle.load(open(file_path_w_storageclient_protocol, "rb"))
    """

    if isinstance(file, str):
        if file.startswith(MSC_PROTOCOL):
            with msc_open(file) as fp:
                return _pickle.load(fp, fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers)
        else:
            with open(file, "rb") as fp:
                return _pickle.load(fp, fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers)
    else:
        # assume a file-like object
        return _pickle.load(file, fix_imports=fix_imports, encoding=encoding,
                            errors=errors, buffers=buffers)  # type: ignore


def dump(obj: Any,
         file_path: str,
         protocol: Optional[int] = None,
         *,
         fix_imports: bool = True,
         buffer_callback: Optional[Callable[[Any], None]] = None,
         ) -> None:
    """
    Adapt pickle.dump.

    This function can take only file path of the target file, it cannot take file-like object
    - multistorageclient.pickle.dump(data, file_path_w_storageclient_protocol, ...)

    Alternatively, user can use native pickle dump, but need to close the file to proactively trigger file upload:
    with multistorageclient.open(file_path_w_storageclient_protocol, "rb") as fp:
        pickle.dump(data, fp, ....)
    """
    if isinstance(file_path, str):
        if file_path.startswith(MSC_PROTOCOL):
            with msc_open(file_path, mode="wb") as fp:
                _pickle.dump(obj, fp, protocol=protocol, fix_imports=fix_imports, buffer_callback=buffer_callback)
        else:
            with open(file_path, "wb") as fp:
                _pickle.dump(obj, fp, protocol=protocol, fix_imports=fix_imports, buffer_callback=buffer_callback)
    else:
        raise NotImplementedError("file object is not supported.")