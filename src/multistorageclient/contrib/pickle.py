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

import os
import pickle as _pickle
from collections.abc import Callable, Iterable
from typing import IO, Any, Optional, Union

from ..pathlib import MultiStoragePath
from ..shortcuts import open as msc_open


def load(
    file: Union[str, os.PathLike[str], IO[bytes]],
    *,
    fix_imports: bool = True,
    encoding: str = "ASCII",
    errors: str = "strict",
    buffers: Optional[Iterable[Any]] = None,
) -> Any:
    """
    Adapt ``pickle.load``.

    This function aims to provide additional flexibility for callers to load from files in the following ways:

    .. code-block:: python
       :linenos:

       multistorageclient.pickle.load(multistorageclient.open(file_path_with_msc_protocol, "rb"))
       multistorageclient.pickle.load(file_path_with_msc_protocol)

    User can also use native pickle function to achieve the same goal:

    .. code-block:: python
       :linenos:

        pickle.load(multistorageclient.open(file_path_with_msc_protocol, "rb"))

    User, however, cannot directly pass the file object as the msc-prefixed file path cannot be used by native
    ``open()`` i.e. ``multistorageclient.pickle.load(open(file_path_with_msc_protocol, "rb"))``
    """

    if isinstance(file, str):
        with msc_open(file) as fp:
            return _pickle.load(fp, fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers)
    elif isinstance(file, MultiStoragePath):
        with file.open("rb") as fp:
            return _pickle.load(fp, fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers)
    else:
        # assume a file-like object
        return _pickle.load(file, fix_imports=fix_imports, encoding=encoding, errors=errors, buffers=buffers)  # type: ignore


def dump(
    obj: Any,
    file_path: Union[str, os.PathLike[str]],
    protocol: Optional[int] = None,
    *,
    fix_imports: bool = True,
    buffer_callback: Optional[Callable[[Any], None]] = None,
    attributes: Optional[dict[str, str]] = None,
) -> None:
    """
    Adapt ``pickle.dump``.

    This function can take only file path of the target file, it cannot take file-like object

    .. code-block:: python
       :linenos:

       multistorageclient.pickle.dump(data, file_path_with_msc_protocol, ...)

    Alternatively, user can use native pickle dump, but need to close the file to proactively trigger file upload:

    .. code-block:: python
       :linenos:

       with multistorageclient.open(file_path_with_msc_protocol, "rb") as fp:
           pickle.dump(data, fp, ....)

    :param attributes: Optional dictionary of custom attributes/metadata to attach to the file.
    """
    attributes_dict = attributes or {}
    if isinstance(file_path, str):
        with msc_open(file_path, mode="wb", attributes=attributes_dict) as fp:
            _pickle.dump(obj, fp, protocol=protocol, fix_imports=fix_imports, buffer_callback=buffer_callback)
    elif isinstance(file_path, MultiStoragePath):
        with file_path.open("wb", attributes=attributes_dict) as fp:
            _pickle.dump(obj, fp, protocol=protocol, fix_imports=fix_imports, buffer_callback=buffer_callback)
    else:
        raise NotImplementedError("file object is not supported.")
