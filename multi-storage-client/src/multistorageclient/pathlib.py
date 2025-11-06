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

import codecs
import logging
import os
import stat
from pathlib import Path, PurePosixPath
from typing import Union

from .client import StorageClient
from .shortcuts import resolve_storage_client
from .types import MSC_PROTOCOL, ObjectMetadata
from .utils import join_paths

logger = logging.getLogger(__name__)


class StatResult:
    """
    A stat-like result object that mimics os.stat_result for remote storage paths.

    This class provides the same interface as os.stat_result but is populated
    from ObjectMetadata obtained from storage providers.
    """

    def __init__(self, metadata: ObjectMetadata):
        """Initialize StatResult from ObjectMetadata."""
        # File type and mode bits
        if metadata.type == "directory":
            # Directory: 0o755 (rwxr-xr-x) + S_IFDIR
            self.st_mode = stat.S_IFDIR | 0o755
        else:
            # Regular file: 0o644 (rw-r--r--) + S_IFREG
            self.st_mode = stat.S_IFREG | 0o644

        # File size
        self.st_size = metadata.content_length

        # Timestamps - convert datetime to epoch seconds
        mtime = metadata.last_modified.timestamp()
        self.st_mtime = mtime
        self.st_atime = mtime
        self.st_ctime = mtime

        # Nanosecond precision timestamps
        mtime_ns = int(mtime * 1_000_000_000)
        self.st_mtime_ns = mtime_ns
        self.st_atime_ns = mtime_ns
        self.st_ctime_ns = mtime_ns

        # Default values for fields we don't have from storage providers
        self.st_ino = 0
        self.st_dev = 0
        self.st_nlink = 1
        self.st_uid = os.getuid() if hasattr(os, "getuid") else 0  # User ID
        self.st_gid = os.getgid() if hasattr(os, "getgid") else 0  # Group ID


class MultiStoragePath:
    """
    A path object similar to pathlib.Path that supports both local and remote file systems.

    MultiStoragePath provides a unified interface for working with paths across different storage systems,
    including local files, S3, GCS, Azure Blob Storage, and more. It uses the "msc://" protocol
    prefix to identify remote storage paths.

    This implementation is based on Python 3.9's pathlib.Path interface, providing compatible behavior
    for local filesystem operations while extending support to remote storage systems.

    Examples:
        >>> import multistorageclient as msc
        >>> msc.Path("/local/path/file.txt")
        >>> msc.Path("msc://my-profile/data/file.txt")
        >>> msc.Path(pathlib.Path("relative/path"))
    """

    _internal_path: PurePosixPath
    _storage_client: StorageClient
    _path: str

    def __init__(self, path: Union[str, os.PathLike]):
        """
        Initialize path object supporting multiple storage backends.

        :param path: String, Path, or MultiStoragePath. Relative paths are automatically converted to absolute.
        """
        self._path = str(path)
        self._storage_client, relative_path = resolve_storage_client(self._path)
        self._internal_path = PurePosixPath(relative_path)

        if self._storage_client.is_default_profile():
            self._internal_path = PurePosixPath("/") / self._internal_path

    def __str__(self) -> str:
        if self._storage_client.is_default_profile():
            return str(self._internal_path)
        return join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path))

    def __repr__(self) -> str:
        return f"MultiStoragePath({str(self)!r})"

    def __eq__(self, other) -> bool:
        if not isinstance(other, MultiStoragePath):
            return False
        return (
            self._storage_client.profile == other._storage_client.profile
            and self._internal_path == other._internal_path
        )

    def __hash__(self) -> int:
        """Return hash of the path."""
        return hash((self._storage_client.profile, self._internal_path))

    def __fspath__(self) -> str:
        return str(self)

    def joinpath(self, *pathsegments):
        return self.with_segments(*pathsegments)

    def __truediv__(self, key):
        try:
            return self.joinpath(key)
        except TypeError:
            return NotImplemented

    def __rtruediv__(self, key):
        try:
            return self.with_segments(key, self)
        except TypeError:
            return NotImplemented

    def __getstate__(self):
        return {"_path": self._path, "_internal_path": self._internal_path}

    def __setstate__(self, state):
        self._path = state["_path"]
        self._internal_path = state["_internal_path"]
        self._storage_client, _ = resolve_storage_client(self._path)

    @property
    def anchor(self) -> str:
        """
        The concatenation of the drive and root, or ''.
        """
        return self._internal_path.anchor

    @property
    def name(self) -> str:
        """
        The final path component, if any.
        """
        return self._internal_path.name

    @property
    def suffix(self) -> str:
        """
        The final path component, if any.
        """
        return self._internal_path.suffix

    @property
    def suffixes(self) -> list[str]:
        """
        A list of the final component's suffixes, if any.

        These include the leading periods. For example: ['.tar', '.gz']
        """
        return self._internal_path.suffixes

    @property
    def stem(self) -> str:
        """
        The final path component, minus its last suffix.
        """
        return self._internal_path.stem

    @property
    def parent(self) -> "MultiStoragePath":
        """
        The logical parent of the path.
        """
        parent_path = self._internal_path.parent
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(parent_path))
        return MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(parent_path)))

    @property
    def parents(self) -> list["MultiStoragePath"]:
        """
        A sequence of this path's logical parents.
        """
        if self._storage_client.is_default_profile():
            return [MultiStoragePath(str(p)) for p in self._internal_path.parents]
        else:
            return [
                MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(p)))
                for p in self._internal_path.parents
            ]

    @property
    def parts(self):
        """
        An object providing sequence-like access to the components in the filesystem path (does not
        include the msc:// and the profile name).
        """
        return self._internal_path.parts

    def as_posix(self) -> str:
        """
        Return the string representation of the path with forward (/) slashes.

        If the path is a remote path, the file content is downloaded to local storage
        (either cached or temporary file) and the local filesystem path is returned.
        This enables access to remote file content through standard filesystem operations.
        """
        if self._storage_client.is_default_profile():
            return self._internal_path.as_posix()

        # Return the local path of the file
        with self._storage_client.open(str(self._internal_path), mode="rb") as fp:
            return fp.resolve_filesystem_path()

    def is_absolute(self) -> bool:
        """
        Paths are always absolute.
        """
        return True

    def is_relative_to(self, other: "MultiStoragePath") -> bool:
        """
        Return True if the path is relative to another path or False.
        """
        return isinstance(other, MultiStoragePath) and self._internal_path.is_relative_to(other._internal_path)

    def is_reserved(self) -> bool:
        if self._storage_client.is_default_profile():
            return self._internal_path.is_reserved()
        raise NotImplementedError("MultiStoragePath.is_reserved() is unsupported for remote storage paths")

    def match(self, pattern) -> bool:
        """
        Return True if this path matches the given pattern.
        """
        return Path(self._internal_path).match(pattern)

    def relative_to(self, other: "MultiStoragePath") -> PurePosixPath:
        """
        Return a version of this path relative to another path.

        Both paths must use the same storage profile. The operation raises ValueError if:
        - The paths have different storage profiles
        - This path is not relative to the other path
        - The other path is not a MultiStoragePath instance

        Note: This method returns a PurePosixPath (not a MultiStoragePath) because
        MultiStoragePath always represents absolute paths.

        :param other: The base path to calculate relative path from
        :return: A PurePosixPath representing the relative path
        :raises ValueError: If the path cannot be made relative to other
        :raises TypeError: If other is not a MultiStoragePath instance
        """
        if not isinstance(other, MultiStoragePath):
            raise TypeError(f"'{type(other).__name__}' object cannot be used as relative base")

        # Check if both paths use the same storage profile
        if self._storage_client.profile != other._storage_client.profile:
            raise ValueError(
                f"Cannot compute relative path between different storage profiles: "
                f"'{self._storage_client.profile}' and '{other._storage_client.profile}'"
            )

        # Use the internal PurePosixPath.relative_to() method
        try:
            return self._internal_path.relative_to(other._internal_path)
        except ValueError:
            # Re-raise with paths that include the profile information for clarity
            raise ValueError(f"{str(self)!r} is not in the subpath of {str(other)!r}")

    def with_name(self, name: str) -> "MultiStoragePath":
        """
        Return a new path with the file name changed.
        """
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(self._internal_path.with_name(name)))
        else:
            return MultiStoragePath(
                join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path.with_name(name)))
            )

    def with_stem(self, stem: str) -> "MultiStoragePath":
        """
        Return a new path with the stem changed.
        """
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(self._internal_path.with_stem(stem)))
        else:
            return MultiStoragePath(
                join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path.with_stem(stem)))
            )

    def with_suffix(self, suffix: str) -> "MultiStoragePath":
        """
        Return a new path with the file suffix changed. If the path has no suffix, add given suffix.
        If the given suffix is an empty string, remove the suffix from the path.
        """
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(self._internal_path.with_suffix(suffix)))
        else:
            return MultiStoragePath(
                join_paths(
                    f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path.with_suffix(suffix))
                )
            )

    def with_segments(self, *pathsegments) -> "MultiStoragePath":
        """
        Construct a new path object from any number of path-like objects.
        """
        if self._storage_client.is_default_profile():
            new_path = self._internal_path.joinpath(*pathsegments)
            return MultiStoragePath(str(new_path))
        else:
            new_path = self._internal_path.joinpath(*pathsegments)
            return MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(new_path)))

    # Expanding and resolving paths

    @classmethod
    def home(cls):
        """
        Return a new path pointing to the user's home directory.
        """
        return Path.home()

    def expanduser(self):
        """
        Return a new path with expanded ~ and ~user constructs (as returned by os.path.expanduser).

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).expanduser()
        raise NotImplementedError("MultiStoragePath.expanduser() is unsupported for remote storage paths")

    @classmethod
    def cwd(cls):
        """
        Return a new path pointing to the current working directory.
        """
        return Path.cwd()

    def absolute(self):
        """
        Return the path itself since it is always absolute.
        """
        return self

    def resolve(self, strict=False):
        """
        Return the absolute path.
        """
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(Path(self._internal_path).resolve(strict=strict)))
        return MultiStoragePath(join_paths(f"{MSC_PROTOCOL}{self._storage_client.profile}", str(self._internal_path)))

    def readlink(self):
        """
        Return the path to which the symbolic link points.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return MultiStoragePath(str(Path(self._internal_path).readlink()))
        raise NotImplementedError("MultiStoragePath.readlink() is unsupported for remote storage paths")

    # Querying file type and status

    def stat(self):
        """
        Return the result of the stat() system call on this path, like os.stat() does.

        If the path is a remote path, the result is a :py:class:`multistorageclient.pathlib.StatResult` object.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).stat()
        info = self._storage_client.info(str(self._internal_path))
        return StatResult(info)

    def lstat(self):
        """
        Like stat(), except if the path points to a symlink, the symlink's status information
        is returned, rather than its target's.

        If the path is a remote path, the result is a :py:class:`multistorageclient.pathlib.StatResult` object.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).lstat()
        info = self._storage_client.info(str(self._internal_path))
        return StatResult(info)

    def exists(self) -> bool:
        """
        Return True if the path exists.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).exists()
        else:
            try:
                self._storage_client.info(str(self._internal_path))
                return True
            except FileNotFoundError:
                return False

    def is_file(self, strict: bool = True) -> bool:
        """
        Return True if the path exists and is a regular file.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_file()
        else:
            try:
                # If the path ends with a "/", assume it is a directory.
                path = str(self._internal_path)
                if path.endswith("/"):
                    return False

                meta = self._storage_client.info(path, strict=strict)
                return meta.type == "file"
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.warning("Error occurred while fetching file info at %s, caused by: %s", self._internal_path, e)
                return False

    def is_dir(self, strict: bool = True) -> bool:
        """
        Return True if the path exists and is a directory.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_dir()
        else:
            try:
                # If the path does not end with a "/", append it to ensure the path is a directory.
                path = str(self._internal_path)
                if not path.endswith("/"):
                    path += "/"

                meta = self._storage_client.info(path, strict=strict)
                return meta.type == "directory"
            except FileNotFoundError:
                return False
            except Exception as e:
                logger.warning("Error occurred while fetching file info at %s, caused by: %s", self._internal_path, e)
                return False

    def is_symlink(self):
        """
        Return True if the path exists and is a symbolic link.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_symlink()
        raise NotImplementedError("MultiStoragePath.is_symlink() is unsupported for remote storage paths")

    def is_mount(self):
        """
        Return True if the path exists and is a mount point.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_mount()
        raise NotImplementedError("MultiStoragePath.is_mount() is unsupported for remote storage paths")

    def is_socket(self):
        """
        Return True if the path exists and is a socket.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_socket()
        raise NotImplementedError("MultiStoragePath.is_socket() is unsupported for remote storage paths")

    def is_fifo(self):
        """
        Return True if the path exists and is a FIFO.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_fifo()
        raise NotImplementedError("MultiStoragePath.is_fifo() is unsupported for remote storage paths")

    def is_block_device(self):
        """
        Return True if the path exists and is a block device.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_block_device()
        raise NotImplementedError("MultiStoragePath.is_block_device() is unsupported for remote storage paths")

    def is_char_device(self):
        """
        Return True if the path exists and is a character device.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).is_char_device()
        raise NotImplementedError("MultiStoragePath.is_char_device() is unsupported for remote storage paths")

    def samefile(self, other_path):
        """
        Return True if both paths point to the same file or directory.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).samefile(other_path)
        return self == other_path

    # Reading and writing files

    def open(self, mode="r", buffering=-1, encoding=None, errors=None, newline=None, **kwargs):
        """
        Open the file and return a file object.

        :param mode: The file mode to open the file in.
        :param buffering: The buffering mode.
        :param encoding: The encoding to use for text files.
        :param errors: How to handle encoding errors.
        :param newline: Controls universal newlines mode.
        :param kwargs: Additional arguments passed to client.open (e.g., check_source_version, prefetch_file, etc.)
        """
        return self._storage_client.open(
            str(self._internal_path), mode=mode, buffering=buffering, encoding=encoding, **kwargs
        )

    def read_bytes(self) -> bytes:
        """
        Open the file in bytes mode, read it, and close the file.
        """
        return self._storage_client.read(str(self._internal_path))

    def read_text(self, encoding: str = "utf-8", errors: str = "strict") -> str:
        """
        Open the file in text mode, read it, and close the file.
        """
        result = self._storage_client.read(str(self._internal_path))
        if not hasattr(result, "decode"):  # Rust's PyBytes does not implement decode
            return codecs.decode(memoryview(result), encoding)
        return result.decode(encoding)

    def write_bytes(self, data: bytes) -> None:
        """
        Open the file in bytes mode, write to it, and close the file.
        """
        self._storage_client.write(str(self._internal_path), data)

    def write_text(self, data: str, encoding: str = "utf-8", errors: str = "strict") -> None:
        """
        Open the file in text mode, write to it, and close the file.
        """
        self._storage_client.write(str(self._internal_path), data.encode(encoding))

    # Reading directories

    def iterdir(self):
        """
        Yield path objects of the directory contents.
        """
        if self._storage_client.is_default_profile():
            for item in Path(self._internal_path).iterdir():
                yield MultiStoragePath(str(item))
        else:
            path = str(self._internal_path)
            if not path.endswith("/"):
                path += "/"
            for item in self._storage_client.list(path, include_directories=True, include_url_prefix=True):
                yield MultiStoragePath(item.key)

    def glob(self, pattern):
        """
        Iterate over this subtree and yield all existing files (of any kind, including directories)
        matching the given relative pattern.
        """
        if self._storage_client.is_default_profile():
            return [MultiStoragePath(str(p)) for p in Path(self._internal_path).glob(pattern)]
        else:
            return [
                MultiStoragePath(str(p))
                for p in self._storage_client.glob(str(self._internal_path / pattern), include_url_prefix=True)
            ]

    def rglob(self, pattern):
        """
        Recursively yield all existing files (of any kind, including directories) matching the
        given relative pattern, anywhere in this subtree.
        """
        if self._storage_client.is_default_profile():
            return [MultiStoragePath(str(p)) for p in Path(self._internal_path).rglob(pattern)]
        else:
            recursive_pattern = f"**/{pattern}"
            return [
                MultiStoragePath(str(p))
                for p in self._storage_client.glob(
                    str(self._internal_path / recursive_pattern), include_url_prefix=True
                )
            ]

    def walk(self, top_down=True, on_error=None, follow_symlinks=False):
        """
        Walk the directory tree from this directory, similar to os.walk().

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).walk(top_down, on_error, follow_symlinks)  # pyright: ignore[reportAttributeAccessIssue]
        raise NotImplementedError("MultiStoragePath.walk() is unsupported for remote storage paths")

    # Creating files and directories

    def touch(self, mode=0o666, exist_ok=False):
        """
        Create this file with the given access mode, if it doesn't exist.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).touch(mode, exist_ok)
        else:
            if self.exists():
                # object storage does not support updating the last modified time of a object without writing the object
                logger.warning("MultiStoragePath.touch() is not supported for remote storage paths")
            else:
                self._storage_client.write(str(self._internal_path), b"")

    def mkdir(self, mode=0o777, parents=False, exist_ok=False) -> None:
        """
        Create a new directory at this given path.

        For remote storage paths, this operation is a no-op.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).mkdir(mode, parents, exist_ok)

    def symlink_to(self, target, target_is_directory=False):
        """
        Make this path a symlink pointing to the target path.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).symlink_to(target, target_is_directory)
        else:
            raise NotImplementedError("MultiStoragePath.symlink_to() is unsupported for remote storage paths")

    # Renaming and deleting

    def rename(self, target) -> "MultiStoragePath":
        """
        Rename this path to the target path.
        """
        if not isinstance(target, MultiStoragePath):
            target = MultiStoragePath(target)

        if self._storage_client.is_default_profile():
            Path(self._internal_path).rename(str(target._internal_path))
        else:
            # Note: This operation is not atomic, and the target path must be a single file.
            self._storage_client.copy(str(self._internal_path), str(target._internal_path))
            self._storage_client.delete(str(self._internal_path))

        return target

    def replace(self, target):
        """
        Rename this path to the target path, overwriting if that path exists.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).replace(target)
        else:
            raise NotImplementedError("MultiStoragePath.replace() is unsupported for remote storage paths")

    def unlink(self, missing_ok: bool = False) -> None:
        """
        Remove this file or link. If the path is a directory, use rmdir() instead.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).unlink(missing_ok=missing_ok)
        else:
            try:
                self._storage_client.delete(str(self._internal_path))
            except FileNotFoundError:
                if not missing_ok:
                    raise

    def rmdir(self) -> None:
        """
        Remove this directory. The directory must be empty.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).rmdir()
        else:
            raise NotImplementedError("MultiStoragePath.rmdir() is unsupported for remote storage paths")

    # Permissions and ownership

    def owner(self):
        """
        Return the login name of the file owner.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).owner()
        raise NotImplementedError("MultiStoragePath.owner() is unsupported for remote storage paths")

    def group(self):
        """
        Return the group name of the file gid.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            return Path(self._internal_path).group()
        raise NotImplementedError("MultiStoragePath.group() is unsupported for remote storage paths")

    def chmod(self, mode):
        """
        Change the permissions of the path, like os.chmod().

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).chmod(mode)
        else:
            raise NotImplementedError("MultiStoragePath.chmod() is unsupported for remote storage paths")

    def lchmod(self, mode):
        """
        Like chmod(), except if the path points to a symlink, the symlink's permissions are changed, rather
        than its target's.

        Not supported for remote storage paths.
        """
        if self._storage_client.is_default_profile():
            Path(self._internal_path).lchmod(mode)
        else:
            raise NotImplementedError("MultiStoragePath.lchmod() is unsupported for remote storage paths")
