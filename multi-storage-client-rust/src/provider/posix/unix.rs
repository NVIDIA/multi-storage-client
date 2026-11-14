// SPDX-FileCopyrightText: Copyright (c) 2026 NVIDIA CORPORATION & AFFILIATES. All rights reserved.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::PosixOptions;
use crate::{
    ByteRange, ByteStream, CopyOptions, CopyScope, DeleteOptions, ErrorKind, GetConditions,
    ListOptions, MetadataStream, MscError, ObjectKind, ObjectMetadata, ObjectPath,
    ProviderCapabilities, PutResult, Result, SourceVersion, StorageProvider, WriteOptions,
};
use async_trait::async_trait;
use bytes::Bytes;
use futures::{StreamExt, stream};
use rustix::fs::{self, AtFlags, FileType, Mode, OFlags, Stat};
use std::os::unix::fs::PermissionsExt;
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    path::PathBuf,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
    sync::Semaphore,
};
#[cfg(any(target_os = "linux", target_os = "macos"))]
use xattr::FileExt;

mod listing;

const BUFFER_SIZE: usize = 64 * 1024;
const ATTR_NAME: &str = "user.json";

/// Descriptor-anchored POSIX storage with atomic publication, bounded I/O, and
/// contained relative symlinks. Supported on Unix; clones share the root and
/// concurrency limit. Construct a client with [`crate::StorageClient::from_provider`]
/// or select the `file` provider in configuration.
#[derive(Clone)]
pub struct PosixProvider {
    root: Arc<File>,
    base: PathBuf,
    semaphore: Arc<Semaphore>,
    options: PosixOptions,
}

impl PosixProvider {
    /// Open an existing absolute directory and anchor all operations to its descriptor.
    ///
    /// # Errors
    /// Returns invalid options, missing roots, symlink-root, or filesystem errors.
    pub fn new(options: PosixOptions) -> Result<Self> {
        if !options.base_path.is_absolute()
            || options.max_concurrency == 0
            || options.max_concurrency > 1024
        {
            return Err(MscError::new(
                ErrorKind::InvalidConfig,
                "POSIX requires an absolute base_path and concurrency between 1 and 1024",
            ));
        }
        let base = std::fs::canonicalize(&options.base_path)?;
        let text = base.to_str().ok_or_else(|| {
            MscError::new(ErrorKind::InvalidConfig, "POSIX base_path must be UTF-8")
        })?;
        ObjectPath::new(text.trim_start_matches('/')).map_err(|_| {
            MscError::new(
                ErrorKind::InvalidConfig,
                "POSIX base_path cannot be represented as an object path",
            )
        })?;
        let root = File::from(
            fs::open(
                &base,
                OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno)?,
        );
        Ok(Self {
            root: Arc::new(root),
            base,
            semaphore: Arc::new(Semaphore::new(options.max_concurrency)),
            options,
        })
    }

    fn temporary_write(&self, key: &ObjectPath) -> Result<(File, TemporaryEntry, String)> {
        let (parent, name) = self.parent(key, true)?;
        let parent = Arc::new(parent);
        let temp_name = format!(".msc-tmp-{}", uuid::Uuid::new_v4());
        let (file, temporary) = TemporaryEntry::create(parent, temp_name, |parent, name| {
            fs::openat(
                parent,
                name,
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                Mode::from_bits_truncate(0o600),
            )
            .map(File::from)
            .map_err(errno)
        })?;
        Ok((file, temporary, name))
    }

    fn scope(&self) -> String {
        format!("file:{}", self.base.display())
    }

    fn version(&self, stat: &Stat) -> SourceVersion {
        SourceVersion {
            scope: self.scope(),
            value: format!(
                "{}:{}:{}:{}:{}:{}:{}",
                stat.st_dev,
                stat.st_ino,
                stat.st_size,
                stat.st_mtime,
                stat.st_mtime_nsec,
                stat.st_ctime,
                stat.st_ctime_nsec
            ),
        }
    }

    fn directory(&self, key: &ObjectPath, create: bool) -> Result<File> {
        self.resolved_directory(key, create)
            .map(|(directory, _)| directory)
    }

    fn resolved_directory(&self, key: &ObjectPath, create: bool) -> Result<(File, ObjectPath)> {
        check_namespace(key)?;
        let mut key = key.clone();
        let mut visited = BTreeSet::new();
        for _ in 0..=8 {
            if !visited.insert(key.clone()) {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "POSIX directory symlink cycle",
                ));
            }
            let parts: Vec<_> = key
                .as_str()
                .split('/')
                .filter(|part| !part.is_empty())
                .collect();
            let mut directory = self.root.try_clone()?;
            let mut prefix = ObjectPath::default();
            let mut restart = None;
            for (index, part) in parts.iter().enumerate() {
                let opened = fs::openat(
                    &directory,
                    *part,
                    OFlags::RDONLY | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
                    Mode::empty(),
                );
                match opened {
                    Ok(opened) => directory = File::from(opened),
                    Err(error) if create && error == rustix::io::Errno::NOENT => {
                        match fs::mkdirat(&directory, *part, Mode::from_bits_truncate(0o755)) {
                            Ok(()) => (),
                            Err(error) if error == rustix::io::Errno::EXIST => (),
                            Err(error) => return Err(errno(error)),
                        }
                        directory = File::from(
                            fs::openat(
                                &directory,
                                *part,
                                OFlags::RDONLY
                                    | OFlags::DIRECTORY
                                    | OFlags::NOFOLLOW
                                    | OFlags::CLOEXEC,
                                Mode::empty(),
                            )
                            .map_err(errno)?,
                        );
                    }
                    Err(error) => {
                        let stat = fs::statat(&directory, *part, AtFlags::SYMLINK_NOFOLLOW)
                            .map_err(errno)?;
                        if FileType::from_raw_mode(stat.st_mode) != FileType::Symlink {
                            return Err(errno(error));
                        }
                        let link_key = prefix.join(&ObjectPath::new(part)?)?;
                        let target =
                            fs::readlinkat(&directory, *part, Vec::new()).map_err(errno)?;
                        let target = target.to_str().map_err(|_| {
                            MscError::new(
                                ErrorKind::InvalidPath,
                                "POSIX symlink target is not UTF-8",
                            )
                        })?;
                        let resolved = self.resolve_target(&link_key, target)?;
                        restart =
                            Some(resolved.join(&ObjectPath::new(parts[index + 1..].join("/"))?)?);
                        break;
                    }
                }
                prefix = prefix.join(&ObjectPath::new(part)?)?;
            }
            if let Some(next) = restart {
                key = next;
            } else {
                return Ok((directory, key));
            }
        }
        Err(MscError::new(
            ErrorKind::Integrity,
            "POSIX directory symlink hop limit exceeded",
        ))
    }

    fn resolve_target(&self, key: &ObjectPath, target: &str) -> Result<ObjectPath> {
        let mut named_component = false;
        for component in target.split('/') {
            match component {
                ".." if named_component => {
                    return Err(MscError::unsupported(
                        "POSIX symlink parent traversal after a named component",
                    ));
                }
                "" | "." | ".." => {}
                _ => named_component = true,
            }
        }
        if target.starts_with('/') {
            let target = std::path::Path::new(target)
                .strip_prefix(&self.base)
                .map_err(|_| {
                    MscError::new(ErrorKind::InvalidPath, "POSIX symlink escapes profile root")
                })?;
            ObjectPath::new(target.to_str().ok_or_else(|| {
                MscError::new(ErrorKind::InvalidPath, "POSIX target is not UTF-8")
            })?)
        } else {
            key.resolve_link(target)
        }
    }

    fn parent(&self, key: &ObjectPath, create: bool) -> Result<(File, String)> {
        check_namespace(key)?;
        if key.is_root() {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "POSIX mutation requires a non-root key",
            ));
        }
        let (parent, name) = key.as_str().rsplit_once('/').unwrap_or(("", key.as_str()));
        Ok((
            self.directory(&ObjectPath::new(parent)?, create)?,
            name.to_owned(),
        ))
    }

    fn metadata(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        self.metadata_with_attributes(key, true)
    }

    fn metadata_with_attributes(
        &self,
        key: &ObjectPath,
        include_attributes: bool,
    ) -> Result<ObjectMetadata> {
        if key.is_root() {
            return self.stat_metadata(
                key.clone(),
                &fs::fstat(&*self.root).map_err(errno)?,
                None,
                BTreeMap::new(),
            );
        }
        check_namespace(key)?;
        let (parent_key, name) = key.as_str().rsplit_once('/').unwrap_or(("", key.as_str()));
        let name = name.to_owned();
        let (directory, physical_parent) =
            self.resolved_directory(&ObjectPath::new(parent_key)?, false)?;
        let physical_key = physical_parent.join(&ObjectPath::new(&name)?)?;
        let mut stat = fs::statat(&directory, &name, AtFlags::SYMLINK_NOFOLLOW).map_err(errno)?;
        let kind = FileType::from_raw_mode(stat.st_mode);
        let link = if kind == FileType::Symlink {
            let target = fs::readlinkat(&directory, &name, Vec::new()).map_err(errno)?;
            let raw = target.to_str().map_err(|_| {
                MscError::new(ErrorKind::InvalidPath, "POSIX symlink target is not UTF-8")
            })?;
            Some(match self.resolve_target(&physical_key, raw) {
                Ok(target) => relative_target(key, &target),
                Err(error) if error.kind == ErrorKind::Unsupported => return Err(error),
                Err(_) => raw.to_owned(),
            })
        } else {
            None
        };
        let attributes = if kind == FileType::RegularFile && include_attributes {
            let file = File::from(
                fs::openat(
                    &directory,
                    &name,
                    OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                    Mode::empty(),
                )
                .map_err(errno)?,
            );
            stat = fs::fstat(&file).map_err(errno)?;
            read_attributes(&file)?
        } else {
            BTreeMap::new()
        };
        self.stat_metadata(key.clone(), &stat, link, attributes)
    }

    fn stat_metadata(
        &self,
        key: ObjectPath,
        stat: &Stat,
        link: Option<String>,
        attributes: BTreeMap<String, serde_json::Value>,
    ) -> Result<ObjectMetadata> {
        let kind = match FileType::from_raw_mode(stat.st_mode) {
            FileType::RegularFile => ObjectKind::File,
            FileType::Directory => ObjectKind::Directory,
            FileType::Symlink => ObjectKind::Symlink,
            _ => {
                return Err(MscError::new(
                    ErrorKind::Unsupported,
                    "POSIX special files are not storage objects",
                ));
            }
        };
        let mut metadata = ObjectMetadata::file(
            key,
            if kind == ObjectKind::File {
                u64::try_from(stat.st_size)
                    .map_err(|_| MscError::new(ErrorKind::Integrity, "negative POSIX file size"))?
            } else {
                0
            },
        );
        metadata.kind = kind;
        metadata.last_modified = chrono::DateTime::from_timestamp(
            stat.st_mtime,
            u32::try_from(stat.st_mtime_nsec)
                .map_err(|_| MscError::new(ErrorKind::Integrity, "invalid POSIX timestamp"))?,
        );
        metadata.source_version = Some(self.version(stat));
        metadata.symlink_target = link;
        metadata.attributes = attributes;
        metadata
            .provider_fields
            .insert("posix_mode".into(), (stat.st_mode & 0o7777).to_string());
        Ok(metadata)
    }

    fn open_read(
        &self,
        key: &ObjectPath,
        conditions: &GetConditions,
    ) -> Result<(File, SourceVersion)> {
        let (directory, name) = self.parent(key, false)?;
        let file = File::from(
            fs::openat(
                &directory,
                &name,
                OFlags::RDONLY | OFlags::NOFOLLOW | OFlags::NONBLOCK | OFlags::CLOEXEC,
                Mode::empty(),
            )
            .map_err(errno)?,
        );
        let stat = fs::fstat(&file).map_err(errno)?;
        if FileType::from_raw_mode(stat.st_mode) != FileType::RegularFile {
            return Err(MscError::new(
                ErrorKind::InvalidPath,
                "POSIX read requires a regular file",
            ));
        }
        if conditions.if_match.is_some() || conditions.if_none_match.is_some() {
            return Err(MscError::unsupported(
                "POSIX ETag conditions; use source versions",
            ));
        }
        let version = self.version(&stat);
        if conditions
            .version
            .as_ref()
            .is_some_and(|expected| expected != &version)
        {
            return Err(MscError::new(
                ErrorKind::PreconditionFailed,
                "POSIX source generation changed",
            ));
        }
        Ok((file, version))
    }

    async fn get_inner(
        &self,
        key: &ObjectPath,
        conditions: &GetConditions,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<ByteStream> {
        let this = self.clone();
        let key = key.clone();
        let conditions = conditions.clone();
        let (file, version) = blocking(move || this.open_read(&key, &conditions)).await?;
        let state = (
            tokio::fs::File::from_std(file),
            version,
            self.clone(),
            permit,
            false,
        );
        Ok(Box::pin(stream::unfold(
            state,
            |(mut file, version, provider, permit, done)| async move {
                if done {
                    return None;
                }
                let mut buffer = vec![0; BUFFER_SIZE];
                let result = match file.read(&mut buffer).await {
                    Ok(0) => {
                        let std_file = match file.try_clone().await {
                            Ok(file) => file.into_std().await,
                            Err(error) => {
                                return Some((
                                    Err(error.into()),
                                    (file, version, provider, permit, true),
                                ));
                            }
                        };
                        let this = provider.clone();
                        let expected = version.clone();
                        match blocking(move || {
                            let stat = fs::fstat(&std_file).map_err(errno)?;
                            if this.version(&stat) == expected {
                                Ok(())
                            } else {
                                Err(MscError::new(
                                    ErrorKind::PreconditionFailed,
                                    "POSIX file changed during read",
                                ))
                            }
                        })
                        .await
                        {
                            Ok(()) => return None,
                            Err(error) => Err(error),
                        }
                    }
                    Ok(length) => {
                        buffer.truncate(length);
                        Ok(Bytes::from(buffer))
                    }
                    Err(error) => Err(error.into()),
                };
                let done = result.is_err();
                Some((result, (file, version, provider, permit, done)))
            },
        )))
    }

    async fn put_inner(
        &self,
        key: &ObjectPath,
        mut body: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        let this = self.clone();
        let key = key.clone();
        let (file, temporary, name) = blocking(move || this.temporary_write(&key)).await?;
        let mut file = tokio::fs::File::from_std(file);
        let mut written = 0_u64;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            written = written
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| MscError::new(ErrorKind::Integrity, "POSIX upload size overflow"))?;
            if size.is_some_and(|size| written > size) {
                return Err(MscError::new(
                    ErrorKind::Integrity,
                    "POSIX upload exceeds its declared length",
                ));
            }
            file.write_all(&chunk).await?;
        }
        if size.is_some_and(|size| size != written) {
            return Err(MscError::new(
                ErrorKind::Integrity,
                "POSIX upload differs from declared length",
            ));
        }
        file.flush().await?;
        file.sync_all().await?;
        let file = file.into_std().await;
        self.publish(file, temporary, name, options).await
    }

    async fn publish(
        &self,
        file: File,
        temporary: TemporaryEntry,
        name: String,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        let attributes = options.attributes.clone();
        let exclusive = options.if_none_match.as_deref() == Some("*");
        let this = self.clone();
        let publication = Publication(Arc::new(Mutex::new(true)));
        let allowed = Arc::clone(&publication.0);
        blocking(move || {
            write_attributes(&file, &attributes)?;
            let allowed = allowed
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !*allowed {
                return Err(MscError::new(
                    ErrorKind::Cancelled,
                    "POSIX publication cancelled",
                ));
            }
            if exclusive {
                fs::linkat(
                    &*temporary.parent,
                    &temporary.name,
                    &*temporary.parent,
                    &name,
                    AtFlags::empty(),
                )
                .map_err(|error| {
                    if error == rustix::io::Errno::EXIST {
                        MscError::new(
                            ErrorKind::PreconditionFailed,
                            "POSIX destination already exists",
                        )
                    } else {
                        errno(error)
                    }
                })?;
            } else {
                fs::renameat(
                    &*temporary.parent,
                    &temporary.name,
                    &*temporary.parent,
                    &name,
                )
                .map_err(errno)?;
            }
            drop(temporary);
            Ok(PutResult {
                source_version: Some(this.version(&fs::fstat(&file).map_err(errno)?)),
                ..PutResult::default()
            })
        })
        .await
    }

    /// Apply preserved source mtime and permissions through an anchored descriptor.
    ///
    /// # Errors
    /// Returns unsupported kinds or filesystem errors before changing attributes.
    pub async fn preserve_metadata(
        &self,
        key: &ObjectPath,
        source: &ObjectMetadata,
        expected: Option<&SourceVersion>,
    ) -> Result<Option<ObjectMetadata>> {
        if !self.options.preserve_permissions && !self.options.preserve_mtime {
            return Ok(None);
        }
        let expected = expected.cloned();
        let this = self.clone();
        let key = key.clone();
        let source = source.clone();
        blocking(move || {
            let (file, _) = this.open_read(
                &key,
                &GetConditions {
                    version: expected,
                    ..GetConditions::default()
                },
            )?;
            if this.options.preserve_permissions
                && let Some(mode) = source
                    .provider_fields
                    .get("posix_mode")
                    .and_then(|mode| mode.parse::<u32>().ok())
            {
                file.set_permissions(std::fs::Permissions::from_mode(mode & 0o7777))?;
            }
            if this.options.preserve_mtime
                && let Some(time) = source.last_modified
            {
                filetime::set_file_handle_times(
                    &file,
                    None,
                    Some(filetime::FileTime::from_unix_time(
                        time.timestamp(),
                        time.timestamp_subsec_nanos(),
                    )),
                )?;
            }
            let stat = fs::fstat(&file).map_err(errno)?;
            let (parent, name) = this.parent(&key, false)?;
            let path_stat = fs::statat(&parent, &name, AtFlags::SYMLINK_NOFOLLOW).map_err(errno)?;
            if this.version(&stat) != this.version(&path_stat) {
                return Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "POSIX destination changed during metadata preservation",
                ));
            }
            this.stat_metadata(key, &stat, None, read_attributes(&file)?)
                .map(Some)
        })
        .await
    }
}

#[async_trait]
impl StorageProvider for PosixProvider {
    fn name(&self) -> &'static str {
        "file"
    }
    fn backend_fingerprint(&self) -> String {
        "file".into()
    }
    fn physical_root(&self) -> ObjectPath {
        ObjectPath::new(self.base.to_string_lossy().trim_start_matches('/'))
            .expect("absolute canonical POSIX base")
    }
    fn is_posix(&self) -> bool {
        true
    }
    fn capabilities(&self) -> ProviderCapabilities {
        ProviderCapabilities {
            ranged_reads: true,
            streaming_reads: true,
            streaming_writes: true,
            server_side_copy: CopyScope::SameProvider,
            conditional_get: true,
            conditional_put: true,
            native_lexical_listing: true,
            symlinks: true,
            user_metadata: cfg!(any(target_os = "linux", target_os = "macos")),
            stable_source_versions: true,
            ..ProviderCapabilities::default()
        }
    }
    async fn preserve_metadata(
        &self,
        key: &ObjectPath,
        source: &ObjectMetadata,
        expected: Option<&SourceVersion>,
    ) -> Result<Option<ObjectMetadata>> {
        Self::preserve_metadata(self, key, source, expected).await
    }
    async fn head(&self, key: &ObjectPath) -> Result<ObjectMetadata> {
        let this = self.clone();
        let key = key.clone();
        blocking(move || this.metadata(&key)).await
    }
    async fn open_posix_file(
        &self,
        key: &ObjectPath,
        conditions: &GetConditions,
    ) -> Result<tokio::fs::File> {
        let this = self.clone();
        let key = key.clone();
        let conditions = conditions.clone();
        let (file, _) = blocking(move || this.open_read(&key, &conditions)).await?;
        Ok(tokio::fs::File::from_std(file))
    }
    async fn get(&self, key: &ObjectPath, conditions: &GetConditions) -> Result<ByteStream> {
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| MscError::new(ErrorKind::Cancelled, "POSIX provider closed"))?;
        self.get_inner(key, conditions, Some(permit)).await
    }
    async fn get_range(
        &self,
        key: &ObjectPath,
        range: ByteRange,
        conditions: &GetConditions,
    ) -> Result<Bytes> {
        range.end()?;
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| MscError::new(ErrorKind::Cancelled, "POSIX provider closed"))?;
        let this = self.clone();
        let key = key.clone();
        let conditions = conditions.clone();
        let (file, version) = blocking(move || this.open_read(&key, &conditions)).await?;
        let mut file = tokio::fs::File::from_std(file);
        let length = range
            .length
            .min(file.metadata().await?.len().saturating_sub(range.offset));
        let length = usize::try_from(length).map_err(|_| {
            MscError::new(ErrorKind::Unsupported, "POSIX range does not fit memory")
        })?;
        file.seek(std::io::SeekFrom::Start(range.offset)).await?;
        let mut output = vec![0; length];
        file.read_exact(&mut output).await?;
        let file = file.into_std().await;
        let this = self.clone();
        blocking(move || {
            if this.version(&fs::fstat(&file).map_err(errno)?) == version {
                Ok(())
            } else {
                Err(MscError::new(
                    ErrorKind::PreconditionFailed,
                    "POSIX file changed during range read",
                ))
            }
        })
        .await?;
        Ok(Bytes::from(output))
    }
    async fn put(
        &self,
        key: &ObjectPath,
        body: ByteStream,
        size: Option<u64>,
        options: &WriteOptions,
    ) -> Result<PutResult> {
        validate_write_options(options)?;
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| MscError::new(ErrorKind::Cancelled, "POSIX provider closed"))?;
        self.put_inner(key, body, size, options).await
    }
    async fn copy(
        &self,
        source: &ObjectPath,
        destination: &ObjectPath,
        options: &CopyOptions,
    ) -> Result<PutResult> {
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|_| MscError::new(ErrorKind::Cancelled, "POSIX provider closed"))?;
        let metadata = self.head(source).await?;
        let mut destination_options = options.destination.clone();
        if destination_options.attributes.is_empty() {
            destination_options.attributes = metadata.attributes;
        }
        let mut conditions = options.source_conditions.clone();
        if conditions.version.is_none() {
            conditions.version = metadata.source_version;
        }
        validate_write_options(&destination_options)?;
        let body = self.get_inner(source, &conditions, None).await?;
        self.put_inner(
            destination,
            body,
            Some(metadata.content_length),
            &destination_options,
        )
        .await
    }
    async fn delete(&self, key: &ObjectPath, conditions: &DeleteOptions) -> Result<()> {
        if conditions.version.is_some()
            || conditions.if_match.is_some()
            || conditions.if_none_match.is_some()
        {
            return Err(MscError::unsupported("POSIX conditional delete"));
        }
        let this = self.clone();
        let key = key.clone();
        blocking(move || {
            let (parent, name) = match this.parent(&key, false) {
                Ok(parent) => parent,
                Err(error) if error.kind == ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            let stat = match fs::statat(&parent, &name, AtFlags::SYMLINK_NOFOLLOW) {
                Ok(stat) => stat,
                Err(error) if error == rustix::io::Errno::NOENT => return Ok(()),
                Err(error) => return Err(errno(error)),
            };
            let flags = if FileType::from_raw_mode(stat.st_mode) == FileType::Directory {
                AtFlags::REMOVEDIR
            } else {
                AtFlags::empty()
            };
            match fs::unlinkat(&parent, &name, flags) {
                Ok(()) => Ok(()),
                Err(error) if error == rustix::io::Errno::NOENT => Ok(()),
                Err(error) => Err(errno(error)),
            }
        })
        .await
    }
    async fn list(&self, options: &ListOptions) -> Result<MetadataStream> {
        listing::open(self.clone(), options.clone())
    }
    async fn make_symlink(&self, key: &ObjectPath, target: &str) -> Result<()> {
        let this = self.clone();
        let key = key.clone();
        let target = target.to_owned();
        blocking(move || {
            check_namespace(&key)?;
            let (parent_key, name) = key.as_str().rsplit_once('/').unwrap_or(("", key.as_str()));
            let name = name.to_owned();
            let (parent, physical_parent) =
                this.resolved_directory(&ObjectPath::new(parent_key)?, true)?;
            this.resolve_target(&physical_parent.join(&ObjectPath::new(&name)?)?, &target)?;
            let ((), temporary) = TemporaryEntry::create(
                Arc::new(parent),
                format!(".msc-tmp-{}", uuid::Uuid::new_v4()),
                |parent, name| fs::symlinkat(&target, parent, name).map_err(errno),
            )?;
            fs::renameat(
                &*temporary.parent,
                &temporary.name,
                &*temporary.parent,
                &name,
            )
            .map_err(errno)
        })
        .await
    }
}

struct TemporaryEntry {
    parent: Arc<File>,
    name: String,
}
impl TemporaryEntry {
    fn create<T>(
        parent: Arc<File>,
        name: String,
        create: impl FnOnce(&File, &str) -> Result<T>,
    ) -> Result<(T, Self)> {
        let result = create(&parent, &name)?;
        Ok((result, Self { parent, name }))
    }
}
impl Drop for TemporaryEntry {
    fn drop(&mut self) {
        let _ = fs::unlinkat(&*self.parent, &self.name, AtFlags::empty());
    }
}

struct Publication(Arc<Mutex<bool>>);
impl Drop for Publication {
    fn drop(&mut self) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = false;
    }
}

fn check_namespace(key: &ObjectPath) -> Result<()> {
    if key
        .as_str()
        .split('/')
        .any(|part| part.starts_with(".msc-tmp-"))
    {
        return Err(MscError::new(
            ErrorKind::InvalidPath,
            "POSIX staging namespace is reserved",
        ));
    }
    Ok(())
}

fn validate_write_options(options: &WriteOptions) -> Result<()> {
    if options.version.is_some()
        || options.if_match.is_some()
        || options
            .if_none_match
            .as_deref()
            .is_some_and(|condition| condition != "*")
    {
        return Err(MscError::unsupported("POSIX conditional replacement"));
    }
    Ok(())
}

async fn blocking<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|_| MscError::new(ErrorKind::Cancelled, "POSIX filesystem task failed"))?
}

fn errno(error: rustix::io::Errno) -> MscError {
    MscError::from(std::io::Error::from(error))
}

fn relative_target(key: &ObjectPath, target: &ObjectPath) -> String {
    let mut source: Vec<_> = key.as_str().split('/').collect();
    source.pop();
    let target: Vec<_> = target
        .as_str()
        .split('/')
        .filter(|part| !part.is_empty())
        .collect();
    let common = source
        .iter()
        .zip(&target)
        .take_while(|(left, right)| left == right)
        .count();
    std::iter::repeat_n("..", source.len() - common)
        .chain(target[common..].iter().copied())
        .collect::<Vec<_>>()
        .join("/")
}

fn read_attributes(file: &File) -> Result<BTreeMap<String, serde_json::Value>> {
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        match file.get_xattr(ATTR_NAME) {
            Ok(Some(bytes)) => serde_json::from_slice(&bytes).map_err(|_| {
                MscError::new(ErrorKind::Integrity, "invalid POSIX user.json attributes")
            }),
            Ok(None) => Ok(BTreeMap::new()),
            Err(error) if error.kind() == std::io::ErrorKind::Unsupported => Ok(BTreeMap::new()),
            Err(error) => Err(error.into()),
        }
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = file;
        Ok(BTreeMap::new())
    }
}

fn write_attributes(file: &File, attributes: &BTreeMap<String, serde_json::Value>) -> Result<()> {
    if attributes.is_empty() {
        return Ok(());
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        let bytes = serde_json::to_vec(attributes)
            .map_err(|_| MscError::new(ErrorKind::InvalidConfig, "invalid POSIX attributes"))?;
        file.set_xattr(ATTR_NAME, &bytes).map_err(MscError::from)
    }
    #[cfg(not(any(target_os = "linux", target_os = "macos")))]
    {
        let _ = file;
        Err(MscError::unsupported(
            "POSIX user attributes on this platform",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cancelled_queued_publication_never_replaces_the_destination() {
        use std::io::Write;
        let directory = tempfile::tempdir().unwrap();
        std::fs::write(directory.path().join("target"), b"old").unwrap();
        let provider = PosixProvider::new(PosixOptions::new(directory.path())).unwrap();
        let (mut file, temporary, name) = provider
            .temporary_write(&ObjectPath::new("target").unwrap())
            .unwrap();
        file.write_all(b"new").unwrap();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let (started, ready) = tokio::sync::oneshot::channel();
            let (release, receiver) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                receiver.recv().unwrap();
            });
            ready.await.unwrap();
            let options = WriteOptions::default();
            let mut publication = Box::pin(provider.publish(file, temporary, name, &options));
            assert!(futures::poll!(publication.as_mut()).is_pending());
            drop(publication);
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::task::spawn_blocking(|| ()).await.unwrap();
        });
        assert_eq!(
            std::fs::read(directory.path().join("target")).unwrap(),
            b"old"
        );
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }
}
