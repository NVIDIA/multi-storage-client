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

use crate::StorageError;
use object_store::{path::Path, GetOptions, ObjectMeta, ObjectStore, ObjectStoreExt};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions, TryLockError};
use std::io::Write;
use std::os::unix::fs::{FileExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path as LocalPath, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::task::JoinSet;

const STATE_FORMAT: &str = "msc-download-v2";
const LEGACY_STATE_FORMAT: &str = "msc-download-v1";
const STATE_SUFFIX: &str = ".msc-dl";
const TEMP_FILE_RANDOM_LENGTH: usize = 6;
const DATA_FILE: &str = "download.part";

/// Resume metadata for retries against the same storage backend and bucket.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct DownloadState {
    format: String,
    key: String,
    size: u64,
    etag: Option<String>,
    version: Option<String>,
    chunk_size: u64,
    #[serde(default)]
    completed: BTreeSet<u64>,
}

impl DownloadState {
    fn same_object(&self, other: &Self) -> bool {
        self.key == other.key
            && self.size == other.size
            && self.etag == other.etag
            && self.version == other.version
    }
}

struct DownloadFiles {
    dir: PathBuf,
    // Keep range writes and writeback error reporting on the same open file description.
    data: File,
    _lock: Option<same_file::Handle>,
    temporary_dir: Option<tempfile::TempDir>,
}

impl DownloadFiles {
    fn persistent(&self) -> bool {
        self.temporary_dir.is_none()
    }

    fn write_range(&self, data: &[u8], offset: u64) -> std::io::Result<()> {
        self.data.write_all_at(data, offset)
    }
}

fn invalid_state(message: impl Into<String>) -> StorageError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn create_private_temp(
    dir: &LocalPath,
    prefix: &str,
) -> std::io::Result<(File, tempfile::TempPath)> {
    let (file, path) = tempfile::Builder::new()
        .prefix(prefix)
        .rand_bytes(TEMP_FILE_RANDOM_LENGTH)
        .suffix(".tmp")
        .tempfile_in(dir)?
        .into_parts();
    Ok((file, path))
}

fn write_private_atomic(dir: &LocalPath, name: &str, data: &[u8]) -> std::io::Result<()> {
    let (mut file, path) = create_private_temp(dir, &format!("{name}."))?;
    file.write_all(data)?;
    file.flush()?;
    drop(file);
    path.persist(dir.join(name)).map_err(|error| error.error)?;
    Ok(())
}

fn persist_state(dir: &LocalPath, state: &DownloadState) -> Result<(), StorageError> {
    let encoded = serde_json::to_vec(state).map_err(|error| invalid_state(error.to_string()))?;
    write_private_atomic(dir, "state.json", &encoded)?;
    Ok(())
}

async fn clear_download_files(dir: &LocalPath, completed_chunks: bool) -> std::io::Result<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let filename = entry.file_name();
        let Some(name) = filename.to_str() else {
            continue;
        };
        // Chunk and "merged" names are msc-download-v1 artifacts; remove with LEGACY_STATE_FORMAT.
        let chunk = |name: &str| {
            name.strip_suffix(".chunk").is_some_and(|index| {
                index.len() >= 6 && index.bytes().all(|byte| byte.is_ascii_digit())
            })
        };
        let temporary = name
            .strip_suffix(".tmp")
            .and_then(|name| name.rsplit_once('.'))
            .is_some_and(|(prefix, id)| {
                (prefix == "merged" || prefix == "state.json" || prefix == "data" || chunk(prefix))
                    && id.len() == TEMP_FILE_RANDOM_LENGTH
                    && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
            });
        if (temporary || completed_chunks && (chunk(name) || name == DATA_FILE))
            && entry.file_type().await?.is_file()
        {
            fs::remove_file(entry.path()).await?;
        }
    }
    Ok(())
}

async fn remove_state(dir: &LocalPath) -> std::io::Result<()> {
    let marker = dir.join("state.json");
    let encoded = fs::read(&marker).await?;
    clear_download_files(dir, true).await?;
    fs::remove_file(&marker).await?;
    let cleanup: std::io::Result<()> = async {
        fs::remove_file(dir.join("lock")).await?;
        fs::remove_dir(dir).await
    }
    .await;
    if let Err(error) = cleanup {
        let _ = write_private_atomic(dir, "state.json", &encoded);
        return Err(error);
    }
    Ok(())
}

/// Missing markers require reinitialization under the directory lock.
async fn read_state(dir: &LocalPath) -> Result<Option<DownloadState>, StorageError> {
    let metadata = match fs::symlink_metadata(dir).await {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let state_path = dir.join("state.json");
    if !metadata.is_dir() {
        return Err(invalid_state(format!(
            "Download state directory conflicts with an existing path: {}",
            dir.display()
        )));
    }
    #[cfg(unix)]
    if metadata.uid() != unsafe { libc::geteuid() } || metadata.mode() & 0o077 != 0 {
        return Err(invalid_state(
            "Download state directory must be owned by the current user and private",
        ));
    }
    match fs::symlink_metadata(&state_path).await {
        Ok(metadata) if metadata.is_file() => {}
        Ok(_) => return Err(invalid_state("Download state marker is not a regular file")),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let encoded = match fs::read(&state_path).await {
        Ok(encoded) => encoded,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let saved: DownloadState = serde_json::from_slice(&encoded)
        .map_err(|e| invalid_state(format!("Invalid download state at {}: {e}", dir.display())))?;
    if !matches!(saved.format.as_str(), STATE_FORMAT | LEGACY_STATE_FORMAT)
        || saved.chunk_size == 0
        || saved
            .completed
            .iter()
            .any(|&index| index >= saved.size.div_ceil(saved.chunk_size))
    {
        return Err(invalid_state(format!(
            "Unrecognized download state at {}",
            dir.display()
        )));
    }
    Ok(Some(saved))
}

async fn prepare_data(
    dir: &LocalPath,
    persistent: bool,
    state: &mut DownloadState,
) -> Result<File, StorageError> {
    let path = dir.join(DATA_FILE);
    match fs::symlink_metadata(&path).await {
        Ok(metadata) if !metadata.is_file() => {
            return Err(invalid_state("Download staging data is not a regular file"));
        }
        Ok(metadata) if metadata.len() == state.size => {
            let mut options = OpenOptions::new();
            options.read(true).write(true);
            #[cfg(unix)]
            options.custom_flags(libc::O_NOFOLLOW);
            let file = options.open(&path)?;
            let opened = file.metadata()?;
            if !opened.is_file() || opened.len() != state.size {
                return Err(invalid_state(
                    "Download staging data changed during preparation",
                ));
            }
            return Ok(file);
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    // Invalidate progress before replacing missing or truncated data, including on cancellation.
    state.completed.clear();
    if persistent {
        persist_state(dir, state)?;
    }
    let (file, temporary) = create_private_temp(dir, "data.")?;
    file.set_len(state.size)?;
    temporary.persist(path).map_err(|error| error.error)?;
    Ok(file)
}

fn try_lock_state(dir: &LocalPath) -> Result<Option<same_file::Handle>, StorageError> {
    let path = dir.join("lock");
    match std::fs::symlink_metadata(&path) {
        Ok(metadata) if !metadata.is_file() => {
            return Err(invalid_state("Download lock is not a regular file"));
        }
        Ok(_) => {}
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }
    let mut options = OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let file = match options.open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(None),
        Err(TryLockError::Error(error)) => return Err(error.into()),
    }
    let lock = same_file::Handle::from_file(file)?;
    // Cleanup can unlink the lock between opening it and acquiring it.
    match same_file::Handle::from_path(&path) {
        Ok(current) if current == lock => Ok(Some(lock)),
        Ok(_) => Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn prepare_state(
    dir: &LocalPath,
    state: DownloadState,
) -> Result<(DownloadState, Option<same_file::Handle>), StorageError> {
    let mut builder = fs::DirBuilder::new();
    #[cfg(unix)]
    builder.mode(0o700);
    let created = match builder.create(dir).await {
        Ok(()) => true,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            read_state(dir).await?;
            false
        }
        Err(e) => return Err(e.into()),
    };
    let result: Result<_, StorageError> = async {
        let Some(lock) = try_lock_state(dir)? else {
            return Ok((state, None));
        };
        if let Some(saved) = read_state(dir).await? {
            if saved.format == STATE_FORMAT && saved.same_object(&state) {
                clear_download_files(dir, false).await?;
                return Ok((saved, Some(lock)));
            }
        }
        // Discard untracked chunks before publishing a marker that makes them reusable.
        clear_download_files(dir, true).await?;
        persist_state(dir, &state)?;
        Ok((state, Some(lock)))
    }
    .await;
    if let Err(error) = result {
        if created {
            let _ = fs::remove_file(dir.join("lock")).await;
            let _ = fs::remove_dir(dir).await;
        }
        return Err(error);
    }
    result
}

async fn download_chunk(
    store: Arc<dyn ObjectStore>,
    remote_path: Path,
    files: Arc<DownloadFiles>,
    index: u64,
    state: Arc<DownloadState>,
) -> Result<u64, StorageError> {
    let start = index * state.chunk_size;
    let end = start.saturating_add(state.chunk_size).min(state.size);
    let options = GetOptions::default()
        .with_range(Some(start..end))
        .with_if_match(state.etag.clone())
        .with_version(state.version.clone());
    let result = store
        .get_opts(&remote_path, options)
        .await
        .map_err(StorageError::from)?;
    if result.range != (start..end)
        || result.meta.size != state.size
        || state
            .etag
            .as_ref()
            .is_some_and(|etag| result.meta.e_tag.as_ref() != Some(etag))
        || state
            .version
            .as_ref()
            .is_some_and(|version| result.meta.version.as_ref() != Some(version))
    {
        return Err(invalid_state(
            "Remote object changed or returned an unexpected range",
        ));
    }
    let data = result.bytes().await.map_err(StorageError::from)?;
    if data.len() as u64 != end - start {
        return Err(invalid_state("Downloaded chunk has an unexpected length"));
    }
    tokio::task::spawn_blocking(move || {
        // Cancellation cannot stop a blocking write; retain its lock and transient directory.
        files.write_range(&data, start)?;
        drop(files);
        Ok(index)
    })
    .await
    .map_err(|error| invalid_state(format!("Download write task failed: {error}")))?
}

async fn finish_chunk(
    tasks: &mut JoinSet<Result<u64, StorageError>>,
    files: &DownloadFiles,
    state: &mut DownloadState,
) -> Result<(), StorageError> {
    match tasks.join_next().await {
        Some(Ok(result)) => {
            state.completed.insert(result?);
            if files.persistent() {
                persist_state(&files.dir, state)?;
            }
            Ok(())
        }
        Some(Err(e)) => Err(invalid_state(format!("Download task failed: {e}"))),
        None => Ok(()),
    }
}

fn sync_and_checkpoint(
    files: &DownloadFiles,
    state: &DownloadState,
    sync: impl FnOnce(&File) -> std::io::Result<()>,
) -> Result<(), StorageError> {
    if files.persistent() {
        // Failed fsync may discard dirty pages; invalidate progress before the non-cancellable sync.
        let mut untrusted = state.clone();
        untrusted.completed.clear();
        persist_state(&files.dir, &untrusted)?;
    }
    let path = files.dir.join(DATA_FILE);
    if !std::fs::symlink_metadata(&path)?.is_file()
        || same_file::Handle::from_file(files.data.try_clone()?)?
            != same_file::Handle::from_path(&path)?
    {
        return Err(invalid_state("Download staging data was replaced"));
    }
    sync(&files.data)?;
    if files.persistent() {
        persist_state(&files.dir, state)?;
    }
    Ok(())
}

async fn sync_data(
    files: Arc<DownloadFiles>,
    state: Arc<DownloadState>,
) -> Result<(), StorageError> {
    tokio::task::spawn_blocking(move || sync_and_checkpoint(&files, &state, File::sync_all))
        .await
        .map_err(|error| invalid_state(format!("Download sync task failed: {error}")))?
}

pub(crate) async fn download(
    store: Arc<dyn ObjectStore>,
    remote_path: Path,
    local_path: String,
    chunk_size: usize,
    concurrency: usize,
) -> Result<u64, StorageError> {
    if chunk_size == 0 || concurrency == 0 {
        return Err(StorageError::ConfigError(
            "Multipart chunk size and concurrency must be positive".to_string(),
        ));
    }
    let ObjectMeta {
        size,
        e_tag,
        version,
        ..
    } = store.head(&remote_path).await.map_err(StorageError::from)?;
    let etag = e_tag.filter(|etag| !etag.is_empty() && !etag.starts_with("W/"));
    let version = version.filter(|version| !version.is_empty() && version != "null");
    let target = LocalPath::new(&local_path);
    let parent = target
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or(LocalPath::new("."));
    let mut state = DownloadState {
        format: STATE_FORMAT.to_string(),
        key: remote_path.to_string(),
        size,
        etag,
        version,
        chunk_size: chunk_size as u64,
        completed: BTreeSet::new(),
    };
    let (persistent_dir, state_lock) = if state.etag.is_some() || state.version.is_some() {
        let mut filename = std::ffi::OsString::from(".");
        filename.push(
            target
                .file_name()
                .ok_or_else(|| invalid_state("Download destination requires a file name"))?,
        );
        filename.push(STATE_SUFFIX);
        let dir = parent.join(filename);
        let (prepared, lock) = prepare_state(&dir, state).await?;
        state = prepared;
        (lock.as_ref().map(|_| dir), lock)
    } else {
        (None, None)
    };
    let (dir, temporary_dir) = match persistent_dir {
        Some(dir) => (dir, None),
        None => {
            let mut builder = tempfile::Builder::new();
            builder.prefix(".msc-dl-").suffix(STATE_SUFFIX);
            #[cfg(unix)]
            builder.permissions(std::fs::Permissions::from_mode(0o700));
            let temp = builder.tempdir_in(parent)?;
            (temp.path().to_path_buf(), Some(temp))
        }
    };
    let data = prepare_data(&dir, temporary_dir.is_none(), &mut state).await?;
    let files = Arc::new(DownloadFiles {
        dir,
        data,
        _lock: state_lock,
        temporary_dir,
    });
    let revision = Arc::new(state.clone());
    let count = state.size.div_ceil(state.chunk_size);
    let mut tasks = JoinSet::new();
    let downloads: Result<(), StorageError> = async {
        for index in 0..count {
            if state.completed.contains(&index) {
                continue;
            }
            if tasks.len() >= concurrency {
                finish_chunk(&mut tasks, &files, &mut state).await?;
            }
            tasks.spawn(download_chunk(
                Arc::clone(&store),
                remote_path.clone(),
                Arc::clone(&files),
                index,
                Arc::clone(&revision),
            ));
        }
        while !tasks.is_empty() {
            finish_chunk(&mut tasks, &files, &mut state).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = downloads {
        tasks.shutdown().await;
        return Err(error);
    }
    let size = state.size;
    sync_data(Arc::clone(&files), Arc::new(state)).await?;
    fs::rename(files.dir.join(DATA_FILE), target).await?;
    let cleanup = if files.temporary_dir.is_some() {
        fs::remove_dir_all(&files.dir).await
    } else {
        remove_state(&files.dir).await
    };
    if let Err(error) = cleanup {
        eprintln!(
            "Downloaded file to {}, but could not remove download state at {}: {error}",
            target.display(),
            files.dir.display()
        );
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn failed_writeback_invalidates_checkpoints_before_retry() {
        let parent = tempfile::tempdir().unwrap();
        let target = parent.path().join("download");
        std::fs::write(&target, b"existing destination").unwrap();
        let dir = parent.path().join(".download.msc-dl");
        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        let remote_path = Path::from("key");
        let body = b"abcdefghijk";
        store
            .put(&remote_path, bytes::Bytes::from_static(body).into())
            .await
            .unwrap();
        let meta = store.head(&remote_path).await.unwrap();
        let state = DownloadState {
            format: STATE_FORMAT.to_string(),
            key: "key".to_string(),
            size: body.len() as u64,
            etag: meta.e_tag,
            version: meta.version,
            chunk_size: 4,
            completed: BTreeSet::new(),
        };
        let (mut state, lock) = prepare_state(&dir, state).await.unwrap();
        let data = prepare_data(&dir, true, &mut state).await.unwrap();
        let files = Arc::new(DownloadFiles {
            dir: dir.clone(),
            data,
            _lock: lock,
            temporary_dir: None,
        });
        files.write_range(body, 0).unwrap();
        state.completed.extend([0, 1, 2]);
        persist_state(&dir, &state).unwrap();

        let result = sync_and_checkpoint(&files, &state, |file| {
            file.set_len(0)?;
            file.set_len(body.len() as u64)?;
            Err(std::io::Error::from_raw_os_error(libc::EIO))
        });
        assert!(result.is_err());
        assert!(read_state(&dir)
            .await
            .unwrap()
            .unwrap()
            .completed
            .is_empty());
        assert_eq!(files.data.metadata().unwrap().len(), body.len() as u64);
        assert_ne!(std::fs::read(dir.join(DATA_FILE)).unwrap(), body);
        assert_eq!(std::fs::read(&target).unwrap(), b"existing destination");
        drop(files);

        let size = download(
            store,
            remote_path,
            target.to_string_lossy().into_owned(),
            4,
            2,
        )
        .await
        .unwrap();
        assert_eq!(size, body.len() as u64);
        assert_eq!(std::fs::read(&target).unwrap(), body);
        assert!(!dir.exists());
    }

    #[test]
    fn cancelled_file_operations_keep_staging_resources_until_task_finishes() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
            let remote_path = Path::from("key");
            store
                .put(&remote_path, bytes::Bytes::from_static(b"data").into())
                .await
                .unwrap();
            let metadata = store.head(&remote_path).await.unwrap();
            let revision = Arc::new(DownloadState {
                format: STATE_FORMAT.to_string(),
                key: "key".to_string(),
                size: 4,
                etag: metadata.e_tag,
                version: metadata.version,
                chunk_size: 4,
                completed: BTreeSet::new(),
            });
            for (disposable, write) in [(false, false), (false, true), (true, false), (true, true)]
            {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().to_path_buf();
                std::fs::write(path.join(DATA_FILE), b"old!").unwrap();
                let lock = try_lock_state(&path).unwrap().unwrap();
                let (temporary_dir, _persistent_dir) = if disposable {
                    (Some(dir), None)
                } else {
                    (None, Some(dir))
                };
                let (release, blocked) = std::sync::mpsc::channel();
                let blocker = tokio::task::spawn_blocking(move || blocked.recv().unwrap());
                let files = Arc::new(DownloadFiles {
                    data: OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(path.join(DATA_FILE))
                        .unwrap(),
                    dir: path.clone(),
                    _lock: Some(lock),
                    temporary_dir,
                });
                let mut operation = Box::pin(async {
                    if write {
                        download_chunk(
                            Arc::clone(&store),
                            remote_path.clone(),
                            files,
                            0,
                            Arc::clone(&revision),
                        )
                        .await
                        .map(|_| ())
                    } else {
                        sync_data(files, Arc::clone(&revision)).await
                    }
                });
                assert!(operation
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()))
                    .is_pending());
                drop(operation);
                assert!(path.is_dir());
                assert!(try_lock_state(&path).unwrap().is_none());
                release.send(()).unwrap();
                blocker.await.unwrap();
                tokio::time::timeout(std::time::Duration::from_secs(5), async {
                    loop {
                        if disposable {
                            if !path.exists() {
                                break;
                            }
                        } else if let Some(_lock) = try_lock_state(&path).unwrap() {
                            break;
                        }
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                if !disposable {
                    assert_eq!(
                        std::fs::read(path.join(DATA_FILE)).unwrap(),
                        if write { b"data" } else { b"old!" }
                    );
                }
            }
        });
    }

    #[test]
    fn directory_creator_rechecks_state_after_another_download() {
        use std::future::Future;
        use std::task::{Context, Waker};

        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let parent = tempfile::tempdir().unwrap();
            let dir = parent.path().join(".model.bin.msc-dl");
            let state = |key: &str| DownloadState {
                format: STATE_FORMAT.to_string(),
                key: key.to_string(),
                size: 8,
                etag: Some(key.to_string()),
                version: None,
                chunk_size: 4,
                completed: BTreeSet::new(),
            };
            let (release, blocked) = std::sync::mpsc::channel();
            let blocker = tokio::task::spawn_blocking(move || blocked.recv().unwrap());
            let mut first = Box::pin(prepare_state(&dir, state("model-v1.bin")));
            // Pause A on mkdir's result, even after the filesystem operation finishes.
            assert!(first
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending());
            release.send(()).unwrap();
            blocker.await.unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while !dir.is_dir() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();

            let (_, second_lock) = prepare_state(&dir, state("model-v2.bin")).await.unwrap();
            let chunk = dir.join("000000.chunk");
            fs::write(&chunk, b"v2!!").await.unwrap();
            drop(second_lock);

            let (prepared, first_lock) = first.await.unwrap();
            assert!(first_lock.is_some());
            assert_eq!(prepared.key, "model-v1.bin");
            assert_eq!(read_state(&dir).await.unwrap().unwrap().key, prepared.key);
            assert!(!chunk.exists(), "A must discard B's same-length chunk");
        });
    }

    #[tokio::test]
    async fn cleanup_failure_restores_valid_marker() {
        let dir = tempfile::tempdir().unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(dir.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let encoded = serde_json::to_vec(&serde_json::json!({
            "format": STATE_FORMAT,
            "key": "key",
            "size": 4,
            "etag": "revision-1",
            "version": null,
            "chunk_size": 4
        }))
        .unwrap();
        fs::write(dir.path().join("state.json"), &encoded)
            .await
            .unwrap();
        fs::write(dir.path().join("000000.chunk"), b"data")
            .await
            .unwrap();
        fs::create_dir(dir.path().join("lock")).await.unwrap();

        assert!(remove_state(dir.path()).await.is_err());
        assert_eq!(
            fs::read(dir.path().join("state.json")).await.unwrap(),
            encoded
        );
        assert!(read_state(dir.path()).await.unwrap().is_some());
        assert!(!dir.path().join("000000.chunk").exists());
    }
}
