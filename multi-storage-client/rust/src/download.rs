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
use std::fs::{OpenOptions, TryLockError};
use std::path::{Path as LocalPath, PathBuf};
use std::sync::Arc;
use tokio::fs;
use tokio::io::AsyncWriteExt;
use tokio::task::JoinSet;

const STATE_FORMAT: &str = "msc-download-v1";
const STATE_SUFFIX: &str = ".msc-dl";
const TEMP_FILE_RANDOM_LENGTH: usize = 6;

/// Resume metadata for retries against the same storage backend and bucket.
#[derive(Debug, Deserialize, Serialize)]
struct DownloadState {
    format: String,
    key: String,
    size: u64,
    etag: Option<String>,
    version: Option<String>,
    chunk_size: u64,
}

impl DownloadState {
    fn same_object(&self, other: &Self) -> bool {
        self.key == other.key
            && self.size == other.size
            && self.etag == other.etag
            && self.version == other.version
    }
}

fn invalid_state(message: impl Into<String>) -> StorageError {
    std::io::Error::new(std::io::ErrorKind::InvalidData, message.into()).into()
}

fn create_private_temp(
    dir: &LocalPath,
    prefix: &str,
) -> std::io::Result<(fs::File, tempfile::TempPath)> {
    let (file, path) = tempfile::Builder::new()
        .prefix(prefix)
        .rand_bytes(TEMP_FILE_RANDOM_LENGTH)
        .suffix(".tmp")
        .tempfile_in(dir)?
        .into_parts();
    Ok((fs::File::from_std(file), path))
}

async fn write_private_atomic(dir: &LocalPath, name: &str, data: &[u8]) -> std::io::Result<()> {
    let (mut file, path) = create_private_temp(dir, &format!("{name}."))?;
    file.write_all(data).await?;
    file.flush().await?;
    drop(file);
    path.persist(dir.join(name)).map_err(|error| error.error)?;
    Ok(())
}

async fn clear_download_files(dir: &LocalPath, completed_chunks: bool) -> std::io::Result<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let filename = entry.file_name();
        let Some(name) = filename.to_str() else {
            continue;
        };
        let chunk = |name: &str| {
            name.strip_suffix(".chunk").is_some_and(|index| {
                index.len() >= 6 && index.bytes().all(|byte| byte.is_ascii_digit())
            })
        };
        let temporary = name
            .strip_suffix(".tmp")
            .and_then(|name| name.rsplit_once('.'))
            .is_some_and(|(prefix, id)| {
                (prefix == "merged" || prefix == "state.json" || chunk(prefix))
                    && id.len() == TEMP_FILE_RANDOM_LENGTH
                    && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
            });
        if (temporary || completed_chunks && chunk(name)) && entry.file_type().await?.is_file() {
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
        let _ = write_private_atomic(dir, "state.json", &encoded).await;
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
    if saved.format != STATE_FORMAT || saved.chunk_size == 0 {
        return Err(invalid_state(format!(
            "Unrecognized download state at {}",
            dir.display()
        )));
    }
    Ok(Some(saved))
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
            if saved.same_object(&state) {
                clear_download_files(dir, false).await?;
                return Ok((saved, Some(lock)));
            }
        }
        // Discard untracked chunks before publishing a marker that makes them reusable.
        clear_download_files(dir, true).await?;
        let encoded = serde_json::to_vec(&state).map_err(|e| invalid_state(e.to_string()))?;
        write_private_atomic(dir, "state.json", &encoded).await?;
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
    dir: PathBuf,
    index: u64,
    state: Arc<DownloadState>,
    _state_lock: Option<Arc<same_file::Handle>>,
) -> Result<(), StorageError> {
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
    write_private_atomic(&dir, &format!("{index:06}.chunk"), &data).await?;
    Ok(())
}

async fn finish_chunk(tasks: &mut JoinSet<Result<(), StorageError>>) -> Result<(), StorageError> {
    match tasks.join_next().await {
        Some(Ok(result)) => result,
        Some(Err(e)) => Err(invalid_state(format!("Download task failed: {e}"))),
        None => Ok(()),
    }
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
        (lock.as_ref().map(|_| dir), lock.map(Arc::new))
    } else {
        (None, None)
    };
    let (dir, temporary_dir) = match persistent_dir {
        Some(dir) => (dir, None),
        None => {
            let temp = tempfile::Builder::new()
                .prefix(".msc-dl-")
                .suffix(STATE_SUFFIX)
                .tempdir_in(parent)?;
            (temp.path().to_path_buf(), Some(temp))
        }
    };
    let state = Arc::new(state);
    let count = state.size.div_ceil(state.chunk_size);
    let mut tasks = JoinSet::new();
    let downloads: Result<(), StorageError> = async {
        for index in 0..count {
            let expected = state.chunk_size.min(state.size - index * state.chunk_size);
            let path = dir.join(format!("{index:06}.chunk"));
            match fs::symlink_metadata(&path).await {
                Ok(metadata) if metadata.is_file() && metadata.len() == expected => continue,
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
            if tasks.len() >= concurrency {
                finish_chunk(&mut tasks).await?;
            }
            tasks.spawn(download_chunk(
                Arc::clone(&store),
                remote_path.clone(),
                dir.clone(),
                index,
                Arc::clone(&state),
                state_lock.clone(),
            ));
        }
        while !tasks.is_empty() {
            finish_chunk(&mut tasks).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = downloads {
        tasks.shutdown().await;
        return Err(error);
    }
    let (mut merged, merged_path) = create_private_temp(&dir, "merged.")?;
    let mut merged_size = 0;
    for index in 0..count {
        let mut chunk = fs::File::open(dir.join(format!("{index:06}.chunk"))).await?;
        merged_size += tokio::io::copy(&mut chunk, &mut merged).await?;
    }
    if merged_size != state.size {
        return Err(invalid_state("Merged download has an unexpected length"));
    }
    merged.flush().await?;
    merged.sync_all().await?;
    drop(merged);
    merged_path.persist(target).map_err(|error| error.error)?;
    let cleanup = if temporary_dir.is_some() {
        fs::remove_dir_all(&dir).await
    } else {
        remove_state(&dir).await
    };
    if let Err(error) = cleanup {
        eprintln!(
            "Downloaded file to {}, but could not remove download state at {}: {error}",
            target.display(),
            dir.display()
        );
    }
    Ok(state.size)
}

#[cfg(test)]
mod tests {
    use super::*;

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
