// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

mod config;
mod metrics;
mod s3;

use std::{
    fmt::Debug,
    io,
    io::{BufReader, Read, Write},
    ops::Deref,
    path::{Path, PathBuf},
    result,
    sync::{atomic::AtomicU64, Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use bytes::Bytes;
pub use config::Config as DFSConfig;
use file_system;
use metrics::*;
pub use s3::*;
use thiserror::Error;
use tikv_util::time::Instant;
use tokio::runtime::Runtime;

// DFS represents a distributed file system.
#[async_trait]
pub trait Dfs: Sync + Send {
    /// read_file reads the whole file to memory.
    /// It can be used by remote compaction server that doesn't have local disk.
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes>;

    /// Create creates a new File.
    /// The shard_id and shard_ver can be used determine where to write the
    /// file.
    async fn create(&self, file_id: u64, data: Bytes, opts: Options) -> Result<()>;

    /// remove removes the file from the DFS.
    /// `file_len` is used to choose proper storage class for cost
    /// efficiency, in bytes.
    /// `None` if file_len is unknown when invoke this method.
    async fn remove(&self, file_id: u64, file_len: Option<u64>, opts: Options);

    /// Remove the file from DFS permanently.
    async fn permanently_remove(&self, file_id: u64, opts: Options) -> Result<()>;

    /// get_runtime gets the tokio runtime for the DFS.
    fn get_runtime(&self) -> &tokio::runtime::Runtime;

    /// set read/write delay for test.
    fn set_delay(&self, _delay: Duration) {}
}

const REMOVE_DELAY: Duration = Duration::from_secs(90);

pub struct InMemFs {
    files: dashmap::DashMap<u64, Bytes>,
    pending_remove: dashmap::DashMap<u64, Instant>,
    runtime: tokio::runtime::Runtime,
    delay: Arc<Mutex<Duration>>,
}

impl Default for InMemFs {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemFs {
    pub fn new() -> Self {
        Self {
            files: Default::default(),
            pending_remove: Default::default(),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
            delay: Arc::new(Mutex::new(Duration::default())),
        }
    }

    fn get_delay(&self) -> Duration {
        let guard = self.delay.lock().unwrap();
        *guard
    }
}

#[async_trait]
impl Dfs for InMemFs {
    async fn read_file(&self, file_id: u64, _opts: Options) -> Result<Bytes> {
        let delay = self.get_delay();
        tokio::time::sleep(delay).await;
        if let Some(file) = self.files.get(&file_id).as_deref() {
            return Ok(file.clone());
        }
        Err(Error::NotExists(file_id))
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> Result<()> {
        let delay = self.get_delay();
        tokio::time::sleep(delay).await;
        self.files.insert(file_id, data);
        Ok(())
    }

    async fn remove(&self, file_id: u64, _file_len: Option<u64>, _opts: Options) {
        if self.pending_remove.contains_key(&file_id) {
            return;
        }
        let now = Instant::now_coarse();
        self.pending_remove.insert(file_id, now);
        self.pending_remove.retain(|id, &mut remove_time| {
            if now.saturating_duration_since(remove_time) > REMOVE_DELAY {
                self.files.remove(id);
                false
            } else {
                true
            }
        });
    }

    async fn permanently_remove(&self, file_id: u64, _opts: Options) -> Result<()> {
        self.files.remove(&file_id);
        Ok(())
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }

    fn set_delay(&self, delay: Duration) {
        let mut guard = self.delay.lock().unwrap();
        *guard = delay;
    }
}

#[derive(Clone)]
pub struct CacheFs {
    cache: moka::future::Cache<u64, Bytes>,
    s3_fs: Arc<S3Fs>,
}

impl CacheFs {
    pub fn new(cache_size: u64, s3_fs: Arc<S3Fs>) -> Self {
        let builder = moka::future::CacheBuilder::new(cache_size);
        let builder = builder.time_to_idle(Duration::from_secs(600));
        let builder = builder.weigher(|_, v: &Bytes| v.len() as u32);
        let cache = builder.build();
        Self { cache, s3_fs }
    }
}

#[async_trait]
impl Dfs for CacheFs {
    async fn read_file(&self, file_id: u64, opts: Options) -> Result<Bytes> {
        let s3_fs = self.s3_fs.clone();
        let file = self
            .cache
            .try_get_with(file_id, async move { s3_fs.read_file(file_id, opts).await })
            .await
            .map_err(|e| e.as_ref().clone())?;
        Ok(file)
    }
    async fn create(&self, _file_id: u64, _data: Bytes, _opts: Options) -> Result<()> {
        panic!("Do not call");
    }

    async fn remove(&self, _file_id: u64, _file_len: Option<u64>, _opts: Options) {
        panic!("Do not call");
    }

    async fn permanently_remove(&self, _file_id: u64, _opts: Options) -> Result<()> {
        panic!("Do not call");
    }

    fn get_runtime(&self) -> &Runtime {
        self.s3_fs.get_runtime()
    }
}

#[derive(Clone)]
pub struct LocalFs {
    core: Arc<LocalFsCore>,
}

impl LocalFs {
    pub fn new(dir: &Path) -> Self {
        let core = Arc::new(LocalFsCore::new(dir));
        Self { core }
    }
    pub fn local_sst_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.sst_filename(file_id))
    }
    pub fn local_blob_file_path(&self, file_id: u64) -> PathBuf {
        self.dir.join(self.blob_filename(file_id))
    }
    pub fn sst_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.sst", file_id))
    }
    pub fn blob_filename(&self, file_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.blob", file_id))
    }
    pub fn tmp_file_path(&self, file_id: u64) -> PathBuf {
        let tmp_id = self
            .tmp_file_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.dir.join(self.new_tmp_filename(file_id, tmp_id))
    }
    pub fn new_tmp_filename(&self, file_id: u64, tmp_id: u64) -> PathBuf {
        PathBuf::from(format!("{:016x}.{}.tmp", file_id, tmp_id))
    }
}

impl Deref for LocalFs {
    type Target = LocalFsCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

pub struct LocalFsCore {
    dir: PathBuf,
    tmp_file_id: AtomicU64,
    runtime: tokio::runtime::Runtime,
}

impl LocalFsCore {
    pub fn new(dir: &Path) -> Self {
        if !dir.exists() {
            std::fs::create_dir_all(dir).unwrap();
        }
        if !dir.is_dir() {
            panic!("path {:?} is not dir", dir);
        }
        Self {
            dir: dir.to_owned(),
            tmp_file_id: AtomicU64::new(0),
            runtime: tokio::runtime::Builder::new_multi_thread()
                .worker_threads(8)
                .enable_all()
                .build()
                .unwrap(),
        }
    }
}

#[async_trait]
impl Dfs for LocalFs {
    async fn read_file(&self, file_id: u64, _opts: Options) -> Result<Bytes> {
        let local_file_name = self.local_sst_file_path(file_id);
        let fd = std::fs::File::open(local_file_name)?;
        let mut reader = BufReader::new(fd);
        let mut buf = Vec::new();
        reader.read_to_end(&mut buf)?;
        KVENGINE_DFS_THROUGHPUT_VEC
            .with_label_values(&["read"])
            .inc_by(buf.len() as u64);
        Ok(Bytes::from(buf))
    }

    async fn create(&self, file_id: u64, data: Bytes, _opts: Options) -> Result<()> {
        let local_file_name = self.local_sst_file_path(file_id);
        let tmp_file_name = self.tmp_file_path(file_id);
        let mut file = std::fs::File::create(&tmp_file_name)?;
        let mut start_off = 0;
        let write_batch_size = 256 * 1024;
        while start_off < data.len() {
            let end_off = std::cmp::min(start_off + write_batch_size, data.len());
            file.write_all(&data[start_off..end_off])?;
            file.sync_data()?;
            start_off = end_off;
        }
        std::fs::rename(&tmp_file_name, local_file_name)?;
        file_system::sync_dir(&self.dir)?;
        KVENGINE_DFS_THROUGHPUT_VEC
            .with_label_values(&["write"])
            .inc_by(data.len() as u64);
        Ok(())
    }

    async fn remove(&self, file_id: u64, _file_len: Option<u64>, _opts: Options) {
        let local_file_path = self.local_sst_file_path(file_id);
        if let Err(err) = std::fs::remove_file(local_file_path) {
            error!("failed to remove local file {:?}", err);
        }
    }

    async fn permanently_remove(&self, file_id: u64, opts: Options) -> Result<()> {
        self.remove(file_id, None, opts).await;
        Ok(())
    }

    fn get_runtime(&self) -> &Runtime {
        &self.runtime
    }
}

#[derive(Clone, Copy)]
pub struct Options {
    pub shard_id: u64,
    pub shard_ver: u64,
}

impl Options {
    pub fn new(shard_id: u64, shard_ver: u64) -> Self {
        Self {
            shard_id,
            shard_ver,
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("IO error: {0}")]
    Io(String),
    #[error("File {0} not exists")]
    NotExists(u64),
    #[error("S3 error {0}")]
    S3(String),
    #[error("Other error {0}")]
    Other(String),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e.to_string())
    }
}

impl<E: Debug> From<rusoto_core::RusotoError<E>> for Error {
    fn from(err: rusoto_core::RusotoError<E>) -> Self {
        Error::S3(format!("{:?}", err))
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::MetadataExt;

    use super::*;
    use crate::dfs::LocalFs;

    #[test]
    fn test_local_fs() {
        crate::tests::init_logger();

        let local_dir = tempfile::tempdir().unwrap();
        let file_data = "abcdefgh".to_string().into_bytes();
        let localfs = LocalFs::new(local_dir.path());
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let file_id = 321u64;
        let fs = localfs.clone();
        let file_data_clone = file_data.clone();
        let f = async move {
            match fs
                .create(
                    file_id,
                    bytes::Bytes::from(file_data_clone),
                    Options::new(1, 1),
                )
                .await
            {
                Ok(_) => {
                    tx.send(true).unwrap();
                    println!("create ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("create error {:?}", err)
                }
            }
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let fs = localfs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            let opts = Options::new(1, 1);
            match fs.read_file(file_id, opts).await {
                Ok(data) => {
                    assert_eq!(&data, &file_data);
                    tx.send(true).unwrap();
                    println!("prefetch ok");
                }
                Err(err) => {
                    tx.send(false).unwrap();
                    println!("prefetch failed {:?}", err)
                }
            }
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        let local_file = localfs.local_sst_file_path(file_id);
        let fd = std::fs::File::open(&local_file).unwrap();
        let meta = fd.metadata().unwrap();
        assert_eq!(meta.size(), 8u64);
        let fs = localfs.clone();
        let (tx, rx) = tikv_util::mpsc::bounded(1);
        let f = async move {
            fs.remove(file_id, None, Options::new(1, 1)).await;
            tx.send(true).unwrap();
        };
        localfs.runtime.spawn(f);
        assert!(rx.recv().unwrap());
        std::fs::File::open(&local_file).unwrap_err();
    }
}
