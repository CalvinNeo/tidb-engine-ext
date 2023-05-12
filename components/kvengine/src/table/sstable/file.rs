// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    os::unix::fs::{FileExt, MetadataExt},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering::Relaxed},
        Arc, Mutex,
    },
};

use bytes::Bytes;

use crate::table::table;

// 30 minutes idle file would be closed.
const FILE_TTL: u64 = 30 * 60;

pub trait File: Sync + Send {
    // id returns the id of the file.
    fn id(&self) -> u64;

    // size returns the size of the file.
    fn size(&self) -> u64;

    // read reads the data at given offset.
    fn read(&self, off: u64, length: usize) -> table::Result<Bytes>;

    // read_at reads the data to the buffer.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()>;

    // expire_open_file closes the file if it's idle for a long time.
    // It will be reopened and cached on next read.
    fn expire_open_file(&self) {}

    fn is_open(&self) -> bool {
        false
    }
}

pub struct LocalFile {
    id: u64,
    size: u64,
    path: PathBuf,
    fd: TtlCache<std::fs::File>,
}

impl LocalFile {
    pub fn open(id: u64, path: &Path, set_mtime: bool) -> table::Result<LocalFile> {
        if set_mtime {
            filetime::set_file_mtime(path, filetime::FileTime::now())?;
        }
        let meta = std::fs::metadata(path)?;
        let local_file = LocalFile {
            id,
            size: meta.size(),
            path: path.to_path_buf(),
            fd: TtlCache::default(),
        };
        Ok(local_file)
    }

    fn get_file(&self) -> table::Result<Arc<std::fs::File>> {
        self.fd.get(|| {
            std::fs::File::open(self.path.as_path())
                .map_err(|e| table::Error::Io(format!("failed to open file {}: {:?}", self.id, e)))
        })
    }
}

impl File for LocalFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, off: u64, length: usize) -> table::Result<Bytes> {
        let mut buf = vec![0; length];
        let fd = self.get_file()?;
        fd.read_at(&mut buf, off)?;
        Ok(Bytes::from(buf))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        let fd = self.get_file()?;
        fd.read_at(buf, offset)?;
        Ok(())
    }

    fn expire_open_file(&self) {
        self.fd.expire(FILE_TTL)
    }

    fn is_open(&self) -> bool {
        self.fd.is_loaded()
    }
}

#[derive(Clone)]
pub struct InMemFile {
    pub id: u64,
    data: Bytes,
    pub size: u64,
}

impl InMemFile {
    pub fn new(id: u64, data: Bytes) -> Self {
        let size = data.len() as u64;
        Self { id, data, size }
    }
}

impl File for InMemFile {
    fn id(&self) -> u64 {
        self.id
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, off: u64, length: usize) -> table::Result<Bytes> {
        let off_usize = off as usize;
        Ok(self.data.slice(off_usize..off_usize + length))
    }

    fn read_at(&self, buf: &mut [u8], offset: u64) -> table::Result<()> {
        let off_usize = offset as usize;
        let length = buf.len();
        buf.copy_from_slice(&self.data[off_usize..off_usize + length]);
        Ok(())
    }
}

pub struct TtlCache<T> {
    access_ns: AtomicU64,
    data: Mutex<Option<Arc<T>>>,
}

impl<T> Default for TtlCache<T> {
    fn default() -> Self {
        Self {
            access_ns: AtomicU64::new(0),
            data: Mutex::new(None),
        }
    }
}

impl<T> TtlCache<T> {
    pub fn get(&self, init: impl FnOnce() -> table::Result<T>) -> table::Result<Arc<T>> {
        let now_ns = time::precise_time_ns();
        self.access_ns.store(now_ns, Relaxed);
        let mut guard = self.data.lock().unwrap();
        if guard.is_none() {
            let data = init()?;
            *guard = Some(Arc::new(data));
        }
        Ok(guard.as_ref().unwrap().clone())
    }

    pub fn expire(&self, dur_secs: u64) {
        let access_ns = self.access_ns.load(Relaxed);
        let now_ns = time::precise_time_ns();
        let dur_nanos = dur_secs * 1_000_000_000;
        if access_ns > 0 && now_ns.saturating_sub(access_ns) > dur_nanos {
            self.data.lock().unwrap().take();
            self.access_ns.store(0, Relaxed);
        }
    }

    pub fn is_loaded(&self) -> bool {
        self.access_ns.load(Relaxed) > 0
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::table::sstable::TtlCache;

    #[test]
    fn test_ttl_cache() {
        let cache: TtlCache<Vec<u8>> = TtlCache::default();
        assert!(!cache.is_loaded());
        cache.get(|| Ok(vec![1, 2, 3, 4])).unwrap();
        assert!(cache.is_loaded());
        cache.expire(1);
        assert!(cache.is_loaded());
        std::thread::sleep(Duration::from_millis(1500));
        cache.expire(1);
        assert!(!cache.is_loaded());
    }

    #[bench]
    fn bench_ttl_cache(b: &mut test::Bencher) {
        let cache: TtlCache<u64> = TtlCache::default();
        b.iter(|| {
            cache.get(|| Ok(1)).unwrap();
        });
    }
}
