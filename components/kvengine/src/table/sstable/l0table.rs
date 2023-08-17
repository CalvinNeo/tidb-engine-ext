// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{ops::Deref, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use cloud_encryption::EncryptionKey;
use moka::sync::SegmentedCache;

use super::*;
use crate::{
    max_ts_by_cf,
    table::{blobtable::BlobRef, table::Result, InnerKey, Value},
    LOCK_CF, NUM_CFS, WRITE_CF,
};

const L0_FOOTER_SIZE: usize = std::mem::size_of::<L0Footer>();

#[derive(Default, Clone)]
struct L0Footer {
    version: u64,
    num_cfs: u32,
    magic: u32,
}

impl L0Footer {
    fn unmarshal(&mut self, bin: &[u8]) {
        self.version = LittleEndian::read_u64(bin);
        self.num_cfs = LittleEndian::read_u32(&bin[8..]);
        self.magic = LittleEndian::read_u32(&bin[12..]);
    }
}

#[derive(Clone)]
pub struct L0Table {
    core: Arc<L0TableCore>,
}

impl Deref for L0Table {
    type Target = L0TableCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl L0Table {
    pub fn new(
        file: Arc<dyn File>,
        cache: Option<SegmentedCache<BlockCacheKey, Bytes>>,
        ignore_lock: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let core = L0TableCore::new(file, cache, ignore_lock, encryption_key)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }
}

pub struct L0TableCore {
    footer: L0Footer,
    file: Arc<dyn File>,
    cfs: [Option<sstable::SsTable>; NUM_CFS],
    max_ts: u64,
    entries: u64,
    kv_size: u64,
    smallest: Bytes,
    biggest: Bytes,
    total_blob_size: u64,
}

impl L0TableCore {
    pub fn new(
        file: Arc<dyn File>,
        cache: Option<SegmentedCache<BlockCacheKey, Bytes>>,
        ignore_lock: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let footer_off = file.size() - L0_FOOTER_SIZE as u64;
        let mut footer = L0Footer::default();
        let footer_buf = file.read(footer_off, L0_FOOTER_SIZE)?;
        footer.unmarshal(footer_buf.chunk());
        let cf_offs_off = footer_off - 4 * NUM_CFS as u64;
        let cf_offs_buf = file.read(cf_offs_off, 4 * NUM_CFS)?;
        let mut cf_offs = [0u32; NUM_CFS];
        for i in 0..NUM_CFS {
            cf_offs[i] = LittleEndian::read_u32(&cf_offs_buf[i * 4..]);
        }
        let mut cfs: [Option<SsTable>; NUM_CFS] = [None, None, None];
        let mut entries = 0;
        let mut kv_size = 0;
        for i in 0..NUM_CFS {
            let start_off = cf_offs[i] as u64;
            let mut end_off = cf_offs_off;
            if i + 1 < NUM_CFS {
                end_off = cf_offs[i + 1] as u64;
            }
            if start_off == end_off || ignore_lock && i == LOCK_CF {
                continue;
            }
            let tbl = sstable::SsTable::new_l0_cf(
                file.clone(),
                start_off,
                end_off,
                cache.clone(),
                encryption_key.clone(),
            )?;
            entries += tbl.entries as u64;
            if i == WRITE_CF {
                kv_size += tbl.kv_size;
            }

            cfs[i] = Some(tbl)
        }
        let (smallest, biggest, max_ts) = Self::compute_smallest_biggest(&cfs);
        let total_blob_size = Self::compute_total_blob_size(&cfs);
        Ok(Self {
            footer,
            file,
            cfs,
            max_ts,
            entries,
            kv_size,
            smallest,
            biggest,
            total_blob_size,
        })
    }

    // Return: smallest, biggest, max_ts
    fn compute_smallest_biggest(cfs: &[Option<SsTable>; NUM_CFS]) -> (Bytes, Bytes, u64) {
        let mut smallest_buf = BytesMut::new();
        let mut biggest_buf = BytesMut::new();
        let mut max_ts = 0;
        for i in 0..NUM_CFS {
            if let Some(cf_tbl) = &cfs[i] {
                let smallest = cf_tbl.smallest();
                if !smallest.is_empty()
                    && (smallest_buf.is_empty() || smallest_buf.chunk() > smallest.deref())
                {
                    smallest_buf.truncate(0);
                    smallest_buf.extend_from_slice(smallest.deref());
                }
                let biggest = cf_tbl.biggest();
                if biggest.deref() > biggest_buf.chunk() {
                    biggest_buf.truncate(0);
                    biggest_buf.extend_from_slice(biggest.deref());
                }
                max_ts = max_ts_by_cf(max_ts, i, cf_tbl.max_ts);
            }
        }
        assert!(!smallest_buf.is_empty());
        assert!(!biggest_buf.is_empty());
        (smallest_buf.freeze(), biggest_buf.freeze(), max_ts)
    }

    fn compute_total_blob_size(cfs: &[Option<SsTable>; NUM_CFS]) -> u64 {
        let mut total_blob_size = 0;
        for i in 0..NUM_CFS {
            if let Some(cf_tbl) = &cfs[i] {
                total_blob_size += cf_tbl.total_blob_size();
            }
        }
        total_blob_size
    }

    pub fn id(&self) -> u64 {
        self.file.id()
    }

    pub fn get_cf(&self, cf: usize) -> &Option<sstable::SsTable> {
        &self.cfs[cf]
    }

    pub fn size(&self) -> u64 {
        self.file.size()
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn entries(&self) -> u64 {
        self.entries
    }

    pub fn kv_size(&self) -> u64 {
        self.kv_size
    }

    pub fn smallest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.smallest.chunk())
    }

    pub fn biggest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.biggest.chunk())
    }

    pub fn version(&self) -> u64 {
        self.footer.version
    }

    pub fn has_data_in_range(&self, start: InnerKey<'_>, end: InnerKey<'_>) -> bool {
        if self.smallest() >= end || self.biggest() < start {
            return false;
        }
        self.cfs
            .iter()
            .filter_map(|t| t.as_ref())
            .any(|t| t.has_overlap(start, end, false))
    }

    pub fn total_blob_size(&self) -> u64 {
        self.total_blob_size
    }
}

pub struct L0Builder {
    builders: Vec<Builder>,
    version: u64,
    count: usize,
    fid: u64,
}

impl L0Builder {
    pub fn new(
        fid: u64,
        block_size: usize,
        version: u64,
        encryption_key: Option<EncryptionKey>,
    ) -> Self {
        let mut builders = Vec::with_capacity(4);
        for _ in 0..NUM_CFS {
            let builder = Builder::new(fid, block_size, NO_COMPRESSION, 0, encryption_key.clone());
            builders.push(builder);
        }
        Self {
            builders,
            version,
            count: 0,
            fid,
        }
    }

    pub fn add(
        &mut self,
        cf: usize,
        key: InnerKey<'_>,
        val: &Value,
        external_link: Option<BlobRef>,
    ) {
        self.builders[cf].add(key, val, external_link);
        self.count += 1;
    }

    pub fn finish(&mut self) -> Bytes {
        let mut estimated_size = 0;
        for builder in &self.builders {
            estimated_size += builder.estimated_size();
        }
        let mut buf = Vec::with_capacity(estimated_size);
        let mut offsets = Vec::with_capacity(NUM_CFS);
        for builder in &mut self.builders {
            let offset = buf.len() as u32;
            offsets.push(offset);
            if !builder.is_empty() {
                builder.finish(offset, &mut buf);
            }
        }
        for offset in offsets {
            buf.put_u32_le(offset);
        }
        buf.put_u64_le(self.version);
        buf.put_u32_le(NUM_CFS as u32);
        buf.put_u32_le(MAGIC_NUMBER);
        Bytes::from(buf)
    }

    pub fn smallest_biggest(&self) -> (Bytes, Bytes) {
        let mut smallest_buf = BytesMut::new();
        let mut biggest_buf = BytesMut::new();
        for builder in &self.builders {
            if !builder.get_smallest().is_empty()
                && (smallest_buf.is_empty() || builder.get_smallest() < smallest_buf)
            {
                smallest_buf.truncate(0);
                smallest_buf.extend_from_slice(builder.get_smallest());
            }
            if builder.get_biggest() > biggest_buf {
                biggest_buf.truncate(0);
                biggest_buf.extend_from_slice(builder.get_biggest());
            }
        }
        (smallest_buf.freeze(), biggest_buf.freeze())
    }

    pub fn total_blob_size(&self) -> u64 {
        let mut total_blob_size = 0;
        for builder in &self.builders {
            total_blob_size += builder.get_total_blob_size();
        }
        total_blob_size
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn get_fid(&self) -> u64 {
        self.fid
    }
}
