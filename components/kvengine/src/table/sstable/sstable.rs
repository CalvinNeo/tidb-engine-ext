// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    cmp::Ordering,
    iter::Iterator as StdIterator,
    ops::Deref,
    path::{Path, PathBuf},
    sync::Arc,
};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};
use cloud_encryption::EncryptionKey;
use moka::sync::SegmentedCache;
use xorf::{BinaryFuse8, Filter};

use super::{builder::*, iterator::TableIterator};
use crate::{
    table::{
        sstable::{File, TtlCache},
        table::{Iterator, Result},
        *,
    },
    util::evenly_distribute,
};

// higher level ttl is longer than lower level.
const IDX_TTL_LEVELS: [u64; 4] = [60 * 8, 60 * 4, 60 * 2, 60];
const FILTER_TTL_LEVELS: [u64; 4] = [60 * 2, 60, 30, 15];
const SMALL_VALUE_SIZE: usize = 128;

#[derive(Clone)]
pub struct SsTable {
    core: Arc<SsTableCore>,
}

impl Deref for SsTable {
    type Target = SsTableCore;
    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl SsTable {
    pub fn new(
        file: Arc<dyn File>,
        cache: Option<SegmentedCache<BlockCacheKey, Bytes>>,
        _load_filter: bool,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let size = file.size();
        let core = SsTableCore::new(file, 0, size, cache, encryption_key)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }

    pub fn new_l0_cf(
        file: Arc<dyn File>,
        start: u64,
        end: u64,
        cache: Option<SegmentedCache<BlockCacheKey, Bytes>>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let core = SsTableCore::new(file, start, end, cache, encryption_key)?;
        Ok(Self {
            core: Arc::new(core),
        })
    }

    pub fn new_iterator(&self, reversed: bool, fill_cache: bool) -> Box<TableIterator> {
        let it = TableIterator::new(self.clone(), reversed, fill_cache);
        Box::new(it)
    }

    // Get the value with the given key and version.
    // It is caller's responsibility to maintain the lifetime of the returned value.
    // Value will be filled in out_val_owner , while the returned value is a parsed
    // slice of it.
    pub fn get(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
        level: usize,
    ) -> table::Value {
        // For small value on level 3, load the filter is not cost-effective.
        // TODO: avoid build filter on level 3 small value table.
        let small_value = self.footer.data_len() < self.entries as usize * SMALL_VALUE_SIZE;
        let skip_filter = small_value && level == 3;
        if self.filter_size() > 0 && !skip_filter {
            let filter = self
                .filter
                .get(|| {
                    let filter_data = self.read_filter_data_from_file()?;
                    let filter = self.decode_filter(&filter_data)?;
                    Ok(filter)
                })
                .expect("load filter");
            if !filter.contains(&key_hash) {
                return table::Value::new();
            }
        }
        let mut it = self.new_iterator(false, true);
        it.seek(key);
        if !it.valid() || key != it.key() {
            return table::Value::new();
        }
        while it.value().version > version {
            if !it.next_version() {
                return table::Value::new();
            }
        }
        // Reconstruct the value to avoid the lifetime issue.
        let val = it.value();
        out_val_owner.resize(val.encoded_size(), 0);
        val.encode(out_val_owner.as_mut_slice());
        Value::decode(out_val_owner.as_slice())
    }

    pub fn has_overlap(&self, start: InnerKey<'_>, end: InnerKey<'_>, include_end: bool) -> bool {
        if start > self.biggest() {
            return false;
        }
        match end.cmp(&self.smallest()) {
            Ordering::Less => {
                return false;
            }
            Ordering::Equal => {
                return include_end;
            }
            _ => {}
        }
        let mut it = self.new_iterator(false, true);
        it.seek(start);
        if !it.valid() {
            return it.error().is_some();
        }
        match it.key().cmp(&end) {
            Ordering::Greater => false,
            Ordering::Equal => include_end,
            _ => true,
        }
    }

    pub fn get_newer(
        &self,
        key: InnerKey<'_>,
        version: u64,
        key_hash: u64,
        out_val_owner: &mut Vec<u8>,
        level: usize,
    ) -> table::Value {
        if self.max_ts < version {
            return table::Value::new();
        }
        let val = self.get(key, u64::MAX, key_hash, out_val_owner, level);
        if val.version >= version {
            return val;
        }
        table::Value::new()
    }
}

pub struct SsTableCore {
    file: Arc<dyn File>,
    cache: Option<SegmentedCache<BlockCacheKey, Bytes>>,
    filter: TtlCache<BinaryFuse8>,
    start_off: u64,
    footer: Footer,
    smallest_buf: Bytes,
    biggest_buf: Bytes,
    pub max_ts: u64,
    pub entries: u32,
    pub old_entries: u32,
    pub tombs: u32,
    pub kv_size: u64,
    pub in_use_total_blob_size: u64,
    idx: TtlCache<Index>,
    old_idx: TtlCache<Index>,
    encryption_key: Option<EncryptionKey>,
    encryption_ver: u32,
}

impl SsTableCore {
    pub fn new(
        file: Arc<dyn File>,
        start_off: u64,
        end_off: u64,
        cache: Option<SegmentedCache<BlockCacheKey, Bytes>>,
        encryption_key: Option<EncryptionKey>,
    ) -> Result<Self> {
        let size = end_off - start_off;
        let mut footer = Footer::default();
        if size < FOOTER_SIZE as u64 {
            return Err(table::Error::InvalidFileSize);
        }
        let footer_data = file.read(end_off - FOOTER_SIZE as u64, FOOTER_SIZE)?;
        footer.unmarshal(footer_data.chunk());
        if footer.magic != MAGIC_NUMBER {
            return Err(table::Error::InvalidMagicNumber);
        }
        let props_data = file.read(
            start_off + footer.properties_offset as u64,
            footer.properties_len(size as usize),
        )?;
        let mut prop_slice = props_data.chunk();
        validate_checksum(prop_slice, footer.checksum_type)?;
        prop_slice = &prop_slice[4..];
        let mut smallest_buf = Bytes::new();
        let mut biggest_buf = Bytes::new();
        let mut max_ts = 0;
        let mut entries = 0;
        let mut old_entries = 0;
        let mut tombs = 0;
        let mut kv_size = None;
        let mut in_use_total_blob_size = 0u64;
        let mut encryption_ver = 0;
        while !prop_slice.is_empty() {
            let (key, val, remain) = parse_prop_data(prop_slice);
            prop_slice = remain;
            if key == PROP_KEY_SMALLEST.as_bytes() {
                smallest_buf = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_BIGGEST.as_bytes() {
                biggest_buf = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_MAX_TS.as_bytes() {
                max_ts = LittleEndian::read_u64(val);
            } else if key == PROP_KEY_ENTRIES.as_bytes() {
                entries = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_OLD_ENTRIES.as_bytes() {
                old_entries = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_TOMBS.as_bytes() {
                tombs = LittleEndian::read_u32(val);
            } else if key == PROP_KEY_KV_SIZE.as_bytes() {
                kv_size = Some(LittleEndian::read_u64(val));
            } else if key == PROP_KEY_IN_USE_TOTAL_BLOB_SIZE.as_bytes() {
                in_use_total_blob_size = LittleEndian::read_u64(val);
            } else if key == PROP_KEY_ENCRYPTION_VER.as_bytes() {
                encryption_ver = LittleEndian::read_u32(val);
            }
        }
        let core = Self {
            file,
            cache,
            filter: TtlCache::default(),
            start_off,
            footer,
            smallest_buf,
            biggest_buf,
            max_ts,
            entries,
            old_entries,
            tombs,
            kv_size: kv_size.unwrap_or(size),
            in_use_total_blob_size,
            idx: TtlCache::default(),
            old_idx: TtlCache::default(),
            encryption_ver,
            encryption_key,
        };
        Ok(core)
    }

    pub fn init_index(&self, offset: u32, length: usize) -> Result<Index> {
        let idx_data = self
            .file
            .read(self.start_off + offset as u64, length)
            .unwrap();
        Index::new(idx_data, self.footer.checksum_type)
    }

    pub fn load_index(&self) -> Arc<Index> {
        self.idx
            .get(|| self.init_index(self.footer.index_offset, self.footer.index_len()))
            .expect("load index")
    }

    pub fn load_old_index(&self) -> Arc<Index> {
        self.old_idx
            .get(|| self.init_index(self.footer.old_index_offset, self.footer.old_index_len()))
            .expect("load old index")
    }

    pub fn expire_cache(&self, level: usize) {
        self.file.expire_open_file();
        self.filter.expire(FILTER_TTL_LEVELS[level]);
        self.idx.expire(IDX_TTL_LEVELS[level]);
        self.old_idx.expire(IDX_TTL_LEVELS[level]);
    }

    pub fn has_open_file(&self) -> bool {
        self.file.is_open()
    }

    pub fn load_block(
        &self,
        idx: &Index,
        pos: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        let addr = idx.get_block_addr(pos);
        let length = if pos + 1 < idx.num_blocks() {
            let next_addr = idx.get_block_addr(pos + 1);
            (next_addr.curr_off - addr.curr_off) as usize
        } else {
            self.start_off as usize + self.footer.data_len() - addr.curr_off as usize
        };
        self.load_block_by_addr_len(addr, length, buf, decryption_buf, fill_cache)
    }

    fn load_block_by_addr_len(
        &self,
        addr: BlockAddress,
        length: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        match &self.cache {
            Some(cache) => {
                let cache_key = BlockCacheKey::new(addr.origin_fid, addr.origin_off);
                if fill_cache {
                    return cache
                        .try_get_with(cache_key, || {
                            crate::metrics::ENGINE_CACHE_MISS.inc_by(1);
                            self.read_block_from_file(addr, length, buf, decryption_buf)
                        })
                        .map_err(|err| err.as_ref().clone());
                }
                if let Some(block) = cache.get(&cache_key) {
                    return Ok(block);
                }
                crate::metrics::ENGINE_CACHE_MISS.inc_by(1);
                self.read_block_from_file(addr, length, buf, decryption_buf)
            }
            None => self.read_block_from_file(addr, length, buf, decryption_buf),
        }
    }

    fn read_block_from_file(
        &self,
        addr: BlockAddress,
        length: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
    ) -> Result<Bytes> {
        let compression_type = self.footer.compression_type;
        if compression_type == NO_COMPRESSION {
            let mut raw_block = self.file.read(addr.curr_off as u64, length)?;
            if let Some(encryption_key) = &self.encryption_key {
                let mut block = Vec::with_capacity(length + encryption_key.encryption_block_size());
                encryption_key.decrypt(
                    raw_block.chunk(),
                    addr.origin_fid,
                    addr.curr_off,
                    self.encryption_ver,
                    &mut block,
                );
                raw_block = Bytes::from(block)
            }
            validate_checksum(raw_block.chunk(), self.footer.checksum_type)?;
            return Ok(raw_block.slice(4..));
        }
        buf.resize(length, 0);
        if let Some(encryption_key) = &self.encryption_key {
            decryption_buf.resize(length, 0);
            self.file.read_at(decryption_buf, addr.curr_off as u64)?;
            buf.truncate(0);
            encryption_key.decrypt(
                decryption_buf,
                addr.origin_fid,
                addr.curr_off,
                self.encryption_ver,
                buf,
            );
        } else {
            self.file.read_at(buf, addr.curr_off as u64)?;
        }
        validate_checksum(buf, self.footer.checksum_type)?;
        let content = &buf[4..];
        match compression_type {
            LZ4_COMPRESSION => {
                let block = lz4::block::decompress(content, None)?;
                Ok(Bytes::from(block))
            }
            ZSTD_COMPRESSION => {
                let capacity = unsafe {
                    zstd_sys::ZSTD_getFrameContentSize(
                        content.as_ptr() as *const libc::c_void,
                        content.len(),
                    ) as usize
                };
                let mut block = Vec::<u8>::with_capacity(capacity);
                unsafe {
                    let result = zstd_sys::ZSTD_decompress(
                        block.as_mut_ptr() as *mut libc::c_void,
                        capacity,
                        content.as_ptr() as *const libc::c_void,
                        content.len(),
                    );
                    assert_eq!(zstd_sys::ZSTD_isError(result), 0u32);
                    block.set_len(capacity);
                }
                Ok(Bytes::from(block))
            }
            _ => panic!("unknown compression type {}", compression_type),
        }
    }

    pub fn load_old_block(
        &self,
        old_idx: &Index,
        pos: usize,
        buf: &mut Vec<u8>,
        decryption_buf: &mut Vec<u8>,
        fill_cache: bool,
    ) -> Result<Bytes> {
        let addr = old_idx.get_block_addr(pos);
        let length = if pos + 1 < old_idx.num_blocks() {
            let next_addr = old_idx.get_block_addr(pos + 1);
            (next_addr.curr_off - addr.curr_off) as usize
        } else {
            self.start_off as usize + self.footer.index_offset as usize - addr.curr_off as usize
        };
        self.load_block_by_addr_len(addr, length, buf, decryption_buf, fill_cache)
    }

    fn read_filter_data_from_file(&self) -> Result<Bytes> {
        let mut data = self
            .file
            .read(self.filter_offset() as u64, self.filter_size() as usize)?;
        validate_checksum(data.chunk(), self.footer.checksum_type)?;
        data.get_u32_le();
        assert_eq!(data.get_u32_le(), AUX_INDEX_BINARY_FUSE8);
        let len = data.get_u32_le();
        assert_eq!(len as usize, data.len());
        Ok(data)
    }

    fn decode_filter(&self, data: &Bytes) -> Result<BinaryFuse8> {
        BinaryFuse8::try_from_bytes(data).map_err(|e| Error::Other(e.to_string()))
    }

    pub fn id(&self) -> u64 {
        self.file.id()
    }

    pub fn size(&self) -> u64 {
        self.file.size()
    }

    /// Get estimated size in [start, end) by number of blocks.
    pub fn estimated_size_in_range(&self, start: InnerKey<'_>, end: InnerKey<'_>) -> u64 {
        let idx = self.load_index();
        let left = idx.seek_block_bigger_or_equal(start.deref());
        let right = idx.seek_block_bigger_or_equal(end.deref());
        self.file.size() * (right - left) as u64 / idx.num_blocks() as u64
    }

    pub fn index_size(&self) -> u64 {
        (self.footer.index_len() + self.footer.old_index_len()) as u64
    }

    pub fn in_mem_index_size(&self) -> u64 {
        let idx_in_mem = if self.idx.is_loaded() {
            self.footer.index_len()
        } else {
            0
        };
        let old_idx_in_mem = if self.old_idx.is_loaded() {
            self.footer.old_index_len()
        } else {
            0
        };
        (idx_in_mem + old_idx_in_mem) as u64
    }

    fn filter_offset(&self) -> u32 {
        self.start_off as u32 + self.footer.aux_index_offset
    }

    pub fn filter_size(&self) -> u64 {
        self.footer.aux_index_len() as u64
    }

    pub fn in_mem_filter_size(&self) -> u64 {
        if self.filter.is_loaded() {
            self.footer.aux_index_len() as u64
        } else {
            0
        }
    }

    pub fn smallest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.smallest_buf.chunk())
    }

    pub fn biggest(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.biggest_buf.chunk())
    }

    pub fn get_suggest_split_key(
        &self,
        start: Option<InnerKey<'_>>,
        end: Option<InnerKey<'_>>,
    ) -> Option<Bytes> {
        // Get the key at 1/2 as split key.
        // Use `test_get_split_keys` to see the result when adjust the split point.
        let split_keys = self.get_evenly_split_keys(start, end, 2);
        split_keys.into_iter().nth(1)
    }

    /// Get evenly split keys by blocks.
    ///
    /// Length of return vector would be less than `count` when number of blocks
    /// less than count.
    ///
    /// The first value of return is the first key of the first block in range.
    pub fn get_evenly_split_keys(
        &self,
        start: Option<InnerKey<'_>>,
        end: Option<InnerKey<'_>>,
        count: usize,
    ) -> Vec<Bytes> {
        debug_assert!(count > 0);
        let idx = self.load_index();
        let left = start.map_or(0, |start| idx.seek_block_bigger_or_equal(start.deref()));
        let right = end.map_or(idx.num_blocks(), |end| {
            idx.seek_block_bigger_or_equal(end.deref())
        });
        let num_blocks = right - left;
        let steps = evenly_distribute(num_blocks, count);
        let mut block_idx = left;
        let mut split_keys = Vec::with_capacity(steps.len());
        for step in steps {
            split_keys.push(idx.block_key(block_idx));
            block_idx += step;
        }
        split_keys
    }

    pub fn compression_type(&self) -> u8 {
        self.footer.compression_type
    }

    pub fn total_blob_size(&self) -> u64 {
        self.in_use_total_blob_size
    }
}

#[derive(Clone)]
pub struct Index {
    common_prefix: Bytes,
    block_key_offs: Bytes,
    block_addrs: Bytes,
    block_keys: Bytes,
}

impl Index {
    fn new(mut data: Bytes, checksum_type: u8) -> Result<Self> {
        validate_checksum(data.chunk(), checksum_type)?;
        let _checksum = data.get_u32_le();
        assert_eq!(data.get_u32_le(), INDEX_FORMAT_V1);
        let num_blocks = data.get_u32_le() as usize;
        let block_key_offs = data.slice(..num_blocks * 4);
        data.advance(block_key_offs.len());
        let block_addrs = data.slice(..num_blocks * BLOCK_ADDR_SIZE);
        data.advance(block_addrs.len());
        let common_prefix_len = data.get_u16_le() as usize;
        let common_prefix = data.slice(..common_prefix_len);
        data.advance(common_prefix.len());
        let block_key_len = data.get_u32_le() as usize;
        let block_keys = data.slice(..block_key_len);
        Ok(Self {
            common_prefix,
            block_key_offs,
            block_addrs,
            block_keys,
        })
    }

    pub(crate) fn get_block_addr(&self, pos: usize) -> BlockAddress {
        let off = pos * BLOCK_ADDR_SIZE;
        BlockAddress::from_slice(&self.block_addrs[off..off + BLOCK_ADDR_SIZE])
    }

    pub fn num_blocks(&self) -> usize {
        self.block_key_offs.len() / 4
    }

    /// Returns the block index of the first block whose key > `key`.
    pub fn seek_block(&self, key: &[u8]) -> usize {
        if key.len() <= self.common_prefix.len() {
            if key <= self.common_prefix.chunk() {
                return 0;
            }
            return self.num_blocks();
        }
        let cmp = key[..self.common_prefix.len()].cmp(self.common_prefix.chunk());
        match cmp {
            Ordering::Less => 0,
            Ordering::Equal => {
                let diff_key = &key[self.common_prefix.len()..];
                search(self.num_blocks(), |i| self.block_diff_key(i) > diff_key)
            }
            Ordering::Greater => self.num_blocks(),
        }
    }

    /// Returns the block index of the first block whose key >= `key`.
    pub fn seek_block_bigger_or_equal(&self, key: &[u8]) -> usize {
        if key.len() <= self.common_prefix.len() {
            if key <= self.common_prefix.chunk() {
                return 0;
            }
            return self.num_blocks();
        }
        let cmp = key[..self.common_prefix.len()].cmp(self.common_prefix.chunk());
        match cmp {
            Ordering::Less => 0,
            Ordering::Equal => {
                let diff_key = &key[self.common_prefix.len()..];
                search(self.num_blocks(), |i| self.block_diff_key(i) >= diff_key)
            }
            Ordering::Greater => self.num_blocks(),
        }
    }

    fn block_diff_key(&self, i: usize) -> &[u8] {
        let off = self.get_block_key_off(i);
        let end_off = if i + 1 < self.num_blocks() {
            self.get_block_key_off(i + 1)
        } else {
            self.block_keys.len()
        };
        &self.block_keys[off..end_off]
    }

    fn get_block_key_off(&self, i: usize) -> usize {
        (&self.block_key_offs[i * 4..]).get_u32_le() as usize
    }

    fn block_key(&self, i: usize) -> Bytes {
        let diff_key = self.block_diff_key(i);
        let mut buf = BytesMut::new();
        buf.extend_from_slice(self.common_prefix.chunk());
        buf.extend_from_slice(diff_key);
        buf.freeze()
    }
}

#[derive(Clone, Copy, Hash, PartialEq, Eq, Debug)]
pub struct BlockCacheKey {
    origin_id: u64,
    origin_off: u32,
}

impl BlockCacheKey {
    pub fn new(origin_id: u64, origin_off: u32) -> Self {
        Self {
            origin_id,
            origin_off,
        }
    }
}

fn validate_checksum(data: &[u8], checksum_type: u8) -> Result<()> {
    if data.len() < 4 {
        return Err(table::Error::InvalidChecksum(String::from(
            "data is too short",
        )));
    }
    let checksum = LittleEndian::read_u32(data);
    let content = &data[4..];
    if checksum_type == CRC32C {
        let got_checksum = crc32c::crc32c(content);
        if checksum != got_checksum {
            return Err(table::Error::InvalidChecksum(format!(
                "checksum mismatch expect {} got {}",
                checksum, got_checksum
            )));
        }
    }
    Ok(())
}

const FILE_SUFFIX: &str = ".sst";

pub fn parse_file_id(path: &Path) -> Result<u64> {
    let name = path.file_name().unwrap().to_str().unwrap();
    if !name.ends_with(FILE_SUFFIX) {
        return Err(table::Error::InvalidFileName);
    }
    let digit_part = &name[..name.len() - FILE_SUFFIX.len()];
    if let Ok(id) = u64::from_str_radix(digit_part, 16) {
        return Ok(id);
    }
    Err(table::Error::InvalidFileName)
}

fn parse_prop_data(mut prop_data: &[u8]) -> (&[u8], &[u8], &[u8]) {
    let key_len = LittleEndian::read_u16(prop_data) as usize;
    prop_data = &prop_data[2..];
    let key = &prop_data[..key_len];
    prop_data = &prop_data[key_len..];
    let val_len = LittleEndian::read_u32(prop_data) as usize;
    prop_data = &prop_data[4..];
    let val = &prop_data[..val_len];
    let remained = &prop_data[val_len..];
    (key, val, remained)
}

pub fn id_to_filename(id: u64) -> String {
    format!("{:016x}.sst", id)
}

pub fn new_filename(id: u64, dir: &Path) -> PathBuf {
    dir.join(id_to_filename(id))
}

#[cfg(test)]
pub(crate) static TEST_ID_ALLOC: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(1);

#[cfg(test)]
pub(crate) fn get_test_value(n: usize) -> String {
    format!("{}", n)
}

#[cfg(test)]
pub(crate) fn generate_key_values(prefix: &str, n: usize) -> Vec<(String, String)> {
    assert!(n <= 10000);
    let mut results = Vec::with_capacity(n);
    for i in 0..n {
        let k = get_test_key(prefix, i);
        let v = get_test_value(i);
        results.push((k, v));
    }
    results
}

#[cfg(test)]
pub(crate) fn build_test_table_with_kvs(kvs: &Vec<(String, String)>, load_filter: bool) -> SsTable {
    let sst_fid = TEST_ID_ALLOC.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
    let mut sst_builder = new_table_builder_for_test(sst_fid);
    let meta = 0u8;

    for (k, v) in kvs {
        let value_buf = Value::encode_buf(meta, &[0], 0, v.as_bytes());
        let value = &mut Value::decode(value_buf.as_slice());
        sst_builder.add(InnerKey::from_inner_buf(k.as_bytes()), value, None);
    }

    let mut buf = Vec::with_capacity(sst_builder.estimated_size());
    sst_builder.finish(0, &mut buf);
    let sst_file = sstable::InMemFile::new(sst_fid, buf.into());

    SsTable::new(Arc::new(sst_file), new_test_cache(), load_filter, None).unwrap()
}

#[cfg(test)]
pub(crate) fn new_table_builder_for_test(sst_fid: u64) -> Builder {
    Builder::new(sst_fid, 4096, NO_COMPRESSION, 0, None)
}

#[cfg(test)]
pub(crate) fn build_test_table_with_prefix(
    prefix: &str,
    n: usize,
    load_filter: bool,
) -> (SsTable, Vec<(String, String)>) {
    let kvs = generate_key_values(prefix, n);
    (build_test_table_with_kvs(&kvs, load_filter), kvs)
}

#[cfg(test)]
pub(crate) fn new_test_cache() -> Option<SegmentedCache<BlockCacheKey, Bytes>> {
    Some(SegmentedCache::new(1024, 4))
}

#[cfg(test)]
pub(crate) fn get_test_key(prefix: &str, i: usize) -> String {
    format!("{}{:04}", prefix, i)
}

#[cfg(test)]
pub(crate) fn create_sst_table(
    prefix: &str,
    n: usize,
    load_filter: bool,
) -> (SsTable, Vec<(String, String)>) {
    let kvs = generate_key_values(prefix, n);
    (build_test_table_with_kvs(&kvs, load_filter), kvs)
}

#[cfg(test)]
mod tests {
    use std::{iter::Iterator as StdIterator, sync::atomic::Ordering};

    use bytes::BytesMut;
    use rand::Rng;

    use super::*;
    use crate::{Iterator, GLOBAL_SHARD_END_KEY};

    fn create_multi_version_sst(mut kvs: Vec<(String, String)>) -> (SsTable, usize) {
        let sst_fid = TEST_ID_ALLOC.fetch_add(1, Ordering::Relaxed) + 1;
        let mut sst_builder = new_table_builder_for_test(sst_fid);
        kvs.sort_by(|a, b| a.0.cmp(&b.0));
        let mut all_cnt = kvs.len();
        let meta = 0u8;
        for (k, v) in &kvs {
            let val_str = format!("{}_{}", v, 9);
            let val_buf = Value::encode_buf(meta, &[0], 9, val_str.as_bytes());
            sst_builder.add(
                InnerKey::from_inner_buf(k.as_bytes()),
                &Value::decode(val_buf.as_slice()),
                None,
            );
            let mut r = rand::thread_rng();
            for i in (1..=8).rev() {
                if r.gen_range(0..4) == 0usize {
                    let val_str = format!("{}_{}", v, i);
                    let val_buf = Value::encode_buf(meta, &[0], i, val_str.as_bytes());
                    sst_builder.add(
                        InnerKey::from_inner_buf(k.as_bytes()),
                        &Value::decode(val_buf.as_slice()),
                        None,
                    );
                    all_cnt += 1;
                }
            }
        }
        let mut sst_buf = Vec::with_capacity(sst_builder.estimated_size());
        sst_builder.finish(0, &mut sst_buf);
        let sst_file = Arc::new(sstable::InMemFile::new(sst_fid, sst_buf.into()));
        (
            SsTable::new(sst_file, new_test_cache(), true, None).unwrap(),
            all_cnt,
        )
    }

    #[test]
    fn test_table_iterator() {
        for n in 99..=101 {
            let (t, _) = create_sst_table("key", n, true);
            let mut it = t.new_iterator(false, true);
            let mut count = 0;
            it.rewind();
            while it.valid() {
                let k = it.key();
                assert_eq!(k.deref(), get_test_key("key", count).as_bytes());
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(count).as_bytes());
                count += 1;
                it.next()
            }
        }
    }

    #[test]
    fn test_point_get() {
        let (t, _) = create_sst_table("key", 8000, true);
        for i in 0..8000 {
            let k = get_test_key("key", i);
            let k_h = farmhash::fingerprint64(k.as_bytes());
            let mut owned_v = vec![];
            let v = t.get(
                InnerKey::from_inner_buf(k.as_bytes()),
                u64::MAX,
                k_h,
                &mut owned_v,
                1,
            );
            assert!(!v.is_empty())
        }
        for i in 8000..10000 {
            let k = get_test_key("key", i);
            let k_h = farmhash::fingerprint64(k.as_bytes());
            let mut owned_v = vec![];
            let v = t.get(
                InnerKey::from_inner_buf(k.as_bytes()),
                u64::MAX,
                k_h,
                &mut owned_v,
                1,
            );
            assert!(v.is_empty())
        }
    }

    #[test]
    fn test_seek_to_first() {
        let nums = &[99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", *n, true);
            let mut it = t.new_iterator(false, true);
            it.rewind();
            assert!(it.valid());
            let v = it.value();
            assert_eq!(v.get_value(), get_test_value(0).as_bytes());
            assert_eq!(v.user_meta(), &[0u8]);
        }
    }

    struct TestData {
        input: &'static str,
        valid: bool,
        output: &'static str,
    }
    impl TestData {
        fn new(input: &'static str, valid: bool, output: &'static str) -> Self {
            Self {
                input,
                valid,
                output,
            }
        }
    }

    #[test]
    fn test_seek_to_last() {
        let nums = vec![99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", n, true);
            let mut it = t.new_iterator(true, true);
            it.rewind();
            assert!(it.valid());
            let v = it.value();
            assert_eq!(v.get_value(), get_test_value(n - 1).as_bytes());
            assert!(!v.is_blob_ref());
            assert_eq!(v.user_meta(), &[0u8]);
            it.next();
            assert!(it.valid());
            let v = it.value();
            assert_eq!(v.get_value(), get_test_value(n - 2).as_bytes());
            assert!(!v.is_blob_ref());
            assert_eq!(v.user_meta(), &[0u8]);
        }
    }

    #[test]
    fn test_seek_basic() {
        let test_datas: Vec<TestData> = vec![
            TestData::new("abc", true, "k0000"),
            TestData::new("k0100", true, "k0100"),
            TestData::new("k0100b", true, "k0101"),
            TestData::new("k1234", true, "k1234"),
            TestData::new("k1234b", true, "k1235"),
            TestData::new("k9999", true, "k9999"),
            TestData::new("z", false, ""),
        ];
        let (t, _) = create_sst_table("k", 10000, true);
        let mut it = t.new_iterator(false, true);
        for td in test_datas {
            it.seek(InnerKey::from_inner_buf(td.input.as_bytes()));
            if !td.valid {
                assert!(!it.valid());
                continue;
            }
            assert!(it.valid());
            assert_eq!(it.key().deref(), td.output.as_bytes());
        }
    }

    #[test]
    fn test_seek_for_prev() {
        let test_datas: Vec<TestData> = vec![
            TestData::new("abc", false, ""),
            TestData::new("k0100", true, "k0100"),
            TestData::new("k0100b", true, "k0100"),
            TestData::new("k1234", true, "k1234"),
            TestData::new("k1234b", true, "k1234"),
            TestData::new("k9999", true, "k9999"),
            TestData::new("z", true, "k9999"),
        ];
        let (t, _) = create_sst_table("k", 10000, true);
        let mut it = t.new_iterator(true, true);
        for td in test_datas {
            it.seek(InnerKey::from_inner_buf(td.input.as_bytes()));
            if !td.valid {
                assert!(!it.valid());
                continue;
            }
            assert!(it.valid());
            assert_eq!(it.key().deref(), td.output.as_bytes());
        }
    }

    #[test]
    fn test_iterate_from_start() {
        let nums = vec![99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", n, true);
            let mut it = t.new_iterator(false, true);
            let mut count = 0;
            it.rewind();
            assert!(it.valid());
            while it.valid() {
                let k = it.key();
                assert_eq!(k.deref(), get_test_key("key", count).as_bytes());
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(count).as_bytes());
                assert!(!v.is_blob_ref());
                count += 1;
                it.next()
            }
        }
    }

    #[test]
    fn test_iterate_from_end() {
        let nums = vec![99, 100, 101, 199, 200, 250, 9999, 10000];
        for n in nums {
            let (t, _) = create_sst_table("key", n, true);
            let mut it = t.new_iterator(true, true);
            it.seek(InnerKey::from_inner_buf("zzzzzz".as_bytes())); // Seek to end, an invalid element.
            assert!(it.valid());
            it.rewind();
            for i in (0..n).rev() {
                assert!(it.valid());
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(i).as_bytes());
                assert!(!v.is_blob_ref());
                it.next();
            }
            it.next();
            assert!(!it.valid());
        }
    }

    #[test]
    fn test_table() {
        let (t, _) = create_sst_table("key", 10000, true);
        let mut it = t.new_iterator(false, true);
        let mut kid = 1010_usize;
        let seek = get_test_key("key", kid);
        it.seek(InnerKey::from_inner_buf(seek.as_bytes()));
        while it.valid() {
            assert_eq!(it.key().deref(), get_test_key("key", kid).as_bytes());
            kid += 1;
            it.next()
        }
        assert_eq!(kid, 10000);

        it.seek(InnerKey::from_inner_buf(
            get_test_key("key", 99999).as_bytes(),
        ));
        assert!(!it.valid());

        it.seek(InnerKey::from_inner_buf(get_test_key("kex", 0).as_bytes()));
        assert!(it.valid());
        assert_eq!(it.key().deref(), get_test_key("key", 0).as_bytes());
    }

    #[test]
    fn test_iterate_back_and_forth() {
        let (t, _) = create_sst_table("key", 10000, true);
        let seek = get_test_key("key", 1010);
        let mut it = t.new_iterator(false, true);
        it.seek(InnerKey::from_inner_buf(seek.as_bytes()));
        assert!(it.valid());
        assert_eq!(it.key().deref(), seek.as_bytes());

        it.set_reversed(true);
        it.next();
        it.next();
        assert!(it.valid());
        assert_eq!(it.key().deref(), get_test_key("key", 1008).as_bytes());

        it.set_reversed(false);
        it.next();
        it.next();
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), get_test_key("key", 1010).as_bytes());

        it.seek(InnerKey::from_inner_buf(
            get_test_key("key", 2000).as_bytes(),
        ));
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), get_test_key("key", 2000).as_bytes());

        it.set_reversed(true);
        it.next();
        assert_eq!(it.valid(), true);
        assert_eq!(it.key().deref(), get_test_key("key", 1999).as_bytes());

        it.set_reversed(false);
        it.rewind();
        assert_eq!(it.key().deref(), get_test_key("key", 0).as_bytes());
    }

    #[test]
    fn test_iterate_multi_version() {
        let num = 4000;
        let kvs = generate_key_values("key", num);
        let (t, all_cnt) = create_multi_version_sst(kvs);
        let mut it = t.new_iterator(false, true);
        let mut it_cnt = 0;
        let mut last_key = BytesMut::new();
        it.rewind();
        while it.valid() {
            if !last_key.is_empty() {
                assert!(last_key < it.key().deref());
            }
            last_key.truncate(0);
            last_key.extend_from_slice(it.key().deref());
            it_cnt += 1;
            while it.next_version() {
                it_cnt += 1;
            }
            it.next();
        }
        assert_eq!(it_cnt, all_cnt);
        let mut r = rand::thread_rng();
        for _ in 0..1000 {
            let k = get_test_key("key", r.gen_range(0..num));
            let ver = 5 + r.gen_range(0..5) as u64;
            let k_h = farmhash::fingerprint64(k.as_bytes());
            let mut owned_v = vec![];
            let val = t.get(
                InnerKey::from_inner_buf(k.as_bytes()),
                ver,
                k_h,
                &mut owned_v,
                1,
            );
            if !val.is_empty() {
                assert!(val.version <= ver);
            }
        }
        let mut rev_it = t.new_iterator(true, true);
        last_key.truncate(0);
        rev_it.rewind();
        while rev_it.valid() {
            if !last_key.is_empty() {
                assert!(last_key > rev_it.key().deref());
            }
            last_key.truncate(0);
            last_key.extend_from_slice(rev_it.key().deref());
            rev_it.next();
        }
        for _ in 0..1000 {
            let k = get_test_key("key", r.gen_range(0..num));
            // reverse iterator never seek to the same key with smaller version.
            rev_it.seek(InnerKey::from_inner_buf(k.as_bytes()));
            if !rev_it.valid() {
                continue;
            }
            assert_eq!(rev_it.value().version, 9);
            assert!(rev_it.key().deref() <= k.as_bytes());
        }
    }

    #[test]
    fn test_uni_iterator() {
        let (t, _) = create_sst_table("key", 10000, true);
        {
            let mut it = t.new_iterator(false, true);
            let mut cnt = 0;
            it.rewind();
            while it.valid() {
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(cnt).as_bytes());
                assert!(!v.is_blob_ref());
                cnt += 1;
                it.next();
            }
            assert_eq!(cnt, 10000);
        }
        {
            let mut it = t.new_iterator(true, true);
            let mut cnt = 0;
            it.rewind();
            while it.valid() {
                let v = it.value();
                assert_eq!(v.get_value(), get_test_value(10000 - 1 - cnt).as_bytes());
                assert!(!v.is_blob_ref());
                cnt += 1;
                it.next();
            }
        }
    }

    #[test]
    fn test_get_split_keys() {
        let cases = vec![
            (
                10000,               // number of kvs
                49,                  // expected number of blocks, about 200 kvs per block
                None,                // range start,
                None,                // range end,
                3,                   // split count
                vec![0, 3514, 6767], // expected evenly split keys
                Some(5136),          // expected suggest split key
            ),
            (
                10000,
                49,
                None,
                None,
                10,
                vec![0, 1072, 2088, 3104, 4120, 5136, 6152, 7168, 8184, 9209],
                Some(5136),
            ),
            (
                10000,
                49,
                Some(2000),
                Some(8000),
                10,
                vec![2088, 2703, 3309, 3924, 4530, 5136, 5751, 6357, 6972, 7578],
                Some(5136),
            ),
            (1, 1, None, None, 1, vec![0], None), // 1 block
            (1, 1, None, None, 2, vec![0], None),
            (1, 1, None, None, 3, vec![0], None),
            (300, 2, None, None, 1, vec![0], Some(222)), // 2 blocks
            (300, 2, None, None, 2, vec![0, 222], Some(222)),
            (300, 2, None, None, 3, vec![0, 222], Some(222)),
            (500, 3, None, None, 1, vec![0], Some(438)), // 3 blocks
            (500, 3, None, None, 2, vec![0, 438], Some(438)),
            (500, 3, None, None, 3, vec![0, 222, 438], Some(438)),
            (500, 3, None, None, 4, vec![0, 222, 438], Some(438)),
            (2000, 10, None, None, 3, vec![0, 870, 1482], Some(1072)), // 10 blocks
            (
                2000,
                10,
                None,
                None,
                4,
                vec![0, 654, 1277, 1687],
                Some(1072),
            ),
            (
                2000,
                10,
                None,
                None,
                8,
                vec![0, 438, 870, 1072, 1277, 1482, 1687, 1892],
                Some(1072),
            ),
            (2000, 10, Some(2000), Some(3000), 4, vec![], None), // with range
            (
                2000,
                10,
                Some(1000),
                Some(3000),
                4,
                vec![1072, 1482, 1687, 1892],
                Some(1687),
            ),
        ];

        let prefix = "key";
        for (i, (n, blocks, start, end, count, evenly_split_keys, suggest_split_key)) in
            cases.into_iter().enumerate()
        {
            let (sst, _) = create_sst_table(prefix, n, true);
            assert_eq!(sst.load_index().num_blocks(), blocks);

            let start = start.map(|x| get_test_key(prefix, x));
            let start = start
                .as_ref()
                .map(|x| InnerKey::from_inner_buf(x.as_bytes()));
            let end = end.map(|x| get_test_key(prefix, x));
            let end = end.as_ref().map(|x| InnerKey::from_inner_buf(x.as_bytes()));

            let evenly_split_keys = evenly_split_keys
                .into_iter()
                .map(|x| get_test_key(prefix, x).as_bytes().to_vec())
                .collect::<Vec<_>>();
            assert_eq!(
                sst.get_evenly_split_keys(start, end, count),
                evenly_split_keys,
                "case {}",
                i
            );

            let suggest_split_key = suggest_split_key
                .map(|x| Bytes::copy_from_slice(get_test_key(prefix, x).as_bytes()));
            assert_eq!(
                sst.get_suggest_split_key(start, end),
                suggest_split_key,
                "{}",
                i
            );
        }
    }

    #[test]
    fn test_estimated_size_in_range() {
        let cases = vec![
            (
                10000, // number of kvs
                49,    // expected number of blocks, about 200 kvs per block
                None,  // range start,
                None,  // range end,
                1.0,   // expect ratio of estimated size to file size.
            ),
            (10000, 49, Some(2000), Some(8000), 0.6),
            (10000, 49, Some(2000), None, 0.8),
            (10000, 49, None, Some(2000), 0.2),
            (1, 1, None, None, 1.0), // 1 block
            (1, 1, Some(0), None, 1.0),
            (1, 1, None, Some(1), 1.0),
            (1, 1, None, Some(0), 0.0),
            (300, 2, None, None, 1.0), // 2 blocks
            (300, 2, Some(100), None, 0.5),
            (300, 2, Some(200), None, 0.5),
            (300, 2, Some(250), None, 0.0),
            (2000, 10, None, None, 1.0), // 10 blocks
            (2000, 10, Some(2000), Some(3000), 0.0),
            (2000, 10, Some(1000), Some(3000), 0.5),
        ];

        let prefix = "key";
        for (i, (n, blocks, start, end, ratio)) in cases.into_iter().enumerate() {
            let (sst, _) = create_sst_table(prefix, n, true);
            assert_eq!(sst.load_index().num_blocks(), blocks);

            let start = start.map_or(b"".to_vec(), |x| get_test_key(prefix, x).into_bytes());
            let start = InnerKey::from_inner_buf(&start);
            let end = end.map_or(GLOBAL_SHARD_END_KEY.to_vec(), |x| {
                get_test_key(prefix, x).into_bytes()
            });
            let end = InnerKey::from_inner_buf(&end);

            let estimated_size = sst.estimated_size_in_range(start, end) as f64;
            let expect = sst.size() as f64 * ratio;
            assert!(
                (estimated_size - expect).abs() <= 0.05 * expect,
                "case {}, estimated_size {}, expect {}, sst.size {}",
                i,
                estimated_size,
                expect,
                sst.size(),
            );
        }
    }

    #[bench]
    fn bench_decode_filter(b: &mut test::Bencher) {
        let (t, _) = create_sst_table("key", 10000, true);
        let data = t.read_filter_data_from_file().unwrap();
        b.iter(|| {
            test::black_box(t.decode_filter(&data).unwrap());
        });
    }
}
