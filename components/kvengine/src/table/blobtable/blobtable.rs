// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.
use std::{cmp, collections::HashMap, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes};

use super::builder::*;
use crate::table::{
    sstable::{File, LZ4_COMPRESSION, NO_COMPRESSION, ZSTD_COMPRESSION},
    Error, Result,
};

#[derive(Clone)]
pub struct BlobTable {
    file: Option<Arc<dyn File>>,
    preloaded_data: Option<Bytes>,
    footer: BlobFooter,
    smallest_key: Bytes,
    biggest_key: Bytes,
}

impl BlobTable {
    pub fn new(file: Arc<dyn File>) -> Result<Self> {
        let mut footer = BlobFooter::default();
        let size = file.size();
        if size < BLOB_TABLE_FOOTER_SIZE as u64 {
            return Err(Error::InvalidFileSize);
        }
        let footer_data =
            file.read(size - BLOB_TABLE_FOOTER_SIZE as u64, BLOB_TABLE_FOOTER_SIZE)?;
        footer.unmarshal(&footer_data);
        let props_data = file.read(
            footer.properties_offset as u64,
            footer.properties_len(size as usize),
        )?;
        let mut prop_slice = props_data.chunk();
        let mut smallest_key = Bytes::new();
        let mut biggest_key = Bytes::new();
        while !prop_slice.is_empty() {
            let (key, val, remain) = parse_prop_data(prop_slice);
            prop_slice = remain;
            if key == PROP_KEY_SMALLEST.as_bytes() {
                smallest_key = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_BIGGEST.as_bytes() {
                biggest_key = Bytes::copy_from_slice(val);
            }
        }
        Ok(Self {
            file: Some(file),
            preloaded_data: None,
            footer,
            smallest_key,
            biggest_key,
        })
    }

    pub fn from_bytes(bytes: Bytes) -> Result<Self> {
        let mut footer = BlobFooter::default();
        let size = bytes.len();
        if size < BLOB_TABLE_FOOTER_SIZE {
            return Err(Error::InvalidFileSize);
        }
        let footer_data = &bytes[size - BLOB_TABLE_FOOTER_SIZE..size];
        footer.unmarshal(footer_data);
        let mut props_data = &bytes[footer.properties_offset as usize
            ..footer.properties_offset as usize + footer.properties_len(size)];
        let mut smallest_key = Bytes::new();
        let mut biggest_key = Bytes::new();
        while !props_data.is_empty() {
            let (key, val, remain) = parse_prop_data(props_data);
            props_data = remain;
            if key == PROP_KEY_SMALLEST.as_bytes() {
                smallest_key = Bytes::copy_from_slice(val);
            } else if key == PROP_KEY_BIGGEST.as_bytes() {
                biggest_key = Bytes::copy_from_slice(val);
            }
        }
        Ok(Self {
            file: None,
            preloaded_data: Some(bytes),
            footer,
            smallest_key,
            biggest_key,
        })
    }

    pub fn get(&self, offset: u32, size: u32) -> Result<Vec<u8>> {
        let data = self
            .file
            .as_ref()
            .unwrap_or_else(|| panic!("file is not set"))
            .read(offset as u64, size as usize + BLOB_ENTRY_META_SIZE)?;
        let mut decompressed = Vec::with_capacity(size as usize);
        if self.decompress(&data, size, &mut decompressed)? {
            Ok(decompressed)
        } else {
            Ok(data[BLOB_ENTRY_VALUE_OFFSET..].to_vec())
        }
    }

    // If data is compressed and the caller want it to be decpressed, the buf will
    // be used to store decompressed data, and the returned slice is a reference
    // to the decompressed data. Otherwise, the returned slice is a reference to
    // the original data.
    pub fn get_from_preloaded<'a>(
        &'a self,
        offset: u32,
        size: u32,
        need_decompress: bool,
        buf: &'a mut Vec<u8>,
    ) -> Result<&[u8]> {
        let data = &self.preloaded_data.as_ref().unwrap()
            [offset as usize..offset as usize + BLOB_ENTRY_VALUE_OFFSET + size as usize];
        if need_decompress && self.decompress(data, size, buf)? {
            Ok(buf.as_slice())
        } else {
            Ok(&data[BLOB_ENTRY_VALUE_OFFSET..])
        }
    }

    pub fn decompress(
        &self,
        data: &[u8],
        size: u32,
        decompressed_buf: &mut Vec<u8>,
    ) -> Result<bool> {
        let total_data_size = size as usize + BLOB_ENTRY_META_SIZE;
        assert!(data.len() >= total_data_size);
        let checksum = LittleEndian::read_u32(data);
        let compressed_len = LittleEndian::read_u32(&data[BLOB_ENTRY_LENGTH_OFFSET..]);
        assert_eq!(compressed_len, size);
        let compressed_data =
            &data[BLOB_ENTRY_VALUE_OFFSET..BLOB_ENTRY_VALUE_OFFSET + size as usize];
        if self.footer.checksum_type == CRC32C && checksum != crc32c::crc32c(compressed_data) {
            return Err(Error::InvalidChecksum("blob checkusm mismatch".to_owned()));
        }
        return match self.footer.compression_type {
            NO_COMPRESSION => Ok(false), // in place decoding
            LZ4_COMPRESSION => unsafe {
                let decompressed = lz4::block::decompress(compressed_data, None)?;
                if decompressed_buf.capacity() < decompressed.len() {
                    decompressed_buf.reserve(decompressed.len() - decompressed_buf.capacity());
                }
                decompressed_buf.copy_from_slice(&decompressed);
                decompressed_buf.set_len(decompressed.len());
                Ok(true)
            },
            ZSTD_COMPRESSION => unsafe {
                let capacity = zstd_sys::ZSTD_getFrameContentSize(
                    compressed_data.as_ptr() as *const libc::c_void,
                    compressed_data.len(),
                ) as usize;
                if decompressed_buf.capacity() < capacity {
                    decompressed_buf.reserve(capacity - decompressed_buf.capacity());
                }
                let result = zstd_sys::ZSTD_decompress(
                    decompressed_buf.as_mut_ptr() as *mut libc::c_void,
                    capacity,
                    compressed_data.as_ptr() as *const libc::c_void,
                    compressed_data.len(),
                );
                assert_eq!(zstd_sys::ZSTD_isError(result), 0u32);
                decompressed_buf.set_len(capacity);
                Ok(true)
            },
            _ => panic!("unknown compression type {}", self.footer.compression_type),
        };
    }

    pub fn id(&self) -> u64 {
        self.file
            .as_ref()
            .unwrap_or_else(|| panic!("file is not set"))
            .id()
    }

    pub fn version(&self) -> u64 {
        self.footer.version
    }

    pub fn smallest_key(&self) -> &[u8] {
        &self.smallest_key
    }

    pub fn biggest_key(&self) -> &[u8] {
        &self.biggest_key
    }

    pub fn size(&self) -> u64 {
        self.file
            .as_ref()
            .unwrap_or_else(|| panic!("file is not set"))
            .size()
    }
    pub fn smallest_biggest_key(&self) -> (&[u8], &[u8]) {
        (self.smallest_key.chunk(), self.biggest_key.chunk())
    }

    pub fn total_blob_size(&self) -> u64 {
        self.footer.total_blob_size
    }

    pub fn compression_tp(&self) -> u8 {
        self.footer.compression_type
    }

    pub fn compression_lvl(&self) -> i32 {
        self.footer.compression_lvl
    }

    pub fn min_blob_size(&self) -> u32 {
        self.footer.min_blob_size
    }
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

pub struct BlobPrefetcher {
    tables: Arc<HashMap<u64, BlobTable>>,
    tbl_buffers: HashMap<u64, (u32, Vec<u8>)>,
    prefetch_size: usize,
    decompressed_buffer: Vec<u8>,
}

impl BlobPrefetcher {
    pub fn new(tables: Arc<HashMap<u64, BlobTable>>, prefetch_size: usize) -> Self {
        Self {
            tables,
            tbl_buffers: Default::default(),
            prefetch_size,
            decompressed_buffer: vec![],
        }
    }

    pub fn get(&mut self, fid: u64, offset: u32, size: u32) -> Result<&[u8]> {
        let blob_table = self
            .tables
            .get(&fid)
            .ok_or_else(|| Error::Other(format!("blob table not found, fid: {}", fid)))?;
        let data_size = size as usize + BLOB_ENTRY_META_SIZE;
        let (buffer_offset, buffer) = self
            .tbl_buffers
            .entry(fid)
            .or_insert_with(|| (offset, vec![]));
        if !(offset >= *buffer_offset
            && offset + data_size as u32 <= *buffer_offset + buffer.len() as u32)
        {
            let file = blob_table.file.as_ref().unwrap_or_else(|| {
                panic!(
                    "blob table file not set, blob table id: {}",
                    blob_table.id()
                )
            });
            let len = cmp::min(
                cmp::max(self.prefetch_size, data_size),
                file.size() as usize - offset as usize,
            );
            if len != buffer.len() {
                buffer.resize(len, 0);
            }
            file.read_at(buffer, offset as u64)?;
            *buffer_offset = offset;
        }
        let data = &buffer[(offset - *buffer_offset) as usize..];
        if blob_table.decompress(data, size, &mut self.decompressed_buffer)? {
            return Ok(&self.decompressed_buffer);
        }
        Ok(
            &buffer[(offset - *buffer_offset) as usize + BLOB_ENTRY_VALUE_OFFSET
                ..(offset - *buffer_offset) as usize + data_size],
        )
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use rand::{distributions::Alphanumeric, rngs::ThreadRng, Rng};

    use super::BlobTable;
    use crate::table::{sstable, ExternalLink, Value};

    fn get_blob_text(max_len: usize, rng: &mut ThreadRng) -> String {
        let len = rng.gen_range(1..max_len);
        rng.sample_iter(&Alphanumeric)
            .take(len)
            .map(char::from)
            .collect()
    }

    struct TestData {
        blob: String,
        external_link: ExternalLink,
    }

    #[test]
    fn test_basic() {
        let mut rng = rand::thread_rng();
        let mut builder =
            super::BlobTableBuilder::new(0, 0, sstable::builder::NO_COMPRESSION, 0, 0);
        let mut test_data = Vec::new();
        let meta: u8 = 0;

        for i in 0..128 {
            let key = format!("key_{:03}", i);
            let blob = get_blob_text(64, &mut rng);
            let encoded = Value::encode_buf(meta, &[0], 0, blob.as_bytes());
            let value = Value::decode(encoded.as_slice());
            // In this test assume that all values are converted to external links.
            let mut external_link = ExternalLink::new();
            let (offset, len) = builder.add(key.as_bytes(), &value);
            external_link.len = len;
            external_link.offset = offset;

            test_data.push(TestData {
                blob,
                external_link,
            });
        }

        let file = sstable::InMemFile::new(1, builder.finish());
        let table = super::BlobTable::new(Arc::new(file)).unwrap();

        for td in test_data {
            let blob = table
                .get(td.external_link.offset, td.external_link.len)
                .unwrap();
            assert_eq!(td.blob.as_bytes(), blob);
        }

        assert_eq!(table.smallest_key, format!("key_{:03}", 0));
        assert_eq!(table.biggest_key, format!("key_{:03}", 127));
    }

    #[test]
    fn test_prefetcher() {
        let mut builder =
            super::BlobTableBuilder::new(0, 0, sstable::builder::NO_COMPRESSION, 0, 0);
        let mut offsets = Vec::new();
        for i in 0..100 {
            let key_str = format!("key_{:03}", i);
            let val_str = format!("val_{:03}", i);
            let val_buf = Value::encode_buf(b'A', &[0], 0, val_str.as_bytes());
            let (offset, _) = builder.add(key_str.as_bytes(), &Value::decode(val_buf.as_slice()));
            offsets.push(offset);
        }
        let file = sstable::InMemFile::new(1, builder.finish());
        let table = super::BlobTable::new(Arc::new(file)).unwrap();
        let blob_tables: HashMap<u64, BlobTable> = [(1, table)].into();
        let mut prefetcher = super::BlobPrefetcher::new(Arc::new(blob_tables), 1000);
        for i in 0..100 {
            let expected_val = format!("val_{:03}", i);
            let val = prefetcher
                .get(1, offsets[i], expected_val.len() as u32)
                .unwrap();
            assert_eq!(val, expected_val.as_bytes());
        }
    }
}
