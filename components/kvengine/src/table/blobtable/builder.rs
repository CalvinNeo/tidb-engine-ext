// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, slice};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{BufMut, Bytes, BytesMut};

use crate::table::{
    sstable::{LZ4_COMPRESSION, NO_COMPRESSION, ZSTD_COMPRESSION},
    Value,
};

pub type ValueLength = u32; // Max value length is 4GB
pub type BlobOffset = u32; // Max blob file size is 4GB
pub type Checksum = u32;

pub const BLOB_FORMAT_V1: u16 = 1;
pub const BLOB_MAGIC_NUMBER: u32 = 0xdeadbeef;
pub const CRC32C: u8 = 1;
pub const PROP_KEY_BIGGEST: &str = "biggest_key";
pub const PROP_KEY_SMALLEST: &str = "smallest_key";

// Blob file format:
//
// +---------------------------------+
// |          blob record 1          |
// +---------------------------------+
// |          blob record 2          |
// +---------------------------------+
// |             ...                 |
// +---------------------------------+
// |          blob record n          |
// +---------------------------------+
// |         blob properties         |
// +---------------------------------+
// |           blob footer           |
// +---------------------------------+
//
// Blob record format:
//
// +---------------------------------+
// |          checksum: u32          |
// +---------------------------------+
// |        value length: u32        |
// +---------------------------------+
// |          value: bytes           |
// +---------------------------------+

#[repr(C)]
#[derive(Default, Clone, Copy)]
pub struct BlobFooter {
    pub properties_offset: u32,
    pub version: u64,
    pub total_blob_size: u64,
    pub compression_type: u8,
    pub checksum_type: u8,
    pub blob_format_version: u16,
    pub compression_lvl: i32,
    pub min_blob_size: u32,
    pub magic: u32,
}

pub const BLOB_TABLE_FOOTER_SIZE: usize = mem::size_of::<BlobFooter>();
pub const BLOB_ENTRY_META_SIZE: usize = mem::size_of::<Checksum>() + mem::size_of::<ValueLength>();
pub const BLOB_ENTRY_LENGTH_OFFSET: usize = mem::size_of::<Checksum>();
pub const BLOB_ENTRY_VALUE_OFFSET: usize = BLOB_ENTRY_META_SIZE;

impl BlobFooter {
    pub fn data_len(&self) -> usize {
        self.properties_offset as usize
    }

    pub fn properties_len(&self, table_size: usize) -> usize {
        table_size - BLOB_TABLE_FOOTER_SIZE - self.properties_offset as usize
    }

    pub fn unmarshal(&mut self, data: &[u8]) {
        let footer_ptr = data.as_ptr() as *const BlobFooter;
        *self = unsafe { *footer_ptr };
    }

    pub fn marshal(&self) -> &[u8] {
        let footer_ptr = self as *const BlobFooter as *const u8;
        unsafe { slice::from_raw_parts(footer_ptr, BLOB_TABLE_FOOTER_SIZE) }
    }
}

#[derive(Default)]
pub struct BlobTableBuilder {
    fid: u64,
    buf: Vec<u8>,
    checksum_tp: u8,
    compression_tp: u8,
    compression_lvl: i32,
    min_blob_size: u32,
    total_blob_size: u64,
    smallest_key: Vec<u8>,
    biggest_key: Vec<u8>,
}

impl BlobTableBuilder {
    pub fn new(
        fid: u64,
        checksum_tp: u8,
        compression_tp: u8,
        compression_lvl: i32,
        min_blob_size: u32,
    ) -> Self {
        Self {
            fid,
            buf: vec![],
            checksum_tp,
            compression_tp,
            compression_lvl,
            min_blob_size,
            total_blob_size: 0,
            smallest_key: vec![],
            biggest_key: vec![],
        }
    }

    pub fn reset(&mut self, fid: u64) {
        self.fid = fid;
        self.buf.clear();
        self.total_blob_size = 0;
        self.smallest_key.clear();
        self.biggest_key.clear();
        self.smallest_key.clear();
    }

    pub fn add_blob(
        &mut self,
        key: &[u8],
        blob: &[u8],
        need_compress_blob: bool,
    ) -> (BlobOffset, ValueLength) {
        assert!(blob.len() >= self.min_blob_size as usize);
        assert!(blob.len() <= ValueLength::max_value() as usize);
        assert!(self.total_blob_size as usize + blob.len() <= BlobOffset::max_value() as usize);
        if self.smallest_key.is_empty() || self.smallest_key.as_slice() > key {
            self.smallest_key.clear();
            self.smallest_key.extend_from_slice(key);
        }
        if self.biggest_key.is_empty() || self.biggest_key.as_slice() < key {
            self.biggest_key.clear();
            self.biggest_key.extend_from_slice(key);
        }
        self.total_blob_size += blob.len() as u64;

        let begin_off = self.buf.len();
        self.buf.resize(self.buf.len() + BLOB_ENTRY_META_SIZE, 0);
        let compressed_len = if need_compress_blob {
            match self.compression_tp {
                NO_COMPRESSION => {
                    self.buf.extend_from_slice(blob);
                    blob.len()
                }
                LZ4_COMPRESSION => Self::compress_lz4(blob, &mut self.buf),
                ZSTD_COMPRESSION => Self::compress_zstd(blob, self.compression_lvl, &mut self.buf),
                _ => panic!("unexpected compression type {}", self.compression_tp),
            }
        } else {
            self.buf.extend_from_slice(blob);
            blob.len()
        };

        let mut checksum = 0u32;
        if self.checksum_tp == CRC32C {
            checksum = crc32c::crc32c(&self.buf[(begin_off + BLOB_ENTRY_VALUE_OFFSET)..]);
        }
        let slice = self.buf.as_mut_slice();
        LittleEndian::write_u32(&mut slice[begin_off..], checksum); // put checksum at the reserved place.
        LittleEndian::write_u32(
            &mut slice[begin_off + BLOB_ENTRY_LENGTH_OFFSET..],
            compressed_len as ValueLength,
        ); // put compressed length at the reserved place.
        (begin_off as BlobOffset, compressed_len as ValueLength)
    }

    pub fn add(&mut self, key: &[u8], value: &Value) -> (BlobOffset, ValueLength) {
        self.add_blob(key, value.get_value(), true)
    }

    fn compress_lz4(uncompressed: &[u8], compressed_buf: &mut Vec<u8>) -> usize {
        unsafe {
            let uncompressed_len = uncompressed.len() as i32;
            let compress_bound = lz4::liblz4::LZ4_compressBound(uncompressed_len);
            let original_len = compressed_buf.len();
            compressed_buf.resize(original_len + compress_bound as usize, 0);
            let dst = &mut compressed_buf[original_len..];
            let size = lz4::liblz4::LZ4_compress_default(
                uncompressed.as_ptr() as *const libc::c_char,
                dst.as_mut_ptr() as *mut libc::c_char,
                uncompressed_len,
                compress_bound,
            ) as usize;
            compressed_buf.set_len(original_len + size);
            size
        }
    }

    fn compress_zstd(
        uncompressed: &[u8],
        compression_lvl: i32,
        compressed_buf: &mut Vec<u8>,
    ) -> usize {
        unsafe {
            let uncompressed_len = uncompressed.len();
            let compress_bound = zstd_sys::ZSTD_compressBound(uncompressed_len);
            let original_len = compressed_buf.len();
            compressed_buf.resize(original_len + compress_bound, 0);
            let dst = &mut compressed_buf[original_len..];
            let size = zstd_sys::ZSTD_compress(
                dst.as_mut_ptr() as *mut libc::c_void,
                compress_bound,
                uncompressed.as_ptr() as *const libc::c_void,
                uncompressed_len,
                compression_lvl as libc::c_int,
            );
            compressed_buf.set_len(original_len + size);
            size
        }
    }

    fn add_property(buf: &mut BytesMut, key: &[u8], val: &[u8]) {
        buf.put_u16_le(key.len() as u16);
        buf.put_slice(key);
        buf.put_u32_le(val.len() as u32);
        buf.put_slice(val);
    }

    // Smallest and biggest key are used to indicate the key range of the blob file.
    pub fn finish(&self) -> Bytes {
        let mut buf = BytesMut::from(&self.buf[..]);

        let properties_offset = buf.len();
        BlobTableBuilder::add_property(&mut buf, PROP_KEY_SMALLEST.as_bytes(), &self.smallest_key);
        BlobTableBuilder::add_property(&mut buf, PROP_KEY_BIGGEST.as_bytes(), &self.biggest_key);

        let mut footer = BlobFooter::default();
        footer.properties_offset = properties_offset as u32;
        footer.total_blob_size = self.total_blob_size;
        footer.compression_type = self.compression_tp;
        footer.checksum_type = self.checksum_tp;
        footer.blob_format_version = BLOB_FORMAT_V1;
        footer.compression_lvl = self.compression_lvl;
        footer.min_blob_size = self.min_blob_size;
        footer.magic = BLOB_MAGIC_NUMBER;
        buf.extend_from_slice(footer.marshal());
        buf.freeze()
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn get_fid(&self) -> u64 {
        self.fid
    }

    pub fn smallest_biggest_key(&self) -> (&[u8], &[u8]) {
        (&self.smallest_key, &self.biggest_key)
    }

    pub fn total_blob_size(&self) -> u64 {
        self.total_blob_size
    }
}
