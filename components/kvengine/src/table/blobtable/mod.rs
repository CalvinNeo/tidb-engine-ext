// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

pub mod blobtable;
pub mod builder;

use std::mem::size_of;

use byteorder::{ByteOrder, LittleEndian};

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd)]
pub struct BlobRef {
    /// ID of file where the data is stored.
    pub(crate) fid: u64,
    /// Absolute offset within the file. Apparently SST max size is <= 4GB.
    pub(crate) offset: builder::BlobOffset,
    /// Compressed value length.
    pub(crate) len: builder::ValueLength,
    /// Original value length.
    pub(crate) original_len: builder::ValueLength,
}

impl BlobRef {
    pub(crate) fn new(
        fid: u64,
        offset: builder::BlobOffset,
        len: builder::ValueLength,
        original_len: builder::ValueLength,
    ) -> Self {
        Self {
            fid,
            offset,
            original_len,
            len,
        }
    }

    pub(crate) fn deserialize(buf: &[u8]) -> Self {
        let mut offset: usize = 0;
        let fid = LittleEndian::read_u64(&buf[offset..]);
        offset += size_of::<u64>();
        let file_offset = LittleEndian::read_u32(&buf[offset..]);
        offset += size_of::<builder::BlobOffset>();
        let len = LittleEndian::read_u32(&buf[offset..]);
        offset += size_of::<builder::ValueLength>();
        let original_len = LittleEndian::read_u32(&buf[offset..]);
        Self {
            fid,
            offset: file_offset,
            len,
            original_len,
        }
    }

    pub(crate) fn serialize(&self, buf: &mut [u8]) {
        let mut offset: usize = 0;
        LittleEndian::write_u64(&mut buf[offset..], self.fid);
        offset += size_of::<u64>();
        LittleEndian::write_u32(&mut buf[offset..], self.offset);
        offset += size_of::<builder::BlobOffset>();
        LittleEndian::write_u32(&mut buf[offset..], self.len);
        offset += size_of::<builder::ValueLength>();
        LittleEndian::write_u32(&mut buf[offset..], self.original_len);
    }
}
