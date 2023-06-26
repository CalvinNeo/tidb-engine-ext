// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{io, mem::size_of, ptr, result, slice};

use byteorder::{ByteOrder, LittleEndian};
use thiserror::Error;

use super::blobtable::BlobRef;
use crate::dfs;

#[derive(Serialize, Deserialize, Debug)]
pub struct Row {
    pub key: Vec<u8>,
    pub user_meta: crate::UserMeta,
    pub value: Vec<u8>,
}

pub trait Iterator: Send {
    // next returns the next entry with different key on the latest version.
    // If old version is needed, call next_version.
    fn next(&mut self);

    // next_version set the current entry to an older version.
    // The iterator must be valid to call this method.
    // It returns true if there is an older version, returns false if there is no
    // older version. The iterator is still valid and on the same key.
    fn next_version(&mut self) -> bool;

    fn rewind(&mut self);

    fn seek(&mut self, key: &[u8]);

    fn key(&self) -> &[u8];

    fn value(&self) -> Value;

    fn valid(&self) -> bool;

    fn seek_to_version(&mut self, version: u64) -> bool {
        if version >= self.value().version {
            return true;
        }
        while self.next_version() {
            if version >= self.value().version {
                return true;
            }
        }
        false
    }

    fn next_all_version(&mut self) {
        if !self.next_version() {
            self.next()
        }
    }
}

pub const BIT_DELETE: u8 = 1;
pub const BIT_HAS_OLD_VERSION: u8 = 2;
pub const BIT_BLOB_REF: u8 = 4;

pub fn is_deleted(meta: u8) -> bool {
    meta & BIT_DELETE > 0
}

pub fn is_blob_ref(meta: u8) -> bool {
    meta & BIT_BLOB_REF > 0
}

pub fn is_old_version(meta: u8) -> bool {
    meta & BIT_HAS_OLD_VERSION > 0
}

/// [ meta: u8, user_meta_len: u8]
pub const VALUE_VERSION_OFF: usize = 2;

// Size in bytes of the version field.
pub const VALUE_VERSION_LEN: usize = std::mem::size_of::<u64>();

pub struct Version(pub u64);

impl Version {
    pub fn serialize(buf: &mut [u8], version: u64) -> usize {
        LittleEndian::write_u64(buf, version);
        size_of::<u64>()
    }

    pub fn deserialize(buf: &[u8]) -> u64 {
        LittleEndian::read_u64(buf)
    }
}

unsafe impl Send for Value {}

// Value is a short life struct used to pass value across iterators.
// It is valid until iterator call next or next_version.
// As long as the value is never escaped, there will be no dangling pointer.
#[derive(Debug, Copy, Clone)]
pub struct Value {
    /// Points to start of user_meta_len at offset VALUE_VERSION_OFF +
    /// serialized(version).
    ptr: *const u8,
    /// Bit flags
    pub meta: u8,
    /// User defined opaque meta data,
    user_meta_len: u8,
    /// Length of the data.
    len: u32,
    /// The row version
    pub version: u64,

    blob_ptr: *const u8,
}

impl Value {
    pub(crate) fn new() -> Self {
        Self {
            ptr: ptr::null(),
            meta: Default::default(),
            user_meta_len: Default::default(),
            len: Default::default(),
            version: Default::default(),
            blob_ptr: ptr::null(),
        }
    }

    fn encode_preamble(buf: &mut [u8], meta: u8, version: u64, user_meta: &[u8]) -> usize {
        buf[0] = meta;
        buf[1] = user_meta.len() as u8;
        let mut offset = VALUE_VERSION_OFF;
        offset += Version::serialize(&mut buf[offset..], version);
        if !user_meta.is_empty() {
            buf[offset..offset + user_meta.len()].copy_from_slice(user_meta);
        }
        offset + user_meta.len()
    }

    pub fn encode_buf(meta: u8, user_meta: &[u8], version: u64, val: &[u8]) -> Vec<u8> {
        assert!(!is_blob_ref(meta));
        let mut buf = vec![0; VALUE_VERSION_OFF + VALUE_VERSION_LEN + user_meta.len() + val.len()];
        let offset = Self::encode_preamble(&mut buf, meta, version, user_meta);
        let buf_slice = buf.as_mut_slice();
        buf_slice[offset..offset + val.len()].copy_from_slice(val);
        buf
    }

    /// Encode the contents in buf.
    pub(crate) fn encode(&self, buf: &mut [u8]) {
        let offset = Self::encode_preamble(buf, self.meta, self.version, self.user_meta());
        unsafe {
            ptr::copy(
                self.ptr.add(self.user_meta_len()),
                buf[offset..].as_mut_ptr(),
                self.value_len(),
            );
        }
    }

    pub fn encode_with_blob_ref(&self, buf: &mut [u8], blob_ref: BlobRef) {
        assert!(self.is_blob_ref());
        let offset = Self::encode_preamble(buf, self.meta, self.version, self.user_meta());
        blob_ref.serialize(&mut buf[offset..offset + size_of::<BlobRef>()]);
    }

    pub fn decode(buf: &[u8]) -> Self {
        let meta = buf[0];
        let user_meta_len = buf[1];
        let mut offset = VALUE_VERSION_OFF;
        let version = Version::deserialize(&buf[offset..]);
        offset += VALUE_VERSION_LEN;
        Self {
            ptr: buf[offset..].as_ptr(),
            meta,
            user_meta_len,
            len: (buf.len() - offset - user_meta_len as usize) as u32,
            version,
            blob_ptr: ptr::null(),
        }
    }

    pub(crate) fn new_with_meta_version(
        meta: u8,
        version: u64,
        user_meta_len: u8,
        buf: &[u8],
    ) -> Self {
        assert!(buf.len() >= user_meta_len as usize);
        Self {
            ptr: buf.as_ptr(),
            meta,
            user_meta_len,
            len: buf.len() as u32 - user_meta_len as u32,
            version,
            blob_ptr: ptr::null(),
        }
    }

    pub(crate) fn new_tombstone(version: u64) -> Self {
        Self {
            ptr: ptr::null(),
            meta: BIT_DELETE,
            user_meta_len: 0,
            len: 0,
            version,
            blob_ptr: ptr::null(),
        }
    }

    #[inline(always)]
    pub(crate) fn set_blob_ref(&mut self) {
        assert!(!self.is_blob_ref());
        self.meta |= BIT_BLOB_REF;
    }

    #[inline(always)]
    pub(crate) fn is_empty(&self) -> bool {
        self.meta == 0 && self.ptr.is_null()
    }

    #[inline(always)]
    pub(crate) fn is_valid(&self) -> bool {
        !self.is_empty()
    }

    #[inline(always)]
    pub(crate) fn is_deleted(&self) -> bool {
        is_deleted(self.meta)
    }

    #[inline(always)]
    pub(crate) fn is_blob_ref(&self) -> bool {
        is_blob_ref(self.meta)
    }

    #[inline(always)]
    pub fn value_len(&self) -> usize {
        self.len as usize
    }

    #[inline(always)]
    pub fn user_meta_len(&self) -> usize {
        self.user_meta_len as usize
    }

    #[inline(always)]
    pub fn user_meta(&self) -> &[u8] {
        unsafe { slice::from_raw_parts::<u8>(self.ptr, self.user_meta_len()) }
    }

    #[inline(always)]
    pub fn is_value_empty(&self) -> bool {
        self.value_len() == 0
    }

    #[inline(always)]
    pub fn get_value(&self) -> &[u8] {
        unsafe {
            if self.blob_ptr.is_null() {
                slice::from_raw_parts::<u8>(self.ptr.add(self.user_meta_len()), self.value_len())
            } else {
                slice::from_raw_parts::<u8>(self.blob_ptr, self.value_len())
            }
        }
    }

    #[inline(always)]
    fn encoded_size_of_preamble(&self) -> usize {
        VALUE_VERSION_OFF + size_of::<u64>() + self.user_meta_len()
    }

    #[inline(always)]
    pub fn encoded_size(&self) -> usize {
        self.encoded_size_of_preamble() + self.value_len()
    }

    #[inline(always)]
    pub fn encoded_size_with_blob_ref(&self) -> usize {
        self.encoded_size_of_preamble() + size_of::<BlobRef>()
    }

    pub fn get_blob_ref(&self) -> BlobRef {
        assert!(self.is_blob_ref());
        BlobRef::deserialize(self.get_value())
    }

    #[inline(always)]
    pub fn get_version(buf: &[u8]) -> u64 {
        Version::deserialize(buf)
    }

    pub fn fill_in_blob(&mut self, blob: &[u8]) {
        assert!(self.is_blob_ref());
        self.blob_ptr = blob.as_ptr();
        self.len = blob.len() as u32;
    }
}

pub struct EmptyIterator;

impl Iterator for EmptyIterator {
    fn next(&mut self) {}

    fn next_version(&mut self) -> bool {
        false
    }

    fn rewind(&mut self) {}

    fn seek(&mut self, _: &[u8]) {}

    fn key(&self) -> &[u8] {
        &[]
    }

    fn value(&self) -> Value {
        Value::new()
    }

    fn valid(&self) -> bool {
        false
    }
}

#[derive(Debug, Error, Clone)]
pub enum Error {
    #[error("Key not found")]
    NotFound,
    #[error("Invalid checksum {0}")]
    InvalidChecksum(String),
    #[error("Invalid filename")]
    InvalidFileName,
    #[error("Invalid file size")]
    InvalidFileSize,
    #[error("Invalid magic number")]
    InvalidMagicNumber,
    #[error("IO error: {0}")]
    Io(String),
    #[error("EOF")]
    Eof,
    #[error("{0}")]
    Other(String),
}

impl From<io::Error> for Error {
    #[inline]
    fn from(e: io::Error) -> Error {
        Error::Io(e.to_string())
    }
}

impl From<dfs::Error> for Error {
    #[inline]
    fn from(e: dfs::Error) -> Error {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = result::Result<T, Error>;

/// simple rewrite of golang sort.Search
pub fn search<F>(n: usize, mut f: F) -> usize
where
    F: FnMut(usize) -> bool,
{
    let mut i = 0;
    let mut j = n;
    while i < j {
        let h = (i + j) / 2;
        if !f(h) {
            i = h + 1;
        } else {
            j = h;
        }
    }
    i
}

#[derive(Clone, Copy, Default)]
pub struct LocalAddr {
    pub start: usize,
    pub end: usize,
}

impl LocalAddr {
    pub fn new(start: usize, end: usize) -> Self {
        Self { start, end }
    }

    pub fn get(self, buf: &[u8]) -> &[u8] {
        &buf[self.start..self.end]
    }

    pub fn len(self) -> usize {
        self.end - self.start
    }

    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
}

pub fn new_merge_iterator<'a>(
    mut iters: Vec<Box<dyn Iterator + 'a>>,
    reverse: bool,
) -> Box<dyn Iterator + 'a> {
    match iters.len() {
        0 => Box::new(EmptyIterator {}),
        1 => iters.pop().unwrap(),
        2 => {
            let second_iter: Box<dyn Iterator + 'a> = iters.pop().unwrap();
            let first_iter: Box<dyn Iterator + 'a> = iters.pop().unwrap();
            let first: Box<super::MergeIteratorChild<'a>> =
                Box::new(super::MergeIteratorChild::new(true, first_iter));
            let second = Box::new(super::MergeIteratorChild::new(false, second_iter));
            let merge_iter = super::MergeIterator::new(first, second, reverse);
            Box::new(merge_iter)
        }
        _ => {
            let mid = iters.len() / 2;
            let mut second = vec![];
            for _ in 0..mid {
                second.push(iters.pop().unwrap())
            }
            second.reverse();
            let first_it = new_merge_iterator(iters, reverse);
            let second_it = new_merge_iterator(second, reverse);
            new_merge_iterator(vec![first_it, second_it], reverse)
        }
    }
}
