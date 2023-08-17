// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{mem, ops::Deref, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, Bytes, BytesMut};

use super::SsTable;
use crate::table::{
    search,
    sstable::{Index, BLOCK_FORMAT_V1},
    table::{self, is_old_version, Value, VALUE_VERSION_LEN},
    InnerKey, LocalAddr,
};

#[derive(Default)]
pub struct BlockIterator {
    b: Bytes,
    idx: i32,
    err: Option<table::Error>,

    // current entry fields.
    diff_key_addr: LocalAddr,
    meta: u8,
    user_meta_len: u8,
    ver: u64,
    old_ver: u64,
    val_addr: LocalAddr,

    // entry index fields.
    entry_offs: Bytes,
    common_prefix_addr: LocalAddr,
    entries_data_addr: LocalAddr,
}

impl BlockIterator {
    fn set_block(&mut self, b: Bytes) {
        self.b = b;
        self.err = None;
        self.idx = 0;
        self.reset_current_entry();
        self.load_entries();
    }

    fn reset_current_entry(&mut self) {
        self.diff_key_addr = LocalAddr::default();
        self.meta = 0;
        self.ver = 0;
        self.old_ver = 0;
        self.user_meta_len = 0;
        self.val_addr = LocalAddr::default();
    }

    fn load_entries(&mut self) {
        let mut data = self.b.clone();
        assert_eq!(data.get_u32_le(), BLOCK_FORMAT_V1);
        let num_entries = data.get_u32_le() as usize;
        self.entry_offs = data.slice(..num_entries * mem::size_of::<u32>());
        data.advance(self.entry_offs.len());
        let common_prefix_len = data.get_u16_le() as usize;
        let common_prefix_off = self.b.len() - data.len();
        let entries_data_off = common_prefix_off + common_prefix_len;
        self.common_prefix_addr = LocalAddr::new(common_prefix_off, entries_data_off);
        self.entries_data_addr = LocalAddr::new(entries_data_off, self.b.len());
    }

    fn num_entries(&self) -> usize {
        self.entry_offs.len() / mem::size_of::<u32>()
    }

    fn get_entry_off(&self, i: usize) -> usize {
        (&self.entry_offs[i * mem::size_of::<u32>()..]).get_u32_le() as usize
    }

    fn get_common_prefix(&self) -> &[u8] {
        self.common_prefix_addr.get(self.b.chunk())
    }

    fn get_diff_key(&self) -> &[u8] {
        self.diff_key_addr.get(self.b.chunk())
    }

    fn seek(&mut self, key: &[u8]) {
        let common_prefix = self.get_common_prefix();
        if key.len() <= common_prefix.len() {
            if key <= common_prefix {
                self.set_idx(0);
            } else {
                self.set_idx(self.num_entries() as i32);
            }
            return;
        }
        use std::cmp::Ordering::*;
        match &key[..common_prefix.len()].cmp(common_prefix.chunk()) {
            Less => {
                self.set_idx(0);
                return;
            }
            Greater => {
                self.set_idx(self.num_entries() as i32);
                return;
            }
            Equal => {}
        };
        let diff_key = &key[common_prefix.len()..];
        let found_idx = search(self.num_entries(), |i| {
            self.set_idx(i as i32);
            self.get_diff_key() >= diff_key
        });
        self.set_idx(found_idx as i32);
    }

    fn seek_to_first(&mut self) {
        self.set_idx(0);
    }

    fn seek_to_last(&mut self) {
        self.set_idx(self.num_entries() as i32 - 1);
    }

    fn get_entry_addr(&self, i: usize) -> LocalAddr {
        let addr = self.entries_data_addr;
        let start = addr.start + self.get_entry_off(i);
        if i + 1 < self.num_entries() {
            let end = addr.start + self.get_entry_off(i + 1);
            return LocalAddr::new(start, end);
        }
        LocalAddr::new(start, addr.end)
    }

    fn set_idx(&mut self, i: i32) {
        self.idx = i;
        if i >= self.num_entries() as i32 || i < 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        self.err = None;
        let cur_entry_addr = self.get_entry_addr(i as usize);
        let entry_data = cur_entry_addr.get(self.b.chunk());

        let diff_key_len = LittleEndian::read_u16(entry_data) as usize;
        let diff_key_start = cur_entry_addr.start + 2;
        self.diff_key_addr = LocalAddr::new(diff_key_start, diff_key_start + diff_key_len);
        let mut field_off = 2 + diff_key_len;
        self.meta = entry_data[field_off];
        field_off += 1;
        {
            let version = Value::get_version(&entry_data[field_off..]);
            self.ver = version;
            field_off += VALUE_VERSION_LEN;
        }
        if is_old_version(self.meta) {
            let version = Value::get_version(&entry_data[field_off..]);
            self.old_ver = version;
            field_off += VALUE_VERSION_LEN;
        } else {
            self.old_ver = 0;
        }
        self.user_meta_len = entry_data[field_off];
        field_off += 1;
        let val_start = cur_entry_addr.start + field_off;
        let val_end = cur_entry_addr.end;
        self.val_addr = LocalAddr::new(val_start, val_end);
    }

    fn next(&mut self) {
        self.set_idx(self.idx + 1);
    }

    fn prev(&mut self) {
        self.set_idx(self.idx - 1);
    }
}

#[derive(PartialEq)]
enum IterState {
    NewVersion,
    OldVersion,
    OldVersioDone,
}

pub struct TableIterator {
    t: SsTable,
    idx: Arc<Index>,
    old_idx: Option<Arc<Index>>,
    b_pos: i32,
    bi: BlockIterator,
    old_b_pos: i32,
    old_bi: BlockIterator,
    reversed: bool,
    fill_cache: bool,
    err: Option<table::Error>,
    key_buf: BytesMut,
    iter_state: IterState,
    block_buf: Vec<u8>,
    decryption_buf: Vec<u8>,
}

impl TableIterator {
    pub fn new(t: SsTable, reversed: bool, fill_cache: bool) -> Self {
        let idx = t.load_index();
        Self {
            t,
            idx,
            old_idx: None,
            b_pos: 0,
            bi: BlockIterator::default(),
            old_b_pos: 0,
            old_bi: BlockIterator::default(),
            reversed,
            fill_cache,
            err: None,
            key_buf: BytesMut::new(),
            iter_state: IterState::NewVersion,
            block_buf: vec![],
            decryption_buf: vec![],
        }
    }

    fn reset(&mut self) {
        self.b_pos = 0;
        self.err = None;
        self.iter_state = IterState::NewVersion;
    }

    pub fn error(&self) -> &Option<table::Error> {
        &self.err
    }

    fn set_block(&mut self, b_pos: i32) -> bool {
        self.b_pos = b_pos;
        let block = self
            .t
            .load_block(
                &self.idx,
                self.b_pos as usize,
                &mut self.block_buf,
                &mut self.decryption_buf,
                self.fill_cache,
            )
            .unwrap();
        self.bi.set_block(block);
        true
    }

    fn set_old_block(&mut self, b_pos: i32) -> bool {
        self.old_b_pos = b_pos;
        let old_block = self.get_old_idx();
        let block = match self.t.load_old_block(
            &old_block,
            self.old_b_pos as usize,
            &mut self.block_buf,
            &mut self.decryption_buf,
            self.fill_cache,
        ) {
            Ok(b) => b,
            Err(e) => {
                self.err = Some(e);
                return false;
            }
        };
        self.old_bi.set_block(block);
        true
    }

    fn seek_to_first(&mut self) {
        self.reset();
        let num_blocks = self.idx.num_blocks();
        if num_blocks == 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if !self.set_block(0) {
            return;
        }
        self.bi.seek_to_first();
        self.sync_block_iterator();
    }

    fn seek_to_last(&mut self) {
        self.reset();
        let num_blocks = self.idx.num_blocks();
        if num_blocks == 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if !self.set_block(num_blocks as i32 - 1) {
            return;
        }
        self.bi.seek_to_last();
        self.sync_block_iterator();
    }

    fn seek_in_block(&mut self, b_pos: usize, key: &[u8]) {
        if !self.set_block(b_pos as i32) {
            return;
        }
        self.bi.seek(key);
        self.sync_block_iterator();
    }

    fn seek_from_offset(&mut self, b_pos: usize, offset: usize, key: &[u8]) {
        if !self.set_block(b_pos as i32) {
            return;
        }
        self.bi.set_idx(offset as i32);
        self.sync_block_iterator();
        if self.key_buf.chunk() >= key {
            return;
        }
        self.bi.seek(key);
        self.sync_block_iterator();
    }

    fn seek_inner(&mut self, key: &[u8]) {
        self.reset();
        let idx = self.idx.seek_block(key);
        if idx == 0 {
            // The smallest key in our table is already strictly > key. We can return that.
            // This is like a SeekToFirst.
            self.seek_from_offset(0, 0, key);
            return;
        }

        // block[idx].smallest is > key.
        // Since idx>0, we know block[idx-1].smallest is <= key.
        // There are two cases.
        // 1) Everything in block[idx-1] is strictly < key. In this case, we should go
        // to the first    element of block[idx].
        // 2) Some element in block[idx-1] is >= key. We should go to that element.
        self.seek_in_block(idx - 1, key);
        if self.err.is_some() {
            // Case 1. Need to visit block[idx].
            if idx == self.idx.num_blocks() {
                // If idx == len(itr.t.blockEndOffsets), then input key is greater than ANY
                // element of table. There's nothing we can do. Valid() should
                // return false as we seek to end of table.
                return;
            }
            self.err = None;
            // Since block[idx].smallest is > key. This is essentially a
            // block[idx].SeekToFirst.
            self.seek_from_offset(idx, 0, key);
        }
        // Case 2: No need to do anything. We already did the seek in
        // block[idx-1].
    }

    fn seek_for_prev(&mut self, key: &[u8]) {
        // TODO: Optimize this. We shouldn't have to take a Prev step.
        self.seek_inner(key);
        if self.key_buf.chunk() != key {
            self.prev_inner();
        }
    }

    fn next_inner(&mut self) {
        self.err = None;
        self.iter_state = IterState::NewVersion;
        if self.b_pos >= self.idx.num_blocks() as i32 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if self.bi.b.is_empty() {
            if !self.set_block(self.b_pos) {
                return;
            }
            self.bi.seek_to_first();
            self.sync_block_iterator();
            return;
        }
        self.bi.next();
        self.sync_block_iterator();
        if self.err.is_some() {
            self.b_pos += 1;
            self.bi.b.clear();
            self.next_inner();
        }
    }

    fn prev_inner(&mut self) {
        self.err = None;
        self.iter_state = IterState::NewVersion;
        if self.b_pos < 0 {
            self.err = Some(table::Error::Eof);
            return;
        }
        if self.bi.b.is_empty() {
            if !self.set_block(self.b_pos) {
                return;
            }
            self.bi.seek_to_last();
            self.sync_block_iterator();
            return;
        }

        self.bi.prev();
        self.sync_block_iterator();
        if self.err.is_some() {
            self.b_pos -= 1;
            self.bi.b.clear();
            self.prev_inner();
        }
    }

    fn sync_block_iterator(&mut self) {
        if self.bi.err.is_none() {
            self.key_buf.truncate(0);
            self.key_buf.extend_from_slice(self.bi.get_common_prefix());
            self.key_buf.extend_from_slice(self.bi.get_diff_key());
        } else {
            self.err = self.bi.err.clone();
        }
    }

    fn same_old_key(&self) -> bool {
        let prefix_len = self.old_bi.common_prefix_addr.len();
        let key = self.key_buf.chunk();
        if prefix_len + self.old_bi.diff_key_addr.len() != key.len() {
            return false;
        }
        &key[..prefix_len] == self.old_bi.get_common_prefix()
            && &key[prefix_len..] == self.old_bi.get_diff_key()
    }

    fn get_old_idx(&mut self) -> Arc<Index> {
        if self.old_idx.is_none() {
            let old_idx = self.t.load_old_index();
            self.old_idx = Some(old_idx);
        }
        self.old_idx.as_ref().unwrap().clone()
    }

    fn seek_old_block(&mut self) -> Option<table::Error> {
        assert!(self.iter_state == IterState::NewVersion);
        let old_idx = self.get_old_idx();
        let mut old_b_pos = old_idx.seek_block(self.key_buf.chunk()) as i32 - 1;
        if old_b_pos == -1 {
            old_b_pos = 0;
        }
        if (self.old_bi.b.is_empty() || old_b_pos != self.old_b_pos)
            && !self.set_old_block(old_b_pos)
        {
            return self.old_bi.err.clone();
        }
        self.old_bi.seek(self.key_buf.chunk());
        assert!(self.old_bi.err.is_none());
        assert!(self.bi.old_ver == self.old_bi.ver);
        self.iter_state = IterState::OldVersion;
        None
    }

    pub fn set_reversed(&mut self, reversed: bool) {
        self.reversed = reversed;
    }
}

impl table::Iterator for TableIterator {
    fn next(&mut self) {
        if !self.reversed {
            self.next_inner();
        } else {
            self.prev_inner();
        }
    }

    fn next_version(&mut self) -> bool {
        if self.bi.old_ver == 0 {
            return false;
        }
        if self.iter_state == IterState::OldVersioDone {
            return false;
        }
        if self.same_old_key() {
            if self.iter_state == IterState::NewVersion {
                // If it's the first time call, and the key is the same,
                // the old version key must be iterated by a previous key, we should not call
                // next.
                assert!(self.bi.old_ver == self.old_bi.ver);
            } else {
                // It's the successive call of next_version, we need to move to the next
                // version.
                self.old_bi.next();
            }
            if self.old_bi.err.is_some() {
                self.iter_state = IterState::OldVersioDone;
                return false;
            }
            if !self.same_old_key() {
                self.iter_state = IterState::OldVersioDone;
                return false;
            }
            self.iter_state = IterState::OldVersion;
            return true;
        }
        self.err = self.seek_old_block();
        assert!(self.err.is_none());
        true
    }

    fn rewind(&mut self) {
        if !self.reversed {
            self.seek_to_first();
        } else {
            self.seek_to_last();
        }
    }

    fn seek(&mut self, key: InnerKey<'_>) {
        if !self.reversed {
            self.seek_inner(key.deref());
        } else {
            self.seek_for_prev(key.deref());
        }
    }

    fn key(&self) -> InnerKey<'_> {
        InnerKey::from_inner_buf(self.key_buf.chunk())
    }

    fn value(&self) -> Value {
        let bi = if self.iter_state == IterState::NewVersion {
            &self.bi
        } else {
            &self.old_bi
        };
        let buf = bi.val_addr.get(bi.b.chunk());
        Value::new_with_meta_version(bi.meta, bi.ver, bi.user_meta_len, buf)
    }

    fn valid(&self) -> bool {
        self.err.is_none()
    }
}
