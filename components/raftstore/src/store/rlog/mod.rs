// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::mem;

use crate::errors::Result;

use byteorder::{ByteOrder, LittleEndian};
use bytes::{Buf, BufMut};
use kvproto::raft_cmdpb::{CustomRequest, RaftCmdRequest};
use protobuf::Message;

pub fn get_custom_log(req: &RaftCmdRequest) -> Option<CustomRaftLog<'_>> {
    if !req.has_custom_request() {
        return None;
    }
    Some(CustomRaftLog {
        data: req.get_custom_request().get_data(),
    })
}

pub type CustomRaftlogType = u8;

pub const TYPE_PREWRITE: CustomRaftlogType = 1;
pub const TYPE_COMMIT: CustomRaftlogType = 2;
pub const TYPE_ROLLBACK: CustomRaftlogType = 3;
pub const TYPE_PESSIMISTIC_LOCK: CustomRaftlogType = 4;
pub const TYPE_PESSIMISTIC_ROLLBACK: CustomRaftlogType = 5;
pub const TYPE_ONE_PC: CustomRaftlogType = 6;
pub const TYPE_ENGINE_META: CustomRaftlogType = 7;
pub const TYPE_RESOLVE_LOCK: CustomRaftlogType = 8;
pub const TYPE_SWITCH_MEM_TABLE: CustomRaftlogType = 9;
pub const TYPE_TRIGGER_TRIM_OVER_BOUND: CustomRaftlogType = 10;

const HEADER_SIZE: usize = 2;

// CustomRaftLog is the raft log format for unistore to store Prewrite/Commit/PessimisticLock.
//  | type(1) | version(1) | entries
//
// It reduces the cost of marshal/unmarshal and avoid DB lookup during apply.
#[derive(Debug)]
pub struct CustomRaftLog<'a> {
    pub(crate) data: &'a [u8],
}

impl<'a> CustomRaftLog<'a> {
    pub fn new_from_data(data: &'a [u8]) -> Self {
        Self { data }
    }

    pub fn get_type(&self) -> CustomRaftlogType {
        self.data[0] as CustomRaftlogType
    }

    // F: (key, val)
    pub fn iterate_lock<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &[u8]),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let val_len = LittleEndian::read_u32(&self.data[i..]) as usize;
            i += 4;
            let val = &self.data[i..i + val_len];
            i += val_len;
            f(key, val)
        }
    }

    // F: (key, commit_ts)
    pub fn iterate_commit<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], u64),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let commit_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            f(key, commit_ts)
        }
    }

    // F: (key, val, is_extra, del_lock, start_ts, commit_ts)
    pub fn iterate_one_pc<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], &[u8], bool, bool, u64, u64),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let val_len = LittleEndian::read_u32(&self.data[i..]) as usize;
            i += 4;
            let val = &self.data[i..i + val_len];
            i += val_len;
            let is_extra = self.data[i] > 0;
            i += 1;
            let del_lock = self.data[i] > 0;
            i += 1;
            let start_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            let commit_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            f(key, val, is_extra, del_lock, start_ts, commit_ts)
        }
    }

    // F: (key, start_ts, delete_lock)
    pub fn iterate_rollback<F>(&self, mut f: F)
    where
        F: FnMut(&[u8], u64, bool),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            let start_ts = LittleEndian::read_u64(&self.data[i..]);
            i += 8;
            let del = self.data[i];
            i += 1;
            f(key, start_ts, del > 0)
        }
    }

    pub fn iterate_del_lock<F>(&self, mut f: F)
    where
        F: FnMut(&[u8]),
    {
        let mut i = HEADER_SIZE;
        while i < self.data.len() {
            let key_len = LittleEndian::read_u16(&self.data[i..]) as usize;
            i += 2;
            let key = &self.data[i..i + key_len];
            i += key_len;
            f(key)
        }
    }

    pub fn get_change_set(&self) -> Result<kvenginepb::ChangeSet> {
        let mut cs = kvenginepb::ChangeSet::new();
        cs.merge_from_bytes(&self.data[HEADER_SIZE..])?;
        Ok(cs)
    }

    pub fn iterate_resolve_lock(&self, mut f: impl FnMut(CustomRaftlogType, &[u8], u64, bool)) {
        let mut data = &self.data[HEADER_SIZE..];
        while !data.is_empty() {
            let tp = data.get_u8() as CustomRaftlogType;
            match tp {
                TYPE_COMMIT => {
                    let key_len = data.get_u16_le() as usize;
                    let key = &data[..key_len];
                    data = &data[key_len..];
                    let commit_ts = data.get_u64_le();
                    f(tp, key, commit_ts, true);
                }
                TYPE_ROLLBACK => {
                    let key_len = data.get_u16_le() as usize;
                    let key = &data[..key_len];
                    data = &data[key_len..];
                    let start_ts = data.get_u64_le();
                    let del = data.get_u8() > 0;
                    f(tp, key, start_ts, del);
                }
                _ => unreachable!("unexpected custom raft log type: {:?}", tp),
            }
        }
    }

    pub(crate) fn get_switch_mem_table(&self) -> u64 {
        let mut bin = &self.data[HEADER_SIZE..];
        bin.get_u64_le()
    }
}

pub struct CustomBuilder {
    buf: Vec<u8>,
    cnt: i32,
}

impl Default for CustomBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CustomBuilder {
    pub fn new() -> Self {
        Self {
            buf: vec![0; HEADER_SIZE],
            cnt: 0,
        }
    }

    pub fn append_lock(&mut self, key: &[u8], val: &[u8]) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u32_le(val.len() as u32);
        self.buf.extend_from_slice(val);
        self.cnt += 1;
    }

    pub fn append_commit(&mut self, key: &[u8], commit_ts: u64) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u64_le(commit_ts);
        self.cnt += 1;
    }

    pub fn append_one_pc(
        &mut self,
        key: &[u8],
        val: &[u8],
        is_extra: bool,
        del_lock: bool,
        start_ts: u64,
        commit_ts: u64,
    ) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u32_le(val.len() as u32);
        self.buf.extend_from_slice(val);
        self.buf.put_u8(is_extra as u8);
        self.buf.put_u8(del_lock as u8);
        self.buf.put_u64_le(start_ts);
        self.buf.put_u64_le(commit_ts);
        self.cnt += 1;
    }

    pub fn append_rollback(&mut self, key: &[u8], start_ts: u64, delete_lock: bool) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.buf.put_u64_le(start_ts);
        self.buf.put_u8(delete_lock as u8);
        self.cnt += 1;
    }

    pub fn append_del_lock(&mut self, key: &[u8]) {
        self.buf.put_u16_le(key.len() as u16);
        self.buf.extend_from_slice(key);
        self.cnt += 1;
    }

    pub fn set_change_set(&mut self, cs: kvenginepb::ChangeSet) {
        assert_eq!(self.buf.len(), HEADER_SIZE);
        let data = cs.write_to_bytes().unwrap();
        self.buf.extend_from_slice(&data);
        self.set_type(TYPE_ENGINE_META);
    }

    pub fn set_switch_mem_table(&mut self, current_size: u64) {
        assert_eq!(self.buf.len(), HEADER_SIZE);
        self.buf.put_u64_le(current_size);
        self.set_type(TYPE_SWITCH_MEM_TABLE)
    }

    pub fn set_type(&mut self, tp: CustomRaftlogType) {
        self.buf[0] = tp as u8;
    }

    pub fn get_type(&self) -> CustomRaftlogType {
        self.buf[0] as CustomRaftlogType
    }

    // Some custom logs may contains multiple types of logs, e.g., resolve-lock can contain both
    // commit and rollback. We use type to distinguish them.
    pub fn append_type(&mut self, tp: CustomRaftlogType) {
        self.buf.push(tp as u8);
    }

    pub fn build(&mut self) -> CustomRequest {
        let mut req = CustomRequest::default();
        let buf = mem::take(&mut self.buf);
        req.set_data(buf);
        req
    }

    pub fn len(&self) -> usize {
        self.cnt as usize
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub fn is_engine_meta_log(data: &[u8]) -> bool {
    data[0] == TYPE_ENGINE_META
}

#[test]
fn test_custom_log() {
    let mut builder = CustomBuilder::new();
    builder.set_switch_mem_table(2022);
    let req = builder.build();
    let cl = CustomRaftLog::new_from_data(req.get_data());
    assert_eq!(cl.get_type(), TYPE_SWITCH_MEM_TABLE);
    assert_eq!(cl.get_switch_mem_table(), 2022);
}
