// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{collections::HashMap, sync::Arc};

use byteorder::{ByteOrder, LittleEndian};
use kvengine::{dfs::DFS, table, table::sstable::InMemFile, ChangeSet};
use kvproto::metapb;
use protobuf::Message;
use tikv_util::{
    codec::{bytes::BytesEncoder, number::NumberEncoder},
    error, info,
};
use txn_types::WriteType;

use super::interfaces_ffi::BaseBuffView;

#[derive(Clone)]
pub struct CloudHelper {
    dfs: Arc<dyn DFS>,
}

impl CloudHelper {
    pub fn new(dfs: Arc<dyn DFS>) -> Self {
        Self { dfs }
    }
}

impl CloudHelper {
    pub fn prepare_change_set(
        &self,
        cs: kvenginepb::ChangeSet,
        cf: usize,
    ) -> kvengine::Result<ChangeSet> {
        let mut ids = HashMap::default();
        let mut cs = ChangeSet::new(cs);
        if cs.has_snapshot() {
            let snap = cs.get_snapshot();
            for l0 in snap.get_l0_creates() {
                ids.insert(l0.id, 0);
            }
            for ln in snap.get_table_creates() {
                if ln.cf as usize == cf {
                    ids.insert(ln.id, ln.get_level());
                }
            }
        }
        if cs.has_ingest_files() {
            let ingest_files = cs.get_ingest_files();
            for l0 in ingest_files.get_l0_creates() {
                ids.insert(l0.id, 0);
            }
            for tbl in ingest_files.get_table_creates() {
                if tbl.cf as usize == cf {
                    ids.insert(tbl.id, tbl.get_level());
                }
            }
        }
        self.load_tables_by_ids(cs.shard_id, cs.shard_ver, ids, &mut cs)?;
        Ok(cs)
    }

    fn load_tables_by_ids(
        &self,
        shard_id: u64,
        shard_ver: u64,
        ids: HashMap<u64, u32>,
        cs: &mut ChangeSet,
    ) -> kvengine::Result<()> {
        let (result_tx, result_rx) = tikv_util::mpsc::bounded(ids.len());
        let runtime = self.dfs.get_runtime();
        let opts = kvengine::dfs::Options::new(shard_id, shard_ver);
        let mut msg_count = 0;
        for (&id, &level) in &ids {
            let fs = self.dfs.clone();
            let tx = result_tx.clone();
            runtime.spawn(async move {
                let res = fs.read_file(id, opts).await;
                tx.send(res.map(|data| (id, level, data))).unwrap();
            });
            msg_count += 1;
        }
        let mut errors = vec![];
        for _ in 0..msg_count {
            match result_rx.recv().unwrap() {
                Ok((id, level, data)) => {
                    let file = InMemFile::new(id, data);
                    cs.add_file(id, Arc::new(file), level, None).unwrap();
                }
                Err(err) => {
                    error!("prefetch failed {:?}", &err);
                    errors.push(err);
                }
            }
        }
        if !errors.is_empty() {
            return Err(errors.pop().unwrap().into());
        }
        Ok(())
    }

    pub fn make_sst_reader(&self, cs_pb: kvenginepb::ChangeSet) -> CloudSstReader {
        let cs = self.prepare_change_set(cs_pb, 0).unwrap();
        CloudSstReader::new(&cs)
    }

    pub fn make_lock_sst_reader(&self, cs_pb: kvenginepb::ChangeSet) -> CloudLockSstReader {
        let cs = self.prepare_change_set(cs_pb, 1).unwrap();
        CloudLockSstReader::new(&cs)
    }
}

pub struct CloudSstReader {
    iter: Box<dyn kvengine::table::Iterator>,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl CloudSstReader {
    pub fn new(cs: &kvengine::ChangeSet) -> Self {
        let mut iter = new_table_iterator(&cs, 0);
        let (start, end) = if cs.has_snapshot() {
            let snap = cs.get_snapshot();
            (snap.start.clone(), snap.end.clone())
        } else {
            (vec![], vec![])
        };
        let mut reader = Self {
            iter,
            key_buf: vec![],
            val_buf: vec![],
            start,
            end,
        };
        reader.iter.seek(&reader.start);
        reader.sync_iter();
        reader
    }

    pub fn ffi_remained(&self) -> u8 {
        (!self.key_buf.is_empty()) as u8
    }

    pub fn ffi_key(&self) -> BaseBuffView {
        self.key_buf.as_slice().into()
    }

    pub fn ffi_val(&self) -> BaseBuffView {
        self.val_buf.as_slice().into()
    }

    fn sync_iter(&mut self) {
        self.key_buf.truncate(0);
        while self.iter.valid() {
            if !self.end.is_empty() && self.iter.key() >= self.end.as_slice() {
                return;
            }
            if table::is_deleted(self.iter.value().meta) {
                self.iter.next();
                continue;
            }
            self.key_buf.encode_bytes(self.iter.key(), false).unwrap();
            let short_value = self.iter.value().get_value().to_vec();
            let user_meta = UserMeta::from_slice(self.iter.value().user_meta());
            self.key_buf.encode_u64_desc(user_meta.commit_ts).unwrap();
            let write_type = if short_value.len() > 0 {
                WriteType::Put
            } else {
                WriteType::Delete
            };
            let write = txn_types::Write::new(
                write_type,
                user_meta.start_ts.into(),
                Some(short_value),
            );
            self.val_buf = write.as_ref().to_bytes();
            return;
        }
    }

    pub fn ffi_next(&mut self) {
        if self.iter.valid() && self.iter.next_version() {
            self.sync_iter();
            return;
        }
        self.iter.next();
        self.sync_iter();
    }
}

pub struct CloudLockSstReader {
    iter: Box<dyn kvengine::table::Iterator>,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    start: Vec<u8>,
    end: Vec<u8>,
}

impl CloudLockSstReader {
    pub fn new(cs: &kvengine::ChangeSet) -> Self {
        let mut iter = new_table_iterator(&cs, 1);
        let (start, end) = if cs.has_snapshot() {
            let snap = cs.get_snapshot();
            (snap.start.clone(), snap.end.clone())
        } else {
            (vec![], vec![])
        };
        let key_buf = vec![];
        let val_buf = vec![];
        let mut reader = Self {
            iter,
            key_buf,
            val_buf,
            start,
            end,
        };
        reader.iter.seek(&reader.start);
        reader.sync_iter();
        reader
    }

    pub fn ffi_remained(&self) -> u8 {
        (!self.key_buf.is_empty()) as u8
    }

    pub fn ffi_key(&self) -> BaseBuffView {
        self.key_buf.as_slice().into()
    }

    pub fn ffi_val(&self) -> BaseBuffView {
        self.val_buf.as_slice().into()
    }

    pub fn ffi_next(&mut self) {
        self.iter.next();
        self.sync_iter();
    }

    fn sync_iter(&mut self) {
        self.key_buf.truncate(0);
        while self.iter.valid() && kvengine::table::is_deleted(self.iter.value().meta) {
            self.iter.next();
        }
        if !self.iter.valid() {
            return;
        }
        if !self.end.is_empty() && self.iter.key() >= self.end.as_slice() {
            return;
        }
        self.key_buf.encode_bytes(self.iter.key(), false).unwrap();
        self.val_buf.truncate(0);
        self.val_buf
            .extend_from_slice(self.iter.value().get_value());
    }
}

fn new_table_iterator(cs: &kvengine::ChangeSet, cf: usize) -> Box<dyn kvengine::table::Iterator> {
    let mut l0_tables = vec![];
    for l0 in cs.l0_tables.values() {
        l0_tables.push(l0.clone())
    }
    l0_tables.sort_unstable_by(|a, b| b.version().cmp(&a.version()));
    let mut iters: Vec<Box<dyn table::Iterator>> = vec![];
    for l0_tbl in l0_tables {
        if let Some(cf_tbl) = l0_tbl.get_cf(cf) {
            let iter = cf_tbl.new_iterator(false, false);
            iters.push(iter);
        }
    }
    for level in 1..=kvengine::CF_LEVELS[cf] {
        if let Some(iter) = new_level_iterator(&cs, cf, level) {
            iters.push(iter);
        }
    }
    kvengine::table::new_merge_iterator(iters, false)
}

fn new_level_iterator(
    cs: &kvengine::ChangeSet,
    cf: usize,
    level: usize,
) -> Option<Box<dyn kvengine::table::Iterator>> {
    let mut tables = vec![];
    let table_creates = if cs.has_snapshot() {
        cs.get_snapshot().get_table_creates()
    } else {
        cs.get_ingest_files().get_table_creates()
    };
    for tbl in table_creates {
        if tbl.get_cf() as usize != cf || tbl.level as usize != level {
            continue;
        }
        tables.push(cs.ln_tables.get(&tbl.get_id()).unwrap().clone())
    }
    if tables.is_empty() {
        return None;
    }
    tables.sort_unstable_by(|a, b| a.smallest().cmp(b.smallest()));
    Some(Box::new(kvengine::ConcatIterator::new_with_tables(
        tables, false, false,
    )))
}

pub const USER_META_FORMAT_V1: u8 = 1;

// format(1) + start_ts(8) + commit_ts(8)
const USER_META_SIZE: usize = 1 + std::mem::size_of::<UserMeta>();

#[derive(Clone, Copy)]
pub struct UserMeta {
    pub start_ts: u64,
    pub commit_ts: u64,
}

impl UserMeta {
    pub fn from_slice(buf: &[u8]) -> Self {
        assert_eq!(buf[0], USER_META_FORMAT_V1);
        Self {
            start_ts: LittleEndian::read_u64(&buf[1..]),
            commit_ts: LittleEndian::read_u64(&buf[9..]),
        }
    }

    pub fn new(start_ts: u64, commit_ts: u64) -> Self {
        Self {
            start_ts,
            commit_ts,
        }
    }

    pub fn to_array(&self) -> [u8; USER_META_SIZE] {
        let mut array = [0u8; USER_META_SIZE];
        array[0] = USER_META_FORMAT_V1;
        LittleEndian::write_u64(&mut array[1..], self.start_ts);
        LittleEndian::write_u64(&mut array[9..], self.commit_ts);
        array
    }
}
