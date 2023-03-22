// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::{Buf, Bytes, BytesMut};
use kvenginepb as pb;
use protobuf::Message;

use crate::{
    table::{
        memtable::{CFTable, Hint},
        sstable::{InMemFile, L0Table, SSTable},
        table,
    },
    *,
};

pub struct Item<'a> {
    val: table::Value,
    pub path: AccessPath,
    phantom: PhantomData<&'a i32>,
}

impl std::ops::Deref for Item<'_> {
    type Target = table::Value;

    fn deref(&self) -> &Self::Target {
        &self.val
    }
}

impl Item<'_> {
    fn new() -> Self {
        Self {
            val: table::Value::new(),
            path: AccessPath::default(),
            phantom: Default::default(),
        }
    }
}

#[derive(Default, Debug, Clone, Copy)]
pub struct AccessPath {
    pub mem_table: u8,
    pub l0: u8,
    pub ln: u8,
}

#[derive(Clone)]
pub struct SnapAccess {
    pub core: Arc<SnapAccessCore>,
}

impl SnapAccess {
    pub fn new(shard: &Shard) -> Self {
        let core = Arc::new(SnapAccessCore::new(shard));
        Self { core }
    }

    pub async fn from_change_set(dfs: Arc<dyn dfs::DFS>, change_set: pb::ChangeSet) -> Self {
        let core = Arc::new(SnapAccessCore::from_change_set(dfs, change_set).await);
        Self { core }
    }
}

impl Deref for SnapAccess {
    type Target = SnapAccessCore;

    fn deref(&self) -> &Self::Target {
        &self.core
    }
}

impl Debug for SnapAccess {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "snap access {}, seq: {}", self.tag, self.write_sequence,)
    }
}

pub struct SnapAccessCore {
    tag: ShardTag,
    managed_ts: u64,
    base_version: u64,
    meta_seq: u64,
    write_sequence: u64,
    data: ShardData,
    get_hint: Mutex<Hint>,
}

impl SnapAccessCore {
    pub fn new(shard: &Shard) -> Self {
        let base_version = shard.base_version;
        let meta_seq = shard.get_meta_sequence();
        let write_sequence = shard.get_write_sequence();
        let data = shard.get_data();
        Self {
            tag: shard.tag(),
            write_sequence,
            meta_seq,
            base_version,
            managed_ts: 0,
            data,
            get_hint: Mutex::new(Hint::new()),
        }
    }

    pub async fn from_change_set(dfs: Arc<dyn dfs::DFS>, change_set: pb::ChangeSet) -> Self {
        let mut cs = ChangeSet::new(change_set);
        let mut ids = HashMap::new();
        if cs.has_snapshot() {
            let snap = cs.get_snapshot();
            for l0 in snap.get_l0_creates() {
                ids.insert(l0.id, 0);
            }
            for ln in snap.get_table_creates() {
                ids.insert(ln.id, ln.level);
            }
        }
        let (result_tx, mut result_rx) = tokio::sync::mpsc::channel(ids.len());
        let runtime = dfs.get_runtime();
        let opts = dfs::Options::new(cs.shard_id, cs.shard_ver);
        let mut msg_count = 0;
        for (&id, &level) in &ids {
            let fs = dfs.clone();
            let tx = result_tx.clone();
            runtime.spawn(async move {
                let res = fs.read_file(id, opts).await;
                tx.send(res.map(|data| (id, level, data))).await.unwrap();
            });
            msg_count += 1;
        }
        let mut errors = vec![];
        for _ in 0..msg_count {
            match result_rx.recv().await.unwrap() {
                Ok((id, level, data)) => {
                    let file = InMemFile::new(id, data);
                    if level == 0 {
                        let l0_table = L0Table::new(Arc::new(file), None, true).unwrap();
                        cs.l0_tables.insert(id, l0_table);
                    } else {
                        let ln_table = SSTable::new(Arc::new(file), None, level == 1).unwrap();
                        cs.ln_tables.insert(id, ln_table);
                    }
                }
                Err(err) => {
                    error!("prefetch failed {:?}", &err);
                    errors.push(err);
                }
            }
        }
        if !errors.is_empty() {
            panic!("errors is not empty");
        }
        let mut shard = Shard::new_for_ingest(0, &cs, Arc::new(Options::default()));
        let (l0s, scfs) = create_snapshot_tables(cs.get_snapshot(), &cs);
        let old_data = shard.get_data();
        let data = ShardData::new(
            shard.start.clone(),
            shard.end.clone(),
            old_data.del_prefixes.clone(),
            old_data.truncate_ts,
            old_data.trim_over_bound,
            vec![CFTable::new()],
            l0s,
            scfs,
        );
        shard.id = cs.shard_id;
        shard.set_data(data);
        Self::new(&shard)
    }

    pub fn new_iterator(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
        fill_cache: bool,
    ) -> Iterator {
        let read_ts = if let Some(ts) = read_ts {
            ts
        } else if CF_MANAGED[cf] && self.managed_ts != 0 {
            self.managed_ts
        } else {
            u64::MAX
        };
        Iterator {
            all_versions,
            reversed,
            read_ts,
            key: BytesMut::new(),
            val: table::Value::new(),
            inner: self.new_table_iterator(cf, reversed, fill_cache),
            start: self.clone_start_key(),
            end: self.clone_end_key(),
            bound: None,
            bound_include: false,
        }
    }

    /// get an Item by key. Caller need to call is_some() before get_value.
    /// We don't return Option because we may need AccessPath even if the item is none.
    pub fn get(&self, cf: usize, key: &[u8], version: u64) -> Item<'_> {
        let mut version = version;
        if version == 0 {
            version = u64::MAX;
        }
        let mut item = Item::new();
        item.val = self.get_value(cf, key, version, &mut item.path);
        item
    }

    fn get_value(
        &self,
        cf: usize,
        key: &[u8],
        version: u64,
        path: &mut AccessPath,
    ) -> table::Value {
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = if i == 0 && cf == 0 {
                // only use hint for the first mem-table and cf 0.
                let mut hint = self.get_hint.lock().unwrap();
                tbl.get_with_hint(key, version, &mut hint)
            } else {
                tbl.get(key, version)
            };
            path.mem_table += 1;
            if v.is_valid() {
                return v;
            }
        }
        let key_hash = farmhash::fingerprint64(key);
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get(key, version, key_hash);
                path.l0 = path.l0.saturating_add(1);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            let v = lh.get(key, version, key_hash);
            path.ln += 1;
            if v.is_valid() {
                return v;
            }
        }
        table::Value::new()
    }

    pub fn multi_get(&self, cf: usize, keys: &[Vec<u8>], version: u64) -> Vec<Item<'_>> {
        let mut items = Vec::with_capacity(keys.len());
        for key in keys {
            let item = self.get(cf, key, version);
            items.push(item);
        }
        items
    }

    pub fn set_managed_ts(&mut self, managed_ts: u64) {
        self.managed_ts = managed_ts;
    }

    fn new_table_iterator(
        &self,
        cf: usize,
        reversed: bool,
        fill_cache: bool,
    ) -> Box<dyn table::Iterator> {
        let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
        for mem_tbl in &self.data.mem_tbls {
            iters.push(Box::new(mem_tbl.get_cf(cf).new_iterator(reversed)));
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                iters.push(tbl.new_iterator(reversed, fill_cache));
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in scf.levels.as_slice() {
            if lh.tables.len() == 0 {
                continue;
            }
            if lh.tables.len() == 1 {
                iters.push(lh.tables[0].new_iterator(reversed, fill_cache));
                continue;
            }
            iters.push(Box::new(ConcatIterator::new(
                lh.clone(),
                reversed,
                fill_cache,
            )));
        }
        table::new_merge_iterator(iters, reversed)
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.data.start.chunk()
    }

    pub fn clone_start_key(&self) -> Bytes {
        self.data.start.clone()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.data.end.chunk()
    }

    pub fn clone_end_key(&self) -> Bytes {
        self.data.end.clone()
    }

    pub fn get_tag(&self) -> ShardTag {
        self.tag
    }

    pub fn get_id(&self) -> u64 {
        self.tag.id_ver.id
    }

    pub fn get_version(&self) -> u64 {
        self.tag.id_ver.ver
    }

    pub(crate) fn contains_in_older_table(&self, key: &[u8], cf: usize) -> bool {
        let key_hash = farmhash::fingerprint64(key);
        for tbl in &self.data.mem_tbls[1..] {
            let val = tbl.get_cf(cf).get(key, u64::MAX);
            if val.is_valid() {
                return !val.is_deleted();
            }
        }
        for l0 in &self.data.l0_tbls {
            let l0_cf = l0.get_cf(cf);
            if l0_cf.is_none() {
                continue;
            }
            let l0_cf = l0_cf.as_ref().unwrap();
            let val = l0_cf.get(key, u64::MAX, key_hash);
            if val.is_valid() {
                return !val.is_deleted();
            }
        }
        for l in self.data.get_cf(cf).levels.as_slice() {
            if let Some(tbl) = l.get_table(key) {
                let val = tbl.get(key, u64::MAX, key_hash);
                if val.is_valid() {
                    return !val.is_deleted();
                }
            }
        }
        false
    }

    fn to_change_set(&self, ranges: &[(Bytes, Bytes)]) -> pb::ChangeSet {
        let mut cs = new_change_set(self.get_tag().id_ver.id, self.get_tag().id_ver.ver);
        let mut snap = pb::Snapshot::new();
        let mut properties = pb::Properties::new();
        properties.shard_id = self.get_tag().id_ver.id;
        let data_sequence = if self.data.l0_tbls.len() > 0 {
            self.data.l0_tbls[0].version() - self.base_version
        } else {
            self.meta_seq
        };
        snap.set_data_sequence(data_sequence);
        snap.set_start(self.get_start_key().to_vec());
        snap.set_end(self.get_end_key().to_vec());
        snap.set_properties(properties);
        let mut count = 0;
        let mut overlapped_count = 0;
        for v in &self.data.l0_tbls {
            count += 1;
            let mut overlap = false;
            for (start, end) in ranges {
                if v.has_data_in_range(start.as_ref(), end.as_ref()) {
                    overlap = true;
                    break;
                }
            }
            if !overlap {
                continue;
            }
            overlapped_count += 1;
            let mut l0 = pb::L0Create::new();
            l0.set_id(v.id());
            l0.set_smallest(v.smallest().to_vec());
            l0.set_biggest(v.biggest().to_vec());
            snap.mut_l0_creates().push(l0);
        }
        self.data.for_each_level(|cf, lh| {
            if cf == LOCK_CF {
                return false;
            }
            for v in lh.tables.iter() {
                count += 1;
                let mut overlap = false;
                for (start, end) in ranges {
                    if v.has_overlap(start.as_ref(), end.as_ref(), false) {
                        overlap = true;
                        break;
                    }
                }
                if !overlap {
                    continue;
                }
                overlapped_count += 1;
                let mut tbl = pb::TableCreate::new();
                tbl.set_id(v.id());
                tbl.set_cf(cf as i32);
                tbl.set_level(lh.level as u32);
                tbl.set_smallest(v.smallest().to_vec());
                tbl.set_biggest(v.biggest().to_vec());
                snap.mut_table_creates().push(tbl);
            }
            false
        });
        info!(
            "convert snap access to change set for {}, total files {}, overlapped files {}",
            self.get_tag(),
            count,
            overlapped_count,
        );
        if overlapped_count > 0 {
            cs.set_snapshot(snap);
        }
        cs
    }

    pub fn marshal(&self, ranges: &[(Bytes, Bytes)]) -> Option<(String, Vec<u8>)> {
        let cs = self.to_change_set(ranges);
        if !cs.has_snapshot() {
            return None;
        }
        let mut ranges_bytes = Vec::new();
        for (start, end) in ranges {
            ranges_bytes.append(start.to_vec().as_mut());
            ranges_bytes.append(end.to_vec().as_mut());
        }
        let ranges_key = hex::encode(ranges_bytes);
        let key = format!(
            "{}:{}:{}:{}",
            cs.shard_id,
            cs.shard_ver,
            cs.get_snapshot().data_sequence,
            ranges_key,
        );
        Some((key, cs.write_to_bytes().unwrap()))
    }

    pub fn get_all_files(&self) -> Vec<u64> {
        self.data.get_all_files()
    }

    pub fn get_newer(&self, cf: usize, key: &[u8], version: u64) -> Item<'_> {
        let mut item = Item::new();
        item.val = self.get_newer_val(cf, key, version);
        item
    }

    fn get_newer_val(&self, cf: usize, key: &[u8], version: u64) -> table::Value {
        let key_hash = farmhash::fingerprint64(key);
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = tbl.get_newer(key, version);
            if v.is_valid() {
                return v;
            }
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get_newer(key, version, key_hash);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            let v = lh.get_newer(key, version, key_hash);
            if v.is_valid() {
                return v;
            }
        }
        table::Value::new()
    }

    pub fn has_data_in_prefix(&self, prefix: &[u8]) -> bool {
        if self.data.del_prefixes.cover_prefix(prefix) {
            return false;
        }
        let mut it = self.new_iterator(0, false, false, Some(u64::MAX), true);
        it.seek(prefix);
        if !it.valid() {
            return false;
        }
        it.key().starts_with(prefix)
    }
}

pub struct Iterator {
    all_versions: bool,
    reversed: bool,
    read_ts: u64,
    pub key: BytesMut,
    val: table::Value,
    pub inner: Box<dyn table::Iterator>,
    start: Bytes,
    end: Bytes,
    pub bound: Option<Bytes>,
    pub bound_include: bool,
}

impl Iterator {
    pub fn valid(&self) -> bool {
        self.val.is_valid()
    }

    pub fn key(&self) -> &[u8] {
        self.key.chunk()
    }

    pub fn item(&self) -> Item<'_> {
        Item {
            val: self.val,
            path: AccessPath::default(),
            phantom: Default::default(),
        }
    }

    pub fn valid_for_prefix(&self, prefix: &[u8]) -> bool {
        self.key.starts_with(prefix)
    }

    pub fn next(&mut self) {
        if self.all_versions && self.valid() && self.inner.next_version() {
            self.update_item();
            return;
        }
        self.inner.next();
        self.parse_item();
    }

    fn update_item(&mut self) {
        self.key.truncate(0);
        self.key.extend_from_slice(self.inner.key());
        self.val = self.inner.value();
    }

    fn parse_item(&mut self) {
        while self.inner.valid() {
            if self.is_inner_key_over_bound() {
                break;
            }
            let val = self.inner.value();
            if val.version > self.read_ts && !self.inner.seek_to_version(self.read_ts) {
                self.inner.next();
                continue;
            }
            self.update_item();
            if !self.all_versions && self.val.is_deleted() {
                self.inner.next();
                continue;
            }
            return;
        }
        self.val = table::Value::new();
    }

    // seek would seek to the provided key if present. If absent, it would seek to the next smallest key
    // greater than provided if iterating in the forward direction. Behavior would be reversed is
    // iterating backwards.
    pub fn seek(&mut self, key: &[u8]) {
        if !self.reversed {
            self.inner.seek(key);
        } else if key.is_empty() {
            self.inner.rewind();
        } else {
            self.inner.seek(key);
        }
        self.parse_item();
    }

    // rewind would rewind the iterator cursor all the way to zero-th position, which would be the
    // smallest key if iterating forward, and largest if iterating backward. It does not keep track of
    // whether the cursor started with a seek().
    pub fn rewind(&mut self) {
        self.inner.rewind();
        if self.inner.valid() {
            if self.reversed {
                if self.inner.key() >= self.end.chunk() {
                    self.inner.seek(self.end.chunk());
                    if self.inner.key() == self.end.chunk() {
                        self.inner.next();
                    }
                }
            } else if self.inner.key() < self.start.chunk() {
                self.inner.seek(self.start.chunk())
            }
        }
        self.parse_item();
    }

    pub fn set_all_versions(&mut self, all_versions: bool) {
        self.all_versions = all_versions;
    }

    pub fn is_reverse(&self) -> bool {
        self.reversed
    }

    pub fn set_bound(&mut self, bound: Bytes, bound_include: bool) {
        self.bound = Some(bound);
        self.bound_include = bound_include;
    }

    pub fn is_inner_key_over_bound(&self) -> bool {
        if let Some(bound) = &self.bound {
            if self.reversed {
                if self.bound_include {
                    self.inner.key() < bound.chunk()
                } else {
                    self.inner.key() <= bound.chunk()
                }
            } else if self.bound_include {
                self.inner.key() > bound.chunk()
            } else {
                self.inner.key() >= bound.chunk()
            }
        } else if self.reversed {
            self.inner.key() < self.start.chunk()
        } else {
            self.inner.key() >= self.end.chunk()
        }
    }
}
