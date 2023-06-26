// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use core::panic;
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Mutex},
};

use bytes::{Buf, Bytes, BytesMut};
use kvenginepb as pb;
use kvenginepb::IngestFiles;
use protobuf::Message;

use crate::{
    table::{
        blobtable::blobtable::{BlobPrefetcher, BlobTable},
        memtable::{CfTable, Hint, WriteBatch},
        sstable::{InMemFile, L0Table, SsTable},
        table,
    },
    *,
};

const MEM_DATA_FORMAT_V1: u32 = 1;

pub struct Item<'a> {
    val: table::Value,
    pub path: AccessPath,
    phantom: PhantomData<&'a i32>,

    // Uses to hold the value's memory when necessary, so that the life time of val can be at least
    // long as the life time of Item itself. This is necessary when the caller is not responsible
    // (or impossible) to manage the life time. e.g. During point get, caller does not hold the
    // memory as in scan (held by the iterator).
    owned_val: Option<Vec<u8>>,
    owned_blob: Option<Vec<u8>>,
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
            owned_val: None,
            owned_blob: None,
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

    pub async fn from_change_set(
        dfs: Arc<dyn dfs::Dfs>,
        change_set: pb::ChangeSet,
        ignore_lock: bool,
    ) -> Self {
        let core =
            Arc::new(SnapAccessCore::from_change_set(dfs, change_set, None, ignore_lock).await);
        Self { core }
    }

    pub async fn from_change_set_and_memtable_data(
        dfs: Arc<dyn dfs::Dfs>,
        change_set: pb::ChangeSet,
        wb: &mut WriteBatch,
    ) -> Self {
        let wb = if wb.is_empty() { None } else { Some(wb) };
        let core = Arc::new(SnapAccessCore::from_change_set(dfs, change_set, wb, true).await);
        Self { core }
    }

    pub async fn construct_snapshot<'a>(
        dfs: Arc<dyn dfs::Dfs>,
        mut mem_table_data: &[u8],
        snapshot: &[u8],
    ) -> Result<Self> {
        let mut wb = crate::table::memtable::WriteBatch::new();
        if !mem_table_data.is_empty() {
            let format_version = mem_table_data.get_u32();
            if format_version != MEM_DATA_FORMAT_V1 {
                return Err(Error::RemoteRead(format!(
                    "unsupported mem data format {}",
                    format_version
                )));
            }
            let rows: Vec<table::Row> = bincode::deserialize(mem_table_data).map_err(|e| {
                Error::RemoteRead(format!("failed to deserialize mem table data: {}", e))
            })?;
            for row in rows {
                wb.put(&row.key, 0, &row.user_meta.to_array(), 0, &row.value);
            }
        }
        let mut change_set = kvenginepb::ChangeSet::default();
        change_set.merge_from_bytes(snapshot).unwrap();
        Ok(Self::from_change_set_and_memtable_data(dfs, change_set, &mut wb).await)
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
    blob_table_prefetch_size: usize,
}

impl SnapAccessCore {
    pub fn new(shard: &Shard) -> Self {
        let base_version = shard.get_base_version();
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
            blob_table_prefetch_size: shard.opt.blob_prefetch_size,
        }
    }

    pub async fn from_change_set(
        dfs: Arc<dyn dfs::Dfs>,
        change_set: pb::ChangeSet,
        wb: Option<&mut WriteBatch>,
        ignore_lock: bool,
    ) -> Self {
        let mut cs = ChangeSet::new(change_set);
        let mut ids = HashMap::new();
        let mem_tbls = vec![CfTable::new()];
        // FIXME: Iterate over all memtables
        if let Some(wb) = wb {
            let mem_tbl = mem_tbls[0].get_cf(WRITE_CF);
            mem_tbl.put_batch(wb, None, WRITE_CF);
            // Insert a dummy record, that should be ignored, so that we
            // don't have to fiddle too much with the code below.
            ids.insert(0, 0);
        }
        if cs.has_snapshot() {
            let snap = cs.get_snapshot();
            for l0 in snap.get_l0_creates() {
                ids.insert(l0.id, 0);
            }
            for ln in snap.get_table_creates() {
                ids.insert(ln.id, ln.level);
            }
            for blob in snap.get_blob_creates() {
                ids.insert(blob.id, BLOB_LEVEL);
            }
        }
        let (result_tx, mut result_rx) = tokio::sync::mpsc::unbounded_channel();
        let runtime = dfs.get_runtime();
        let opts = dfs::Options::new(cs.shard_id, cs.shard_ver);
        let mut msg_count = 0;
        for (&id, &level) in &ids {
            let tx = result_tx.clone();
            if id == 0 {
                tx.send(Ok((0, 0, None))).unwrap();
            } else {
                let fs = dfs.clone();
                let tx = result_tx.clone();
                runtime.spawn(async move {
                    let res = fs.read_file(id, opts).await;
                    tx.send(res.map(|data| (id, level, Some(data))))
                        .map_err(|_| "send file data failed")
                        .unwrap();
                });
            }
            msg_count += 1;
        }
        let mut errors = vec![];
        for _ in 0..msg_count {
            match result_rx.recv().await.unwrap() {
                Ok((id, _, None)) => {
                    assert_eq!(id, 0);
                }
                Ok((id, level, Some(data))) => {
                    assert!(id != 0);
                    let file = InMemFile::new(id, data);
                    if is_blob_file(level) {
                        let blob_table = BlobTable::new(Arc::new(file)).unwrap();
                        cs.blob_tables.insert(id, blob_table);
                    } else if level == 0 {
                        let l0_table = L0Table::new(Arc::new(file), None, ignore_lock).unwrap();
                        cs.l0_tables.insert(id, l0_table);
                    } else {
                        let ln_table = SsTable::new(Arc::new(file), None, level == 1).unwrap();
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
            panic!("errors is not empty: {:?}", errors);
        }
        let mut shard = Shard::new_for_ingest(0, &cs, Arc::new(Options::default()));
        let (l0s, blob_tbls, scfs) = create_snapshot_tables(cs.get_snapshot(), &cs, false);
        let old_data = shard.get_data();
        let data = ShardData::new(
            shard.range.clone(),
            old_data.del_prefixes.clone(),
            old_data.truncate_ts,
            old_data.trim_over_bound,
            mem_tbls,
            l0s,
            Arc::new(blob_tbls),
            scfs,
            HashMap::new(),
        );
        shard.id = cs.shard_id;
        shard.set_data(data);
        Self::new(&shard)
    }

    pub fn new_iterator_skip_blob(
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
        let data = self.data.clone();
        let mut key = BytesMut::new();
        key.extend_from_slice(data.prefix());
        Iterator {
            all_versions,
            reversed,
            read_ts,
            key,
            val: table::Value::new(),
            inner: self.new_table_iterator(cf, reversed, fill_cache),
            blob_prefetcher: None,
            data,
            range: None,
        }
    }

    pub fn new_iterator(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
        fill_cache: bool,
    ) -> Iterator {
        let blob_prefetcher = Some(BlobPrefetcher::new(
            self.data.blob_tbl_map.clone(),
            self.blob_table_prefetch_size,
        ));
        let data = self.data.clone();
        let mut key = BytesMut::new();
        key.extend_from_slice(data.prefix());
        Iterator {
            all_versions,
            reversed,
            read_ts: self.get_read_ts(cf, read_ts),
            key,
            val: table::Value::new(),
            inner: self.new_table_iterator(cf, reversed, fill_cache),
            blob_prefetcher,
            data,
            range: None,
        }
    }

    pub fn new_memtable_iterator(
        &self,
        cf: usize,
        reversed: bool,
        all_versions: bool,
        read_ts: Option<u64>,
    ) -> Iterator {
        Iterator {
            all_versions,
            reversed,
            read_ts: self.get_read_ts(cf, read_ts),
            key: BytesMut::new(),
            val: table::Value::new(),
            inner: self.new_mem_table_iterator(cf, reversed),
            blob_prefetcher: None,
            data: self.data.clone(),
            range: None,
        }
    }

    fn get_read_ts(&self, cf: usize, read_ts: Option<u64>) -> u64 {
        if let Some(ts) = read_ts {
            ts
        } else if CF_MANAGED[cf] && self.managed_ts != 0 {
            self.managed_ts
        } else {
            u64::MAX
        }
    }

    fn fetch_blob(&self, key: &[u8], val: &table::Value) -> Vec<u8> {
        assert!(val.is_blob_ref());
        let blob_ref = val.get_blob_ref();
        let blob_table = self
            .data
            .blob_tbl_map
            .get(&blob_ref.fid)
            .unwrap_or_else(|| {
                panic!(
                    "[{}] blob table not found {:?}, blob table id: {}",
                    self.tag, key, blob_ref.fid
                )
            });
        blob_table
            .get(&blob_ref)
            .unwrap_or_else(|e| panic!("[{}] blob table get failed {:?} {:?}", self.tag, key, e))
    }

    /// get an Item by key. Caller need to call is_some() before get_value.
    /// We don't return Option because we may need AccessPath even if the item
    /// is none.
    pub fn get(&self, cf: usize, key: &[u8], version: u64) -> Item<'_> {
        let mut version = version;
        if version == 0 {
            version = u64::MAX;
        }

        debug_assert_eq!(self.data.prefix(), &key[..self.data.inner_key_off]);
        let inner_key = &key[self.data.inner_key_off..];
        let mut item = Item::new();
        item.owned_val = Some(vec![]);
        item.val = self.get_value(
            cf,
            inner_key,
            version,
            &mut item.path,
            item.owned_val.as_mut().unwrap(),
        );
        if item.val.is_blob_ref() {
            item.owned_blob = Some(self.fetch_blob(inner_key, &item.val));
            item.val.fill_in_blob(item.owned_blob.as_ref().unwrap());
        }
        item
    }

    fn get_value(
        &self,
        cf: usize,
        inner_key: &[u8],
        version: u64,
        path: &mut AccessPath,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = if i == 0 && cf == 0 {
                // only use hint for the first mem-table and cf 0.
                let mut hint = self.get_hint.lock().unwrap();
                tbl.get_with_hint(inner_key, version, &mut hint)
            } else {
                tbl.get(inner_key, version)
            };
            path.mem_table += 1;
            if v.is_valid() {
                out_val_owner.resize(v.encoded_size(), 0);
                v.encode(out_val_owner.as_mut_slice());
                return table::Value::decode(out_val_owner.as_slice());
            }
        }
        let key_hash = farmhash::fingerprint64(inner_key);
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get(inner_key, version, key_hash, out_val_owner, 0);
                path.l0 = path.l0.saturating_add(1);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            let v = lh.get(inner_key, version, key_hash, out_val_owner);
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

    pub fn new_mem_table_iterator(&self, cf: usize, reversed: bool) -> Box<dyn table::Iterator> {
        let mut iters: Vec<Box<dyn table::Iterator>> = Vec::new();
        for mem_tbl in &self.data.mem_tbls {
            iters.push(Box::new(mem_tbl.get_cf(cf).new_iterator(reversed)));
        }
        table::new_merge_iterator(iters, reversed)
    }

    pub fn get_write_sequence(&self) -> u64 {
        self.write_sequence
    }

    pub fn get_start_key(&self) -> &[u8] {
        self.data.outer_start.chunk()
    }

    pub fn get_end_key(&self) -> &[u8] {
        self.data.outer_end.chunk()
    }

    pub fn clone_end_key(&self) -> Bytes {
        self.data.outer_end.clone()
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
            let mut owned_val = vec![];
            let val = l0_cf.get(key, u64::MAX, key_hash, &mut owned_val, 0);
            if val.is_valid() {
                return !val.is_deleted();
            }
        }
        for l in self.data.get_cf(cf).levels.as_slice() {
            if let Some(tbl) = l.get_table(key) {
                let mut owned_val = vec![];
                let val = tbl.get(key, u64::MAX, key_hash, &mut owned_val, l.level);
                if val.is_valid() {
                    return !val.is_deleted();
                }
            }
        }
        false
    }

    fn to_change_set(&self, ranges: &[(Bytes, Bytes)], ignore_locks: bool) -> pb::ChangeSet {
        let mut cs = new_change_set(self.get_tag().id_ver.id, self.get_tag().id_ver.ver);
        let mut snap = pb::Snapshot::new();
        let mut properties = pb::Properties::new();
        properties.shard_id = self.get_tag().id_ver.id;
        let data_sequence = if !self.data.l0_tbls.is_empty() {
            self.data.l0_tbls[0].version() - self.base_version
        } else {
            self.meta_seq
        };
        snap.set_data_sequence(data_sequence);
        snap.set_outer_start(self.get_start_key().to_vec());
        snap.set_outer_end(self.get_end_key().to_vec());
        snap.set_inner_key_off(self.data.range.inner_key_off as u32);
        snap.set_properties(properties);
        let mut count = 0;
        let mut overlapped_count = 0;
        for v in &self.data.l0_tbls {
            count += 1;
            if ignore_locks {
                if let Some(cf) = v.get_cf(WRITE_CF) {
                    if cf.size() == 0 {
                        continue;
                    }
                } else {
                    continue;
                }
            }
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
        for (k, v) in self.data.blob_tbl_map.iter() {
            assert_eq!(k, &v.id());
            count += 1;
            // FIXME: Overlap check
            let mut blob = pb::BlobCreate::new();
            blob.set_id(v.id());
            blob.set_smallest(v.smallest_key().to_vec());
            blob.set_biggest(v.biggest_key().to_vec());
            snap.mut_blob_creates().push(blob);
        }
        self.data.for_each_level(|cf, lh| {
            if cf == LOCK_CF {
                return false;
            }
            for v in lh.tables.iter() {
                count += 1;
                if ignore_locks && cf == WRITE_CF && v.size() == 0 {
                    continue;
                }
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

    pub fn marshal(
        &self,
        ranges: &[(Bytes, Bytes)],
        ignore_locks: bool,
    ) -> Option<(String, Vec<u8>)> {
        let cs = self.to_change_set(ranges, ignore_locks);
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
        let inner_key = &key[self.data.inner_key_off..];
        let mut item = Item::new();
        item.owned_val = Some(vec![]);
        item.val = self.get_newer_val(cf, inner_key, version, item.owned_val.as_mut().unwrap());
        if item.val.is_blob_ref() {
            item.owned_blob = Some(self.fetch_blob(inner_key, &item.val));
            item.val.fill_in_blob(item.owned_blob.as_ref().unwrap());
        }
        item
    }

    fn get_newer_val(
        &self,
        cf: usize,
        inner_key: &[u8],
        version: u64,
        out_val_owner: &mut Vec<u8>,
    ) -> table::Value {
        let key_hash = farmhash::fingerprint64(inner_key);
        for i in 0..self.data.mem_tbls.len() {
            let tbl = self.data.mem_tbls.as_slice()[i].get_cf(cf);
            let v = tbl.get_newer(inner_key, version);
            if v.is_valid() {
                out_val_owner.resize(v.encoded_size(), 0);
                v.encode(out_val_owner.as_mut_slice());
                return table::Value::decode(out_val_owner.as_slice());
            }
        }
        for l0 in &self.data.l0_tbls {
            if let Some(tbl) = &l0.get_cf(cf) {
                let v = tbl.get_newer(inner_key, version, key_hash, out_val_owner, 0);
                if v.is_valid() {
                    return v;
                }
            }
        }
        let scf = self.data.get_cf(cf);
        for lh in &scf.levels {
            let v = lh.get_newer(inner_key, version, key_hash, out_val_owner);
            if v.is_valid() {
                return v;
            }
        }
        table::Value::new()
    }

    pub fn has_data_in_prefix<'a>(&'a self, mut prefix: &'a [u8]) -> bool {
        let shard_prefix = self.data.prefix();

        let min_off = std::cmp::min(prefix.len(), self.data.inner_key_off);
        if min_off > 0 {
            if prefix[0..min_off] != shard_prefix[0..min_off] {
                return false;
            }
            if prefix.len() < self.data.inner_key_off {
                // If prefix is less than inner_key_off, the data in shard always have
                // `shard_prefix`, just update prefix to shard_prefix.
                prefix = shard_prefix;
            }
        }

        let inner_prefix = &prefix[self.data.inner_key_off..];
        if self.data.del_prefixes.cover_prefix(inner_prefix) {
            return false;
        }
        let mut it = self.new_iterator(0, false, false, Some(u64::MAX), true);
        it.seek(prefix);
        if !it.valid() {
            return false;
        }
        it.key().starts_with(prefix)
    }

    pub fn has_unloaded_tables(&self) -> bool {
        !self.data.unloaded_tbls.is_empty()
    }

    // check if the ingest files overlaps with the existing data in the shard.
    pub fn overlap_ingest_files(&self, ingest_files: &IngestFiles) -> bool {
        let table_creates = ingest_files.get_table_creates();
        let inner_smallest = table_creates.first().unwrap().get_smallest();
        let inner_biggest = table_creates.last().unwrap().get_biggest();
        let mut tbl_it = self.new_table_iterator(0, false, false);
        tbl_it.seek(inner_smallest);
        tbl_it.valid() && tbl_it.key() <= inner_biggest
    }
}

pub struct Iterator {
    all_versions: bool,
    reversed: bool,
    read_ts: u64,
    pub(crate) key: BytesMut,
    val: table::Value,
    pub(crate) inner: Box<dyn table::Iterator>,
    blob_prefetcher: Option<BlobPrefetcher>,
    data: ShardData,
    range: Option<(Bytes, Bytes)>, // [lower_bound, upper_bound)
}

impl Iterator {
    pub fn valid(&self) -> bool {
        self.val.is_valid()
    }

    pub fn key(&self) -> &[u8] {
        self.key.chunk()
    }

    pub fn val(&mut self) -> &[u8] {
        if self.val.is_blob_ref() {
            if let Some(prefetcher) = &mut self.blob_prefetcher {
                let blob_ref = self.val.get_blob_ref();
                return prefetcher.get(&blob_ref).unwrap_or_else(|e| {
                    panic!("failed to get blob, blob_ref: {:?}, err: {:?}", blob_ref, e)
                });
            }
        }
        self.val.get_value()
    }

    pub fn meta(&self) -> u8 {
        self.val.meta
    }

    pub fn user_meta(&self) -> &[u8] {
        self.val.user_meta()
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
        self.key.truncate(self.data.inner_key_off);
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

    // seek would seek to the provided key if present. If absent, it would seek to
    // the next smallest key greater than provided if iterating in the forward
    // direction. Behavior would be reversed is iterating backwards.
    pub fn seek(&mut self, key: &[u8]) {
        if key.len() <= self.data.inner_key_off {
            self.inner.rewind();
        } else {
            self.inner.seek(&key[self.data.inner_key_off..]);
        }
        self.parse_item();
    }

    // rewind would rewind the iterator cursor all the way to zero-th position,
    // which would be the smallest key if iterating forward, and largest if
    // iterating backward. It does not keep track of whether the cursor started
    // with a seek().
    pub fn rewind(&mut self) {
        self.inner.rewind();
        if self.inner.valid() {
            if self.reversed {
                if self.inner.key() >= self.data.inner_end() {
                    self.inner.seek(self.data.inner_end());
                    if self.inner.key() == self.data.inner_end() {
                        self.inner.next();
                    }
                }
            } else if self.inner.key() < self.data.inner_start() {
                self.inner.seek(self.data.inner_start())
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

    // set the new range of the iterator, it the range is monotonic, we can avoid
    // seek. return true if seek is performed.
    #[allow(clippy::collapsible_else_if)]
    pub fn set_range(
        &mut self,
        outer_lower_bound_include: Bytes,
        outer_upper_bound_exclude: Bytes,
    ) -> bool {
        let inner_lower_bound = outer_lower_bound_include.slice(self.data.inner_key_off..);
        let inner_upper_bound = outer_upper_bound_exclude.slice(self.data.inner_key_off..);
        let mut seeked = false;
        // reset monotonic range can be optimized to avoid seek.
        if self.is_reset_monotonic_range(&inner_lower_bound, &inner_upper_bound) {
            // If inner is not valid, the iterator has reached the end, there is no more
            // data to return, we can avoid the seek.
            if self.inner.valid() {
                if self.reversed {
                    // If the new inner_upper_bound is greater than the current key, we can
                    // continue to use the current key to iterate backward, avoid the seek.
                    if self.inner.key() > inner_upper_bound.chunk() {
                        self.inner.seek(inner_upper_bound.chunk());
                        seeked = true;
                    }
                    // the upper bound is exclusive, so we need to skip the current key.
                    if self.inner.key() == inner_upper_bound.chunk() {
                        self.inner.next();
                    }
                } else {
                    // If the new inner_lower_bound is greater than or equal to the current key,
                    // we can continue to use the current key to iterate forward, avoid the seek.
                    if self.inner.key() < inner_lower_bound.chunk() {
                        self.inner.seek(inner_lower_bound.chunk());
                        seeked = true;
                    }
                }
            }
        } else {
            // always seek if not reset monotonic range.
            if self.reversed {
                self.inner.seek(inner_upper_bound.chunk());
                if self.inner.key() == inner_upper_bound.chunk() {
                    self.inner.next();
                }
            } else {
                self.inner.seek(inner_lower_bound.chunk());
            }
            seeked = true;
        }
        self.range = Some((inner_lower_bound, inner_upper_bound));
        self.parse_item();
        seeked
    }

    fn is_reset_monotonic_range(&self, inner_lower_bound: &[u8], inner_upper_bound: &[u8]) -> bool {
        if let Some((old_lower, old_upper)) = &self.range {
            if self.reversed {
                inner_upper_bound <= old_lower.chunk()
            } else {
                old_upper <= inner_lower_bound.chunk()
            }
        } else {
            false
        }
    }

    pub(crate) fn is_inner_key_over_bound(&self) -> bool {
        if let Some((lower, upper)) = &self.range {
            if self.inner.valid() {
                if self.reversed {
                    self.inner.key() < lower.chunk()
                } else {
                    self.inner.key() >= upper.chunk()
                }
            } else {
                true
            }
        } else if self.reversed {
            self.inner.key() < self.data.inner_start()
        } else {
            self.inner.key() >= self.data.inner_end()
        }
    }
}
