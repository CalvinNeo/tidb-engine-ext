// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    convert::TryInto,
    sync::{Arc, RwLock},
};

use byteorder::{ByteOrder, LittleEndian};
use cloud_encryption::MasterKey;
use engine_traits::CF_RAFT;
use kvengine::{dfs::Dfs, read::SnapAccess, table::is_deleted};
use kvproto::raft_serverpb::RegionLocalState;
use protobuf::Message;
use tikv_util::{
    codec::{bytes::BytesEncoder, number::NumberEncoder},
    info,
};
use txn_types::{Key, WriteType};

use crate::{interfaces_ffi::BaseBuffView, RaftStoreProxyEngineTrait};

const CF_WRITE_IDX: usize = 0;
const CF_LOCK_IDX: usize = 1;

#[derive(Clone)]
pub struct CloudHelper {
    dfs: Arc<dyn Dfs>,
}

impl CloudHelper {
    pub fn new(dfs: Arc<dyn Dfs>) -> Self {
        Self { dfs }
    }
}

impl CloudHelper {
    pub fn make_sst_reader(
        &self,
        cs_pb: kvenginepb::ChangeSet,
        kv_engine: &RwLock<Option<Box<dyn RaftStoreProxyEngineTrait + Sync + Send>>>,
        master_key: Option<MasterKey>,
    ) -> CloudSstReader {
        let cs_pb = Self::transform_cs(cs_pb, kv_engine, CF_WRITE_IDX);
        let runtime = self.dfs.get_runtime();
        let snap_access = runtime.block_on(SnapAccess::from_change_set(
            self.dfs.clone(),
            cs_pb,
            false,
            &master_key.unwrap(),
        ));
        CloudSstReader::new(&snap_access, CF_WRITE_IDX)
    }

    pub fn make_lock_sst_reader(
        &self,
        cs_pb: kvenginepb::ChangeSet,
        kv_engine: &RwLock<Option<Box<dyn RaftStoreProxyEngineTrait + Sync + Send>>>,
        master_key: Option<MasterKey>,
    ) -> CloudSstReader {
        let cs_pb = Self::transform_cs(cs_pb, kv_engine, CF_LOCK_IDX);
        let runtime = self.dfs.get_runtime();
        let snap_access = runtime.block_on(SnapAccess::from_change_set(
            self.dfs.clone(),
            cs_pb,
            false,
            &master_key.unwrap(),
        ));
        CloudSstReader::new(&snap_access, CF_LOCK_IDX)
    }

    fn transform_cs(
        cs_pb: kvenginepb::ChangeSet,
        kv_engine: &RwLock<Option<Box<dyn RaftStoreProxyEngineTrait + Sync + Send>>>,
        cf: usize,
    ) -> kvenginepb::ChangeSet {
        if cs_pb.has_ingest_files() {
            let ingest_files = cs_pb.get_ingest_files();
            let mut cs = kvenginepb::ChangeSet::default();
            cs.set_shard_id(cs_pb.get_shard_id());
            cs.set_shard_ver(cs_pb.get_shard_ver());
            let mut snap = kvenginepb::Snapshot::default();

            let region_id = cs.get_shard_id();
            let guard = kv_engine.read().unwrap();
            let engine = guard.as_ref().unwrap();
            let region_state_key = keys::region_state_key(region_id);
            let region_inner_key = keys::region_inner_key_off_key(region_id);
            let mut state = RegionLocalState::default();
            engine.get_value_cf(CF_RAFT, &region_state_key, &mut |value: Result<
                Option<&[u8]>,
                String,
            >| {
                match value {
                    Ok(Some(s)) => {
                        state.merge_from_bytes(s).unwrap();
                    }
                    e => panic!("failed to get regions state of {:?}: {:?}", region_id, e),
                }
            });
            let enc_outer_start = state.get_region().get_start_key().to_vec();
            let enc_outer_end = state.get_region().get_end_key().to_vec();
            let mut inner_key_off = 0;
            engine.get_value_cf(CF_RAFT, &region_inner_key, &mut |value: Result<
                Option<&[u8]>,
                String,
            >| {
                match value {
                    Ok(Some(v)) => {
                        inner_key_off = u32::from_be_bytes(v[0..4].try_into().unwrap()) as usize
                    }
                    Ok(None) => inner_key_off = 0,
                    e => panic!(
                        "failed to get regions inner_key_off of {:?}: {:?}",
                        region_id, e
                    ),
                }
            });

            let outter_start = Key::from_encoded_slice(&enc_outer_start).to_raw().unwrap();
            let outter_end = Key::from_encoded_slice(&enc_outer_end).to_raw().unwrap();
            snap.set_outer_start(outter_start);
            snap.set_outer_end(outter_end);
            snap.set_inner_key_off(inner_key_off as u32);
            snap.set_l0_creates(ingest_files.get_l0_creates().into());

            // Filter tables in cf, avoid useless table downloads
            snap.set_table_creates(
                ingest_files
                    .get_table_creates()
                    .iter()
                    .filter(|tc| tc.get_cf() == cf as i32)
                    .cloned()
                    .collect(),
            );

            // Blob only used in write cf
            if cf == CF_WRITE_IDX {
                snap.set_blob_creates(ingest_files.get_blob_creates().into());
            }

            snap.set_properties(ingest_files.get_properties().clone());
            cs.set_snapshot(snap);
            cs
        } else {
            assert!(cs_pb.has_snapshot());

            let mut cs = cs_pb.clone();
            let mut snap = cs.take_snapshot();
            let table_creates = snap.take_table_creates();

            // Filter tables in cf, avoid useless table downloads
            snap.set_table_creates(
                table_creates
                    .iter()
                    .filter(|tc| tc.get_cf() == cf as i32)
                    .cloned()
                    .collect(),
            );

            // Blob only used in write cf, remove from snapshot if cf is lock
            if cf == CF_LOCK_IDX {
                _ = snap.take_blob_creates();
            }
            cs.set_snapshot(snap);
            cs
        }
    }
}

pub struct CloudSstReader {
    iter: kvengine::read::Iterator,
    key_buf: Vec<u8>,
    val_buf: Vec<u8>,
    cf: usize,
}

impl CloudSstReader {
    pub fn new(snap_access: &SnapAccess, cf: usize) -> Self {
        let all_versions = cf == 0;
        let iter = snap_access.new_iterator(cf, false, all_versions, None, false);
        let outer_start = snap_access.get_start_key();
        let outer_end = snap_access.get_end_key();

        info!(
            "new sst reader cf: {} start: {:?} end: {:?}",
            cf, outer_start, outer_end
        );
        let mut reader = Self {
            iter,
            key_buf: vec![],
            val_buf: vec![],
            cf,
        };
        reader.iter.seek(&outer_start);
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

    fn sync_write_cf_iter(&mut self) {
        self.key_buf.truncate(0);
        while self.iter.valid() {
            assert!(
                !is_deleted(self.iter.meta()),
                "key: {:?}, meta: {:?}",
                self.iter.key(),
                self.iter.meta()
            );

            self.key_buf.encode_bytes(self.iter.key(), false).unwrap();
            let short_value = self.iter.val().to_vec();
            let user_meta = UserMeta::from_slice(self.iter.user_meta());
            self.key_buf.encode_u64_desc(user_meta.commit_ts).unwrap();
            let write_type = if short_value.len() > 0 {
                WriteType::Put
            } else {
                WriteType::Delete
            };
            let write =
                txn_types::Write::new(write_type, user_meta.start_ts.into(), Some(short_value));
            self.val_buf.truncate(0);
            self.val_buf = write.as_ref().to_bytes();
            return;
        }
    }

    fn sync_lock_cf_iter(&mut self) {
        self.key_buf.truncate(0);
        while self.iter.valid() {
            assert!(
                !is_deleted(self.iter.meta()),
                "key: {:?}, meta: {:?}",
                self.iter.key(),
                self.iter.meta()
            );

            self.key_buf.encode_bytes(self.iter.key(), false).unwrap();
            self.val_buf.truncate(0);
            self.val_buf.extend_from_slice(self.iter.val());
            return;
        }
    }

    fn sync_iter(&mut self) {
        if self.cf == 0 {
            self.sync_write_cf_iter();
        } else if self.cf == 1 {
            self.sync_lock_cf_iter();
        } else {
            panic!("invalid cf: {}", self.cf);
        }
    }

    pub fn ffi_next(&mut self) {
        self.iter.next();
        self.sync_iter();
    }
}

pub const USER_META_FORMAT_V1: u8 = 1;

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
}
