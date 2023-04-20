// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use engine_traits::{Checkpointer, KvEngine, SyncMutable};
use grpcio::Environment;
use kvproto::raft_serverpb::{RaftMessage, RaftSnapshotData};
use mock_engine_store::{
    interfaces_ffi::{BaseBuffView, SSTReaderPtr},
    mock_cluster::v1::server::new_server_cluster,
};
use proxy_ffi::{
    interfaces_ffi::{ColumnFamilyType, EngineIteratorSeekType},
    snapshot_reader_impls::{tablet_reader::TabletReader, *},
};
use raft::eraftpb::Snapshot;
use raftstore::store::{snap::TABLET_SNAPSHOT_VERSION, TabletSnapKey, TabletSnapManager};
use rand::Rng;
use test_raftstore::RawEngine;
use test_raftstore_v2::WrapFactory;
use tikv::server::tablet_snap::send_snap as send_snap_v2;
use tikv_util::time::Limiter;

use crate::utils::v1::*;

fn random_long_vec(length: usize) -> Vec<u8> {
    let mut rng = rand::thread_rng();
    let mut value = Vec::with_capacity(1024);
    (0..length).for_each(|_| value.push(rng.gen::<u8>()));
    value
}

fn generate_snap<EK: KvEngine>(
    engine: &WrapFactory<EK>,
    region_id: u64,
    snap_mgr: &TabletSnapManager,
) -> (RaftMessage, TabletSnapKey) {
    let tablet = engine.get_tablet_by_id(region_id).unwrap();
    let region_state = engine.region_local_state(region_id).unwrap().unwrap();
    let apply_state = engine.raft_apply_state(region_id).unwrap().unwrap();

    // Construct snapshot by hand
    let mut snapshot = Snapshot::default();
    snapshot.mut_metadata().set_term(apply_state.commit_term);
    snapshot.mut_metadata().set_index(apply_state.applied_index);
    let conf_state = raftstore::store::util::conf_state_from_region(region_state.get_region());
    snapshot.mut_metadata().set_conf_state(conf_state);

    let mut snap_data = RaftSnapshotData::default();
    snap_data.set_region(region_state.get_region().clone());
    snap_data.set_version(TABLET_SNAPSHOT_VERSION);
    use protobuf::Message;
    snapshot.set_data(snap_data.write_to_bytes().unwrap().into());
    let snap_key = TabletSnapKey::from_region_snap(region_id, 1, &snapshot);
    let checkpointer_path = snap_mgr.tablet_gen_path(&snap_key);
    let mut checkpointer = tablet.new_checkpointer().unwrap();
    checkpointer
        .create_at(checkpointer_path.as_path(), None, 0)
        .unwrap();

    let mut msg = RaftMessage::default();
    msg.region_id = region_id;
    msg.set_to_peer(new_peer(1, 1));
    msg.mut_message().set_snapshot(snapshot);
    msg.mut_message().set_msg_type(MessageType::MsgSnapshot);
    msg.set_region_epoch(region_state.get_region().get_region_epoch().clone());

    (msg, snap_key)
}

#[test]
fn test_parse_tablet_snapshot() {
    let test_parse_snap = |key_num| {
        let mut cluster_v1 = new_server_cluster(1, 1);
        let mut cluster_v2 = test_raftstore_v2::new_server_cluster(1, 1);

        cluster_v1
            .cfg
            .server
            .labels
            .insert(String::from("engine"), String::from("tiflash"));

        cluster_v1.run();
        cluster_v2.run();

        let s1_addr = cluster_v1.get_addr(1);
        let region = cluster_v2.get_region(b"");
        let region_id = region.get_id();
        let engine = cluster_v2.get_engine(1);
        let tablet = engine.get_tablet_by_id(region_id).unwrap();

        for i in 0..key_num {
            let k = format!("zk{:04}", i);
            tablet.put(k.as_bytes(), &random_long_vec(1024)).unwrap();
            tablet
                .put_cf(CF_LOCK, k.as_bytes(), &random_long_vec(1024))
                .unwrap();
            tablet
                .put_cf(CF_WRITE, k.as_bytes(), &random_long_vec(1024))
                .unwrap();
        }

        let snap_mgr = cluster_v2.get_snap_mgr(1);
        let security_mgr = cluster_v2.get_security_mgr();
        let (msg, snap_key) = generate_snap(&engine, region_id, &snap_mgr);
        let cfg = tikv::server::Config::default();
        let limit = Limiter::new(f64::INFINITY);
        let env = Arc::new(Environment::new(1));
        let _ = block_on(async {
            send_snap_v2(env, snap_mgr, security_mgr, &cfg, &s1_addr, msg, limit)
                .unwrap()
                .await
        });

        // The snapshot has been received by cluster v1, so check it's completeness
        let snap_mgr = cluster_v1.get_snap_mgr(1);
        let path = snap_mgr.tablet_snap_manager().final_recv_path(&snap_key);

        let validate = |cf: ColumnFamilyType| unsafe {
            let reader =
                TabletReader::ffi_get_cf_file_reader(path.as_path().to_str().unwrap(), cf, None);

            let k = format!("zk{:04}", 5);
            let bf = BaseBuffView {
                data: k.as_ptr() as *const _,
                len: k.len() as u64,
            };
            ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
            for i in 5..key_num {
                let k = format!("k{:04}", i);
                assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 1);
                let kbf = ffi_sst_reader_key(reader.clone(), cf);
                assert_eq!(kbf.to_slice(), k.as_bytes());
                ffi_sst_reader_next(reader.clone(), cf);
            }
            assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 0);

            // If the sst is "empty" to this region. Will not panic, and remained should be
            // false.
            let k = format!("zk{:04}", key_num + 10);
            let bf = BaseBuffView {
                data: k.as_ptr() as *const _,
                len: k.len() as u64,
            };
            ffi_sst_reader_seek(reader.clone(), cf, EngineIteratorSeekType::Key, bf);
            assert_eq!(ffi_sst_reader_remained(reader.clone(), cf), 0);
        };
        validate(ColumnFamilyType::Default);
        validate(ColumnFamilyType::Write);
        validate(ColumnFamilyType::Lock);
    };

    test_parse_snap(20);
}
