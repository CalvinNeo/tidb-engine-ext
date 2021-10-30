// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_rocks::Compat;
use engine_traits::{IterOptions, Iterable, Iterator, Peekable};
use kvproto::{kvrpcpb, metapb, raft_serverpb};
use mock_engine_store;
use raftstore::engine_store_ffi::*;
use std::time::Duration;
use test_raftstore::*;

#[test]
fn test_batch_read_index() {
    let pd_client = Arc::new(TestPdClient::new(0, false));
    let sim = Arc::new(RwLock::new(NodeCluster::new(pd_client.clone())));
    let mut cluster = Cluster::new(0, 3, sim, pd_client);

    cluster.run();

    let k = b"k1";
    let v = b"v1";
    cluster.must_put(k, v);

    let key = cluster.ffi_helper_set.keys().next().unwrap();
    let proxy_helper = cluster
        .ffi_helper_set
        .get(&key)
        .unwrap()
        .proxy_helper
        .as_ref();

    let mut req = kvrpcpb::ReadIndexRequest::default();

    let region = cluster.get_region(b"k1");

    let mut key_range = kvrpcpb::KeyRange::default();
    key_range.set_start_key(region.get_start_key().to_vec());
    key_range.set_end_key(region.get_end_key().to_vec());
    req.mut_ranges().push(key_range);

    let context = req.mut_context();

    context.set_region_id(region.get_id());
    context.set_peer(region.get_peers().first().unwrap().clone());
    context
        .mut_region_epoch()
        .set_version(region.get_region_epoch().get_version());
    context
        .mut_region_epoch()
        .set_conf_ver(region.get_region_epoch().get_conf_ver());

    sleep_ms(100);
    let req_vec = vec![req];
    let res = unsafe {
        proxy_helper
            .proxy_ptr
            .as_ref()
            .read_index_client
            .batch_read_index(req_vec, Duration::from_millis(100))
    };

    assert_eq!(res.len(), 1);
    let res = &res[0];
    // Put (k1,v1) has index 7
    assert_eq!(res.0.get_read_index(), 7);
    assert_eq!(res.1, region.get_id());

    cluster.shutdown();
}
