// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::{Arc, RwLock};

use engine_rocks::Compat;
use engine_traits::{IterOptions, Iterable, Iterator, Peekable};
use kvproto::{metapb, raft_serverpb};
use mock_engine_store;
use test_raftstore::*;
use raftstore::engine_store_ffi::*;
use std::time::Duration;


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
    let proxy_helper = cluster.ffi_helper_set.get(&key).unwrap().proxy_helper.as_ref();

    let req = kvrpcpb::ReadIndexRequest::default();
    let req_vec = vec![req];
    let res = proxy_helper.proxy_ptr
        .read_index_client
        .batch_read_index(req_vec, Duration::from_millis(100));



    cluster.shutdown();
}
