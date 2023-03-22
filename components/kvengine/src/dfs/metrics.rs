// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use lazy_static::lazy_static;
use prometheus::*;

lazy_static! {
    pub static ref KVENGINE_DFS_THROUGHPUT_VEC: IntCounterVec = register_int_counter_vec!(
        "kv_engine_dfs_throughput_bytes",
        "Throughput of kvengine dfs",
        &["type"]
    )
    .unwrap();
    pub static ref KVENGINE_DFS_LATENCY_VEC: HistogramVec = register_histogram_vec!(
        "kv_engine_dfs_latency_ms",
        "Latency of kvengine dfs in ms",
        &["type"],
        exponential_buckets(1.0, 2.0, 21).unwrap()
    )
    .unwrap();
    pub static ref KVENGINE_DFS_RETRY_COUNTER_VEC: IntCounterVec = register_int_counter_vec!(
        "kv_engine_dfs_rw_retry_count",
        "Retries counter of kvengine dfs read/write",
        &["type"]
    )
    .unwrap();
}
