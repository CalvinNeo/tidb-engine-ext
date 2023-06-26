// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{path::PathBuf, sync::Arc, time::Duration};

use dyn_clone::DynClone;

use crate::{table::sstable, *};

// Options are params for creating Engine object.
//
// This package provides DefaultOptions which contains options that should
// work for most applications. Consider using that as a starting point before
// customizing it for your own needs.
pub struct Options {
    pub local_dir: PathBuf,
    // base_size is th maximum L1 size before trigger a compaction.
    // The L2 size is 10x of the base size, L3 size is 100x of the base size.
    pub base_size: u64,

    pub max_block_cache_size: i64,

    // Number of compaction workers to run concurrently.
    pub num_compactors: usize,

    pub table_builder_options: sstable::TableBuilderOptions,

    pub remote_compactor_addr: String,

    pub recovery_concurrency: usize,

    pub preparation_concurrency: usize,

    pub max_mem_table_size: u64,

    pub allow_fallback_local: bool,

    pub min_blob_size: u32,

    pub max_blob_table_size: usize,

    pub blob_table_gc_ratio: f64,

    pub blob_prefetch_size: usize,

    // Target size of a blob table, if the estimated size of a blob table is smaller than this
    // value, don't bother creating it.
    pub blob_table_target_size: usize,

    pub max_del_range_delay: Duration,

    pub enable_inner_key_offset: bool,

    /// Indicate kvengine is used for restore or not.
    pub for_restore: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            local_dir: PathBuf::from("/tmp"),
            base_size: 16 << 20,
            max_block_cache_size: 0,
            num_compactors: 3,
            table_builder_options: Default::default(),
            remote_compactor_addr: Default::default(),
            recovery_concurrency: Default::default(),
            preparation_concurrency: Default::default(),
            max_mem_table_size: 96 << 20,
            allow_fallback_local: true,
            min_blob_size: 0,
            max_blob_table_size: 64 * 1024 * 1024,
            blob_table_gc_ratio: 0.5,
            blob_prefetch_size: 256 * 1024,
            blob_table_target_size: 2 * 1024 * 1024,
            max_del_range_delay: Duration::from_secs(3600),
            enable_inner_key_offset: false,
            for_restore: false,
        }
    }
}

#[derive(Default, Clone, Copy)]
pub struct CfConfig {
    pub managed: bool,
    pub max_levels: usize,
}

impl CfConfig {
    pub fn new(managed: bool, max_levels: usize) -> Self {
        Self {
            managed,
            max_levels,
        }
    }
}

pub trait IdAllocator: Sync + Send {
    // alloc_id returns the last id, and last_id - count is valid.
    fn alloc_id(&self, count: usize) -> Result<Vec<u64>>;
}

pub trait RecoverHandler: Clone + Send {
    // Recovers from the shard's state to the state that is stored in the toState
    // property. So the Engine has a chance to execute pre-split command.
    // If toState is nil, the implementation should recovers to the latest state.
    fn recover(&self, engine: &Engine, shard: &Arc<Shard>, info: &ShardMeta) -> Result<()>;
}

pub trait MetaIterator {
    fn iterate<F>(&mut self, f: F) -> Result<()>
    where
        F: FnMut(kvenginepb::ChangeSet);

    fn engine_id(&self) -> u64;
}

pub trait MetaChangeListener: DynClone + Sync + Send {
    fn on_change_set(&self, cs: kvenginepb::ChangeSet);
}

dyn_clone::clone_trait_object!(MetaChangeListener);
