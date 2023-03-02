// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

#![allow(unused_variables)]
use std::sync::Arc;

use engine_traits::{self, Mutable, Result, WriteBatchExt, WriteOptions};
use proxy_ffi::interfaces_ffi::RawCppPtr;
use rocksdb::{WriteBatch as RawWriteBatch, DB};
use tikv_util::Either;

use crate::{engine::RocksEngine, ps_engine::add_prefix, r2e, PageStorageExt};

pub struct MixedWriteBatch {
    pub inner: Either<crate::rocks_engine::RocksWriteBatchVec, crate::ps_engine::PSRocksWriteBatchVec>,
}

impl WriteBatchExt for RocksEngine {
    type WriteBatch = MixedWriteBatch;

    fn write_batch(&self) -> MixedWriteBatch {
        self.element_engine.as_ref().unwrap().write_batch()
    }

    fn write_batch_with_cap(&self, cap: usize) -> MixedWriteBatch {
        self.element_engine.as_ref().unwrap().write_batch_with_cap(cap)
    }
}

impl engine_traits::WriteBatch for MixedWriteBatch {
    fn write_opt(&mut self, opts: &WriteOptions) -> Result<u64> {
        // write into ps
        self.inner.write_opt(opts)
    }

    fn data_size(&self) -> usize {
        self.inner.data_size()
    }

    fn count(&self) -> usize {
        self.inner.count()
    }

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn should_write_to_engine(&self) -> bool {
        // Disable TiKV's logic, and using Proxy's instead.
        false
    }

    fn clear(&mut self) {
        self.inner.clear();
    }

    fn set_save_point(&mut self) {
        self.wbs[self.index].set_save_point();
        self.save_points.push(self.index);
    }

    fn pop_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            return self.wbs[x].pop_save_point().map_err(r2e);
        }
        Err(r2e("no save point"))
    }

    fn rollback_to_save_point(&mut self) -> Result<()> {
        if let Some(x) = self.save_points.pop() {
            for i in x + 1..=self.index {
                self.wbs[i].clear();
            }
            self.index = x;
            return self.wbs[x].rollback_to_save_point().map_err(r2e);
        }
        Err(r2e("no save point"))
    }

    fn merge(&mut self, other: Self) -> Result<()> {
        self.inner.merge(other)
    }
}

impl Mutable for MixedWriteBatch {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.do_write(engine_traits::CF_DEFAULT, key) {
            return Ok(());
        }
        self.put(key, value)
    }

    fn put_cf(&mut self, cf: &str, key: &[u8], value: &[u8]) -> Result<()> {
        if !self.do_write(cf, key) {
            return Ok(());
        }
        self.put_cf(cf, key, value)
    }

    fn delete(&mut self, key: &[u8]) -> Result<()> {
        if !self.do_write(engine_traits::CF_DEFAULT, key) {
            return Ok(());
        }
        self.delete(key)
    }

    fn delete_cf(&mut self, cf: &str, key: &[u8]) -> Result<()> {
        if !self.do_write(cf, key) {
            return Ok(());
        }
        self.delete_cf(cf, key)
    }

    fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        Ok(())
    }

    fn delete_range_cf(&mut self, cf: &str, begin_key: &[u8], end_key: &[u8]) -> Result<()> {
        Ok(())
    }
}
