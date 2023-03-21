// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicBool, Arc},
};

use tikv::config::TikvConfig;

use super::common::*;

#[derive(Clone, Default)]
pub struct MockConfig {
    pub panic_when_flush_no_found: Arc<AtomicBool>,
    /// Whether our mock server should compat new proxy.
    pub proxy_compat: bool,
}

#[derive(Clone)]
pub struct MixeClusterConfig {
    pub test_raftstore_cfg: test_raftstore::Config,
    pub proxy_cfg: ProxyConfig,
    pub mock_cfg: MockConfig,
}

impl Deref for MixeClusterConfig {
    type Target = test_raftstore::Config;
    #[inline]
    fn deref(&self) -> &test_raftstore::Config {
        &self.test_raftstore_cfg
    }
}

impl DerefMut for MixeClusterConfig {
    #[inline]
    fn deref_mut(&mut self) -> &mut test_raftstore::Config {
        &mut self.test_raftstore_cfg
    }
}
