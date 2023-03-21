// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    ops::{Deref, DerefMut},
    sync::{atomic::AtomicBool, Arc},
};

use tikv::config::TikvConfig;

#[derive(Clone, Default)]
pub struct MockConfig {
    pub panic_when_flush_no_found: Arc<AtomicBool>,
    /// Whether our mock server should compat new proxy.
    pub proxy_compat: bool,
}

#[derive(Clone)]
pub struct Config {
    pub tikv: TikvConfig,
    pub prefer_mem: bool,
}

impl Deref for Config {
    type Target = TikvConfig;
    #[inline]
    fn deref(&self) -> &TikvConfig {
        &self.tikv
    }
}

impl DerefMut for Config {
    #[inline]
    fn deref_mut(&mut self) -> &mut TikvConfig {
        &mut self.tikv
    }
}
