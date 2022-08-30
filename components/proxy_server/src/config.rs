// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{hash_map::RandomState, HashSet},
    iter::FromIterator,
    path::{Path, PathBuf},
};

use itertools::Itertools;
use online_config::OnlineConfig;
use serde_derive::{Deserialize, Serialize};
use serde_with::with_prefix;
use tikv::config::TiKvConfig;
use tikv_util::crit;

use crate::fatal;

with_prefix!(prefix_apply "apply-");
with_prefix!(prefix_store "store-");
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct RaftstoreConfig {
    pub snap_handle_pool_size: usize,
}

impl Default for RaftstoreConfig {
    fn default() -> Self {
        RaftstoreConfig {
            snap_handle_pool_size: 2,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ServerConfig {
    pub engine_addr: String,
    pub engine_store_version: String,
    pub engine_store_git_hash: String,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            engine_addr: DEFAULT_ENGINE_ADDR.to_string(),
            engine_store_version: String::default(),
            engine_store_git_hash: String::default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, OnlineConfig)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct ProxyConfig {
    #[online_config(submodule)]
    pub server: ServerConfig,

    #[online_config(submodule)]
    #[serde(rename = "raftstore")]
    pub raft_store: RaftstoreConfig,
}

pub const DEFAULT_ENGINE_ADDR: &str = if cfg!(feature = "failpoints") {
    "127.0.0.1:20206"
} else {
    ""
};

impl Default for ProxyConfig {
    fn default() -> Self {
        ProxyConfig {
            raft_store: RaftstoreConfig::default(),
            server: ServerConfig::default(),
        }
    }
}

impl ProxyConfig {
    pub fn from_file(
        path: &Path,
        unrecognized_keys: Option<&mut Vec<String>>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let s = std::fs::read_to_string(path)?;
        let mut deserializer = toml::Deserializer::new(&s);
        let cfg: ProxyConfig = if let Some(keys) = unrecognized_keys {
            serde_ignored::deserialize(&mut deserializer, |key| keys.push(key.to_string()))
        } else {
            <ProxyConfig as serde::Deserialize>::deserialize(&mut deserializer)
        }?;
        deserializer.end()?;
        Ok(cfg)
    }
}

pub fn ensure_no_common_unrecognized_keys(
    proxy_unrecognized_keys: &[String],
    unrecognized_keys: &[String],
) -> Result<(), String> {
    // We can't just compute intersection, since `rocksdb.z` equals not `rocksdb`.
    let proxy_part = HashSet::<_>::from_iter(proxy_unrecognized_keys.iter());
    let inter = unrecognized_keys
        .iter()
        .filter(|s| {
            let mut pref: String = String::from("");
            for p in s.split('.') {
                if !pref.is_empty() {
                    pref += "."
                }
                pref += p;
                if proxy_part.contains(&pref) {
                    // common unrecognized by both config.
                    return true;
                }
            }
            false
        })
        .collect::<Vec<_>>();
    if inter.len() != 0 {
        return Err(inter.iter().join(", "));
    }
    Ok(())
}

pub fn address_proxy_config(config: &mut TiKvConfig) {
    // We must add engine label to our TiFlash config
    pub const DEFAULT_ENGINE_LABEL_KEY: &str = "engine";
    let engine_name = match option_env!("ENGINE_LABEL_VALUE") {
        None => {
            fatal!("should set engine name with env variable `ENGINE_LABEL_VALUE`");
        }
        Some(name) => name.to_owned(),
    };
    config
        .server
        .labels
        .insert(DEFAULT_ENGINE_LABEL_KEY.to_owned(), engine_name);
}

#[allow(dead_code)]
pub fn validate_and_persist_config(config: &mut TiKvConfig, persist: bool) {
    config.compatible_adjust();
    if let Err(e) = config.validate() {
        fatal!("invalid configuration: {}", e);
    }

    if let Err(e) = check_critical_config(config) {
        fatal!("critical config check failed: {}", e);
    }

    if persist {
        if let Err(e) = tikv::config::persist_config(config) {
            fatal!("persist critical config failed: {}", e);
        }
    }
}

/// Prevents launching with an incompatible configuration
///
/// Loads the previously-loaded configuration from `last_tikv.toml`,
/// compares key configuration items and fails if they are not
/// identical.
pub fn check_critical_config(config: &TiKvConfig) -> Result<(), String> {
    // Check current critical configurations with last time, if there are some
    // changes, user must guarantee relevant works have been done.
    if let Some(mut cfg) = get_last_config(&config.storage.data_dir) {
        cfg.compatible_adjust();
        if let Err(e) = cfg.validate() {
            warn!("last_tikv.toml is invalid but ignored: {:?}", e);
        }
        config.check_critical_cfg_with(&cfg)?;
    }
    Ok(())
}

fn get_last_config(data_dir: &str) -> Option<TiKvConfig> {
    let store_path = Path::new(data_dir);
    let last_cfg_path = store_path.join(LAST_CONFIG_FILE);
    if last_cfg_path.exists() {
        return Some(
            TiKvConfig::from_file(&last_cfg_path, None).unwrap_or_else(|e| {
                fatal!(
                    "invalid auto generated configuration file {}, err {}",
                    last_cfg_path.display(),
                    e
                );
            }),
        );
    }
    None
}