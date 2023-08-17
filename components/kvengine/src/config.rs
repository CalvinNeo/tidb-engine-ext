// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashMap, HashSet},
    error::Error,
};

use tikv_util::config::ReadableDuration;

use crate::table::blobtable::builder::BlobTableBuildOptions;

#[derive(Default, Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct PerKeyspaceConfig {
    pub keyspace: u32,
    pub blob_table_build_options: BlobTableBuildOptions,
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The maximum delay duration for delete range.
    pub max_del_range_delay: ReadableDuration,
    pub compaction_request_version: u32,
    pub per_keyspace_configs: Vec<PerKeyspaceConfig>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_del_range_delay: ReadableDuration::secs(3600),
            compaction_request_version: 2,
            per_keyspace_configs: vec![],
        }
    }
}

impl Config {
    pub fn validate(&self) -> Result<(), Box<dyn Error>> {
        let mut keyspace_set = HashSet::new();
        for cfg in &self.per_keyspace_configs {
            if !keyspace_set.insert(cfg.keyspace) {
                return Err(format!("duplicate keyspace {}", cfg.keyspace).into());
            }
        }
        Ok(())
    }

    pub fn get_per_keyspace_configs(&self) -> HashMap<u32, PerKeyspaceConfig> {
        let mut map = HashMap::new();
        for cfg in &self.per_keyspace_configs {
            map.insert(cfg.keyspace, cfg.clone());
        }
        map
    }
}
