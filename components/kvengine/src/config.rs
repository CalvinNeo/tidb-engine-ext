// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use tikv_util::config::ReadableDuration;

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// The maximum delay duration for delete range.
    pub max_del_range_delay: ReadableDuration,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_del_range_delay: ReadableDuration::secs(3600),
        }
    }
}
