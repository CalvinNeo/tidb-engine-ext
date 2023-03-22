// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use std::{env, error::Error};

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub prefix: String,

    pub s3_endpoint: String,

    pub s3_key_id: String,

    pub s3_secret_key: String,

    pub s3_bucket: String,

    pub s3_region: String,

    pub remote_compactor_addr: String,

    pub zstd_compression_level: String,

    pub remote_analyzer_addr: String,

    pub allow_fallback_local: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            prefix: "".to_string(),
            s3_endpoint: "".to_string(),
            s3_key_id: "".to_string(),
            s3_secret_key: "".to_string(),
            s3_bucket: "".to_string(),
            s3_region: "".to_string(),
            remote_compactor_addr: "".to_string(),
            zstd_compression_level: "".to_string(),
            remote_analyzer_addr: "".to_string(),
            allow_fallback_local: true,
        }
    }
}

impl Config {
    #[allow(dead_code)]
    fn validate(&self) -> Result<(), Box<dyn Error>> {
        // TODO(x) validate dfs config
        Ok(())
    }

    fn env_or_default(name: &str, val: &mut String) {
        if let Ok(v) = env::var(name) {
            *val = v;
        }
    }

    fn env_or_default_bool(name: &str, val: &mut bool) {
        if let Ok(v) = env::var(name) {
            *val = v == "true";
        }
    }

    pub fn override_from_env(&mut self) {
        Self::env_or_default("DFS_S3_BUCKET", &mut self.s3_bucket);
        Self::env_or_default("DFS_S3_ENDPOINT", &mut self.s3_endpoint);
        Self::env_or_default("DFS_PREFIX", &mut self.prefix);
        Self::env_or_default("DFS_S3_KEY_ID", &mut self.s3_key_id);
        Self::env_or_default("DFS_S3_SECRET_KEY", &mut self.s3_secret_key);
        Self::env_or_default("DFS_S3_REGION", &mut self.s3_region);
        Self::env_or_default("DFS_REMOTE_COMPACTOR_ADDR", &mut self.remote_compactor_addr);
        Self::env_or_default("DFS_REMOTE_ANALYZER_ADDR", &mut self.remote_analyzer_addr);
        Self::env_or_default(
            "DFS_ZSTD_COMPRESSION_LEVEL",
            &mut self.zstd_compression_level,
        );

        Self::env_or_default_bool("DFS_ALLOW_FALLBACK_LOCAL", &mut self.allow_fallback_local);
    }
}
