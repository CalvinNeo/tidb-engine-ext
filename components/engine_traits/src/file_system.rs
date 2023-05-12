// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

use std::sync::Arc;

use bytes::Bytes;
use file_system::{get_io_rate_limiter, get_io_type, IoOp, IoRateLimiter};

use crate::Result;

pub trait FileSystemInspector: Sync + Send {
    fn read(&self, len: usize) -> Result<usize>;
    fn write(&self, len: usize) -> Result<usize>;
}

pub struct EngineFileSystemInspector {
    limiter: Option<Arc<IoRateLimiter>>,
}

impl EngineFileSystemInspector {
    #[allow(dead_code)]
    pub fn new() -> Self {
        EngineFileSystemInspector {
            limiter: get_io_rate_limiter(),
        }
    }

    pub fn from_limiter(limiter: Option<Arc<IoRateLimiter>>) -> Self {
        EngineFileSystemInspector { limiter }
    }
}

impl Default for EngineFileSystemInspector {
    fn default() -> Self {
        Self::new()
    }
}

impl FileSystemInspector for EngineFileSystemInspector {
    fn read(&self, len: usize) -> Result<usize> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IoOp::Read, len))
        } else {
            Ok(len)
        }
    }

    fn write(&self, len: usize) -> Result<usize> {
        if let Some(limiter) = &self.limiter {
            let io_type = get_io_type();
            Ok(limiter.request(io_type, IoOp::Write, len))
        } else {
            Ok(len)
        }
    }
}

#[derive(Debug, Default)]
pub struct GetObjectOptions {
    pub start_off: u64,
    pub end_off: Option<u64>,
}

impl GetObjectOptions {
    pub fn is_full_range(&self) -> bool {
        self.start_off == 0 && self.end_off.is_none()
    }

    pub fn range_string(&self) -> String {
        format!(
            "{}-{}",
            self.start_off,
            self.end_off.map_or(String::new(), |e| format!("{}", e))
        )
    }
}

pub trait ObjectStorage: Sync + Send {
    fn put_objects(&self, objects: Vec<(String, Bytes)>) -> std::result::Result<(), String>;
    fn get_objects(
        &self,
        keys: Vec<(String, GetObjectOptions)>,
    ) -> std::result::Result<Vec<(String, Bytes)>, String>;
}
