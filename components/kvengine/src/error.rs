// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{dfs, table};

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("key not found")]
    KeyNotFound,
    #[error("shard not found")]
    ShardNotFound,
    #[error("key not match")]
    ShardNotMatch,
    #[error("already splitting")]
    AlreadySplitting,
    #[error("alloc id error {0}")]
    ErrAllocId(String),
    #[error("open error {0}")]
    ErrOpen(String),
    #[error("table error {0}")]
    TableError(table::Error),
    #[error("dfs error {0}")]
    DfsError(dfs::Error),
    #[error("IO error {0}")]
    Io(std::io::Error),
    #[error("remote compaction {0}")]
    RemoteCompaction(String),
    #[error("apply change set {0}")]
    ApplyChangeSet(String),
    #[error("ingest files {0}")]
    IngestFiles(String),
    #[error("check merge {0}")]
    CheckMerge(String),
    #[error("incompatible remote compactor {}:{}", .url, .msg)]
    IncompatibleRemoteCompactor { url: String, msg: String },
    #[error("fallback to local compactor disabled")]
    FallbackLocalCompactorDisabled,
    #[error("remote read error {0}")]
    RemoteRead(String),
}

impl From<table::Error> for Error {
    fn from(e: table::Error) -> Self {
        Error::TableError(e)
    }
}

impl From<dfs::Error> for Error {
    fn from(e: dfs::Error) -> Self {
        Error::DfsError(e)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e)
    }
}

impl From<hyper::Error> for Error {
    fn from(e: hyper::Error) -> Self {
        Error::RemoteCompaction(e.to_string())
    }
}

impl From<http::Error> for Error {
    fn from(e: http::Error) -> Self {
        Error::RemoteCompaction(e.to_string())
    }
}
