// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

pub mod blobtable;
pub mod memtable;
pub mod merge_iterator;
pub mod sstable;
pub mod table;
mod tests;

pub use merge_iterator::*;
pub use table::*;

pub struct EncryptionProperty {
    pub data_key_id: u64,
    pub encrypted_data_key: Vec<u8>,
    pub method: u8,
}
