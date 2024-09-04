// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
#![allow(stable_features)]
#![feature(vec_into_raw_parts)]
#![feature(slice_take)]
#![feature(return_position_impl_trait_in_trait)]

pub mod mock_cluster;
pub mod mock_store;

pub use engine_store_ffi::ffi::interfaces_ffi;
pub use mock_store::*;
pub use tikv_util::{box_err, box_try, debug, error, info, warn};
