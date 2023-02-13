// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.
use engine_tiflash::{RawPSWriteBatchPtr, RawPSWriteBatchWrapper};

use crate::ffi::interfaces_ffi::{EngineStoreServerHelper, PageAndCppStrWithView};

pub struct TiFlashFFIHub {
    pub engine_store_server_helper: &'static EngineStoreServerHelper,
}
unsafe impl Send for TiFlashFFIHub {}
unsafe impl Sync for TiFlashFFIHub {}
impl engine_tiflash::FFIHubInner for TiFlashFFIHub {
    fn create_write_batch(&self) -> RawPSWriteBatchWrapper {
        // TODO There are too many dummy write batch created in non-uni-ps impl.
        // Need to work out a solution for this.
        // See engine_tiflash/src/write_batch.rs.
        self.engine_store_server_helper.create_write_batch().into()
    }

    fn destroy_write_batch(&self, wb_wrapper: &RawPSWriteBatchWrapper) {
        self.engine_store_server_helper
            .gc_raw_cpp_ptr(wb_wrapper.ptr, wb_wrapper.type_);
    }

    fn consume_write_batch(&self, wb: RawPSWriteBatchPtr) {
        self.engine_store_server_helper.consume_write_batch(wb)
    }

    fn write_batch_size(&self, wb: RawPSWriteBatchPtr) -> usize {
        self.engine_store_server_helper.write_batch_size(wb) as usize
    }

    fn write_batch_is_empty(&self, wb: RawPSWriteBatchPtr) -> bool {
        self.engine_store_server_helper.write_batch_is_empty(wb) != 0
    }

    fn write_batch_merge(&self, lwb: RawPSWriteBatchPtr, rwb: RawPSWriteBatchPtr) {
        self.engine_store_server_helper.write_batch_merge(lwb, rwb)
    }

    fn write_batch_clear(&self, wb: RawPSWriteBatchPtr) {
        self.engine_store_server_helper.write_batch_clear(wb)
    }

    fn write_batch_put_page(&self, wb: RawPSWriteBatchPtr, page_id: &[u8], page: &[u8]) {
        self.engine_store_server_helper
            .write_batch_put_page(wb, page_id.into(), page.into())
    }

    fn write_batch_del_page(&self, wb: RawPSWriteBatchPtr, page_id: &[u8]) {
        self.engine_store_server_helper
            .write_batch_del_page(wb, page_id.into())
    }

    fn read_page(&self, page_id: &[u8]) -> Option<Vec<u8>> {
        // TODO maybe we can steal memory from C++ here to reduce redundant copy?
        let value = self.engine_store_server_helper.read_page(page_id.into());
        return if value.view.len == 0 {
            None
        } else {
            Some(value.view.to_slice().to_vec())
        };
    }

    fn scan_page(
        &self,
        start_page_id: &[u8],
        end_page_id: &[u8],
        f: &mut dyn FnMut(&[u8], &[u8]) -> engine_traits::Result<bool>,
    ) {
        let values = self
            .engine_store_server_helper
            .scan_page(start_page_id.into(), end_page_id.into());
        let arr = values.inner as *mut PageAndCppStrWithView;
        for i in 0..values.len {
            let value = unsafe { &*arr.offset(i as isize) };
            if value.page_view.len != 0 {
                f(value.key_view.to_slice(), value.page_view.to_slice()).unwrap();
            }
        }
    }
}
