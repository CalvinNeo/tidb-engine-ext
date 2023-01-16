// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use core::ops::Bound::{Excluded, Included, Unbounded};
use std::{
    collections::BTreeMap,
    sync::{atomic::AtomicU64, Arc, RwLock},
};

pub use engine_store_ffi::{
    interfaces::root::DB as ffi_interfaces, BaseBuffView, CppStrWithView, EngineStoreServerHelper,
    PageAndCppStrWithView, RaftStoreProxyFFIHelper, RawCppPtr, RawCppPtrCarr, RawVoidPtr,
};

use crate::{
    create_cpp_str, create_cpp_str_parts,
    mock_store::{into_engine_store_server_wrap, RawCppPtrTypeImpl},
};

pub enum MockPSSingleWrite {
    Put((Vec<u8>, MockPSUniversalPage)),
    Delete(Vec<u8>),
}

pub struct MockPSWriteBatch {
    pub data: Vec<(u64, MockPSSingleWrite)>,
    core: Arc<RwLock<MockPageStorageCore>>,
}

impl MockPSWriteBatch {
    fn new(core: Arc<RwLock<MockPageStorageCore>>) -> Self {
        Self {
            data: Default::default(),
            core,
        }
    }
}

pub struct MockPSUniversalPage {
    data: Vec<u8>,
}

impl Into<MockPSUniversalPage> for BaseBuffView {
    fn into(self) -> MockPSUniversalPage {
        MockPSUniversalPage {
            data: self.to_slice().to_owned(),
        }
    }
}

pub struct MockPageStorageCore {
    current_id: AtomicU64,
}

impl MockPageStorageCore {
    pub fn alloc_id(&mut self) -> u64 {
        self.current_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

impl Default for MockPageStorageCore {
    fn default() -> Self {
        Self {
            current_id: AtomicU64::new(1),
        }
    }
}

#[derive(Default)]
pub struct MockPageStorage {
    pub data: RwLock<BTreeMap<Vec<u8>, MockPSUniversalPage>>,
    pub core: Arc<RwLock<MockPageStorageCore>>,
}

pub unsafe extern "C" fn ffi_mockps_create_write_batch(
    wrap: *const ffi_interfaces::EngineStoreServerWrap,
) -> RawCppPtr {
    let store = into_engine_store_server_wrap(wrap);
    let core = (*store.engine_store_server).page_storage.core.clone();
    let ptr = Box::into_raw(Box::new(MockPSWriteBatch::new(core)));
    RawCppPtr {
        ptr: ptr as RawVoidPtr,
        type_: RawCppPtrTypeImpl::PSWriteBatch.into(),
    }
}

impl From<RawVoidPtr> for &mut MockPSWriteBatch {
    fn from(value: RawVoidPtr) -> Self {
        unsafe { &mut *(value as *mut MockPSWriteBatch) }
    }
}

pub unsafe extern "C" fn ffi_mockps_write_batch_put_page(
    wb: RawVoidPtr,
    page_id: BaseBuffView,
    page: BaseBuffView,
) {
    let wb: &mut MockPSWriteBatch = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(wb);
    let wid = wb.core.write().unwrap().alloc_id();
    let write = MockPSSingleWrite::Put((page_id.to_slice().to_vec(), page.into()));
    wb.data.push((wid, write));
}

pub unsafe extern "C" fn ffi_mockps_write_batch_del_page(wb: RawVoidPtr, page_id: BaseBuffView) {
    let wb: &mut MockPSWriteBatch = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(wb);
    let wid = wb.core.write().unwrap().alloc_id();
    let write = MockPSSingleWrite::Delete(page_id.to_slice().to_vec());
    wb.data.push((wid, write));
}

pub unsafe extern "C" fn ffi_mockps_write_batch_size(wb: RawVoidPtr) -> u64 {
    let wb: _ = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(wb);
    wb.data.len() as u64
}

pub unsafe extern "C" fn ffi_mockps_write_batch_is_empty(wb: RawVoidPtr) -> u8 {
    let wb: _ = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(wb);
    if wb.data.is_empty() { 1 } else { 0 }
}

pub unsafe extern "C" fn ffi_mockps_write_batch_merge(lwb: RawVoidPtr, rwb: RawVoidPtr) {
    let lwb: _ = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(lwb);
    let rwb: _ = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(rwb);
    lwb.data.extend(rwb.data.drain(..));
}

pub unsafe extern "C" fn ffi_mockps_write_batch_clear(wb: RawVoidPtr) {
    let wb: _ = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(wb);
    wb.data.clear();
}

pub unsafe extern "C" fn ffi_mockps_consume_write_batch(
    wrap: *const ffi_interfaces::EngineStoreServerWrap,
    wb: RawVoidPtr,
) {
    let store = into_engine_store_server_wrap(wrap);
    let wb: _ = <&mut MockPSWriteBatch as From<RawVoidPtr>>::from(wb);
    let mut guard = (*store.engine_store_server)
        .page_storage
        .data
        .write()
        .unwrap();
    wb.data.sort_by_key(|k| k.0);
    for (_, write) in wb.data.drain(..) {
        match write {
            MockPSSingleWrite::Put(w) => {
                guard.insert(w.0, w.1);
            }
            MockPSSingleWrite::Delete(w) => {
                guard.remove(&w);
            }
        }
    }
}

pub unsafe extern "C" fn ffi_mockps_handle_read_page(
    wrap: *const ffi_interfaces::EngineStoreServerWrap,
    page_id: BaseBuffView,
) -> CppStrWithView {
    let store = into_engine_store_server_wrap(wrap);
    let guard = (*store.engine_store_server)
        .page_storage
        .data
        .read()
        .unwrap();
    let key = page_id.to_slice().to_vec();
    match guard.get(&key) {
        Some(p) => create_cpp_str(Some(p.data.clone())),
        None => create_cpp_str(None),
    }
}

pub unsafe extern "C" fn ffi_mockps_handle_scan_page(
    wrap: *const ffi_interfaces::EngineStoreServerWrap,
    start_page_id: BaseBuffView,
    end_page_id: BaseBuffView,
) -> RawCppPtrCarr {
    let store = into_engine_store_server_wrap(wrap);
    let guard = (*store.engine_store_server)
        .page_storage
        .data
        .read()
        .unwrap();
    let range = guard.range((
        Included(start_page_id.to_slice().to_vec()),
        Excluded(end_page_id.to_slice().to_vec()),
    ));
    let range = range.collect::<Vec<_>>();
    let mut result: Vec<PageAndCppStrWithView> = Vec::with_capacity(range.len());
    for (k, v) in range.into_iter() {
        let (page, page_view) = create_cpp_str_parts(Some(v.data.clone()));
        let (key, key_view) = create_cpp_str_parts(Some(k.clone()));
        let pacwv = PageAndCppStrWithView {
            page,
            key,
            page_view,
            key_view,
        };
        result.push(pacwv)
    }
    let (result_ptr, l, c) = result.into_raw_parts();
    assert_eq!(l, c);
    RawCppPtrCarr {
        inner: result_ptr as RawVoidPtr,
        len: c as u64,
        type_: RawCppPtrTypeImpl::PSPageAndCppStr.into(),
    }
}

pub unsafe extern "C" fn ffi_mockps_handle_purge_pagestorage(
    _wrap: *const ffi_interfaces::EngineStoreServerWrap,
) {
    // TODO
}

pub unsafe extern "C" fn ffi_mockps_handle_seek_ps_key(
    wrap: *const ffi_interfaces::EngineStoreServerWrap,
    page_id: BaseBuffView,
) -> CppStrWithView {
    // Find the first great or equal than
    let store = into_engine_store_server_wrap(wrap);
    let guard = (*store.engine_store_server)
        .page_storage
        .data
        .read()
        .unwrap();
    let mut range = guard.range((Included(page_id.to_slice().to_vec()), Unbounded));
    let kv = range.next().unwrap();
    create_cpp_str(Some(kv.0.clone()))
}

pub unsafe extern "C" fn ffi_mockps_ps_is_empty(
    wrap: *const ffi_interfaces::EngineStoreServerWrap,
) -> u8 {
    let store = into_engine_store_server_wrap(wrap);
    let guard = (*store.engine_store_server)
        .page_storage
        .data
        .read()
        .unwrap();
    if guard.is_empty() { 1 } else { 0 }
}