use engine_rocks::raw::DB;
use engine_rocks::{Compat, RocksEngine, RocksSnapshot};
use engine_store_ffi::interfaces::root::DB as ffi_interfaces;
use engine_store_ffi::EngineStoreServerHelper;
use engine_store_ffi::RaftStoreProxyFFIHelper;
use engine_traits::IterOptions;
use engine_traits::Iterable;
use engine_traits::Iterator;
use engine_traits::Peekable;
use engine_traits::{Engines, SyncMutable};
use engine_traits::{CF_DEFAULT, CF_LOCK, CF_WRITE};
use protobuf::Message;
use raftstore::engine_store_ffi;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::pin::Pin;
use tikv_util::{debug, error, info, warn};
// use kvproto::raft_serverpb::{
//     MergeState, PeerState, RaftApplyState, RaftLocalState, RaftSnapshotData, RegionLocalState,
// };

type RegionId = u64;
#[derive(Default, Clone)]
pub struct Region {
    region: kvproto::metapb::Region,
    peer: kvproto::metapb::Peer,
    data: [BTreeMap<Vec<u8>, Vec<u8>>; 3],
    apply_state: kvproto::raft_serverpb::RaftApplyState,
}

pub struct EngineStoreServer {
    pub id: u64,
    pub engines: Option<Engines<RocksEngine, RocksEngine>>,
    pub kvstore: HashMap<RegionId, Box<Region>>,
}

impl EngineStoreServer {
    pub fn new(id: u64, engines: Option<Engines<RocksEngine, RocksEngine>>) -> Self {
        EngineStoreServer {
            id,
            engines,
            kvstore: Default::default(),
        }
    }
}

pub struct EngineStoreServerWrap {
    pub engine_store_server: *mut EngineStoreServer,
    pub maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
}

impl EngineStoreServerWrap {
    pub fn new(
        engine_store_server: *mut EngineStoreServer,
        maybe_proxy_helper: std::option::Option<*mut RaftStoreProxyFFIHelper>,
    ) -> Self {
        Self {
            engine_store_server,
            maybe_proxy_helper,
        }
    }

    unsafe fn handle_admin_raft_cmd(
        &mut self,
        req: &kvproto::raft_cmdpb::AdminRequest,
        resp: &kvproto::raft_cmdpb::AdminResponse,
        header: ffi_interfaces::RaftCmdHeader,
    ) -> ffi_interfaces::EngineStoreApplyRes {
        let region_id = header.region_id;
        info!("handle admin raft cmd"; "request"=>?req, "response"=>?resp, "index"=>header.index, "region-id"=>header.region_id);
        let do_handle_admin_raft_cmd = move |region: &mut Region| {
            if region.apply_state.get_applied_index() >= header.index {
                return ffi_interfaces::EngineStoreApplyRes::Persist;
            }
            ffi_interfaces::EngineStoreApplyRes::Persist
        };
        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_admin_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!("region {} not found", region_id);
                do_handle_admin_raft_cmd(v.insert(Default::default()))
            }
        }
    }

    unsafe fn handle_write_raft_cmd(
        &mut self,
        cmds: ffi_interfaces::WriteCmdsView,
        header: ffi_interfaces::RaftCmdHeader,
    ) -> ffi_interfaces::EngineStoreApplyRes {
        let region_id = header.region_id;
        let server = &mut (*self.engine_store_server);
        let kv = &mut (*self.engine_store_server).engines.as_mut().unwrap().kv;

        let do_handle_write_raft_cmd = move |region: &mut Region| {
            if region.apply_state.get_applied_index() >= header.index {
                return ffi_interfaces::EngineStoreApplyRes::None;
            }
            for i in 0..cmds.len {
                let key = &*cmds.keys.add(i as _);
                let val = &*cmds.vals.add(i as _);
                debug!(
                    "handle_write_raft_cmd add K {:?} V {:?} to region {} node id {}",
                    key.to_slice(),
                    val.to_slice(),
                    region_id,
                    server.id
                );
                let tp = &*cmds.cmd_types.add(i as _);
                let cf = &*cmds.cmd_cf.add(i as _);
                let cf_index = (*cf) as u8;
                let data = &mut region.data[cf_index as usize];
                match tp {
                    engine_store_ffi::WriteCmdType::Put => {
                        let _ = data.insert(key.to_slice().to_vec(), val.to_slice().to_vec());
                        let tikv_key = keys::data_key(key.to_slice());
                        kv.put_cf(
                            cf_to_name(cf.to_owned().into()),
                            &tikv_key,
                            &val.to_slice().to_vec(),
                        );
                    }
                    engine_store_ffi::WriteCmdType::Del => {
                        let tikv_key = keys::data_key(key.to_slice());
                        data.remove(tikv_key.as_slice());
                    }
                }
            }
            // Do not advance apply index
            ffi_interfaces::EngineStoreApplyRes::None
        };

        match (*self.engine_store_server).kvstore.entry(region_id) {
            std::collections::hash_map::Entry::Occupied(mut o) => {
                do_handle_write_raft_cmd(o.get_mut())
            }
            std::collections::hash_map::Entry::Vacant(v) => {
                warn!("region {} not found", region_id);
                do_handle_write_raft_cmd(v.insert(Default::default()))
            }
        }
    }
}

pub fn gen_engine_store_server_helper(
    wrap: Pin<&EngineStoreServerWrap>,
) -> EngineStoreServerHelper {
    EngineStoreServerHelper {
        magic_number: ffi_interfaces::RAFT_STORE_PROXY_MAGIC_NUMBER,
        version: ffi_interfaces::RAFT_STORE_PROXY_VERSION,
        inner: &(*wrap) as *const EngineStoreServerWrap as *mut _,
        fn_gen_cpp_string: Some(ffi_gen_cpp_string),
        fn_handle_write_raft_cmd: Some(ffi_handle_write_raft_cmd),
        fn_handle_admin_raft_cmd: Some(ffi_handle_admin_raft_cmd),
        fn_atomic_update_proxy: Some(ffi_atomic_update_proxy),
        fn_handle_destroy: Some(ffi_handle_destroy),
        fn_handle_ingest_sst: None,
        fn_handle_check_terminated: None,
        fn_handle_compute_store_stats: None,
        fn_handle_get_engine_store_server_status: None,
        fn_pre_handle_snapshot: Some(ffi_pre_handle_snapshot),
        fn_apply_pre_handled_snapshot: Some(ffi_apply_pre_handled_snapshot),
        fn_handle_http_request: None,
        fn_check_http_uri_available: None,
        fn_gc_raw_cpp_ptr: Some(ffi_gc_raw_cpp_ptr),
        fn_insert_batch_read_index_resp: None,
        fn_set_server_info_resp: None,
    }
}

unsafe fn into_engine_store_server_wrap(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
) -> &'static mut EngineStoreServerWrap {
    &mut *(arg1 as *mut EngineStoreServerWrap)
}

unsafe extern "C" fn ffi_handle_admin_raft_cmd(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::BaseBuffView,
    arg3: ffi_interfaces::BaseBuffView,
    arg4: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    let mut req = kvproto::raft_cmdpb::AdminRequest::default();
    let mut resp = kvproto::raft_cmdpb::AdminResponse::default();
    req.merge_from_bytes(arg2.to_slice()).unwrap();
    resp.merge_from_bytes(arg3.to_slice()).unwrap();
    store.handle_admin_raft_cmd(&req, &resp, arg4)
}

unsafe extern "C" fn ffi_handle_write_raft_cmd(
    arg1: *const ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::WriteCmdsView,
    arg3: ffi_interfaces::RaftCmdHeader,
) -> ffi_interfaces::EngineStoreApplyRes {
    let store = into_engine_store_server_wrap(arg1);
    store.handle_write_raft_cmd(arg2, arg3)
}

enum RawCppPtrTypeImpl {
    None = 0,
    String,
    PreHandledSnapshotWithBlock,
    PreHandledSnapshotWithFiles,
}

impl From<ffi_interfaces::RawCppPtrType> for RawCppPtrTypeImpl {
    fn from(o: ffi_interfaces::RawCppPtrType) -> Self {
        match o {
            0 => RawCppPtrTypeImpl::None,
            1 => RawCppPtrTypeImpl::String,
            2 => RawCppPtrTypeImpl::PreHandledSnapshotWithBlock,
            3 => RawCppPtrTypeImpl::PreHandledSnapshotWithFiles,
            _ => unreachable!(),
        }
    }
}

impl Into<ffi_interfaces::RawCppPtrType> for RawCppPtrTypeImpl {
    fn into(self) -> ffi_interfaces::RawCppPtrType {
        match self {
            RawCppPtrTypeImpl::None => 0,
            RawCppPtrTypeImpl::String => 1,
            RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => 2,
            RawCppPtrTypeImpl::PreHandledSnapshotWithFiles => 3,
        }
    }
}

#[no_mangle]
extern "C" fn ffi_gen_cpp_string(s: ffi_interfaces::BaseBuffView) -> ffi_interfaces::RawCppPtr {
    let str = Box::new(Vec::from(s.to_slice()));
    let ptr = Box::into_raw(str);
    ffi_interfaces::RawCppPtr {
        ptr: ptr as *mut _,
        type_: RawCppPtrTypeImpl::String.into(),
    }
}

#[no_mangle]
extern "C" fn ffi_gc_raw_cpp_ptr(
    ptr: ffi_interfaces::RawVoidPtr,
    tp: ffi_interfaces::RawCppPtrType,
) {
    match RawCppPtrTypeImpl::from(tp) {
        RawCppPtrTypeImpl::None => {}
        RawCppPtrTypeImpl::String => unsafe {
            Box::<Vec<u8>>::from_raw(ptr as *mut _);
        },
        RawCppPtrTypeImpl::PreHandledSnapshotWithBlock => unsafe {
            // Box::<Vec<u8>>::from_raw(ptr as *mut _);
        },
        RawCppPtrTypeImpl::PreHandledSnapshotWithFiles => unreachable!(),
    }
}

unsafe extern "C" fn ffi_atomic_update_proxy(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: *mut ffi_interfaces::RaftStoreProxyFFIHelper,
) {
    let store = into_engine_store_server_wrap(arg1);
    store.maybe_proxy_helper = Some(&mut *(arg2 as *mut RaftStoreProxyFFIHelper));
}

unsafe extern "C" fn ffi_handle_destroy(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: u64,
) {
    let store = into_engine_store_server_wrap(arg1);
    (*store.engine_store_server).kvstore.remove(&arg2);
}

type TiFlashRaftProxyHelper = RaftStoreProxyFFIHelper;

trait UnwrapExternCFunc<T> {
    unsafe fn into_inner(&self) -> &T;
}

impl<T> UnwrapExternCFunc<T> for std::option::Option<T> {
    unsafe fn into_inner(&self) -> &T {
        std::mem::transmute::<&Self, &T>(self)
    }
}

pub struct SSTReader<'a> {
    proxy_helper: &'a TiFlashRaftProxyHelper,
    inner: ffi_interfaces::SSTReaderPtr,
    type_: ffi_interfaces::ColumnFamilyType,
}

impl<'a> SSTReader<'a> {
    pub unsafe fn new(
        proxy_helper: &'a TiFlashRaftProxyHelper,
        view: &'a ffi_interfaces::SSTView,
    ) -> Self {
        SSTReader {
            proxy_helper,
            inner: (proxy_helper
                .sst_reader_interfaces
                .fn_get_sst_reader
                .into_inner())(view.clone(), proxy_helper.proxy_ptr.clone()),
            type_: view.type_,
        }
    }

    pub unsafe fn drop(&mut self) {
        (self.proxy_helper.sst_reader_interfaces.fn_gc.into_inner())(
            self.inner.clone(),
            self.type_,
        );
    }

    pub unsafe fn remained(&mut self) -> bool {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_remained
            .into_inner())(self.inner.clone(), self.type_)
            != 0
    }

    pub unsafe fn key(&mut self) -> ffi_interfaces::BaseBuffView {
        (self.proxy_helper.sst_reader_interfaces.fn_key.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }

    pub unsafe fn value(&mut self) -> ffi_interfaces::BaseBuffView {
        (self
            .proxy_helper
            .sst_reader_interfaces
            .fn_value
            .into_inner())(self.inner.clone(), self.type_)
    }

    pub unsafe fn next(&mut self) {
        (self.proxy_helper.sst_reader_interfaces.fn_next.into_inner())(
            self.inner.clone(),
            self.type_,
        )
    }
}

unsafe extern "C" fn ffi_pre_handle_snapshot(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    region_buff: ffi_interfaces::BaseBuffView,
    peer_id: u64,
    snaps: ffi_interfaces::SSTViewVec,
    index: u64,
    term: u64,
) -> ffi_interfaces::RawCppPtr {
    let store = into_engine_store_server_wrap(arg1);
    let proxy_helper = &mut *(store.maybe_proxy_helper.unwrap());
    let kvstore = &mut (*store.engine_store_server).kvstore;

    let mut req = kvproto::metapb::Region::default();
    assert_ne!(region_buff.data, std::ptr::null());
    assert_ne!(region_buff.len, 0);
    req.merge_from_bytes(region_buff.to_slice()).unwrap();

    let req_id = req.id;

    let mut region = Box::new(Region {
        region: req,
        peer: Default::default(),
        data: Default::default(),
        apply_state: Default::default(),
    });

    debug!("apply snaps with len {}", snaps.len);
    for i in 0..snaps.len {
        let mut snapshot = snaps.views.add(i as usize);
        let mut sst_reader =
            SSTReader::new(proxy_helper, &*(snapshot as *mut ffi_interfaces::SSTView));

        {
            region.apply_state.set_applied_index(index);
            region.apply_state.mut_truncated_state().set_index(index);
            region.apply_state.mut_truncated_state().set_term(term);
        }

        while sst_reader.remained() {
            let key = sst_reader.key();
            let value = sst_reader.value();
            // new_region->insert(snaps.views[i].type, TiKVKey(key.data, key.len), TiKVValue(value.data, value.len));

            let cf_index = (*snapshot).type_ as u8;
            let data = &mut region.data[cf_index as usize];
            let _ = data.insert(key.to_slice().to_vec(), value.to_slice().to_vec());

            sst_reader.next();
        }
    }

    ffi_interfaces::RawCppPtr {
        // ptr: std::ptr::null_mut(),
        ptr: Box::into_raw(region) as *const Region as ffi_interfaces::RawVoidPtr,
        // ptr: (region.as_ref()) as *const Region as ffi_interfaces::RawVoidPtr,
        type_: RawCppPtrTypeImpl::PreHandledSnapshotWithBlock.into(),
    }
}

pub fn cf_to_name(cf: ffi_interfaces::ColumnFamilyType) -> &'static str {
    match cf {
        ffi_interfaces::ColumnFamilyType::Lock => CF_LOCK,
        ffi_interfaces::ColumnFamilyType::Write => CF_WRITE,
        ffi_interfaces::ColumnFamilyType::Default => CF_DEFAULT,
        _ => unreachable!(),
    }
}

unsafe extern "C" fn ffi_apply_pre_handled_snapshot(
    arg1: *mut ffi_interfaces::EngineStoreServerWrap,
    arg2: ffi_interfaces::RawVoidPtr,
    arg3: ffi_interfaces::RawCppPtrType,
) {
    let store = into_engine_store_server_wrap(arg1);
    let req = &mut *(arg2 as *mut Region);
    let node_id = (*store.engine_store_server).id;

    // let region = req;

    let mut region = Box::from_raw(req);
    let req_id = region.region.id;

    &(*store.engine_store_server).kvstore.insert(req_id, region);

    let region = (*store.engine_store_server).kvstore.get(&req_id).unwrap();

    let kv = &mut (*store.engine_store_server).engines.as_mut().unwrap().kv;
    for cf in 0..3 {
        for (k, v) in &region.data[cf] {
            let tikv_key = keys::data_key(k.as_slice());
            let cf_name = cf_to_name(cf.into());
            kv.put_cf(cf_name, &tikv_key, &v);
        }
    }
}
