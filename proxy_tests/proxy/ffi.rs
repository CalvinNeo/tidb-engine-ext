// Copyright 2022 TiKV Project Authors. Licensed under Apache-2.0.

use engine_store_ffi::{
    get_engine_store_server_helper, EngineStoreServerHelper, RawCppPtr, RawCppPtrArr, RawCppPtrCArr,
    RawCppPtrTuple, RawVoidPtr, UnwrapExternCFunc,
};
use new_mock_engine_store::{
    mock_cluster::init_global_ffi_helper_set, mock_store::RawCppPtrTypeImpl,
};

#[test]
fn test_tuple_of_raw_cpp_ptr() {
    tikv_util::set_panic_hook(true, "./");
    unsafe {
        init_global_ffi_helper_set();
        let helper = get_engine_store_server_helper();

        let len = 10;
        let mut v: Vec<RawCppPtr> = vec![];

        for i in 0..len {
            let s = format!("s{}", i);
            let raw_cpp_ptr = (helper.fn_gen_cpp_string.into_inner())(s.as_bytes().into());
            v.push(raw_cpp_ptr);
        }

        let (ptr_v, l, cap) = v.into_raw_parts();
        assert_ne!(l, cap);
        let cpp_ptr_tp = RawCppPtrTuple {
            inner: ptr_v,
            len: cap as u64,
        };
        for i in 0..cap {
            let inner_i = cpp_ptr_tp.inner.add(i);
        }
        drop(cpp_ptr_tp);
    }
}

#[test]
fn test_array_of_raw_cpp_ptr() {
    tikv_util::set_panic_hook(true, "./");
    unsafe {
        init_global_ffi_helper_set();
        let helper = get_engine_store_server_helper();

        let len = 10;
        let mut v: Vec<RawVoidPtr> = vec![];

        for i in 0..len {
            let s = format!("s{}", i);
            let raw_cpp_ptr = (helper.fn_gen_cpp_string.into_inner())(s.as_bytes().into());
            let raw_void_ptr = raw_cpp_ptr.into_raw();
            v.push(raw_void_ptr);
        }

        let (ptr_v, l, cap) = v.into_raw_parts();
        assert_ne!(l, cap);
        let cpp_ptr_arr = RawCppPtrArr {
            inner: ptr_v,
            type_: RawCppPtrTypeImpl::String.into(),
            len: cap as u64,
        };
        drop(cpp_ptr_arr);
    }
}

#[test]
fn test_carray_of_raw_cpp_ptr() {
    tikv_util::set_panic_hook(true, "./");
    unsafe {
        init_global_ffi_helper_set();
        let helper = get_engine_store_server_helper();

        const len: usize = 10;
        let mut v: [RawVoidPtr; len] = [std::ptr::null_mut(); len];

        for i in 0..len {
            let s = format!("s{}", i);
            let raw_cpp_ptr = (helper.fn_gen_cpp_string.into_inner())(s.as_bytes().into());
            let raw_void_ptr = raw_cpp_ptr.into_raw();
            v[i] = raw_void_ptr;
            println!("!!!! raw_void_ptr {} {:?}", i, v[i]);
        }

        let slice_ptr = v.as_mut_slice();
        let pv = slice_ptr.as_mut_ptr();
        let pv1 = v[0].add(0);
        println!("!!!! raw_void_ptr pv1 {:?}", pv1);
        
        let cpp_ptr_arr = RawCppPtrCArr {
            inner: pv1,
            type_: RawCppPtrTypeImpl::String.into(),
            len: len as u64,
        };
        drop(cpp_ptr_arr);
    }
}
