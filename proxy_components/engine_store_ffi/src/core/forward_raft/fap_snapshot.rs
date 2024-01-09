// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

use crate::{
    core::{common::*, ProxyForwarder},
    fatal,
};

impl<T: Transport + 'static, ER: RaftEngine> ProxyForwarder<T, ER> {
    // Check if the snapshot is the first after restart.
    pub fn is_first_snapshot(
        &self,
        region_id: u64,
        cached_info: Option<Arc<CachedRegionInfo>>,
    ) -> bool {
        if let Some(c) = cached_info {
            // After restarted, inited_or_fallback and snapshot_inflight could both be
            // cleaned. We must ask TiFlash to check if there is already inited
            // region peer.
            if (!c.inited_or_fallback.load(Ordering::SeqCst))
                && c.snapshot_inflight.load(Ordering::SeqCst) != 0
            {
                return true;
            }
        }
        self.engine_store_server_helper
            .kvstore_region_exist(region_id)
    }

    pub fn pre_apply_snapshot_for_fap_snapshot(
        &self,
        ob_region: &Region,
        peer_id: u64,
        snap_key: &store::SnapKey,
    ) -> bool {
        let region_id = ob_region.get_id();
        let mut should_skip = false;
        #[allow(clippy::collapsible_if)]
        if self.packed_envs.engine_store_cfg.enable_fast_add_peer {
            if self.get_cached_manager().access_cached_region_info_mut(
                region_id,
                |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                    MapEntry::Occupied(o) => {
                        if self.is_first_snapshot(region_id, Some(o.get().clone())) {
                            info!("fast path: prehandle first snapshot {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                            );
                            should_skip = true;
                        }
                    }
                    MapEntry::Vacant(_) => {
                        // If there is an fap snapshot, and we'll skip here after restared.
                        // Otherwise, there could be redunduant prehandling.
                        let pstate = self.engine_store_server_helper.query_fap_snapshot_state(region_id, peer_id);
                        if pstate == proxy_ffi::interfaces_ffi::FapSnapshotState::Persisted {
                            info!("fast path: prehandle first snapshot skipped after restart {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                            );
                            should_skip = true;
                        } else {
                            info!("fast path: prehandle first snapshot no skipped after restart {}:{} {}", self.store_id, region_id, peer_id;
                                "snap_key" => ?snap_key,
                                "region_id" => region_id,
                                "state" => ?pstate,
                                "inited" => false,
                            );
                        }
                    }
                },
            ).is_err() {
                fatal!("post_apply_snapshot poisoned")
            };
        }
        should_skip
    }

    pub fn post_apply_snapshot_for_fap_snapshot(
        &self,
        ob_region: &Region,
        peer_id: u64,
        snap_key: &store::SnapKey,
    ) -> bool {
        let region_id = ob_region.get_id();
        let try_apply_fap_snapshot = |c: Arc<CachedRegionInfo>, restarted: bool| {
            info!("fast path: start applying first snapshot {}:{} {}", self.store_id, region_id, peer_id;
                "snap_key" => ?snap_key,
                "region_id" => region_id,
            );
            // Even if the feature is not enabled, the snapshot could still be a previously
            // generated fap snapshot. So we have to also handle this snapshot,
            // to prevent error data.
            let current_enabled = self.packed_envs.engine_store_cfg.enable_fast_add_peer;
            let snapshot_sent_time = c.snapshot_inflight.load(Ordering::SeqCst);
            let fap_start_time = c.fast_add_peer_start.load(Ordering::SeqCst);
            let current = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap();

            let assert_exist = if !restarted {
                snapshot_sent_time != 0
            } else {
                false
            };
            if !self
                .engine_store_server_helper
                .apply_fap_snapshot(region_id, peer_id, assert_exist)
            {
                // This is not a fap snapshot.
                info!("fast path: this is not fap snapshot {}:{} {}, goto tikv snapshot", self.store_id, region_id, peer_id;
                    "snap_key" => ?snap_key,
                    "region_id" => region_id,
                    "cost_snapshot" => current.as_millis() - snapshot_sent_time,
                    "cost_total" => current.as_millis() - fap_start_time,
                    "current_enabled" => current_enabled,
                    "from_restart" => restarted,
                );
                c.snapshot_inflight.store(0, Ordering::SeqCst);
                c.fast_add_peer_start.store(0, Ordering::SeqCst);
                c.inited_or_fallback.store(true, Ordering::SeqCst);
                return false;
            }
            info!("fast path: finished applied first snapshot {}:{} {}, recover MsgAppend", self.store_id, region_id, peer_id;
                "snap_key" => ?snap_key,
                "region_id" => region_id,
                "cost_snapshot" => current.as_millis() - snapshot_sent_time,
                "cost_total" => current.as_millis() - fap_start_time,
                "current_enabled" => current_enabled,
                "from_restart" => restarted,
            );
            c.snapshot_inflight.store(0, Ordering::SeqCst);
            c.fast_add_peer_start.store(0, Ordering::SeqCst);
            c.inited_or_fallback.store(true, Ordering::SeqCst);
            true
        };

        // We should handle fap snapshot even if enable_fast_add_peer is false.
        // However, if enable_unips, by no means can we handle fap snapshot.
        #[allow(unused_mut)]
        let mut should_check_fap_snapshot = self.packed_envs.engine_store_cfg.enable_unips;
        #[allow(clippy::redundant_closure_call)]
        (|| {
            fail::fail_point!("post_apply_snapshot_allow_no_unips", |_| {
                // UniPS can't provide a snapshot currently
                should_check_fap_snapshot = true;
            });
        })();

        let mut applied_fap = false;
        #[allow(clippy::collapsible_if)]
        if should_check_fap_snapshot {
            if self
                .get_cached_manager()
                .access_cached_region_info_mut(
                    region_id,
                    |info: MapEntry<u64, Arc<CachedRegionInfo>>| match info {
                        MapEntry::Occupied(o) => {
                            if self.is_first_snapshot(region_id, Some(o.get().clone())) {
                                applied_fap = try_apply_fap_snapshot(o.get().clone(), false);
                            }
                        }
                        MapEntry::Vacant(_) => {
                            // It could be an fap snapshot after restart.
                            // However, the region state is initialized.
                            assert!(self.is_initialized(region_id));
                            let o = Arc::new(CachedRegionInfo::default());
                            applied_fap = try_apply_fap_snapshot(o, true);
                        }
                    },
                )
                .is_err()
            {
                fatal!("poisoned");
            }
        }
        applied_fap
    }
}
