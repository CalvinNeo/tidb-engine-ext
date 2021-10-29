cargo clean
rustup component list | grep "llvm-tools-preview-x86_64-unknown-linux-gnu (installed)"
if [ $? -ne 0 ]; then
  rustup component add llvm-tools-preview
fi
cargo install --list | grep grcov
if [ $? -ne 0 ]; then
  cargo install grcov
fi

if [ ${GENERATE_COV:-0} -ne 0 ]; then
  export RUSTFLAGS="-Zinstrument-coverage"
  export LLVM_PROFILE_FILE="tidb-engine-ext-%p-%m.profraw"
fi

cargo test --package tests --test failpoints -- cases::test_normal && \
cargo test --package tests --test failpoints -- cases::test_bootstrap && \
cargo test --package tests --test failpoints -- cases::test_compact_log && \
cargo test --package tests --test failpoints -- cases::test_early_apply && \
cargo test --package tests --test failpoints -- cases::test_encryption && \
cargo test --package tests --test failpoints -- cases::test_pd_client && \
cargo test --package tests --test failpoints -- cases::test_pending_peers && \
cargo test --package tests --test failpoints -- cases::test_transaction && \
cargo test --package tests --test failpoints -- cases::test_cmd_epoch_checker && \
cargo test --package tests --test failpoints -- cases::test_disk_full && \
cargo test --package tests --test failpoints -- cases::test_stale_peer && \
cargo test --package tests --test failpoints -- cases::test_import_service && \
cargo test --package tests --test failpoints -- cases::test_split_region --skip test_report_approximate_size_after_split_check && \
cargo test --package tests --test failpoints -- cases::test_snap && \
cargo test --package tests --test failpoints -- cases::test_merge && \
cargo test --package tests --test failpoints -- cases::test_replica_read && \
# TiFlash do not support stale read currently
#cargo test --package tests --test failpoints -- cases::test_replica_stale_read && \
cargo test --package tests --test failpoints -- cases::test_server

cargo test --package tests --test integrations -- raftstore::test_bootstrap && \
cargo test --package tests --test integrations -- raftstore::test_clear_stale_data && \
cargo test --package tests --test integrations -- raftstore::test_compact_after_delete && \
cargo test --package tests --test integrations -- raftstore::test_compact_log && \
# Sometimes fails
#cargo test --package tests --test integrations -- raftstore::test_conf_change && \
cargo test --package tests --test integrations -- raftstore::test_early_apply && \
cargo test --package tests --test integrations -- raftstore::test_hibernate && \
cargo test --package tests --test integrations -- raftstore::test_joint_consensus && \
cargo test --package tests --test integrations -- raftstore::test_replica_read && \
cargo test --package tests --test integrations -- raftstore::test_snap && \
# Sometimes fails
#cargo test --package tests --test integrations -- raftstore::test_split_region && \
cargo test --package tests --test integrations -- raftstore::test_stale_peer && \
cargo test --package tests --test integrations -- raftstore::test_status_command && \
cargo test --package tests --test integrations -- raftstore::test_prevote && \
cargo test --package tests --test integrations -- raftstore::test_region_change_observer && \
cargo test --package tests --test integrations -- raftstore::test_region_heartbeat && \
cargo test --package tests --test integrations -- raftstore::test_region_info_accessor && \
cargo test --package tests --test integrations -- raftstore::test_transfer_leader && \
cargo test --package tests --test integrations -- raftstore::test_single && \
# Sometimes fails
cargo test --package tests --test integrations -- raftstore::test_merge && \
cargo test --package tests --test integrations -- raftstore::test_tombstone && \
cargo test --package tests --test integrations -- server::kv_service::test_read_index_check_memory_locks && \


if [ ${GENERATE_COV:-0} -ne 0 ]; then
  grcov . --binary-path target/debug/ . -t html --branch --ignore-not-existing -o ./coverage/
fi