#!/bin/bash

cd "$(dirname "$0")"
pwd
set -ex

ffi_path="../raftstore-proxy/ffi"
./${ffi_path}/format.sh
cargo run --package gen-proxy-ffi
