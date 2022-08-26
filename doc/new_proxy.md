# New TiFlash Proxy

Author(s): [CalvinNeo](github.com/CalvinNeo)

## Motivation

TiFlash Proxy is used to be a fork of TiKV which can replicate data from TiKV to TiFlash. However, since TiKV is upgrading rapidly, it brings lots of trouble for the old version:
1. Proxy can't cherry-pick TiKV 's bugfix in time.
2. Proxy can't take advantage of up-to-date TiKV release.
3. It is hard to take proxy into account when TiKV is evolving.

## Overview Design

Generally speaking, there are two storage components in TiKV for maintaining multi-raft RSM: `RaftEngine` and `KvEngine`：
1. KvEngine is mainly used for applying raft command and providing key-value services.
   Multiple modifications about region data/meta/apply-state will be encapsulated into one `Write Batch` and written into KvEngine atomically.
1. RaftEngine will parse its own committed raft log into corresponding normal/admin raft commands, which will be handled by the apply process.

Majority writes to TiFlash will be forwarded into TiFlash by Proxy, so we don't need to write them again into Proxy's KvEngine. However, we still need to store some meta data in KvEngine, so TiKV's original `RocksEngine` is also necessary.
An idea to solve this is to use a self-defined KvEngine which hold TiKV's RocksEngine and implements TiKV's `Engine Traits`. We can skip unwanted writes into Proxy's side with this new KvEngine, including:
1. Skip writes into `Write Batch` or RocksDB for specific cfs and keys.
2. Skip ingesting SST files into RocksDB.

However, it may cost a lot to perform such a replacement:
1. It's not easy to guarantee atomicity while writing/reading dynamic key-value pair(such as meta/apply-state) and patterned data(strong schema) together for other storage systems.
1. A few modules and components(like importer or lighting) reply on the SST format of KvEngine in TiKV. Foe example, thoses SST files shall be transformed to adapt a column storage engine.
1. A flush to storage layer may be more expensive in other storage engine than TiKV.

Therefore, apart from engine traits, we also need `coprocessor`s to observe and control some behaviors of raftstore:
1. Observers triggered before and after execution of every raft command which can filter execution of some commands and suggest a persistence after execution.
2. Observers triggered before and after execution of applying snapshot which can do some transformation of the pending snapshot.
3. Observers triggered when we received an empty raft entry.
4. Observers triggered when a region peer is destroyed.
5. Observers that helps compute storage status like used/total size.

With help of these observers and engine traits, we can move Proxy's logic out of raftstore and TiKV.

## Detailed Design

The whole work can be divided into two parts:
1. TiKV side
   TiKV provides new engine traits interfaces and observers.
1. TiFlash side
   By implementing these new interfaces and observers, TiFlash can receive data from TiKV and control's behavior of raftstore.

### TiKV side

As described in [tikv#12849](https://github.com/tikv/tikv/issues/12849).

### TiFlash side

As described in [tiflash#5170](https://github.com/pingcap/tiflash/issues/5170).

