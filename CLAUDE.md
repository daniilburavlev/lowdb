# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

LowDB is a from-scratch, LSM-tree-based key-value store written in Rust, split into a Cargo workspace of small crates. It follows the classic LSM design: writes go to a WAL + in-memory skip list (memtable), memtables freeze and flush to on-disk sorted tables (SSTables) organized in levels, and reads check memtable → frozen memtables → SSTable levels (newest first) in that order.

## Commands

- Build: `cargo build`
- Test everything: `cargo test --workspace`
- Test one crate: `cargo test -p <crate>` (e.g. `cargo test -p memtable`)
- Test a single test: `cargo test -p <crate> <test_name>` (e.g. `cargo test -p sstable table_scan`)
- Doctests (crates have runnable examples in doc comments): `cargo test --doc -p <crate>`
- Lint: `cargo clippy --workspace --all-targets`
- Format: `cargo fmt`

There is no CI config or lint config file in the repo yet — `cargo clippy`/`cargo fmt` use defaults.

## Workspace layout and dependency order

Crates depend on each other bottom-up; when changing a lower crate, everything above it may need to adapt:

```
common → memtable, wal, sstable → storage → engine
                                → transaction (currently just a stub)
```

- **common**: shared types used everywhere else. `key::Key(String, u64)` is a user key paired with a sequence number; ordering is by user key ascending, then **seq descending** (newest version of a key sorts first). `value::Value` is `Set(String) | Delete` (tombstone). `lookup::Lookup` is the tri-state result of a point read: `Found(String) | Deleted | Absent` — callers must distinguish "explicitly deleted" from "never present" to correctly shadow older levels. `error::DbError` is the one error type used across all crates via `common::DbResult<T>`.

- **memtable**: `MemTable` wraps a hand-rolled lock-free skip list (`memtable/src/list/list.rs`, `node.rs`) that supports concurrent inserts (via CAS loops) but is insert-only — no update/delete of existing nodes, since `Key` already encodes versioning (a new seq is just a new node). Skip list ordering means, for a given user key, iteration yields newest-seq-first. `MemTable::is_full()` gates against a max heap-size (default 64MB) which callers use to decide when to freeze/flush.

- **sstable**: on-disk sorted table format. Written via `SSTableWriter` (`writer.rs`/`write.rs`), read via `SSTable`/`TableScan` (`table.rs`, `cursor.rs`, `scan.rs`). Files are organized into fixed-size blocks (`BLOCK_HEADER` prefix) with a block index (`index.rs`) and a Bloom filter (`bloom.rs`) plus footer/metadata (`footer.rs`, `meta.rs`) for fast negative lookups and point reads without scanning the whole file. `SSTable::get` short-circuits using the key range and bloom filter before touching disk, then binary-searches the block index and scans within one block. Block reads happen off the async runtime via `spawn_blocking` + positional file reads (`read_exact_at`/Windows equivalent).

- **wal**: append-only log of `(Key, Value)` records (`WalWriter`/`WalReader`), each `fsync`'d on append. Record layout is `[key_len:u16][key_bytes][seq:u64][value_len:u16][value_bytes]` (`value_len == 0` encodes a tombstone). The WAL's filename *is* its id, reused to name the memtable rebuilt from it.

- **storage**: ties memtable + wal + sstable together into the actual engine state machine.
  - `wal.rs` (`storage::wal::Wal`) manages the directory of WAL files: allocates new ids, restores all existing WALs into memtables on startup (newest WAL first), tracks the max sequence number seen.
  - `storage.rs` (`storage::storage::DiskStorage`) manages on-disk SSTables by level (`DashMap<u32 /* level */, SSTables>`), loads them from a directory of `{level}_{id}` files at startup, flushes a memtable to a new level-0 table (`l0`), and does leveled point lookups (`get_seq` walks levels low-to-high, tables within a level newest-first, stopping at the first non-`Absent` result).
  - `tables.rs` is a thin `Vec<Arc<SSTable>>` wrapper per level.
  - `state.rs` (`State`) is an immutable snapshot struct — `{ wal, mem_table, frozen: Vec<memtable>, storage }` — swapped in wholesale under a lock (`Storage.state: RwLock<Arc<State>>`) so readers can clone an `Arc` and read lock-free against a consistent view while writers install a new snapshot. This copy-on-write-snapshot pattern is the key concurrency idiom in this crate — follow it rather than mutating `State` fields in place.
  - `lib.rs` (`Storage`) is the top-level orchestrator: `set` appends to WAL + memtable then triggers freeze/backpressure if full; `get` checks active memtable → frozen memtables → `DiskStorage` in order; `flush_oldest`/`flush_all`/`flush_loop` drive background flushing of frozen memtables to level 0 with retry/backoff, gated by `MAX_FROZEN` backpressure so writers block (polling `await_flush_capacity`) if flushing falls behind.

- **engine**: `DB`, the public-facing handle. `open()` restores WAL + disk state, spawns the background `flush_loop` task, and seeds the sequence counter from `max(wal_max_seq, disk_max_seq) + 1`. `set`/`get` assign sequence numbers and delegate to `Storage`. `close()` flushes everything and cleanly stops the flush task; `Drop` only signals shutdown (does not block), so prefer explicit `close().await` when you need flushing guaranteed to finish.

- **transaction**: currently a stub (`WriteBatch {}`) — no real transaction/batch support implemented yet.

## Conventions to know before editing

- Every public crate uses `#![deny(unreachable_pub)]` and `#![warn(missing_docs)]` — new public items need doc comments or the build will warn/fail accordingly.
- Sequence numbers are the sole mechanism for versioning and snapshot reads (`get_seq`/`get_at`/`get_entry_at` exist at the memtable, skip-list, and sstable layers precisely to support reading as-of a given seq, not just the latest). When touching read paths, check whether a `_seq`/`_at` variant needs updating too.
- Levels/tables/memtables are read newest-to-oldest and the first non-`Absent` `Lookup` wins — this shadowing logic is duplicated across `Storage::get`, `DiskStorage::get_seq`, and the skip list's tombstone handling; keep them consistent.
- Flushing to level 0 drops all but the newest version of each user key and preserves tombstones (see `DiskStorage::write_l0`) — compaction into higher levels is not yet implemented.
