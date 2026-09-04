# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Working style in this repo

The owner uses this repo as a learning/design exercise. Default to **explaining logic, reviewing code, and
suggesting improvements** rather than writing code. Only edit source when explicitly asked to.

## Commands

```bash
cargo build                      # build the whole workspace
cargo test                       # all tests (unit tests live in `#[cfg(test)] mod tests` inside each source file)
cargo test -p sstable            # one crate
cargo test -p memtable list::tests::concurrent_inserts   # one test (substring match on the full path)
cargo test -- --nocapture        # show println!/panic output
cargo clippy --all-targets
cargo fmt

npm run docs:dev                 # VitePress docs site (docs/)
npm run docs:build
```

Rust edition 2024 is used across all crates; `let ... && ...` let-chains appear in `sstable/writer.rs`, so a
recent toolchain is required. Async code is Tokio-based, tests use `#[tokio::test]`.

## Architecture

LSM-tree key-value store, split into a Cargo workspace of five crates. Dependency direction is
`collection` → {`memtable`, `sstable`, `wal`} → `common`.

- **`common`** — shared vocabulary types. `Key(String, u64)` is a *user key + sequence number*; its `Ord` sorts
  ascending by user key then **descending by seq**, so the newest version of a key sorts first everywhere
  (skip list, SSTable blocks, merge heap). `Value` is `Set(String) | Delete`, where `Delete` is a tombstone.
  `DbError`/`DbResult` are the single error type.
- **`wal`** — append-only log. `WalWriter::append` writes `u16 klen | key | u64 seq | u16 vlen | value`
  (vlen 0 = tombstone) and `flush + sync_all` on **every** append. `WalReader::next` replays it, returning
  `None` at clean EOF.
- **`memtable`** — `MemTable` wraps a lock-free `SkipList` (`memtable/src/list.rs`). The skip list is built on
  raw pointers + `AtomicPtr` towers with CAS insert; nodes are never removed (a `Drop` impl frees the level-0
  chain), which is what makes concurrent readers safe without epochs/hazard pointers. `get_at(key, snapshot)`
  gives MVCC reads by seeking to `(key, snapshot)` and taking the first node at or after it. `insert` returns
  `false` for an exact `(key, seq)` duplicate. Iteration yields entries in `Key` order, i.e. newest version of
  each user key first — a flush is expected to emit only the first entry of each user-key run.
- **`sstable`** — immutable on-disk table. Layout (see `docs/sstable.md`):
  `[block]* [bloom filter] [index: (last_block_key, block_off, block_len)*N] [footer: 40 bytes]`.
  - Each block is `u32 crc32 | u16 payload_len | entries...`, flushed once it exceeds `BLOCK_SIZE` (4 KiB).
  - `SSTableWriter` writes to `<path>.sst.tmp`, then `sync_all` + `rename` + `sync_dir` for atomic publish;
    `finish()` returns `None` and deletes the temp file when nothing was added.
  - `SSTableMeta::read` loads footer → bloom → index into memory; `SSTable::get` filters by
    first/last key range, then bloom, then binary-searches the index (`partition_point`) and scans one block
    via `Cursor` with positioned `pread` on a `spawn_blocking` thread.
  - `TableScan` is a sequential full-table reader (verifies each block's CRC); `MergeScan` k-way-merges several
    `TableScan`s through a min-heap of `(Key, source idx)`; `scan::compact` is `MergeScan` → `SSTableWriter`.
  - Two parallel encode paths exist: the `WriteTo`/`WriteToBuf`/`ReadFrom` traits and the free functions in
    `encode.rs`. They must stay byte-compatible.
- **`collection`** — the database façade tying it together. `Collection` holds an `RwLock<Arc<State>>`, where
  `State` (active memtable, frozen memtables, WAL writer, storage) is **copy-on-write**: readers clone the
  `Arc` and never block writers; mutations clone the `State`, swap fields, and publish a new `Arc` under a
  separate `state_lock` mutex that serializes freezes. Write path: allocate seq → WAL append → memtable set →
  freeze if full. Read path: active memtable → frozen memtables (newest first) → `Storage` (SSTables).
  `collection/src/wal.rs` owns WAL file naming/rotation (`<dir>/wal/<id>`) and crash recovery;
  `collection/src/storage.rs` owns the SSTable directory (`<dir>/ss/<level>_<id>`), grouping tables by level
  in a `DashMap<u32, Vec<Arc<SSTable>>>` kept newest-first inside each level.

## Known gaps (relevant when reviewing or extending)

These are unfinished, not bugs to silently fix — mention them rather than assuming they are intentional:

- No compaction driver and no flush loop: `flush_notify` is notified but nothing awaits it, so
  `Storage::l0` is never called, frozen memtables accumulate forever and WAL files are never removed.
- `Collection` has no `delete`; tombstones can only enter through WAL replay.
- SSTables do not persist a sequence range: `SSTableWriter` tracks `smallest_seq`/`largest_seq` but drops
  them, and the footer has no field for them. `Collection::open` therefore seeds `seq` from the WAL alone,
  which is only correct while nothing truncates the WAL after a flush.
- Block length is a `u16` while a block may exceed 4 KiB by one entry — a single long key/value makes
  `flush_block` fail with `InvalidInt` instead of writing. Widening it means changing `IndexedKey` on disk.
- `MergeScan`/`scan::compact` never drop duplicates, older versions, or tombstones; two inputs holding the
  same `(key, seq)` both end up in the output because `SSTableWriter::add` only rejects a *decreasing* key.
- `MemTable::size` is maintained by `set` but never read (`is_full` uses `SkipList::mem_usage`), and `set`
  only counts the key's bytes.
- `TableScan::check_block` seeks back over the block to verify its CRC, which discards the `BufReader`
  buffer — every block is read from the OS twice.
- L0 recency is inferred from the file id in `<level>_<id>`; there is no manifest.
- `sstable/src/lib.rs` puts `#[deny(unreachable_pub)]` / `#[warn(missing_docs)]` on a `use` item, so they
  apply to that import instead of the crate (they need to be inner `#![...]` attributes).
- `docs/sstable.md` names table files `l<level>_<id>`; the code writes and parses `<level>_<id>`.
- `docs/index.md`, `docs/api-examples.md`, `docs/markdown-examples.md` are still VitePress boilerplate.
