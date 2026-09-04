# Background flush and SSTable compaction — algorithm

Design notes only. Nothing here is implemented yet; this file describes *what steps to take*, in what
order, and which invariants each step must preserve. Written against the current code
(`collection/src/{lib,state,storage,wal}.rs`, `sstable/src/{writer,scan}.rs`).

---

## 0. Where the code stands today

- `Collection::force_freeze` moves the active memtable to the head of `State.frozen`, opens a fresh WAL
  writer, publishes a new `Arc<State>`, and calls `flush_notify.notify_one()`.
- **Nobody awaits `flush_notify`.** `Storage::l0` exists but is never called, so `frozen` grows forever
  and WAL files are never deleted.
- `sstable::scan::compact` (MergeScan → SSTableWriter) exists but nothing drives it, and it does not
  drop duplicates, superseded versions, or tombstones.
- There is no manifest: levels and recency are re-derived on startup from file names `<level>_<id>`,
  sorted by level ascending then id descending.

The two background jobs are therefore: a **flush loop** (frozen memtable → new L0 table) and a
**compaction loop** (N tables → fewer tables at a deeper level).

---

## 1. Prerequisites (must be settled before either loop is written)

These are data-model decisions, not code:

1. **A frozen memtable must remember its WAL file id.** Today `State.frozen: Vec<Arc<MemTable>>` carries
   no provenance, so after flushing there is no way to know which WAL file became redundant. Make the
   frozen entry a pair *(memtable, wal_id)*: the id of the WAL that was sealed at the moment of the
   freeze. `Wal::new_writer` already allocates ids; the freeze step must capture the id it just retired.
   Recovery must produce the same pairing (`Wal::restore` currently returns bare memtables).
2. **`frozen` must have a defined order.** `force_freeze` inserts at index 0, so index 0 is the newest and
   the last element is the oldest. Flush consumes from the **tail**; reads scan from the **head**. Write
   this down as an invariant, because both loops depend on it.
3. **Table identity must be stable.** A flush or a compaction is identified by the output file name
   `<level>_<id>` where `id` comes from `Storage.id`. Ids must be monotonic across process restarts
   (`Storage::load` already seeds from `max_id + 1`) because L0 recency is inferred from them.
4. **Decide the snapshot horizon.** Compaction may only drop a superseded version if no live reader can
   still see it. Until read snapshots exist, the horizon is "the newest version of each user key wins,
   everything older is droppable". If MVCC snapshots are added later, the horizon becomes the minimum
   snapshot sequence held by any open reader, and it must be computed *before* inputs are picked.
5. **Choose whether a manifest is needed.** Everything below works without one, provided install is done
   as *create-new → fsync → publish in memory → delete-old*, and recovery tolerates both crash windows
   (see §5.5). A manifest becomes mandatory only when a compaction may write a table whose level cannot be
   inferred from its file name, or when partial overlap sets are allowed at the same level.

---

## 2. Flush loop — structure

### 2.1 Ownership and lifecycle

1. `Collection::open` builds the state as today, then spawns one background task per job (one flusher,
   one compactor) and keeps their `JoinHandle`s plus a shutdown signal inside `Collection`.
2. Both tasks need access to the collection internals but must not keep `Collection` alive. Either make
   `Collection` an inner `Arc<Inner>` façade and hand the tasks a `Weak<Inner>`, or move the shared parts
   (state `RwLock`, `state_lock`, `Storage`, `Wal`, notifies) into a separate `Arc`-held struct that both
   the façade and the tasks reference. Decide this first — it dictates every signature below.
3. Shutdown: a `tokio::sync::Notify` or a watch channel set on `Collection::close`/`Drop`. Each loop
   selects over *work notify* and *shutdown notify*; on shutdown it drains the remaining work (or exits
   immediately, if `close` is documented as "flush what you can and stop") and then returns.

### 2.2 The loop body

1. `select!` on `flush_notify.notified()` and the shutdown signal.
2. On wake: take a read snapshot of the state and look at `frozen`. If it is empty, go back to waiting.
   Do not hold the read guard while flushing.
3. Otherwise pick the **last** element (the oldest frozen memtable) and flush that one only, then loop
   again without waiting. Flushing oldest-first keeps WAL deletion contiguous — WAL files can only be
   removed in id order.
4. `Notify::notify_one` coalesces: several freezes may collapse into one wake-up. The loop must therefore
   re-check `frozen` after every flush instead of assuming one notification == one memtable. Conversely,
   after finishing a flush, call `notify_one` again if `frozen` is still non-empty (or simply do not
   await when work remains).

### 2.3 Flushing one memtable — steps

Given the oldest frozen entry *(mt, wal_id)*:

1. Allocate a table id and build the path `<dir>/ss/0_<id>` (this is `Storage::l0`'s job; it already
   takes `Storage.lock`, which serializes writers against each other).
2. Open an `SSTableWriter` on that path (it writes `<path>.sst.tmp`).
3. Iterate `mt.iter()`. The skip list yields `Key` order: user key ascending, sequence **descending**, so
   the newest version of each user key comes first in each run.
4. **Emit only the first entry of each user-key run** — skip every subsequent entry with the same user
   key. This is required, not an optimisation: `SSTableWriter::add` only rejects a *decreasing* key, so
   duplicates would silently land on disk and inflate every later read and compaction. (`Storage::l0`
   currently forwards every entry; this is the one behavioural fix flush needs.)
   - Keep tombstones (`Value::Delete`) — an L0 table must be able to shadow older tables.
5. `writer.finish()`. It fsyncs the temp file, renames it into place, and fsyncs the directory, so a table
   file is either absent or complete. `None` means the memtable was empty; treat that as success and skip
   step 6.
6. Wrap the returned `SSTableMeta` in an `SSTable` and insert it at **index 0 of level 0** so that
   newest-first order inside the level is preserved (this is what `Storage::get` relies on).
7. Publish the memtable removal (§2.4).
8. Reclaim the WAL (§2.5).
9. Signal the compactor: after a successful L0 install, notify the compaction loop so it can evaluate its
   trigger.

### 2.4 Publishing the state change — ordering rules

The read path is: active memtable → frozen (newest first) → storage. A key must never be invisible in
between, so the ordering is fixed:

1. The table is registered in `Storage` **before** the memtable is removed from `frozen`. During the
   overlap window the key is found twice; the memtable copy wins, which is correct because it is the same
   data.
2. Removing the memtable follows the same copy-on-write protocol as `force_freeze`: take `state_lock`,
   take the state write guard, clone the `State`, remove the tail entry from the cloned `frozen`,
   publish the new `Arc`, drop the guards.
3. Remove **by identity**, not by index: compare `Arc::ptr_eq` against the memtable that was flushed.
   Between picking it and finishing the flush, `force_freeze` may have pushed new entries at the head, so
   an index captured earlier is stale.
4. Never hold `state_lock` across the SSTable write. The write is seconds-scale I/O; `state_lock`
   serializes freezes, and holding it would stall every writer that fills a memtable.
5. Readers that cloned the old `Arc<State>` keep seeing the memtable until they drop it. That is safe —
   the data is identical to the table just installed — and the memtable's memory is reclaimed when the
   last `Arc` goes away.

### 2.5 WAL reclamation

1. Only after the table is durable (renamed + directory fsynced) and the memtable is out of `frozen`,
   delete the WAL file `<dir>/wal/<wal_id>`.
2. Delete in ascending id order and never delete a WAL whose memtable has not been flushed. Because
   flush consumes the oldest frozen entry first, "delete the id just flushed" satisfies this
   automatically.
3. A crash between the rename and the unlink leaves a redundant WAL. Recovery replays it into a memtable
   whose data is already in an L0 table; the entries carry their original sequence numbers, so the
   memtable copy simply shadows the identical table copy. Harmless, but it means recovery must not assume
   WAL and SSTable contents are disjoint.
4. `Collection::open` seeds `seq` from `restored.max_seq + 1` and from the WAL alone. Deleting WAL files
   therefore **loses sequence numbers** unless the largest sequence is recoverable from the tables. Fix
   one of these before enabling deletion:
   - persist `smallest_seq`/`largest_seq` in the footer (the writer already tracks them and throws them
     away) and take `max_seq` as the max over WAL and all tables; **or**
   - keep a tiny `CURRENT`/manifest file recording the highest sequence flushed.
   Without this, a restart after a WAL deletion can reissue sequence numbers that already exist on disk,
   and newer writes will sort *older* than the data they should replace. This is the single most
   dangerous step in the whole design.

### 2.6 Backpressure

1. Define a limit on `frozen.len()` (a count, or a byte budget summed from `SkipList::mem_usage`).
2. In `Collection::set`, after `try_freeze`, if the limit is exceeded: notify the flusher and wait —
   either on a "flush progressed" `Notify` with a timeout, or by returning a "too many pending flushes"
   error if a stall is preferable to unbounded latency. Pick one and document it.
3. Without backpressure, a writer faster than the disk turns the frozen list into an unbounded memory
   leak; this is the failure mode the current code already has.

### 2.7 Failure handling

1. A failed flush must leave the memtable in `frozen` and the WAL file on disk — never remove either
   before the rename succeeded.
2. Delete the `.sst.tmp` file on failure (a crash leaves it behind; `Storage::load` already ignores names
   that do not parse as `<level>_<id>`, so stray temp files are inert — but a startup sweep that deletes
   `*.sst.tmp` from the table and WAL directories keeps the directory honest).
3. Retry with backoff. After N consecutive failures, mark the collection read-only / poisoned rather than
   spinning: the disk is full or the file system is gone, and quietly retrying forever hides it.

---

## 3. Compaction loop — structure

### 3.1 Level model

Fix the model before writing anything, because the trigger and the input picker differ:

- **L0** — tables come straight from flushes, so their key ranges **overlap freely** and recency is
  file-id order. A read must consult every L0 table, newest first.
- **L1 and deeper** — after the first compaction, each level should hold tables with **disjoint** key
  ranges, sorted by key. Then a read touches at most one table per level and can binary-search the level.
  `Storage::get` currently scans every table in every level; keeping ranges disjoint is what makes that
  cheap later.

Two workable policies:

- **Size-tiered** (simpler): when a level holds ≥ T tables, merge *all* of them into one table at
  level+1. No range bookkeeping, no partial overlap. Good first implementation.
- **Leveled** (better reads): level L has a byte budget growing by a factor (e.g. ×10). When exceeded,
  pick one table from L and every table in L+1 whose range overlaps it, and rewrite them into L+1.
  Requires disjoint-range maintenance and a per-level size accounting.

Recommendation: implement size-tiered L0→L1 first, get the concurrency and installation right, then
generalise.

### 3.2 Trigger

1. The compactor waits on its own `Notify`, signalled after every successful L0 install and after every
   completed compaction (a compaction can create the condition for the next one).
2. On wake, evaluate the policy top-down: L0 first (count ≥ T0, e.g. 4), then each deeper level against
   its budget. Pick **one** job, run it, notify itself again, re-evaluate.
3. Add a periodic tick (seconds) as a safety net so a missed notification cannot wedge the loop.

### 3.3 Choosing inputs

1. Take a consistent view of the level: clone the `Vec<Arc<SSTable>>` out of the `DashMap` (under
   `Storage.lock` if the picker must not race an install).
2. For L0→L1: take **all** L0 tables (they overlap, so a subset would break recency), plus — for leveled —
   every L1 table overlapping their combined `[first_key, last_key]` range.
3. For Ln→Ln+1 size-tiered: take all tables at Ln.
4. Record the chosen inputs in an "in-progress" set so a second compaction cannot pick the same files.
   With a single compaction task this is trivially satisfied; if compaction is ever parallelised, the set
   becomes mandatory.
5. **Input order matters for the merge.** `MergeScan` breaks ties on equal `Key` by source index, so the
   input vector must be ordered newest-first: L0 tables in descending file id, then deeper-level tables
   (which are always older). With that ordering, "first occurrence wins" is exactly "newest wins".

### 3.4 Producing the output

1. Open a `TableScan` per input; build a `MergeScan`; open an `SSTableWriter` at
   `<dir>/ss/<level+1>_<new_id>`.
2. Walk the merged stream. Because `Key` sorts by user key ascending then sequence descending, all
   versions of one user key arrive together, newest first. For each entry decide:
   - **First entry of a user-key run** → candidate for output.
   - **Any later entry of the same user-key run** → drop it if its sequence is below the snapshot
     horizon (§1.4); otherwise keep it so live snapshots can still read it. With no snapshots
     implemented, drop unconditionally.
   - **Identical `(user key, seq)` from two inputs** → drop the duplicate. It cannot be passed to
     `SSTableWriter::add`, which only rejects *decreasing* keys and would happily write both.
   - **Tombstone that is the surviving newest version** → keep it, *unless* this compaction's output is
     the bottom-most level and no lower level can hold an older value for that key. Only then is dropping
     the tombstone safe. Dropping it earlier resurrects deleted keys.
3. Roll to a new output file when the current one reaches a target size (e.g. 64 MiB) — required for
   leveled compaction, optional for the first size-tiered version. Never split a user-key run across two
   output files if versions below the horizon are being kept.
4. `finish()` each writer; collect the resulting `SSTableMeta`s. An output with zero entries (everything
   was dropped) returns `None` and is simply not installed.

### 3.5 Installing the result

1. All outputs are renamed into place and directory-fsynced by `SSTableWriter::finish` before anything is
   published — so at this point both old and new files exist on disk and both are self-consistent.
2. Under `Storage.lock`, in one critical section: remove the input tables from their level vectors and
   insert the outputs into the destination level, keeping the level's ordering invariant (newest-first
   for L0, key-ascending for deeper levels).
3. Drop the `Arc<SSTable>` handles for the inputs and delete their files. On Unix, unlinking a file a
   reader still has open is safe: the reader's `Arc<File>` keeps the inode alive and its `pread` calls
   keep working until it drops. On Windows the delete will fail while handles are open — retry later, or
   keep a "pending delete" list drained by the loop.
4. A reader holding a stale snapshot of the level list may read an input table that is no longer
   published; it sees older-but-correct data for keys the output also contains, and the key set is
   identical, so results stay correct.

### 3.6 Crash recovery without a manifest

The crash windows and what recovery sees:

1. **Crash before the output rename** — only `.sst.tmp` files exist; inputs are intact. Recovery deletes
   stray temp files. Nothing lost.
2. **Crash after the rename, before deleting the inputs** — both inputs and output exist on disk and
   contain the same keys. `Storage::load` will load all of them. Reads stay correct only if the output
   (deeper level) is consulted *after* the inputs (shallower level) — which is exactly what
   `Storage::get`'s level-ascending scan does. Space is wasted until the next compaction re-merges them.
   To reclaim it deterministically, add a startup step that detects a deeper-level table whose range is
   fully covered by shallower tables of the same generation, or accept the waste.
3. **Crash midway through deleting inputs** — same as case 2 with fewer inputs left.

If that ambiguity is unacceptable, add a manifest: append a record `{added: [...], removed: [...]}`,
fsync it, and only then delete the input files. Recovery then rebuilds the level map from the manifest
and deletes every table file not referenced by it. This also solves the `max_seq` problem in §2.5.

### 3.7 Interaction with the flush loop

1. Flush writes only to L0 and only *adds*; compaction removes from L0. Both must take `Storage.lock`
   around the mutation of the level map, and neither may hold it across file I/O.
2. Compaction runs on a snapshot of the input list. If a flush installs a new L0 table while an L0→L1
   compaction is running, that new table is simply not part of the job; the install step must remove only
   the inputs it actually picked (compare by path or table id, never by index).
3. If flush and compaction contend badly, give compaction a concurrency limit (a semaphore of 1, or 1 per
   level) and let flush always win — stalling flushes stalls writes, stalling compaction only degrades
   read amplification.

---

## 4. Suggested implementation order

1. Attach WAL ids to frozen memtables (§1.1) and to WAL recovery.
2. Persist the sequence range in the SSTable footer, or add a manifest (§2.5.4). Nothing else may ship
   before this — WAL deletion is unsafe without it.
3. Dedupe user-key runs in `Storage::l0` (§2.3.4).
4. Spawn the flush loop; remove flushed memtables (§2.4); delete WAL files (§2.5).
5. Add backpressure and shutdown/drain (§2.6, §2.1.3).
6. Add drop rules to `MergeScan`/`compact` (§3.4).
7. Spawn the compaction loop with size-tiered L0→L1, single job at a time (§3.2–3.5).
8. Generalise to deeper levels / leveled policy, then teach `Storage::get` to binary-search
   disjoint levels instead of scanning them.

---

## 5. What to test

- Freeze N memtables, run the flusher, assert `frozen` drains to empty and exactly N table files appear.
- A key written, then overwritten, then flushed: the L0 table holds one entry, the newest.
- Read-during-flush: a key must be readable at every instant while its memtable moves to L0 (interleave
  reads with a flush in the same test).
- Kill the process (or simulate by dropping without cleanup) between rename and WAL unlink; reopen;
  assert no data loss and no duplicate-sequence reuse.
- Compact two tables containing the same `(key, seq)`; assert the output has one entry.
- Delete a key in L0 over a value in L1; compact L0→L1 with L1 *not* bottom-most; assert the tombstone
  survives and the key still reads as absent.
- Compact into the bottom level; assert the tombstone is dropped and the file shrinks.
- Flush and compaction running concurrently: assert no table is lost from the level map and no file is
  deleted while still published.

---

## 6. Existing gaps that will bite

- `SSTableWriter::flush_block` stores block length in a `u16`; a block may exceed 4 KiB by one entry, so a
  long key/value makes it fail with `InvalidInt`. Compaction concentrates large entries and will hit this
  sooner than the write path does.
- `MergeScan` currently emits everything; §3.4 is a change to it, not a wrapper around it.
- `TableScan::check_block` seeks backwards to verify the CRC, discarding the `BufReader` buffer, so every
  block is read from the OS twice — compaction is the workload where that doubles real I/O.
- `Collection` has no `delete`, so tombstones only enter via WAL replay; the tombstone rules in §3.4
  cannot be exercised end-to-end until it exists.
- L0 recency depends entirely on file ids; any id reuse silently reorders history.
