
## Fixes for `bugs.md`

Most of the bugs come from two design choices, so one redesign fixes most of the list. After that
come a few small targeted fixes, and two problems not listed in `bugs.md`.

## The two root causes

1. **Sequence numbers are handed out at `set` time, not at commit time.** This causes bugs 5 and 6.
2. **Nothing records which sequence numbers are safe to read.** Snapshots and plain reads use the
   raw counter or `u64::MAX`, so they can see writes that are still being applied. This causes
   bugs 7 and 9.

## Proposed design

**Split the counter.** Keep `next_seq: AtomicU64` for allocating numbers, and add
`visible: AtomicU64`, the highest number whose writes have all been applied.

- **Begin:** `read_ts = visible.load()`. This is the snapshot. Transaction ids get their own
  counter and no longer use up sequence numbers.
- **`tx.set`:** buffer only the user key and value, with no sequence number. A
  `BTreeMap<String, Value>` is enough and removes duplicate writes to the same key.
- **`tx.get`:** check the buffer first, then read the snapshot at `read_ts`.
- **Commit:** all of this happens under the commit lock:
  1. **Check for conflicts:** fail with `CommitConflict` if any buffered key has
     `recent[k] > read_ts`.
  2. `commit_ts = next_seq.fetch_add(1)`. **Every write in the transaction shares this one
     number.** That's safe because the buffer has at most one entry per key.
  3. Write the whole write set as **one WAL record** with one fsync, then insert everything into
     the memtable. Take both from the same `State` snapshot.
  4. `recent[k] = commit_ts` for each key, then `visible.store(commit_ts)`.
- **Plain `DB::set` and `DB::delete`** go through the same path as a one-key transaction with no
  conflict check. They still update `recent`, which fixes bug 4.
- **Plain `DB::get`** reads at `visible.load()` instead of `u64::MAX`.

## What that fixes, bug by bug

| # | Fix |
|---|---|
| 1 | The conflict check is `recent[k] > read_ts` instead of "any entry from another transaction", so a commit that finished before this transaction began doesn't count. |
| 2 | Prune `recent` by timestamp: remove entries with `commit_ts <= min(read_ts of active transactions)`. No running transaction can conflict with those. Never prune by transaction id. |
| 3 | Store active transactions as `BTreeMap<read_ts, count>` behind a **`std::sync::Mutex`**, separate from the tokio commit lock, so a sync `Drop` can remove them. Unregister in `Drop` and on every early return from `commit`. |
| 4 | Plain writes use the commit path and update `recent`, so a transaction that later writes the same key sees the conflict. |
| 5 | Commit timestamps are allocated under the lock, so a committed write always ends up newer than any earlier plain write. |
| 6 | Other transactions get their numbers at commit, which is always above an earlier `read_ts`. |
| 7 | Snapshots use `visible`, which only moves forward after a write has been fully applied. |
| 8 | See [Recovery](#recovery-bug-8) below. |
| 9 | Writes sit in the memtable but stay invisible until `visible` moves to `commit_ts`, so they appear all at once. |
| 10 | Make it `commit(self)` so a second commit is a compile error, and remove `committed_tx_cannot_commit_again`. Or keep `&self` with a state flag (`Active`/`Committed`/`Aborted`) that returns an error. |

### Recovery (bug 8)

- **Preferred:** add `WalCmd::Batch(Vec<(Key, Value)>)` with a length and checksum. Replay then
  keeps or skips each record as a whole. This also fixes two of the design issues in `bugs.md`:
  records not tagged by transaction, and one commit's records spanning two WAL files.
- **If you keep `TxBegin`/`TxCommit`:**
  - Replay should go through **every** record: apply plain `Op`s directly, buffer the ones between
    `TxBegin(id)` and `TxCommit(id)`, and throw the buffer away at the end of the file if no commit
    arrived.
  - `max_seq` must count every record.
  - Since plain writes now take the commit lock too, records from different transactions can't
    interleave.
- **Either way:** `WalReader::next` should treat a cut-off last record as the end of the file.
  Right now `read_u64` errors on a partial record, so a crash in the middle of a write makes
  `DB::open` fail.

### Failure halfway through a commit

Write the WAL record first, then insert into the memtable, which can't fail. If the WAL append
fails, nothing has been applied. Do the freeze and backpressure wait after releasing the commit
lock, so the lock isn't held while waiting on flushes (the last design issue in `bugs.md`).

## Two problems not in `bugs.md`

1. **`commit_is_atomic_for_concurrent_readers` is flawed.** Its reader makes 50 separate `db.get`
   calls. Even if commits are atomic, a commit can land between two of those reads, so the test
   can still see a partial result. The reader should take one snapshot, for example with
   `db.transaction()` or a new `db.snapshot()`, and read all 50 keys from it.
2. **Flushing breaks snapshots.** `DiskStorage::write_l0` (`storage/src/storage.rs:100`) keeps only
   the newest version of each key. Suppose a transaction with `read_ts = 10` needs `k@5`, and the
   memtable holds both `k@5` and `k@12`. After the flush only `k@12` is left, so the read at 10
   skips it and falls through to older tables, which can return an even older value or nothing.
   The fix: pass `min_active_read_ts` to `l0` and keep every version above it, plus the newest
   version at or below it. Compaction, once it exists, will need the same rule.

## Missing features and performance

- **Delete:** add `tx.delete(k)`, which buffers `Value::Delete`.
- **Rollback:** add `rollback(self)`, which is just dropping the transaction.
- **Write skew:** for serializable isolation, also record the keys the transaction read and apply
  the same `recent[k] > read_ts` check to them.
- **Faster commits:** hold the lock only for the conflict check and allocating `commit_ts`, and do
  the WAL write and memtable insert outside it. `visible` then has to move forward only once every
  lower timestamp has finished, which is how Badger does it. Grouping several commits into one
  fsync is a further option.

## Suggested order

1. The redesign: bugs 1–7 and 9.
2. The WAL batch record and replay: bug 8 and the recovery design issues.
3. `Drop` and commit-once: bugs 3 and 10.
4. The flush fix and the test fix.
