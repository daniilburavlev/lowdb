
## Bugs

### Conflict detection (`transaction/src/locks.rs`, `Transaction::commit`)

1. **Transactions that run one after another on the same key always conflict.**
   `commit` treats any `recent` entry from another transaction as a conflict, even one committed
   before this transaction began. `remove_tx` also keeps the committing transaction's own entries
   (`retain(|_, v| *v == id)`), so those entries stay in `recent`.
   - Test: `sequential_transactions_on_same_key_do_not_conflict` fails with `CommitConflict`.

2. **Real conflicts can be missed, losing updates.**
   When the oldest active transaction commits, `remove_tx` deletes every other transaction's
   entries from `recent`, including ones that still-running transactions need to check against.
   - Test: `pruning_recent_on_oldest_commit_does_not_hide_conflicts` — t3 overwrites a key that
     t2 committed after t3 began.

3. **Aborted or dropped transactions stay registered as active forever.**
   On conflict, `commit` returns early without calling `remove_tx`, and there is no rollback or
   `Drop`. Because pruning only runs when the committing transaction is the oldest active one, one
   leftover id stops pruning for the rest of the process.
   - Test: `aborted_and_dropped_txs_leave_active_set`.

4. **Plain `DB::set` bypasses conflict detection.**
   A transaction commits successfully over a concurrent plain write to the same key.
   - Test: `plain_write_to_a_tx_key_is_detected_as_conflict`.

### Sequence numbers and snapshot reads

5. **A successful commit can be invisible.**
   A transaction's write keeps the sequence number it got at `set`, not at commit. A plain write
   made between `tx.set` and `tx.commit` gets a higher number, so it hides the committed value even
   though `commit` returned `Ok`.
   - Test: `successful_commit_is_visible` reads `"plain"` instead of `"tx"`.

6. **Snapshot isolation is broken.**
   The snapshot is just the transaction's id. If another transaction called `set` before this one
   began and commits after, that write shows up in the middle of this transaction (the same read
   returns different values).
   - Test: `snapshot_does_not_see_writes_committed_after_it_began`.

7. **The snapshot point ignores writes that are still in progress.**
   A plain write can take a lower sequence number before a transaction begins and land after it,
   and that write becomes visible mid-transaction. This is the ordering race described in
   `steps.md`, item 2.
   - Test: `in_flight_lower_seq_write_is_not_visible_to_later_snapshot` simulates that ordering.

### Commit and recovery

8. **Crash recovery loses committed data.**
   `restore_tables` (`storage/src/wal.rs`) loops with `while let Some(WalCmd::Op(..))`, so replay
   stops at the first `TxBegin` record. The committed transaction and every plain write after it
   are lost. `max_seq` also comes out too low, so sequence numbers get reused after restart.
   - Test: `committed_tx_and_later_writes_survive_crash_recovery`.

9. **Other readers can see a half-applied commit.**
   The commit applies its writes one `Storage::set` at a time, each with its own fsync.
   - Test: `commit_is_atomic_for_concurrent_readers` saw 10 of 50 keys; it failed in 5 of 5 runs.

10. **A transaction can commit twice.**
    `commit(&self)` can be called again after success and re-applies the whole write set.
    - Test: `committed_tx_cannot_commit_again`.

## Design issues with no test yet

- **Records aren't tagged by transaction.** Write records in the WAL carry no transaction id, and
  plain writes can land between `TxBegin` and `TxCommit`. Even a fixed replay couldn't tell which
  writes belong to a transaction that never committed.
- **One commit's WAL records can span two files.** `begin`, each `set` and `commit` each take a
  fresh state snapshot. If the memtable freezes mid-commit, `TxBegin` and `TxCommit` end up in
  different WAL files.
- **A failed write leaves a half-applied commit.** If `storage.set` fails partway, the writes
  already applied stay, the transaction isn't removed from the active set, and `recent` isn't
  updated.
- **Missing features:** there is no delete or rollback, and conflicts are only checked on writes,
  so write skew is allowed.
- **Commits are fully serialized.** The global lock is held through fsyncs and possible
  backpressure waits, so every commit and every transaction start waits on it. This is slow but
  not incorrect.
