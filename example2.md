# `apply_batch` and `maybe_freeze`

These are the two `Storage` methods that `example.md` assumes (see its "Assumed storage API"
section). Together they are today's `Storage::set` (`storage/src/lib.rs:80`) split in two:

- `apply_batch` is the write half, extended to take many keys.
- `maybe_freeze` is the freeze half.

The split lets `Oracle::commit` do the write while it holds the commit lock, and the freeze after it
releases the lock.

> Not compiled or tested.

## `apply_batch(batch: Vec<(Key, Value)>) -> DbResult<()>`

This is the write half of `set`, but for a whole write set that shares one `commit_ts`:

```rust
pub async fn apply_batch(&self, batch: Vec<(Key, Value)>) -> DbResult<()> {
    let guard = self.state.read().await;                    // one State for both steps
    guard.wal.append(WalCmd::Batch(batch.clone())).await?;  // one record, one fsync
    for (k, v) in batch {
        guard.mem_table.put(k, v);
    }
    Ok(())
}
```

Requirements:

- **One WAL record for the whole batch.** If the process crashes partway through writing it,
  recovery replays all of the batch or none of it. Writing one `Op` per key could leave half a
  transaction on disk.
- **The same `State` for the WAL write and the memtable insert.** Hold one read guard, as `set`
  does now. If you read `self.state` twice, a freeze could swap in a new memtable between the two
  reads. The WAL record would then be in the old WAL while the data sits in the new memtable, and
  the memtable, the WAL and recovery would no longer agree.
- **Don't freeze and don't wait for flushing here.** It runs while the commit lock is held.

### Changes needed elsewhere

`WalCmd::Batch` doesn't exist yet:

- `wal/src/lib.rs`: add the `WalCmd::Batch(Vec<(Key, Value)>)` variant and a new command-tag
  constant.
- `wal/src/writer.rs` and `wal/src/reader.rs`: encode and decode the record. The encoding is a
  count followed by that many `(key, seq, value)` entries. A batch that was only partly written must
  decode as "no record", not as some of its entries.
- `storage/src/wal.rs:104`: update the restore loop. `while let Some(WalCmd::Op(..))` stops at the
  first record that isn't an `Op`, so a `Batch` record would quietly end the replay. It should
  `match` on every variant, put each entry of a `Batch` into the memtable, and update `max_seq`.

## `maybe_freeze() -> DbResult<()>`

This is the `if is_full { ... }` tail of `set`:

```rust
pub async fn maybe_freeze(&self) -> DbResult<()> {
    if self.state.read().await.mem_table.is_full() {
        self.try_freeze().await?;
        self.await_flush_capacity().await;
    }
    Ok(())
}
```

- **It runs after `drop(recent)`, outside the commit lock.** `await_flush_capacity` can block until
  the background flush catches up. If it ran inside `commit_lock`, every other commit would wait on
  disk I/O too.
- **It's safe when several committers call it at once.** Several committers may all see a full
  memtable. `try_freeze` checks `is_full()` again after taking `state_lock`, so only one of them
  freezes.

## What happens to `Storage::set`

When both methods exist, `Storage::set` becomes:

```rust
pub async fn set(&self, key: Key, value: Value) -> DbResult<()> {
    self.apply_batch(vec![(key, value)]).await?;
    self.maybe_freeze().await
}
```

You could also remove it, because in `example.md` `DB::set` goes through `Oracle::commit`.
