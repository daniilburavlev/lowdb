# Deduplication in `scan::compact`

Notes on how to filter duplicates during compaction. Reference points:
`sstable/src/scan.rs` (`compact`), `sstable/src/scan/merge.rs` (`MergeScan`),
`common/src/key.rs` (`Key::cmp`), `sstable/src/writer.rs` (`SSTableWriter::add`).

The merge stream already gives everything needed — today `compact` just doesn't filter it.

## The three distinct things "dedup" means

They are often conflated, but each has a different correctness condition.

### 1. Exact `(user_key, seq)` collisions across inputs

Two tables holding the same key *and* the same seq. Same logical write, so either copy is
fine — but the choice must be deterministic.

`HeapKey::cmp` (`scan/merge.rs:17-19`) already breaks the tie on `idx`, and with `Reverse`
the *smallest* index pops first. So this is free, provided `compact` documents a contract:

> **Inputs must be passed newest-first.**

That is how `Storage` already keeps each level's `Vec<Arc<SSTable>>`. No code needed, just
an invariant.

### 2. Superseded versions of the same user key

`Key::cmp` sorts ascending by user key, descending by seq (`common/src/key.rs:24-28`), so
all versions of one user key arrive as one contiguous run, newest first. The first of the
run is the live version; the rest are older. Dropping them is only safe if no live reader
can still ask for them.

### 3. Tombstones

A `Value::Delete` can be dropped entirely — not just the tombstone but the whole run — only
when nothing *underneath* the compaction could still hold an older `Set` for that key.

## Where to put it

Not in `MergeScan`. Keep that as the raw ordered merge; it is also usable for range scans,
where the raw stream is what you want (the reader picks per-snapshot). Put the filter in
`compact`, or in a thin `CompactionFilter` wrapper around `MergeScan` that owns the state.

State needed: the last user key *emitted*, the seq it was emitted at, plus two parameters
describing the compaction's context.

## Sketch

```
compact(out, inputs /* newest first */, oldest_snapshot, bottom_level):
    last_user_key = None
    last_kept_seq = 0

    while (k, v) = merge.next():
        first_in_run = last_user_key != Some(k.0)

        if first_in_run:
            last_user_key = Some(k.0)
            last_kept_seq = k.1
            if v is Delete and bottom_level and k.1 <= oldest_snapshot:
                continue          # nothing below: erase the key entirely
            writer.add(k, v)
        else:
            # older version of a key already emitted
            if last_kept_seq <= oldest_snapshot:
                continue          # every live reader sees the newer one -> drop
            writer.add(k, v)      # a snapshot sits between them; must keep
```

## Why the `oldest_snapshot` condition is the one that matters

A snapshot at seq `s` reads the newest version with seq `<= s`. If a version at seq `n` was
kept and the current entry is an older one at seq `c` (`c < n`), the old one is still
reachable iff some live snapshot `s` satisfies `c <= s < n`.

Testing `n <= oldest_snapshot` says "every live reader's seq is at least `n`, so they all
land on the newer version" — which makes the old one unreachable. That is the
LevelDB/RocksDB rule, reduced to a scalar.

Two consequences for lowdb specifically:

- **There is no snapshot registry today.** Reads use `MemTable::get_at(key, snapshot)`, but
  nothing pins a minimum. Until a registry exists, `oldest_snapshot` is either the current
  global seq (drop aggressively — correct only if no long-lived iterator exists) or `0`
  (drop nothing, dedup only exact `(key, seq)` pairs). Passing it as a parameter is the
  right shape either way; a real registry can be wired in later without touching the loop.
- **`bottom_level` cannot be inferred inside `sstable`.** It is a property of the level
  layout, which `collection/src/storage.rs` owns, so it has to be an argument.

## Two side benefits

- `SSTableWriter::add` currently only rejects a *decreasing* key (`sstable/src/writer.rs:70-77`,
  `last > key`), so equal keys pass through — exactly the gap in the known-gaps list. After
  this filter the output stream is strictly increasing in `Key`, so that check could be
  tightened to `last >= key`, making the writer assert the invariant rather than silently
  accept duplicates.
- The existing test (`sstable/src/scan.rs:39-63`) does not actually prove dedup: it reads one
  entry and checks it is `(1, 9)`, which passes whether or not the other nine are still in
  the file. A dedup test needs to drain the scan and assert the entry count.
