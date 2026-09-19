# `read_ts`, `visible`, and `next_seq`

These are the three numbers in the MVCC design proposed in `fix.md`. Two are counters on the DB, and
one is a per-transaction snapshot.

## `next_seq`: the allocation counter

This is the `AtomicU64` that already exists. Each write takes a fresh number with `fetch_add(1)`,
and that number goes into `Key(user_key, seq)`. The only change in the fix is **when** you take it:
at commit time and under the commit lock, not at `tx.set`. One `fetch_add` gives the whole
transaction a single `commit_ts`.

Handing out a number doesn't mean the write is readable yet. After `fetch_add`, the commit still
has to write the WAL and insert into the memtable.

## `visible`: the high-water mark of finished writes

This is a new `AtomicU64` holding the highest seq whose writes have **all** been applied (WAL +
memtable). A commit only does `visible.store(commit_ts)` after step 3 finishes.

This fixes the current bugs:

- **Bug 7:** today a snapshot is effectively "the counter right now," so it can include a seq that
  was allocated but hasn't landed. It shows up mid-transaction once it lands. With `visible`, you
  only snapshot at a point where everything at or below it has already landed.
- **Bug 9:** a 50-key commit can sit half-inserted in the memtable, but readers at
  `visible < commit_ts` skip all of it. Once `visible` moves, all 50 keys appear together.

In short, `next_seq` is the ticket number you've handed out, and `visible` is the ticket number
that has finished being served. When `visible` is below `next_seq - 1`, some writes are still in
flight.

## `read_ts`: a transaction's snapshot

This is set once at begin: `read_ts = visible.load()`. Every read in that transaction is
`get_seq(key, read_ts)`, which returns the newest version with `seq <= read_ts`. The existing
`_seq`/`_at` read paths already support this.

It's used in two places:

1. **Reads:** the transaction always sees the database as of `read_ts`, so the same key never
   changes mid-transaction (bug 6).
2. **Conflict check at commit:** `recent[k] > read_ts` means someone committed `k` after this
   transaction took its snapshot, so this is a write-write conflict. A commit at or below
   `read_ts` was already visible to you, so it isn't a conflict. That fixes bug 1, where
   sequential transactions on the same key currently always conflict.

## Timeline example

```
visible=10, next_seq=11
T1 begins              → T1.read_ts = 10
T2 begins              → T2.read_ts = 10
T2 commits k           → commit_ts = 11 (next_seq→12), write WAL+memtable,
                         recent[k]=11, visible=11
T1 reads k             → get_seq(k, 10) → still sees old value (snapshot holds)
T1 commits k           → recent[k]=11 > T1.read_ts=10 → CommitConflict
T3 begins              → T3.read_ts = 11 (sees T2's write)
T3 commits k           → recent[k]=11 ≤ 11 → OK, commit_ts = 12
```

Plain `DB::get` would read at `visible.load()` rather than `u64::MAX`. With `u64::MAX`, a plain
read can also see half-applied writes.

## Caveat: commits outside the lock

The last bullet under "Missing features and performance" in `fix.md` has a catch. If you move the
WAL write and memtable insert outside the lock, commits can finish out of order. For example, 13
might finish before 12. Then `visible` can't simply be stored as the latest `commit_ts`. It has to
wait until every lower timestamp has finished, which is what Badger's "watermark" does. With the
simple design that holds the lock through the whole commit, `visible.store(commit_ts)` is correct
as written.
