---
layout: doc
---

# Key elements

**WAL (write-ahead log)** - an append only file on disk for crash recovery.
**Memtable** - an in memory sorted structure (skip list) holding recent writes.
**Immutable memtables**- full memtables frozen and queued for flushing
**SSTables** - immutable sorted files on disk, organized into levels(L0, L1, ... Ln). Each carries a Bloom filter and a spare index over its data blocks.
**Sequence numbers** - every write gets a monotonically increasing number, so multiple versions of a key can coexist and the newest one can be identified.
