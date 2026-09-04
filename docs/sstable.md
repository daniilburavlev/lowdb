---
layout: doc
---

# File layout

```
[block]
[bloom filter]
[index: (last_block_key, block_offset, block_len) * N]
[footer: fixed 56 bytes layout]
```

The footer records `entries`, the bloom and index offsets/lengths, the
`smallest_seq`/`largest_seq` stored in the table, and a magic number. The
sequence range is what lets a flushed WAL file be deleted: on recovery the next
sequence number is seeded from the maximum over every table footer and every
surviving log, so a sequence already on disk is never reissued.

# SSTable writer

Creates new tmp file with sss.tmp extension

Buffers key values insertions/deletions
If block len goes upper 4Kb flush block in temp file
