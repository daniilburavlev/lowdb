---
layout: doc
---

# File layout

```
[block]
[bloom filter]
[index: (last_block_key, block_offset, block_len) * N]
[footer: fixed 40 bytes layout]
```

# SSTable writer

Creates new tmp file with sss.tmp extension

Buffers key values insertions/deletions
If block len goes upper 4Kb flush block in temp file
