use common::{DbResult, error::DbError, key::Key};

use crate::BLOCK_HEADER;

pub(crate) struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

#[derive(Debug)]
pub(crate) enum ValueRef<'a> {
    Set(&'a str),
    Delete,
}

#[derive(Debug)]
pub(crate) struct EntryRef<'a> {
    pub(crate) key: &'a str,
    pub(crate) seq: u64,
    pub(crate) value: ValueRef<'a>,
}

impl<'a> Eq for EntryRef<'a> {}

impl<'a> PartialEq for EntryRef<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.seq == other.seq
    }
}

impl<'a> PartialEq<Key> for EntryRef<'a> {
    fn eq(&self, other: &Key) -> bool {
        self.key == other.0 && self.seq == other.1
    }
}

impl<'a> Ord for EntryRef<'a> {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        self.key.cmp(o.key).then(o.seq.cmp(&self.seq))
    }
}

impl<'a> PartialOrd for EntryRef<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<'a> PartialOrd<Key> for EntryRef<'a> {
    fn partial_cmp(&self, other: &Key) -> Option<std::cmp::Ordering> {
        Some(self.key.cmp(&other.0).then(other.1.cmp(&self.seq)))
    }
}

impl<'a> Cursor<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> DbResult<Self> {
        let mut cursor = Self { buf, pos: 0 };
        let checksum = cursor.u32()?;
        let len = cursor.u16()?;
        let payload = buf
            .get(BLOCK_HEADER..BLOCK_HEADER + len as usize)
            .ok_or(DbError::invalid_state("invalid block bytes"))?;
        let hash = crc32fast::hash(payload);
        if hash != checksum {
            return Err(DbError::invalid_state("block corrupted"));
        }
        Ok(cursor)
    }

    #[inline]
    pub(crate) fn take(&mut self, n: usize) -> DbResult<&'a [u8]> {
        let end = self.pos.checked_add(n).ok_or_else(corrupt)?;
        let s = self.buf.get(self.pos..end).ok_or_else(corrupt)?;
        self.pos = end;
        Ok(s)
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn u8(&mut self) -> DbResult<u8> {
        Ok(self.take(1)?[0])
    }

    #[inline]
    pub(crate) fn u16(&mut self) -> DbResult<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into()?))
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn u32(&mut self) -> DbResult<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into()?))
    }

    #[inline]
    pub(crate) fn u64(&mut self) -> DbResult<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }

    pub(crate) fn next_entry(&mut self) -> DbResult<Option<EntryRef<'a>>> {
        if self.pos >= self.buf.len() {
            return Ok(None);
        }
        let klen = self.u16()?;
        let key = std::str::from_utf8(self.take(klen as usize)?).map_err(|_| corrupt())?;
        let seq = self.u64()?;
        let value = match self.u16()? {
            0 => ValueRef::Delete,
            vlen => {
                let value =
                    std::str::from_utf8(self.take(vlen as usize)?).map_err(|_| corrupt())?;
                ValueRef::Set(value)
            }
        };
        Ok(Some(EntryRef { key, seq, value }))
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn position(&self) -> usize {
        self.pos
    }

    #[inline]
    #[allow(dead_code)]
    pub(crate) fn seek_to(&mut self, pos: usize) {
        self.pos = pos
    }
}

fn corrupt() -> DbError {
    DbError::invalid_state("corrupt sstable block")
}
