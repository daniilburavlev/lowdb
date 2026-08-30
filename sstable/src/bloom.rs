use common::DbResult;
use tokio::io::AsyncReadExt;

use crate::write::WriteTo;

#[derive(Clone, Debug)]
pub struct BloomFilter(Vec<u8>);

impl BloomFilter {
    pub async fn read<R>(read: &mut R, len: usize) -> DbResult<Self>
    where
        R: AsyncReadExt + Unpin,
    {
        let mut bloom = vec![0u8; len];
        read.read_exact(&mut bloom).await?;
        Ok(Self(bloom))
    }

    pub fn build(hashes: &[u64], bits_per_key: usize) -> Self {
        let k = ((bits_per_key as f64 * 0.69) as u32).clamp(1, 30);
        let nbytes = (hashes.len() * bits_per_key).max(64).div_ceil(8);
        let nbits = (nbytes * 8) as u64;
        let mut out = vec![0u8; nbytes + 1];
        for &h in hashes {
            let delta = h.rotate_left(15);
            let mut b = h;
            for _ in 0..k {
                let pos = (b % nbits) as usize;
                out[pos / 8] |= 1 << (pos % 8);
                b = b.wrapping_add(delta);
            }
        }
        out[nbytes] = k as u8;
        Self(out)
    }

    pub(crate) fn may_contain(&self, key: &str) -> bool {
        if self.0.len() < 2 {
            return true;
        }
        let k = self.0[self.0.len() - 1] as u32;
        let bits = &self.0[..self.0.len() - 1];
        let nbits = (bits.len() * 8) as u64;
        let h = Self::hash64(key);
        let delta = h.rotate_left(15);
        let mut b = h;
        for _ in 0..k {
            let pos = (b % nbits) as usize;
            if bits[pos / 8] & (1 << (pos % 8)) == 0 {
                return false;
            }
            b = b.wrapping_add(delta);
        }
        true
    }

    #[allow(dead_code)]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn hash64(key: &str) -> u64 {
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        let b = key.as_bytes();
        for &x in b {
            h ^= x as u64;
            h = h.wrapping_mul(0x100_0000_01b3);
        }
        h
    }

    pub(crate) fn disk_size(&self) -> usize {
        self.0.len()
    }
}

impl WriteTo for BloomFilter {
    async fn write<W>(&self, w: &mut W) -> DbResult<()>
    where
        W: tokio::io::AsyncWriteExt + Unpin,
    {
        w.write_all(&self.0).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use common::key::Key;

    use super::*;

    #[test]
    fn contains() {
        let mut exists = HashSet::new();
        let mut not_exists = HashSet::new();
        let mut hashes = vec![0u64; 10_000];
        for i in 0..10_000 {
            let key = Key(format!("key: {i}"), i);
            if i % 3 == 0 {
                hashes[i as usize] = BloomFilter::hash64(&key.0);
                exists.insert(key);
            } else {
                not_exists.insert(key);
            }
        }
        let bloom = BloomFilter::build(&hashes, 10);
        for e in &exists {
            assert!(bloom.may_contain(&e.0));
        }
        let mut false_pos = 0;
        for n in &not_exists {
            if bloom.may_contain(&n.0) {
                false_pos += 1;
            }
        }
        assert!(false_pos < 10);
    }
}
