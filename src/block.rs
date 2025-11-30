//! Block format with Restart Points for efficient lookups

use crate::util::simple_checksum;
use anyhow::{anyhow, Result};

/// Number of entries between restart points
pub const RESTART_INTERVAL: usize = 16;

/// Magic number to identify block format
pub const BLOCK_MAGIC: u32 = 0x5242_4C4B; // "RBLK"

/// Block builder with restart points and prefix compression
pub struct BlockBuilder {
    data: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    entry_count: u32,
    entries_since_restart: usize,
    max_size: usize,
}

impl BlockBuilder {
    pub fn new(max_size: usize) -> Self {
        Self {
            data: Vec::with_capacity(max_size),
            restarts: vec![0],
            last_key: Vec::new(),
            entry_count: 0,
            entries_since_restart: 0,
            max_size,
        }
    }

    pub fn try_push(&mut self, key: &[u8], value: &[u8]) -> bool {
        let (shared, unshared_key) = if self.entries_since_restart == 0 {
            (0, key)
        } else {
            let shared = Self::shared_prefix_len(&self.last_key, key);
            (shared, &key[shared..])
        };

        let entry_size = 2 + 2 + 4 + unshared_key.len() + value.len();
        let trailer_size = (self.restarts.len() + 1) * 4 + 16; // +4 for magic
        let restart_overhead = if self.entries_since_restart >= RESTART_INTERVAL - 1 { 4 } else { 0 };
        
        if self.data.len() + entry_size + trailer_size + restart_overhead > self.max_size {
            return false;
        }

        if self.entries_since_restart >= RESTART_INTERVAL {
            self.restarts.push(self.data.len() as u32);
            self.entries_since_restart = 0;
            self.write_entry(0, key, value);
        } else {
            self.write_entry(shared as u16, unshared_key, value);
        }

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.entry_count += 1;
        self.entries_since_restart += 1;
        true
    }

    fn write_entry(&mut self, shared_len: u16, unshared_key: &[u8], value: &[u8]) {
        self.data.extend_from_slice(&shared_len.to_le_bytes());
        self.data.extend_from_slice(&(unshared_key.len() as u16).to_le_bytes());
        self.data.extend_from_slice(&(value.len() as u32).to_le_bytes());
        self.data.extend_from_slice(unshared_key);
        self.data.extend_from_slice(value);
    }

    fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
        a.iter().zip(b.iter()).take_while(|(x, y)| x == y).count()
    }

    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    pub fn finish(mut self) -> Vec<u8> {
        for &offset in &self.restarts {
            self.data.extend_from_slice(&offset.to_le_bytes());
        }
        self.data.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        self.data.extend_from_slice(&self.entry_count.to_le_bytes());
        let checksum = simple_checksum(&self.data);
        self.data.extend_from_slice(&checksum.to_le_bytes());
        self.data.extend_from_slice(&BLOCK_MAGIC.to_le_bytes());
        
        let payload_len = self.data.len() as u32;
        let mut result = Vec::with_capacity(4 + self.data.len());
        result.extend_from_slice(&payload_len.to_le_bytes());
        result.extend_from_slice(&self.data);
        
        if result.len() < self.max_size {
            result.resize(self.max_size, 0);
        }
        result
    }
}

/// Parsed block with restart points for efficient binary search
pub struct Block {
    data: Vec<u8>,
    #[allow(dead_code)] // Used for binary search optimization (future)
    restarts: Vec<u32>,
    entry_count: u32,
}

impl Block {
    pub fn parse(buf: &[u8]) -> Result<Self> {
        if buf.len() < 20 {
            return Err(anyhow!("block too small"));
        }

        let payload_len = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        if payload_len + 4 > buf.len() {
            return Err(anyhow!("invalid payload length"));
        }

        let payload = &buf[4..4 + payload_len];
        if payload.len() < 16 {
            return Err(anyhow!("payload too small"));
        }

        let magic = u32::from_le_bytes(payload[payload.len() - 4..].try_into().unwrap());
        if magic != BLOCK_MAGIC {
            return Err(anyhow!("invalid block magic"));
        }

        let checksum_stored = u32::from_le_bytes(payload[payload.len() - 8..payload.len() - 4].try_into().unwrap());
        let checksum_actual = simple_checksum(&payload[..payload.len() - 8]);
        if checksum_stored != checksum_actual {
            return Err(anyhow!("checksum mismatch"));
        }

        let entry_count = u32::from_le_bytes(payload[payload.len() - 12..payload.len() - 8].try_into().unwrap());
        let num_restarts = u32::from_le_bytes(payload[payload.len() - 16..payload.len() - 12].try_into().unwrap()) as usize;

        if num_restarts == 0 {
            return Err(anyhow!("no restart points"));
        }

        let restarts_start = payload.len() - 16 - num_restarts * 4;
        let mut restarts = Vec::with_capacity(num_restarts);
        for i in 0..num_restarts {
            let offset = u32::from_le_bytes(payload[restarts_start + i * 4..restarts_start + i * 4 + 4].try_into().unwrap());
            restarts.push(offset);
        }

        Ok(Self { data: payload[..restarts_start].to_vec(), restarts, entry_count })
    }

    /// Zero-copy get: returns a reference to the value if found
    #[allow(dead_code)] // Reserved for future binary search optimization
    pub fn get<'a>(&'a self, target: &[u8]) -> Option<&'a [u8]> {
        if self.entry_count == 0 { return None; }

        let restart_idx = self.find_restart_point(target);
        let mut cursor = self.restarts[restart_idx] as usize;
        let mut current_key = Vec::new();
        let end = if restart_idx + 1 < self.restarts.len() { self.restarts[restart_idx + 1] as usize } else { self.data.len() };

        while cursor < end {
            let (key, value_range, next) = self.decode_entry_range(cursor, &current_key)?;
            match key.as_slice().cmp(target) {
                std::cmp::Ordering::Equal => return Some(&self.data[value_range.0..value_range.1]),
                std::cmp::Ordering::Greater => return None,
                std::cmp::Ordering::Less => { current_key = key; cursor = next; }
            }
        }
        None
    }

    /// Get with owned value (for compatibility)
    #[allow(dead_code)] // Reserved for future binary search optimization
    pub fn get_owned(&self, target: &[u8]) -> Option<Vec<u8>> {
        self.get(target).map(|v| v.to_vec())
    }

    /// Get all entries as owned vectors
    pub fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut result = Vec::with_capacity(self.entry_count as usize);
        let mut cursor = 0;
        let mut current_key = Vec::new();
        while cursor < self.data.len() {
            match self.decode_entry(cursor, &current_key) {
                Some((key, value, next)) => { result.push((key.clone(), value)); current_key = key; cursor = next; }
                None => break,
            }
        }
        Ok(result)
    }

    #[allow(dead_code)] // Used by get() for binary search
    fn find_restart_point(&self, target: &[u8]) -> usize {
        let mut left = 0;
        let mut right = self.restarts.len();
        while left < right {
            let mid = left + (right - left) / 2;
            // Zero-copy comparison: directly compare with key slice in data
            match self.key_at_restart_ref(mid).cmp(target) {
                std::cmp::Ordering::Less => left = mid + 1,
                std::cmp::Ordering::Greater => right = mid,
                std::cmp::Ordering::Equal => return mid,
            }
        }
        left.saturating_sub(1)
    }

    /// Zero-copy key access at restart point - returns a slice reference
    #[allow(dead_code)] // Used by find_restart_point() for binary search
    fn key_at_restart_ref(&self, idx: usize) -> &[u8] {
        let offset = self.restarts[idx] as usize;
        if offset + 8 > self.data.len() { return &[]; }
        let unshared_len = u16::from_le_bytes(self.data[offset + 2..offset + 4].try_into().unwrap()) as usize;
        if offset + 8 + unshared_len > self.data.len() { return &[]; }
        &self.data[offset + 8..offset + 8 + unshared_len]
    }

    fn decode_entry_range(&self, offset: usize, prev_key: &[u8]) -> Option<(Vec<u8>, (usize, usize), usize)> {
        if offset + 8 > self.data.len() { return None; }
        let shared_len = u16::from_le_bytes(self.data[offset..offset + 2].try_into().unwrap()) as usize;
        let unshared_len = u16::from_le_bytes(self.data[offset + 2..offset + 4].try_into().unwrap()) as usize;
        let value_len = u32::from_le_bytes(self.data[offset + 4..offset + 8].try_into().unwrap()) as usize;
        let key_start = offset + 8;
        let value_start = key_start + unshared_len;
        let next = value_start + value_len;
        if next > self.data.len() { return None; }
        let mut key = Vec::with_capacity(shared_len + unshared_len);
        if shared_len > 0 && shared_len <= prev_key.len() { key.extend_from_slice(&prev_key[..shared_len]); }
        key.extend_from_slice(&self.data[key_start..value_start]);
        Some((key, (value_start, next), next))
    }

    fn decode_entry(&self, offset: usize, prev_key: &[u8]) -> Option<(Vec<u8>, Vec<u8>, usize)> {
        let (key, value_range, next) = self.decode_entry_range(offset, prev_key)?;
        Some((key, self.data[value_range.0..value_range.1].to_vec(), next))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_basic() {
        let mut builder = BlockBuilder::new(4096);
        assert!(builder.try_push(b"apple", b"red"));
        assert!(builder.try_push(b"apricot", b"orange"));
        assert!(builder.try_push(b"banana", b"yellow"));
        assert!(builder.try_push(b"cherry", b"red"));
        
        let data = builder.finish();
        let block = Block::parse(&data).unwrap();
        assert_eq!(block.get(b"apple"), Some(b"red".as_slice()));
        assert_eq!(block.get(b"apricot"), Some(b"orange".as_slice()));
        assert_eq!(block.get(b"banana"), Some(b"yellow".as_slice()));
        assert_eq!(block.get(b"cherry"), Some(b"red".as_slice()));
        assert_eq!(block.get(b"grape"), None);
    }

    #[test]
    fn test_restart_points() {
        let mut builder = BlockBuilder::new(16 * 1024);
        for i in 0..50 {
            assert!(builder.try_push(format!("key{:04}", i).as_bytes(), format!("val{}", i).as_bytes()));
        }
        let data = builder.finish();
        let block = Block::parse(&data).unwrap();
        assert_eq!(block.get(b"key0000"), Some(b"val0".as_slice()));
        assert_eq!(block.get(b"key0025"), Some(b"val25".as_slice()));
        assert_eq!(block.get(b"key0049"), Some(b"val49".as_slice()));
        assert_eq!(block.get(b"key9999"), None);
    }

    #[test]
    fn test_entries_iteration() {
        let mut builder = BlockBuilder::new(4096);
        let pairs = vec![(b"a".to_vec(), b"1".to_vec()), (b"b".to_vec(), b"2".to_vec()), (b"c".to_vec(), b"3".to_vec())];
        for (k, v) in &pairs { assert!(builder.try_push(k, v)); }
        let data = builder.finish();
        let block = Block::parse(&data).unwrap();
        assert_eq!(block.entries().unwrap(), pairs);
    }
}