//! Block Cache with O(1) LRU and Parsed Block Cache
//!
//! Uses Block's restart points for O(log n) binary search within blocks.

use std::{
    alloc::{self, Layout},
    collections::HashMap,
    ptr::NonNull,
    sync::{Arc, Mutex},
};

use anyhow::{anyhow, Result};
use io_uring::{opcode, types};

use crate::block::Block;
use crate::lru::LruCache;
use crate::scheduler::IoRingHandle;

pub const CACHE_SLOT_SIZE: usize = 16 * 1024;

/// Cached block with pre-parsed entries for efficient binary search.
/// Entries are parsed once and cached, then binary search is O(log n) on decoded keys.
///
/// This is faster than using Block::get() with restart points because:
/// 1. Keys are already decoded (no per-lookup prefix decompression)
/// 2. Binary search on in-memory Vec<(Vec<u8>, Vec<u8>)> is very efficient
#[derive(Clone)]
pub struct CachedBlock {
    /// Pre-parsed and sorted entries for O(log n) binary search
    entries: Arc<Vec<(Vec<u8>, Vec<u8>)>>,
}

impl CachedBlock {
    pub fn new(data: Vec<u8>) -> Result<Self> {
        let block = Block::parse(&data)?;
        let entries = block.entries()?;
        Ok(Self { entries: Arc::new(entries) })
    }

    /// Binary search for a key. O(log n) lookup on pre-decoded keys.
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        match self.entries.binary_search_by(|(k, _)| k.as_slice().cmp(key)) {
            Ok(idx) => Some(self.entries[idx].1.clone()),
            Err(_) => None,
        }
    }

    /// Get all entries (for iteration).
    #[allow(dead_code)] // Reserved for future iteration support
    pub fn entries(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Ok((*self.entries).clone())
    }
}

/// O(1) LRU cached block cache
pub struct CachedBlockCache {
    inner: Mutex<LruCache<(u64, u64), CachedBlock>>,
}

impl CachedBlockCache {
    pub fn new(capacity: usize) -> Self {
        Self { inner: Mutex::new(LruCache::new(capacity)) }
    }

    pub fn get(&self, sst_id: u64, offset: u64) -> Option<CachedBlock> {
        self.inner.lock().unwrap().get(&(sst_id, offset))
    }

    pub fn insert(&self, sst_id: u64, offset: u64, block: CachedBlock) {
        self.inner.lock().unwrap().insert((sst_id, offset), block);
    }
}

pub struct BlockCache {
    inner: Mutex<CacheInner>,
    ring: Arc<IoRingHandle>,
    memory: NonNull<u8>,
    layout: Layout,
    buf_index: u16,
    is_mmap: bool,
    total_size: usize,
    cached_blocks: Arc<CachedBlockCache>,
}

unsafe impl Send for BlockCache {}
unsafe impl Sync for BlockCache {}

struct CacheInner {
    lru: LruCache<usize, (u64, u64)>,
    reverse_map: HashMap<(u64, u64), usize>,
    free_slots: Vec<usize>,
}

impl BlockCache {
    pub fn new(ring: Arc<IoRingHandle>, num_slots: usize) -> Result<Self> {
        let total_size = CACHE_SLOT_SIZE * num_slots;
        let (ptr, layout, is_mmap) = match Self::try_huge_pages(total_size) {
            Ok(ptr) => (ptr, Layout::new::<u8>(), true),
            Err(_) => {
                let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
                let layout = Layout::from_size_align(total_size, page_size)?;
                let ptr = unsafe { alloc::alloc(layout) };
                if ptr.is_null() {
                    return Err(anyhow!("failed to allocate"));
                }
                (unsafe { NonNull::new_unchecked(ptr) }, layout, false)
            }
        };

        let buf_index = ring.allocate_buffer_slot().ok_or_else(|| {
            Self::cleanup(ptr, layout, is_mmap, total_size);
            anyhow!("no buffer slots")
        })?;

        let iovec = libc::iovec { iov_base: ptr.as_ptr() as *mut _, iov_len: total_size };
        if let Err(e) = ring.register_buffer_at_slot(buf_index, &iovec) {
            Self::cleanup(ptr, layout, is_mmap, total_size);
            return Err(e);
        }

        let free_slots: Vec<usize> = (0..num_slots).rev().collect();
        let cached_blocks = Arc::new(CachedBlockCache::new(num_slots * 4));

        Ok(Self {
            inner: Mutex::new(CacheInner {
                lru: LruCache::new(num_slots),
                reverse_map: HashMap::with_capacity(num_slots),
                free_slots,
            }),
            ring, memory: ptr, layout, buf_index, is_mmap, total_size, cached_blocks,
        })
    }

    fn try_huge_pages(size: usize) -> Result<NonNull<u8>> {
        let flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | 0x40000;
        let ptr = unsafe { libc::mmap(std::ptr::null_mut(), size, libc::PROT_READ | libc::PROT_WRITE, flags, -1, 0) };
        if ptr == libc::MAP_FAILED {
            return Err(anyhow!("mmap failed"));
        }
        NonNull::new(ptr as *mut u8).ok_or_else(|| anyhow!("null"))
    }

    fn cleanup(ptr: NonNull<u8>, layout: Layout, is_mmap: bool, size: usize) {
        if is_mmap {
            unsafe { libc::munmap(ptr.as_ptr() as *mut _, size) };
        } else {
            unsafe { alloc::dealloc(ptr.as_ptr(), layout) };
        }
    }

    /// Read a block and return a CachedBlock that uses restart points for efficient binary search.
    /// This is the preferred method for point lookups as it avoids parsing all entries.
    pub fn read_block_parsed(&self, sst_id: u64, offset: u64, fixed_file_slot: u32) -> Result<CachedBlock> {
        // Check cached blocks first (already parsed/cached)
        if let Some(cached) = self.cached_blocks.get(sst_id, offset) {
            return Ok(cached);
        }

        // Check if raw data is in memory cache
        let cached_slot = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&slot) = inner.reverse_map.get(&(sst_id, offset)) {
                inner.lru.get(&slot);
                Some(slot)
            } else {
                None
            }
        };

        if let Some(slot) = cached_slot {
            let data = self.read_slot(slot);
            let cached = CachedBlock::new(data)?;
            self.cached_blocks.insert(sst_id, offset, cached.clone());
            return Ok(cached);
        }

        // Need to read from disk
        let slot = self.alloc_slot(sst_id, offset);
        let buf_off = slot * CACHE_SLOT_SIZE;
        let entry = opcode::ReadFixed::new(
            types::Fixed(fixed_file_slot),
            unsafe { self.memory.as_ptr().add(buf_off) },
            CACHE_SLOT_SIZE as u32, self.buf_index,
        ).offset(offset).build().user_data(self.ring.next_user_data());

        let cqes = self.ring.run_batch_with_expected(vec![entry], 1)?;
        let cqe = cqes.first().ok_or_else(|| anyhow!("no cqe"))?;
        if cqe.result < 0 { return Err(anyhow!("read error: {}", cqe.result)); }

        {
            let mut inner = self.inner.lock().unwrap();
            inner.lru.insert(slot, (sst_id, offset));
            inner.reverse_map.insert((sst_id, offset), slot);
        }

        let data = unsafe { std::slice::from_raw_parts(self.memory.as_ptr().add(buf_off), cqe.result as usize) };
        let cached = CachedBlock::new(data.to_vec())?;
        self.cached_blocks.insert(sst_id, offset, cached.clone());
        Ok(cached)
    }

    pub fn read_block(&self, sst_id: u64, offset: u64, fixed_file_slot: u32) -> Result<Vec<u8>> {
        let cached = {
            let mut inner = self.inner.lock().unwrap();
            if let Some(&slot) = inner.reverse_map.get(&(sst_id, offset)) {
                inner.lru.get(&slot);
                Some(slot)
            } else {
                None
            }
        };
        if let Some(slot) = cached { return Ok(self.read_slot(slot)); }

        let slot = self.alloc_slot(sst_id, offset);
        let buf_off = slot * CACHE_SLOT_SIZE;
        let entry = opcode::ReadFixed::new(
            types::Fixed(fixed_file_slot),
            unsafe { self.memory.as_ptr().add(buf_off) },
            CACHE_SLOT_SIZE as u32, self.buf_index,
        ).offset(offset).build().user_data(self.ring.next_user_data());

        let cqes = self.ring.run_batch_with_expected(vec![entry], 1)?;
        let cqe = cqes.first().ok_or_else(|| anyhow!("no cqe"))?;
        if cqe.result < 0 { return Err(anyhow!("read error: {}", cqe.result)); }

        {
            let mut inner = self.inner.lock().unwrap();
            inner.lru.insert(slot, (sst_id, offset));
            inner.reverse_map.insert((sst_id, offset), slot);
        }

        Ok(unsafe { std::slice::from_raw_parts(self.memory.as_ptr().add(buf_off), cqe.result as usize).to_vec() })
    }

    fn alloc_slot(&self, sst_id: u64, offset: u64) -> usize {
        let mut inner = self.inner.lock().unwrap();
        if let Some(slot) = inner.free_slots.pop() { return slot; }
        // Evict: find any slot in LRU
        for slot in 0..self.total_size / CACHE_SLOT_SIZE {
            if let Some((old_id, old_off)) = inner.lru.get(&slot) {
                inner.reverse_map.remove(&(old_id, old_off));
                inner.lru.insert(slot, (sst_id, offset));
                inner.reverse_map.insert((sst_id, offset), slot);
                return slot;
            }
        }
        0
    }

    fn read_slot(&self, slot: usize) -> Vec<u8> {
        unsafe { std::slice::from_raw_parts(self.memory.as_ptr().add(slot * CACHE_SLOT_SIZE), CACHE_SLOT_SIZE).to_vec() }
    }
}

impl Drop for BlockCache {
    fn drop(&mut self) {
        let _ = self.ring.unregister_buffer_at_slot(self.buf_index);
        Self::cleanup(self.memory, self.layout, self.is_mmap, self.total_size);
    }
}
