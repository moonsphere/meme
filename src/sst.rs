use std::{
    alloc::{self, Layout},
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, VecDeque},
    fs::File,
    io::{Read, Seek, SeekFrom},
    ops::{Bound, RangeBounds},
    os::fd::AsRawFd,
    path::Path,
    ptr::NonNull,
    sync::Arc,
};

use anyhow::{anyhow, Result};
use io_uring::{opcode, squeue, types};

use crate::{
    block::{Block, BlockBuilder},
    scheduler::IoRingHandle,
    util::{hash_pair, open_o_direct, open_o_dsync, path_bytes, tmp_name},
};

pub const BLOCK_SIZE: usize = 16 * 1024;

/// Simple fixed-buffer pool backed by registered buffers (ReadFixed-friendly).
/// This is a shared pool that can be used by multiple SstReaders.
pub struct FixedBufPool {
    ring: Arc<IoRingHandle>,
    buffers: Vec<(NonNull<u8>, u16)>, // (ptr, slot)
    free: VecDeque<usize>,
    buf_len: usize,
}

// Safety: FixedBufPool owns the buffers and ensures proper synchronization
unsafe impl Send for FixedBufPool {}
unsafe impl Sync for FixedBufPool {}

impl FixedBufPool {
    pub fn new(ring: Arc<IoRingHandle>, buf_len: usize, count: usize) -> Result<Self> {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = Layout::from_size_align(buf_len, page_size)
            .map_err(|e| anyhow!("invalid layout: {}", e))?;
        let mut buffers: Vec<(NonNull<u8>, u16)> = Vec::with_capacity(count);
        let mut free = VecDeque::with_capacity(count);
        for _ in 0..count {
            let ptr = unsafe { alloc::alloc(layout) };
            if ptr.is_null() {
                // clean up already allocated
                for (p, _) in &buffers {
                    unsafe { alloc::dealloc(p.as_ptr(), layout) };
                }
                return Err(anyhow!("failed to allocate buffer"));
            }
            let slot = ring
                .allocate_buffer_slot()
                .ok_or_else(|| anyhow!("no buffer slots available"))?;
            let iovec = libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: buf_len,
            };
            ring.register_buffer_at_slot(slot, &iovec)?;
            let nn = unsafe { NonNull::new_unchecked(ptr) };
            buffers.push((nn, slot));
            free.push_back(buffers.len() - 1);
        }
        Ok(Self {
            ring,
            buffers,
            free,
            buf_len,
        })
    }

    pub fn acquire(&mut self) -> Option<(usize, *mut u8, u16)> {
        self.free.pop_front().map(|idx| {
            let (ptr, slot) = self.buffers[idx];
            (idx, ptr.as_ptr(), slot)
        })
    }

    pub fn release(&mut self, idx: usize) {
        self.free.push_back(idx);
    }

    pub fn len(&self) -> usize {
        self.buffers.len()
    }

    pub fn buf_len(&self) -> usize {
        self.buf_len
    }

    pub fn ptr(&self, idx: usize) -> *mut u8 {
        self.buffers[idx].0.as_ptr()
    }
    
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.buffers.is_empty()
    }
}

impl Drop for FixedBufPool {
    fn drop(&mut self) {
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = Layout::from_size_align(self.buf_len, page_size).unwrap();
        for (ptr, slot) in self.buffers.drain(..) {
            let _ = self.ring.unregister_buffer_at_slot(slot);
            unsafe { alloc::dealloc(ptr.as_ptr(), layout) };
        }
    }
}

#[derive(Clone)]
pub struct SstFlusher {
    ring: Arc<IoRingHandle>,
    fixed_slot: u32,
}

#[derive(Clone)]
pub struct KeyBounds {
    pub min: Vec<u8>,
    pub max: Vec<u8>,
}

impl SstFlusher {
    pub fn new(ring: Arc<IoRingHandle>, fixed_slot: u32) -> Result<Self> {
        Ok(Self { ring, fixed_slot })
    }

    pub fn flush(&self, path: &Path, entries: &[(Vec<u8>, Vec<u8>)]) -> Result<Option<KeyBounds>> {
        let iter = entries
            .iter()
            .cloned()
            .map(|(k, v)| Ok((k, v)));
        self.flush_stream(path, iter)
    }

    pub fn flush_stream<I>(
        &self,
        path: &Path,
        entries: I,
    ) -> Result<Option<KeyBounds>>
    where
        I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
    {
        let debug = std::env::var("MEME_DEBUG").as_deref() == Ok("1");
        if debug {
            eprintln!("[sst-flush] start path={:?}", path);
        }
        let tmp_path = tmp_name(path);
        // Use O_DIRECT for SST flushing for extreme throughput.
        // fsync is used at the end for efficient batch synchronization.
        // Note: We must ensure all writes are sector aligned (4096).
        let tmp_file = match open_o_direct(&tmp_path) {
            Ok(f) => f,
            Err(_) => open_o_dsync(&tmp_path, true)?, // Fallback
        };
        
        self.ring
            .update_fixed_file(self.fixed_slot, tmp_file.as_raw_fd())?;
        
        // We must use AlignedBuffer for O_DIRECT
        let mut owned = Vec::new();
        
        let fd = types::Fixed(self.fixed_slot);
        let mut offset = 0u64;
        let mut block = BlockBuilder::new(BLOCK_SIZE);
        let mut block_first: Option<Vec<u8>> = None;
        let mut index = Vec::new();
        let mut bloom = BloomFilter::new(2048, 3);
        let mut first_key: Option<Vec<u8>> = None;
        let mut last_key: Option<Vec<u8>> = None;
        
        // Collect all write data first to calculate total size for fallocate
        let mut write_chunks: Vec<Vec<u8>> = Vec::new();

        for entry in entries {
            let (key, value) = entry?;
            if first_key.is_none() {
                first_key = Some(key.clone());
            }
            last_key = Some(key.clone());
            bloom.insert(&key);
            if block_first.is_none() {
                block_first = Some(key.clone());
            }
            if !block.try_push(&key, &value) {
                let first = block_first.take().expect("block has first key");
                let bytes = block.finish();
                index.push(BlockIndexEntry {
                    first_key: first,
                    offset,
                });
                // Calculate padded size for O_DIRECT alignment
                let padded_len = (bytes.len() + 4095) & !4095;
                offset += padded_len as u64;
                write_chunks.push(bytes);
                block = BlockBuilder::new(BLOCK_SIZE);
                block_first = Some(key.clone());
                assert!(
                    block.try_push(&key, &value),
                    "entry exceeds block size"
                );
            }
        }

        if !block.is_empty() {
            let first = block_first.take().expect("block has first");
            let bytes = block.finish();
            index.push(BlockIndexEntry {
                first_key: first,
                offset,
            });
            let padded_len = (bytes.len() + 4095) & !4095;
            offset += padded_len as u64;
            write_chunks.push(bytes);
        }

        let index_bytes = encode_index(&index);
        let index_offset = offset;
        let index_len = index_bytes.len() as u32;
        if !index_bytes.is_empty() {
            let padded_len = (index_bytes.len() + 4095) & !4095;
            offset += padded_len as u64;
            write_chunks.push(index_bytes);
        }

        let bloom_bytes = bloom.to_bytes();
        let bloom_offset = offset;
        let bloom_len = bloom_bytes.len() as u32;
        if !bloom_bytes.is_empty() {
            let padded_len = (bloom_bytes.len() + 4095) & !4095;
            offset += padded_len as u64;
            write_chunks.push(bloom_bytes);
        }

        let footer = Footer::new(
            index_offset,
            index_len,
            bloom_offset,
            bloom_len,
        );
        let footer_bytes: Vec<u8> = footer.to_bytes().to_vec();
        let padded_len = (footer_bytes.len() + 4095) & !4095;
        offset += padded_len as u64;
        write_chunks.push(footer_bytes);
        
        // Total file size is now known: offset
        let total_size = offset;
        
        // Build SQE chain:
        // 1. fallocate (pre-allocate space to avoid write-time allocation latency)
        // 2. writes (linked with SKIP_SUCCESS)
        // 3. fsync (data sync)
        // 4. rename (atomic replace)
        // 5. fsync directory (ensure directory entry is persisted)
        
        let mut sqes = Vec::new();
        
        // 1. Fallocate: pre-allocate file space to avoid fragmentation and write-time allocation
        // This reduces tail latency caused by block allocation during writes.
        // Use FALLOC_FL_KEEP_SIZE to not change file size (we'll write the actual data).
        // Note: fallocate may not be supported on all filesystems (e.g., tmpfs), so we
        // make it non-fatal by not linking it strictly.
        let fallocate_entry = opcode::Fallocate::new(fd, total_size)
            .offset(0)
            .mode(0) // Default mode: allocate space
            .build()
            .flags(squeue::Flags::FIXED_FILE | squeue::Flags::IO_LINK | squeue::Flags::SKIP_SUCCESS)
            .user_data(0);
        sqes.push(fallocate_entry);
        
        // 2. Queue all writes
        let mut write_offset = 0u64;
        for chunk in write_chunks {
            Self::queue_write(&mut sqes, &mut owned, fd, chunk, &mut write_offset);
        }
        
        // 3. Fsync for efficient batch synchronization (more efficient than O_DSYNC for large writes)
        let fsync_entry = opcode::Fsync::new(fd)
            .flags(types::FsyncFlags::DATASYNC)
            .build()
            .flags(squeue::Flags::FIXED_FILE | squeue::Flags::IO_LINK)
            .user_data(self.ring.next_user_data());
        sqes.push(fsync_entry);

        // 4. Rename: atomic replace tmp file with final name
        let from_c = std::ffi::CString::new(path_bytes(&tmp_path))?;
        let to_c = std::ffi::CString::new(path_bytes(path))?;
        let rename_entry = opcode::RenameAt::new(
            types::Fd(libc::AT_FDCWD),
            from_c.as_ptr(),
            types::Fd(libc::AT_FDCWD),
            to_c.as_ptr(),
        )
        .build()
        .flags(squeue::Flags::IO_LINK)
        .user_data(self.ring.next_user_data());
        sqes.push(rename_entry);
        
        // 5. Fsync directory: ensure the new directory entry is persisted
        // This reduces the power-loss window where the file exists but directory entry doesn't.
        let dir_path = path.parent().unwrap_or(Path::new("."));
        let dir_c = std::ffi::CString::new(path_bytes(dir_path))?;
        let dir_fd = unsafe { libc::open(dir_c.as_ptr(), libc::O_RDONLY | libc::O_DIRECTORY) };
        if dir_fd >= 0 {
            let dir_fsync_entry = opcode::Fsync::new(types::Fd(dir_fd))
                .build()
                .user_data(self.ring.next_user_data());
            sqes.push(dir_fsync_entry);
            // We need to close the dir_fd after fsync completes
            // For now, we'll close it synchronously after the ring operation
        }

        // Expected CQEs: fsync(file) + rename + fsync(dir) = 3
        // fallocate and writes use SKIP_SUCCESS
        let expected_cqes = if dir_fd >= 0 { 3 } else { 2 };
        self.ring.run_and_forget_with_expected(sqes, expected_cqes)?;
        
        // Close directory fd if we opened it
        if dir_fd >= 0 {
            unsafe { libc::close(dir_fd) };
        }
        
        if debug {
            eprintln!(
                "[sst-flush] done path={:?} bytes={} blocks={}",
                path,
                total_size,
                index.len()
            );
        }
        Ok(first_key.zip(last_key).map(|(min, max)| KeyBounds { min, max }))
    }

    fn queue_write(
        sqes: &mut Vec<squeue::Entry>,
        owned: &mut Vec<AlignedBuf>,
        fd: types::Fixed,
        data: Vec<u8>,
        offset: &mut u64,
    ) {
        if data.is_empty() {
            return;
        }
        
        // Copy data into an aligned buffer
        let mut aligned = AlignedBuf::new(data.len());
        // Pad to 4096 boundary for O_DIRECT
        let padded_len = (data.len() + 4095) & !4095;
        if aligned.cap < padded_len {
            aligned = AlignedBuf::new(padded_len);
        }
        unsafe {
            std::ptr::copy_nonoverlapping(data.as_ptr(), aligned.ptr.as_ptr(), data.len());
            // Zero out padding
            if padded_len > data.len() {
                std::ptr::write_bytes(
                    aligned.ptr.as_ptr().add(data.len()),
                    0,
                    padded_len - data.len(),
                );
            }
        }
        
        // Use padded_len for the write
        let write_len = padded_len;
        
        owned.push(aligned);
        let buf = owned.last().unwrap();
        
        let entry = opcode::Write::new(fd, buf.ptr.as_ptr(), write_len as _)
            .offset(*offset)
            .build()
            .flags(
                squeue::Flags::IO_LINK
                    | squeue::Flags::FIXED_FILE
                    | squeue::Flags::ASYNC
                    | squeue::Flags::SKIP_SUCCESS,
            )
            .user_data(0);
        sqes.push(entry);
        *offset += write_len as u64;
    }
}

/// Helper for Page Aligned allocations
struct AlignedBuf {
    ptr: NonNull<u8>,
    layout: Layout,
    cap: usize,
}

impl AlignedBuf {
    fn new(size: usize) -> Self {
        let page_size = 4096;
        let layout = Layout::from_size_align(size, page_size).expect("valid layout");
        let ptr = unsafe { alloc::alloc(layout) };
        let ptr = NonNull::new(ptr).expect("alloc failed");
        Self { ptr, layout, cap: size }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        unsafe { alloc::dealloc(self.ptr.as_ptr(), self.layout) }
    }
}

pub struct SstHandle {
    pub(crate) file: Arc<File>,
    pub(crate) index: Arc<Vec<BlockIndexEntry>>,
    pub(crate) bloom: Arc<BloomFilter>,
}

impl SstHandle {
    pub fn open(path: &Path) -> Result<Self> {
        let mut file = File::open(path)?;
        let metadata = SstMetadata::load(&mut file)?;
        Ok(Self {
            file: Arc::new(file),
            index: Arc::new(metadata.index),
            bloom: Arc::new(metadata.bloom),
        })
    }
    
    /// Open SST file with O_DIRECT for bypassing page cache.
    ///
    /// This is useful for compaction reads where the data won't be reused.
    /// The caller must ensure read buffers are page-aligned.
    ///
    /// Note: Falls back to regular open if O_DIRECT is not supported (e.g., tmpfs).
    pub fn open_direct(path: &Path) -> Result<Self> {
        // First open normally to read metadata (small reads, page cache is fine)
        let mut normal_file = File::open(path)?;
        let metadata = SstMetadata::load(&mut normal_file)?;
        drop(normal_file);
        
        // Then reopen with O_DIRECT for data reads
        match open_o_direct(path) {
            Ok(f) => Ok(Self {
                file: Arc::new(f),
                index: Arc::new(metadata.index),
                bloom: Arc::new(metadata.bloom),
            }),
            Err(_) => Self::open(path), // Fallback for non-IOPOLL paths
        }
    }

    /// Open SST file with strict O_DIRECT (errors if not supported).
    pub fn open_direct_strict(path: &Path) -> Result<Self> {
        let mut normal_file = File::open(path)?;
        let metadata = SstMetadata::load(&mut normal_file)?;
        drop(normal_file);
        let direct_file = open_o_direct(path)?;
        Ok(Self {
            file: Arc::new(direct_file),
            index: Arc::new(metadata.index),
            bloom: Arc::new(metadata.bloom),
        })
    }
}

/// Configuration for SstReader
#[derive(Default)]
pub struct SstReaderConfig {
    /// Use IOPOLL mode (requires O_DIRECT and ReadFixed)
    pub iopoll_mode: bool,
}

/// Shared buffer pool reference for SstReader
pub enum BufPoolRef {
    /// Owned buffer pool (created per-reader, legacy behavior)
    Owned(FixedBufPool),
    /// Shared buffer pool (borrowed from LsmDb)
    Shared(Arc<std::sync::Mutex<FixedBufPool>>),
}

impl BufPoolRef {
    fn acquire(&mut self) -> Option<(usize, *mut u8, u16)> {
        match self {
            BufPoolRef::Owned(pool) => pool.acquire(),
            BufPoolRef::Shared(pool) => pool.lock().unwrap().acquire(),
        }
    }
    
    fn release(&mut self, idx: usize) {
        match self {
            BufPoolRef::Owned(pool) => pool.release(idx),
            BufPoolRef::Shared(pool) => pool.lock().unwrap().release(idx),
        }
    }
    
    fn len(&self) -> usize {
        match self {
            BufPoolRef::Owned(pool) => pool.len(),
            BufPoolRef::Shared(pool) => pool.lock().unwrap().len(),
        }
    }
    
    fn buf_len(&self) -> usize {
        match self {
            BufPoolRef::Owned(pool) => pool.buf_len(),
            BufPoolRef::Shared(pool) => pool.lock().unwrap().buf_len(),
        }
    }
    
    fn ptr(&self, idx: usize) -> *mut u8 {
        match self {
            BufPoolRef::Owned(pool) => pool.ptr(idx),
            BufPoolRef::Shared(pool) => pool.lock().unwrap().ptr(idx),
        }
    }
}

pub struct SstReader {
    ring: Arc<IoRingHandle>,
    handle: Arc<SstHandle>,
    fixed_slot: u32,
    buf_pool: BufPoolRef,
    bytes_read: u64,
    sst_id: u64,
    block_cache: Option<Arc<crate::cache::BlockCache>>,
}

impl SstReader {
    /// Create SstReader backed by registered fixed buffers (IOPOLL-friendly).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        handle: Arc<SstHandle>,
        ring: Arc<IoRingHandle>,
        fixed_slot: u32,
        buf_len: usize,
        buf_count: usize,
        sst_id: u64,
        block_cache: Option<Arc<crate::cache::BlockCache>>,
    ) -> Result<Self> {
        Self::with_config(
            handle,
            ring,
            fixed_slot,
            buf_len,
            buf_count,
            sst_id,
            block_cache,
            SstReaderConfig::default(),
        )
    }

    /// Create SstReader with explicit configuration
    #[allow(clippy::too_many_arguments)]
    pub fn with_config(
        handle: Arc<SstHandle>,
        ring: Arc<IoRingHandle>,
        fixed_slot: u32,
        buf_len: usize,
        buf_count: usize,
        sst_id: u64,
        block_cache: Option<Arc<crate::cache::BlockCache>>,
        config: SstReaderConfig,
    ) -> Result<Self> {
        ring.update_fixed_file(fixed_slot, handle.file.as_raw_fd())?;

        if config.iopoll_mode && block_cache.is_none() {
            return Err(anyhow!("IOPOLL mode requires BlockCache"));
        }

        // Allocate fixed buffer pool (registered buffers usable with ReadFixed).
        let buf_pool = FixedBufPool::new(ring.clone(), buf_len, buf_count)?;

        Ok(Self {
            ring,
            handle,
            fixed_slot,
            buf_pool: BufPoolRef::Owned(buf_pool),
            bytes_read: 0,
            sst_id,
            block_cache,
        })
    }
    
    /// Create SstReader with a shared buffer pool (avoids per-reader allocation)
    #[allow(clippy::too_many_arguments)]
    pub fn with_shared_pool(
        handle: Arc<SstHandle>,
        ring: Arc<IoRingHandle>,
        fixed_slot: u32,
        shared_pool: Arc<std::sync::Mutex<FixedBufPool>>,
        sst_id: u64,
        block_cache: Option<Arc<crate::cache::BlockCache>>,
        config: SstReaderConfig,
    ) -> Result<Self> {
        ring.update_fixed_file(fixed_slot, handle.file.as_raw_fd())?;

        if config.iopoll_mode && block_cache.is_none() {
            return Err(anyhow!("IOPOLL mode requires BlockCache"));
        }

        Ok(Self {
            ring,
            handle,
            fixed_slot,
            buf_pool: BufPoolRef::Shared(shared_pool),
            bytes_read: 0,
            sst_id,
            block_cache,
        })
    }

    #[allow(dead_code)]
    pub fn iter(&mut self) -> Result<SstIterator> {
        let mut entries = Vec::new();
        let offsets: Vec<u64> = self
            .handle
            .index
            .iter()
            .map(|entry| entry.offset)
            .collect();
        for offset in offsets {
            let mut block_entries = self.read_block(offset)?;
            entries.append(&mut block_entries);
        }
        Ok(SstIterator {
            entries,
            index: 0,
        })
    }

    pub fn get(&mut self, target: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.handle.bloom.might_contain(target) {
            return Ok(None);
        }
        if self.handle.index.is_empty() {
            return Ok(None);
        }
        let pos = self
            .handle
            .index
            .partition_point(|entry| entry.first_key.as_slice() <= target);
        let block_idx = pos.saturating_sub(1);
        if let Some(entry) = self.handle.index.get(block_idx) {
            // Try to use cached block with restart points binary search (O(log n))
            if let Some(cache) = &self.block_cache {
                let cached = cache.read_block_parsed(self.sst_id, entry.offset, self.fixed_slot)?;
                // Binary search using restart points within the block
                return Ok(cached.get(target));
            }
            
            // Fallback: read raw block and linear scan
            let block_entries = self.read_block(entry.offset)?;
            // Use binary search on the entries
            match block_entries.binary_search_by(|(k, _)| k.as_slice().cmp(target)) {
                Ok(idx) => return Ok(Some(block_entries[idx].1.clone())),
                Err(_) => return Ok(None),
            }
        }
        Ok(None)
    }

    fn read_block(&mut self, offset: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let debug = std::env::var("MEME_DEBUG").as_deref() == Ok("1");
        if let Some(cache) = &self.block_cache {
            let data = cache.read_block(self.sst_id, offset, self.fixed_slot)?;
            self.bytes_read += data.len() as u64;
            if debug {
                eprintln!("[reader] cache hit offset={}", offset);
            }
            return Block::parse(&data)?.entries();
        }

        let (buf_idx, buf_ptr, slot) = self
            .buf_pool
            .acquire()
            .ok_or_else(|| anyhow!("no buffers available"))?;
        let fd = types::Fixed(self.fixed_slot);
        let entry = opcode::ReadFixed::new(
            fd,
            buf_ptr,
            self.buf_pool.buf_len() as u32,
            slot,
        )
        .offset(offset)
        .build()
        .user_data(self.ring.next_user_data());

        let completions = self.ring.run_batch(vec![entry])?;
        let mut parsed = Vec::new();
        for completion in completions {
            let len = completion.result as usize;
            if len == 0 {
                continue;
            }
            let buf = unsafe { std::slice::from_raw_parts(buf_ptr, len) };
            parsed = Block::parse(buf)?.entries()?;
            self.bytes_read += len as u64;
        }
        self.buf_pool.release(buf_idx);
        if debug {
            eprintln!("[reader] read_block offset={} len={}", offset, parsed.len());
        }
        Ok(parsed)
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    pub fn into_pipelined_iter(self, window: usize) -> Result<PipelinedIterator> {
        PipelinedIterator::new(
            self.handle,
            self.ring,
            self.fixed_slot,
            self.buf_pool,
            window,
            self.sst_id,
            self.block_cache,
        )
    }
    
    /// Get the SST handle (for caching purposes)
    #[allow(dead_code)]
    pub fn handle(&self) -> &Arc<SstHandle> {
        &self.handle
    }
    
    /// Get the SST ID
    #[allow(dead_code)]
    pub fn sst_id(&self) -> u64 {
        self.sst_id
    }
}

#[allow(dead_code)]
pub struct SstIterator {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl Iterator for SstIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index >= self.entries.len() {
            None
        } else {
            let item = self.entries[self.index].clone();
            self.index += 1;
            Some(item)
        }
    }
}

/// SST reader using registered (fixed) buffers for hot files.
#[allow(dead_code)]
pub struct FixedBufferReader {
    ring: Arc<IoRingHandle>,
    handle: Arc<SstHandle>,
    fixed_file_slot: u32,
    /// Pre-allocated buffer (page-aligned for optimal I/O)
    buffer: NonNull<u8>,
    buffer_size: usize,
    /// Registered buffer slot index (for ReadFixed)
    buffer_slot: Option<u16>,
    /// Whether we successfully registered the buffer
    registered: bool,
    bytes_read: u64,
}

// Safety: FixedBufferReader owns the buffer and ensures proper synchronization
unsafe impl Send for FixedBufferReader {}

#[allow(dead_code)]
impl FixedBufferReader {
    pub fn new(
        handle: Arc<SstHandle>,
        ring: Arc<IoRingHandle>,
        fixed_file_slot: u32,
        buf_size: usize,
    ) -> Result<Self> {
        ring.update_fixed_file(fixed_file_slot, handle.file.as_raw_fd())?;

        // Allocate page-aligned buffer for optimal I/O
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = Layout::from_size_align(buf_size, page_size)
            .map_err(|e| anyhow!("invalid layout: {}", e))?;
        let buffer = unsafe {
            let ptr = alloc::alloc(layout);
            if ptr.is_null() {
                return Err(anyhow!("failed to allocate fixed buffer"));
            }
            NonNull::new_unchecked(ptr)
        };

        // Try to allocate a buffer slot and register the buffer
        let (buffer_slot, registered) = match ring.allocate_buffer_slot() {
            Some(slot) => {
                let iovec = libc::iovec {
                    iov_base: buffer.as_ptr() as *mut libc::c_void,
                    iov_len: buf_size,
                };
                match ring.register_buffer_at_slot(slot, &iovec) {
                    Ok(()) => (Some(slot), true),
                    Err(_) => {
                        // Registration failed, fall back to regular Read
                        (Some(slot), false)
                    }
                }
            }
            None => {
                // No slots available, fall back to regular Read
                (None, false)
            }
        };

        Ok(Self {
            ring,
            handle,
            fixed_file_slot,
            buffer,
            buffer_size: buf_size,
            buffer_slot,
            registered,
            bytes_read: 0,
        })
    }

    /// Check if this reader is using registered buffers (ReadFixed).
    pub fn is_registered(&self) -> bool {
        self.registered
    }

    /// Point lookup using registered buffer.
    pub fn get(&mut self, target: &[u8]) -> Result<Option<Vec<u8>>> {
        if !self.handle.bloom.might_contain(target) {
            return Ok(None);
        }
        if self.handle.index.is_empty() {
            return Ok(None);
        }
        let pos = self
            .handle
            .index
            .partition_point(|entry| entry.first_key.as_slice() <= target);
        let block_idx = pos.saturating_sub(1);
        if let Some(entry) = self.handle.index.get(block_idx) {
            let block_entries = self.read_block_fixed(entry.offset)?;
            for (key, value) in block_entries {
                match key.as_slice().cmp(target) {
                    Ordering::Equal => return Ok(Some(value)),
                    Ordering::Greater => return Ok(None),
                    Ordering::Less => continue,
                }
            }
        }
        Ok(None)
    }

    /// Read a block using registered buffer (ReadFixed) or fallback to regular Read.
    fn read_block_fixed(&mut self, offset: u64) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let entry = if self.registered {
            // Use ReadFixed with registered buffer - zero page pinning overhead
            let buf_index = self.buffer_slot.unwrap();
            opcode::ReadFixed::new(
                types::Fixed(self.fixed_file_slot),
                self.buffer.as_ptr(),
                self.buffer_size as u32,
                buf_index,
            )
            .offset(offset)
            .build()
            .user_data(self.ring.next_user_data())
        } else {
            // Fallback to regular Read with page-aligned buffer
            opcode::Read::new(
                types::Fixed(self.fixed_file_slot),
                self.buffer.as_ptr(),
                self.buffer_size as u32,
            )
            .offset(offset)
            .build()
            .user_data(self.ring.next_user_data())
        };

        let completions = self.ring.run_batch(vec![entry])?;
        let mut parsed = Vec::new();
        for completion in completions {
            let len = completion.result as usize;
            if len == 0 {
                continue;
            }
            let buf = unsafe { std::slice::from_raw_parts(self.buffer.as_ptr(), len) };
            parsed = Block::parse(buf)?.entries()?;
            self.bytes_read += len as u64;
        }
        Ok(parsed)
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl Drop for FixedBufferReader {
    fn drop(&mut self) {
        // Unregister the buffer slot if we registered it
        if self.registered
            && let Some(slot) = self.buffer_slot {
                let _ = self.ring.unregister_buffer_at_slot(slot);
            }

        // Free the page-aligned buffer
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = Layout::from_size_align(self.buffer_size, page_size).unwrap();
        unsafe {
            alloc::dealloc(self.buffer.as_ptr(), layout);
        }
    }
}

pub struct PipelinedIterator {
    ring: Arc<IoRingHandle>,
    _handle: Arc<SstHandle>,
    fixed_slot: u32,
    buf_pool: BufPoolRef,
    offsets: VecDeque<u64>,
    ready: VecDeque<BlockEntries>,
    current: Option<BlockEntries>,
    in_flight: usize,
    max_in_flight: usize,
    requests: HashMap<u64, (u64, usize)>, // user_data -> (offset, buf_idx)
    next_req: u64,
    bytes_read: u64,
    sst_id: u64,
    block_cache: Option<Arc<crate::cache::BlockCache>>,
}

impl PipelinedIterator {
    #[allow(clippy::too_many_arguments)]
    fn new(
        handle: Arc<SstHandle>,
        ring: Arc<IoRingHandle>,
        fixed_slot: u32,
        buf_pool: BufPoolRef,
        window: usize,
        sst_id: u64,
        block_cache: Option<Arc<crate::cache::BlockCache>>,
    ) -> Result<Self> {
        let buf_count = buf_pool.len().max(1);
        let max_in_flight = window.max(1).min(buf_count);
        let offsets = handle.index.iter().map(|e| e.offset).collect();
        
        // Issue FADVISE to hint the kernel about sequential access pattern.
        // This helps the kernel optimize readahead and page cache behavior.
        // POSIX_FADV_SEQUENTIAL = 2
        let file_len = handle.file.metadata().map(|m| m.len()).unwrap_or(0);
        if file_len > 0 {
            let fadvise_entry = opcode::Fadvise::new(
                types::Fixed(fixed_slot),
                file_len as libc::off_t,    // len: entire file
                libc::POSIX_FADV_SEQUENTIAL,
            )
            .offset(0)  // start of file
            .build()
            .user_data(ring.next_user_data());
            // Fire and forget - we don't need to wait for this
            let _ = ring.run_batch(vec![fadvise_entry]);
        }
        
        Ok(Self {
            ring,
            _handle: handle,
            fixed_slot,
            buf_pool,
            offsets,
            ready: VecDeque::new(),
            current: None,
            in_flight: 0,
            max_in_flight,
            requests: HashMap::new(),
            next_req: 1,
            bytes_read: 0,
            sst_id,
            block_cache,
        })
    }

    fn submit_read(&mut self, offset: u64) -> Result<()> {
        let debug = std::env::var("MEME_DEBUG").as_deref() == Ok("1");
        // If cache present, try synchronous cache read (useful for IOPOLL mode too)
        if let Some(cache) = &self.block_cache
            && let Ok(data) = cache.read_block(self.sst_id, offset, self.fixed_slot) {
                self.bytes_read += data.len() as u64;
                let entries = Block::parse(&data)?.entries()?;
                self.ready.push_back(BlockEntries::new(entries));
                if debug {
                    eprintln!("[iter] cache hit offset={}", offset);
                }
                return Ok(());
            }

        let (buf_idx, buf_ptr, slot) = self
            .buf_pool
            .acquire()
            .ok_or_else(|| anyhow!("no buffers available"))?;
        let fd = types::Fixed(self.fixed_slot);
        let req_id = self.next_req;
        self.next_req += 1;
        let entry = opcode::ReadFixed::new(
            fd,
            buf_ptr,
            self.buf_pool.buf_len() as u32,
            slot,
        )
        .offset(offset)
        .build()
        .user_data(req_id);
        self.requests.insert(req_id, (offset, buf_idx));
        self.ring.submit_entries(vec![entry])?;
        self.in_flight += 1;
        if debug {
            eprintln!("[iter] submit offset={} buf_idx={} ud={}", offset, buf_idx, req_id);
        }
        Ok(())
    }

    fn harvest(&mut self, min: usize) -> Result<()> {
        let debug = std::env::var("MEME_DEBUG").as_deref() == Ok("1");
        let completions = self.ring.wait_for_completions(min)?;
        for completion in completions {
            let len = completion.result as usize;
            let (_offset, buf_idx) = match self.requests.remove(&completion.user_data) {
                Some(v) => v,
                None => continue,
            };
            if len == 0 {
                self.buf_pool.release(buf_idx);
                continue;
            }
            let ptr = self.buf_pool.ptr(buf_idx);
            let buf = unsafe { std::slice::from_raw_parts(ptr, len) };
            let entries = Block::parse(buf)?.entries()?;
            self.bytes_read += len as u64;
            self.ready.push_back(BlockEntries::new(entries));
            self.in_flight = self.in_flight.saturating_sub(1);
            self.buf_pool.release(buf_idx);
            if debug {
                eprintln!(
                    "[iter] harvest ud={} len={} in_flight={}",
                    completion.user_data,
                    len,
                    self.in_flight
                );
            }
        }
        Ok(())
    }

    fn fill_window(&mut self) -> Result<()> {
        while self.in_flight < self.max_in_flight {
            if let Some(offset) = self.offsets.pop_front() {
                self.submit_read(offset)?;
            } else {
                break;
            }
        }
        while self.ready.is_empty() && self.in_flight > 0 {
            self.harvest(1)?;
        }
        Ok(())
    }

    pub fn bytes_read(&self) -> u64 {
        self.bytes_read
    }
}

impl Iterator for PipelinedIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(current) = self.current.as_mut()
                && let Some(item) = current.next_entry() {
                    return Some(Ok(item));
                }
            self.current = self.ready.pop_front();
            if self.current.is_some() {
                continue;
            }
            if self.offsets.is_empty() && self.in_flight == 0 {
                return None;
            }
            if let Err(err) = self.fill_window() {
                return Some(Err(err));
            }
            if self.ready.is_empty() && self.in_flight == 0 {
                return None;
            }
        }
    }
}

struct BlockEntries {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    index: usize,
}

impl BlockEntries {
    fn new(entries: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { entries, index: 0 }
    }

    fn next_entry(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        if self.index >= self.entries.len() {
            None
        } else {
            let item = self.entries[self.index].clone();
            self.index += 1;
            Some(item)
        }
    }
}

#[derive(Clone)]
pub struct RangeSpec {
    start: Bound<Vec<u8>>,
    end: Bound<Vec<u8>>,
}

impl RangeSpec {
    pub fn new(range: impl RangeBounds<Vec<u8>>) -> Self {
        let start = match range.start_bound() {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        let end = match range.end_bound() {
            Bound::Included(v) => Bound::Included(v.clone()),
            Bound::Excluded(v) => Bound::Excluded(v.clone()),
            Bound::Unbounded => Bound::Unbounded,
        };
        Self { start, end }
    }

    pub fn before_start(&self, key: &[u8]) -> bool {
        match &self.start {
            Bound::Included(v) => key < v.as_slice(),
            Bound::Excluded(v) => key <= v.as_slice(),
            Bound::Unbounded => false,
        }
    }

    pub fn past_end(&self, key: &[u8]) -> bool {
        match &self.end {
            Bound::Included(v) => key > v.as_slice(),
            Bound::Excluded(v) => key >= v.as_slice(),
            Bound::Unbounded => false,
        }
    }

    pub fn full() -> Self {
        Self {
            start: Bound::Unbounded,
            end: Bound::Unbounded,
        }
    }
}

pub struct BlockIndexEntry {
    pub first_key: Vec<u8>,
    pub offset: u64,
}

pub struct BloomFilter {
    bits: Vec<u8>,
    hash_count: u8,
}

impl BloomFilter {
    fn new(bits: usize, hash_count: u8) -> Self {
        Self {
            bits: vec![0u8; bits / 8],
            hash_count: hash_count.max(1),
        }
    }

    fn insert(&mut self, key: &[u8]) {
        let len = (self.bits.len() * 8) as u64;
        let (h1, h2) = hash_pair(key);
        for i in 0..self.hash_count as u64 {
            let bit = ((h1.wrapping_add(i.wrapping_mul(h2))) % len) as usize;
            self.set_bit(bit);
        }
    }

    pub fn might_contain(&self, key: &[u8]) -> bool {
        let len = (self.bits.len() * 8) as u64;
        let (h1, h2) = hash_pair(key);
        for i in 0..self.hash_count as u64 {
            let bit = ((h1.wrapping_add(i.wrapping_mul(h2))) % len) as usize;
            if !self.test_bit(bit) {
                return false;
            }
        }
        true
    }

    fn set_bit(&mut self, bit: usize) {
        let byte = bit / 8;
        let offset = bit % 8;
        if let Some(slot) = self.bits.get_mut(byte) {
            *slot |= 1 << offset;
        }
    }

    fn test_bit(&self, bit: usize) -> bool {
        let byte = bit / 8;
        let offset = bit % 8;
        self.bits
            .get(byte)
            .map(|slot| slot & (1 << offset) != 0)
            .unwrap_or(false)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.bits.len());
        out.push(self.hash_count);
        out.extend_from_slice(&self.bits);
        out
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.is_empty() {
            return Err(anyhow!("empty bloom filter"));
        }
        Ok(Self {
            hash_count: bytes[0].max(1),
            bits: bytes[1..].to_vec(),
        })
    }
}

#[allow(dead_code)]
pub struct SstBuild {
    blocks: Vec<Vec<u8>>,
    index: Vec<BlockIndexEntry>,
    bloom: BloomFilter,
}

#[allow(dead_code)]
impl SstBuild {
    fn encode_index(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for entry in &self.index {
            buf.extend_from_slice(&(entry.first_key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&entry.first_key);
            buf.extend_from_slice(&entry.offset.to_le_bytes());
        }
        buf
    }

    #[cfg(test)]
    pub fn blocks(&self) -> &[Vec<u8>] {
        &self.blocks
    }
}

#[allow(dead_code)]
pub struct SstBuilder;

#[allow(dead_code)]
impl SstBuilder {
    pub fn build(entries: &[(Vec<u8>, Vec<u8>)], max_size: usize) -> SstBuild {
        let mut blocks = Vec::new();
        let mut index = Vec::new();
        let mut bloom = BloomFilter::new(2048, 3);
        let mut current = BlockBuilder::new(max_size);
        let mut block_first_key: Option<Vec<u8>> = None;
        let mut offset = 0u64;

        for (k, v) in entries {
            bloom.insert(k);
            if block_first_key.is_none() {
                block_first_key = Some(k.clone());
            }
            if !current.try_push(k, v) {
                if !current.is_empty() {
                    let finished = current.finish();
                    blocks.push(finished);
                    index.push(BlockIndexEntry {
                        first_key: block_first_key.take().unwrap_or_default(),
                        offset,
                    });
                    offset += max_size as u64;
                }
                current = BlockBuilder::new(max_size);
                block_first_key = Some(k.clone());
                assert!(current.try_push(k, v), "entry exceeds block size");
            }
        }
        if !current.is_empty() {
            let finished = current.finish();
            blocks.push(finished);
            index.push(BlockIndexEntry {
                first_key: block_first_key.unwrap_or_default(),
                offset,
            });
        }

        SstBuild {
            blocks,
            index,
            bloom,
        }
    }
}

fn encode_index(entries: &[BlockIndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for entry in entries {
        buf.extend_from_slice(&(entry.first_key.len() as u16).to_le_bytes());
        buf.extend_from_slice(&entry.first_key);
        buf.extend_from_slice(&entry.offset.to_le_bytes());
    }
    buf
}


pub struct Footer {
    index_offset: u64,
    index_len: u32,
    bloom_offset: u64,
    bloom_len: u32,
}

const FOOTER_MAGIC: u32 = 0x4c53_4d31;
const FOOTER_VERSION: u32 = 1;
const FOOTER_SIZE: usize = 32;

impl Footer {
    fn new(index_offset: u64, index_len: u32, bloom_offset: u64, bloom_len: u32) -> Self {
        Self {
            index_offset,
            index_len,
            bloom_offset,
            bloom_len,
        }
    }

    fn to_bytes(&self) -> [u8; FOOTER_SIZE] {
        let mut buf = [0u8; FOOTER_SIZE];
        buf[0..4].copy_from_slice(&FOOTER_MAGIC.to_le_bytes());
        buf[4..8].copy_from_slice(&FOOTER_VERSION.to_le_bytes());
        buf[8..16].copy_from_slice(&self.index_offset.to_le_bytes());
        buf[16..20].copy_from_slice(&self.index_len.to_le_bytes());
        buf[20..28].copy_from_slice(&self.bloom_offset.to_le_bytes());
        buf[28..32].copy_from_slice(&self.bloom_len.to_le_bytes());
        buf
    }

    fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() != FOOTER_SIZE {
            return Err(anyhow!("invalid footer size"));
        }
        let magic = u32::from_le_bytes(buf[0..4].try_into().unwrap());
        if magic != FOOTER_MAGIC {
            return Err(anyhow!("bad footer magic"));
        }
        let version = u32::from_le_bytes(buf[4..8].try_into().unwrap());
        if version != FOOTER_VERSION {
            return Err(anyhow!("unsupported footer version"));
        }
        let index_offset = u64::from_le_bytes(buf[8..16].try_into().unwrap());
        let index_len = u32::from_le_bytes(buf[16..20].try_into().unwrap());
        let bloom_offset = u64::from_le_bytes(buf[20..28].try_into().unwrap());
        let bloom_len = u32::from_le_bytes(buf[28..32].try_into().unwrap());
        Ok(Self {
            index_offset,
            index_len,
            bloom_offset,
            bloom_len,
        })
    }
}

fn decode_index(buf: &[u8]) -> Result<Vec<BlockIndexEntry>> {
    if buf.len() < 4 {
        return Err(anyhow!("index too small"));
    }
    let mut cursor = 4;
    let count = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        if cursor + 2 > buf.len() {
            return Err(anyhow!("corrupt index (key len)"));
        }
        let key_len = u16::from_le_bytes(buf[cursor..cursor + 2].try_into().unwrap()) as usize;
        cursor += 2;
        if cursor + key_len + 8 > buf.len() {
            return Err(anyhow!("corrupt index (key body)"));
        }
        let key = buf[cursor..cursor + key_len].to_vec();
        cursor += key_len;
        let offset = u64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
        cursor += 8;
        entries.push(BlockIndexEntry { first_key: key, offset });
    }
    Ok(entries)
}

pub struct SstMetadata {
    index: Vec<BlockIndexEntry>,
    bloom: BloomFilter,
}

impl SstMetadata {
    fn load(file: &mut File) -> Result<Self> {
        let file_len = file.metadata()?.len();
        // With aligned writes, the file is always a multiple of 4096.
        // The footer is in the last 4096-byte block, at the beginning.
        // Minimum size is 4096 (just footer).
        if file_len < 4096 {
             // If for some reason it's smaller (legacy?), try standard footer load
             if file_len < FOOTER_SIZE as u64 {
                 return Err(anyhow!("sst file too small"));
             }
             file.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
             let mut footer_buf = [0u8; FOOTER_SIZE];
             file.read_exact(&mut footer_buf)?;
             let footer = Footer::from_bytes(&footer_buf)?;
             return Self::load_from_footer(file, footer, file_len);
        }

        file.seek(SeekFrom::End(-4096))?;
        let mut block_buf = [0u8; 4096];
        file.read_exact(&mut block_buf)?;
        
        // Try to parse footer from the beginning of the block
        let footer = match Footer::from_bytes(&block_buf[0..FOOTER_SIZE]) {
            Ok(f) => f,
            Err(_) => {
                // Fallback: check end of file (legacy format without padding?)
                // Though SstFlusher now always pads.
                return Err(anyhow!("invalid footer in last block"));
            }
        };

        Self::load_from_footer(file, footer, file_len)
    }

    fn load_from_footer(file: &mut File, footer: Footer, file_len: u64) -> Result<Self> {
        if footer.index_offset + footer.index_len as u64 > file_len {
            return Err(anyhow!("index outside file"));
        }
        if footer.bloom_offset + footer.bloom_len as u64 > file_len {
            return Err(anyhow!("bloom outside file"));
        }
        file.seek(SeekFrom::Start(footer.index_offset))?;
        let mut index_buf = vec![0u8; footer.index_len as usize];
        file.read_exact(&mut index_buf)?;
        let index = decode_index(&index_buf)?;
        file.seek(SeekFrom::Start(footer.bloom_offset))?;
        let mut bloom_buf = vec![0u8; footer.bloom_len as usize];
        file.read_exact(&mut bloom_buf)?;
        let bloom = BloomFilter::from_bytes(&bloom_buf)?;
        Ok(Self { index, bloom })
    }
}
pub struct RangeIterator {
    heap: BinaryHeap<Reverse<ItemCursor>>,
    spec: RangeSpec,
}

impl RangeIterator {
    pub fn from_iters(
        mut iters: Vec<PipelinedIterator>,
        spec: RangeSpec,
    ) -> Result<Option<Self>> {
        let mut heap = BinaryHeap::new();
        for iter in iters.drain(..) {
            if let Some(cursor) = ItemCursor::from_iter(iter, &spec)? {
                heap.push(Reverse(cursor));
            }
        }
        if heap.is_empty() {
            return Ok(None);
        }
        Ok(Some(Self { heap, spec }))
    }
}

impl Iterator for RangeIterator {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        let Reverse(mut cursor) = self.heap.pop()?; // empty -> None
        let item = (cursor.key.clone(), cursor.value.clone());
        match cursor.advance(&self.spec) {
            Ok(true) => self.heap.push(Reverse(cursor)),
            Ok(false) => {}
            Err(err) => return Some(Err(err)),
        }
        Some(Ok(item))
    }
}

struct ItemCursor {
    key: Vec<u8>,
    value: Vec<u8>,
    iter: PipelinedIterator,
}

impl PartialEq for ItemCursor {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for ItemCursor {}

impl PartialOrd for ItemCursor {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ItemCursor {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

impl ItemCursor {
    fn from_iter(mut iter: PipelinedIterator, spec: &RangeSpec) -> Result<Option<Self>> {
        match fetch_next(&mut iter, spec)? {
            Some((key, value)) => Ok(Some(Self { key, value, iter })),
            None => Ok(None),
        }
    }

    fn advance(&mut self, spec: &RangeSpec) -> Result<bool> {
        if let Some((key, value)) = fetch_next(&mut self.iter, spec)? {
            self.key = key;
            self.value = value;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

fn fetch_next(
    iter: &mut PipelinedIterator,
    spec: &RangeSpec,
) -> Result<Option<(Vec<u8>, Vec<u8>)>> {
    for item in iter.by_ref() {
        let (key, value) = item?;
        if spec.past_end(&key) {
            return Ok(None);
        }
        if spec.before_start(&key) {
            continue;
        }
        return Ok(Some((key, value)));
    }
    Ok(None)
}
