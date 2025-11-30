use std::{
    alloc,
    fs::File,
    os::fd::AsRawFd,
    path::Path,
    ptr::NonNull,
    sync::OnceLock,
    time::Duration,
};

use anyhow::{anyhow, Result};
use io_uring::{squeue, types};

use crate::{
    memtable::WalEntry,
    scheduler::IoRingHandle,
    util::{open_o_direct_dsync, open_o_dsync},
};

/// Default WAL buffer size (64KB)
/// Reduced from 256KB to fit within memlock limits when using multiple io_uring rings.
const WAL_BUFFER_SIZE: usize = 64 * 1024;

/// Batch size threshold to trigger auto-flush (64KB)
const BATCH_FLUSH_THRESHOLD: usize = 64 * 1024;

/// Alignment required for O_DIRECT + IOPOLL
const WAL_ALIGN: usize = 4096;

pub struct WalWriter {
    ring: std::sync::Arc<IoRingHandle>,
    file: File,
    fixed_slot: u32,
    offset: u64,
    use_iopoll: bool,
    
    // Double Buffering
    buffers: [NonNull<u8>; 2],
    #[allow(dead_code)]
    buf_indices: [u16; 2],  // Reserved for future WriteFixed support
    current_buf: usize, // 0 or 1
    
    // Group Commit / Pending
    pending: Vec<WalEntry>,
    pending_size: usize,
    
    buffer_size: usize,
}

// Safety: WalWriter owns the buffers and ensures proper synchronization
unsafe impl Send for WalWriter {}

#[inline]
fn align_up(val: usize, align: usize) -> usize {
    (val + align - 1) & !(align - 1)
}

fn wal_debug_enabled() -> bool {
    static FLAG: OnceLock<bool> = OnceLock::new();
    *FLAG.get_or_init(|| std::env::var("MEME_DEBUG").as_deref() == Ok("1"))
}

macro_rules! wal_dbg {
    ($($arg:tt)*) => {
        if wal_debug_enabled() {
            eprintln!($($arg)*);
        }
    };
}

impl WalWriter {
    pub fn new(
        path: &Path,
        ring: std::sync::Arc<IoRingHandle>,
        fixed_slot: u32,
        use_iopoll: bool,
    ) -> Result<Self> {
        let file = if use_iopoll {
            open_o_direct_dsync(path, false)?
        } else {
            open_o_dsync(path, false)?
        };
        let offset = file.metadata()?.len();
        ring.update_fixed_file(fixed_slot, file.as_raw_fd())?;

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = alloc::Layout::from_size_align(WAL_BUFFER_SIZE, page_size)
            .map_err(|e| anyhow!("invalid layout: {}", e))?;

        let mut buffers = [NonNull::dangling(); 2];
        let mut buf_indices = [0u16; 2];

        for i in 0..2 {
            let ptr = unsafe { alloc::alloc(layout) };
            if ptr.is_null() {
                return Err(anyhow!("failed to allocate WAL buffer"));
            }
            buffers[i] = unsafe { NonNull::new_unchecked(ptr) };

            // Register buffer for WriteFixed (required for IOPOLL)
            let iovec = libc::iovec {
                iov_base: ptr as *mut _,
                iov_len: WAL_BUFFER_SIZE,
            };
            let slot = ring
                .allocate_buffer_slot()
                .ok_or_else(|| anyhow!("failed to allocate buffer slot"))?;
            ring.register_buffer_at_slot(slot, &iovec)?;
            buf_indices[i] = slot;
        }

        Ok(Self {
            ring,
            file,
            fixed_slot,
            offset,
            use_iopoll,
            buffers,
            buf_indices,
            current_buf: 0,
            pending: Vec::with_capacity(100),
            pending_size: 0,
            buffer_size: WAL_BUFFER_SIZE,
        })
    }

    /// Append an entry to the pending batch.
    /// If the batch gets too large, it may trigger a flush.
    pub fn append(&mut self, entry: WalEntry) -> Result<()> {
        let encoded_len = entry.encoded_len();
        if self.pending_size + encoded_len > self.buffer_size {
            // Buffer full, force flush
            self.flush()?;
        }
        
        self.pending_size += encoded_len;
        self.pending.push(entry);
        
        if self.pending_size >= BATCH_FLUSH_THRESHOLD {
            self.flush()?;
        }
        
        Ok(())
    }

    /// Encode pending entries into the current registered buffer.
    /// Returns (padded_len, payload_len).
    fn encode_pending_inplace(&mut self) -> Result<(usize, usize)> {
        let mut write_offset = 4usize; // reserve for frame_len
        let payload_cap = self.buffer_size.saturating_sub(4);
        for entry in &self.pending {
            let encoded = entry.encode();
            if write_offset + encoded.len() > payload_cap {
                return Err(anyhow!(
                    "WAL batch too large: {} bytes exceeds buffer size {}",
                    write_offset + encoded.len(),
                    self.buffer_size
                ));
            }
            unsafe {
                std::ptr::copy_nonoverlapping(
                    encoded.as_ptr(),
                    self.buffers[self.current_buf].as_ptr().add(write_offset),
                    encoded.len(),
                );
            }
            write_offset += encoded.len();
        }
        let payload_len = write_offset - 4;
        let total_len = 4 + payload_len;
        let padded_len = align_up(total_len, WAL_ALIGN);
        if padded_len > self.buffer_size {
            return Err(anyhow!(
                "WAL batch too large after alignment: {} > {}",
                padded_len,
                self.buffer_size
            ));
        }
        // Frame header: payload length
        let header = (payload_len as u32).to_le_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(
                header.as_ptr(),
                self.buffers[self.current_buf].as_ptr(),
                header.len(),
            );
        }
        // Zero padding
        if padded_len > total_len {
            unsafe {
                std::ptr::write_bytes(
                    self.buffers[self.current_buf].as_ptr().add(total_len),
                    0,
                    padded_len - total_len,
                );
            }
        }
        Ok((padded_len, payload_len))
    }

    /// Flush pending entries to disk.
    /// This uses double buffering to pipeline writes if possible.
    pub fn flush(&mut self) -> Result<()> {
        if self.pending.is_empty() {
            return Ok(());
        }

        // Fallback synchronous path for non-IOPOLL to avoid io_uring hangs in tests.
        // Uses the same frame format as io_uring path for WAL recovery compatibility.
        if !self.use_iopoll {
            // Encode with frame header (4-byte payload length)
            let mut payload = Vec::new();
            for entry in &self.pending {
                payload.extend_from_slice(&entry.encode());
            }
            let payload_len = payload.len();
            let total_len = 4 + payload_len;
            let padded_len = align_up(total_len, WAL_ALIGN);
            
            let mut buf = Vec::with_capacity(padded_len);
            buf.extend_from_slice(&(payload_len as u32).to_le_bytes());
            buf.extend_from_slice(&payload);
            buf.resize(padded_len, 0); // Zero padding
            
            use std::io::{Seek, SeekFrom, Write};
            self.file.seek(SeekFrom::Start(self.offset))?;
            self.file.write_all(&buf)?;
            self.file.sync_data()?;
            self.offset += padded_len as u64;
            self.pending.clear();
            self.pending_size = 0;
            self.current_buf = 0;
            return Ok(());
        }

        // Encode pending entries into registered buffer with frame header + padding
        let (padded_len, payload_len) = self.encode_pending_inplace()?;

        // Submit write
        let _ud = self.submit_write(padded_len)?;
        wal_dbg!(
            "[wal] flush offset={} len={} padded={}",
            self.offset,
            payload_len,
            padded_len
        );
        // Advance offset and clear pending (write+fsync completed)
        self.offset += padded_len as u64;
        self.pending.clear();
        self.pending_size = 0;
        
        // Switch to next buffer
        self.current_buf = 1 - self.current_buf;
        
        Ok(())
    }

    // Legacy support for LsmDb
    pub fn append_batch(&mut self, batch: &[WalEntry], _timeout: Duration) -> Result<()> {
        for entry in batch {
            self.append(entry.clone())?;
        }
        self.flush()?;
        Ok(())
    }

    fn submit_write(&mut self, len: usize) -> Result<u64> {
        let fd = types::Fixed(self.fixed_slot);
        let buffer_ptr = self.buffers[self.current_buf].as_ptr();
        let buf_idx = self.buf_indices[self.current_buf];
        
        let ud = self.ring.next_user_data();

        // Use WriteFixed with registered buffer (IOPOLL/O_DIRECT friendly)
        // O_DSYNC ensures data is persisted on write completion, no fsync needed.
        // This is compatible with IOPOLL mode.
        let write_entry = io_uring::opcode::WriteFixed::new(
            fd,
            buffer_ptr,
            len as u32,
            buf_idx,
        )
        .offset(self.offset)
        .build()
        .flags(squeue::Flags::FIXED_FILE)
        .user_data(ud);

        // Submit and wait for write completion
        // With O_DSYNC, the write completion means data is persisted
        let completions = self.ring.run_batch(vec![write_entry])?;
        let cqe = completions
            .first()
            .ok_or_else(|| anyhow!("no completion for WAL write"))?;
        if cqe.result < 0 {
            return Err(anyhow!("wal write failed: {}", cqe.result));
        }
        wal_dbg!("[wal] submit_write done ud={} res={}", ud, cqe.result);
        Ok(ud)
    }

    pub fn trim(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.sync_data()?;
        self.offset = 0;
        self.current_buf = 0;
        self.pending.clear();
        self.pending_size = 0;
        Ok(())
    }
}

impl Drop for WalWriter {
    fn drop(&mut self) {
        // Attempt to flush any pending data
        let _ = self.flush();

        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = alloc::Layout::from_size_align(self.buffer_size, page_size).unwrap();
        
        for i in 0..2 {
            let _ = self.ring.unregister_buffer_at_slot(self.buf_indices[i]);
            unsafe {
                alloc::dealloc(self.buffers[i].as_ptr(), layout);
            }
        }
    }
}
