use std::{
    cell::UnsafeCell,
    os::fd::AsRawFd,
    sync::{
        atomic::{AtomicU64, AtomicUsize, AtomicBool, Ordering},
        Arc, Mutex, OnceLock,
    },
    time::Duration,
};

use anyhow::{anyhow, Result};
use io_uring::{cqueue, squeue, IoUring};

// Constants for io_uring_register opcodes
// Reference: include/uapi/linux/io_uring.h
const IORING_REGISTER_BUFFERS2: libc::c_uint = 15;        // kernel 5.13+, sparse buffer registration
const IORING_REGISTER_BUFFERS_UPDATE: libc::c_uint = 16;  // kernel 5.13+, update registered buffers
const IORING_REGISTER_RING_FDS: libc::c_uint = 20;        // kernel 5.18+, register ring fd

// IORING_SETUP_NO_SQARRAY: Don't allocate the SQ array.
#[allow(dead_code)]
const IORING_SETUP_NO_SQARRAY: u32 = 1 << 16; // 0x10000

fn io_debug_enabled() -> bool {
    std::env::var("MEME_DEBUG").as_deref() == Ok("1")
}

fn wait_timeout() -> Duration {
    static TIMEOUT: OnceLock<Duration> = OnceLock::new();
    *TIMEOUT.get_or_init(|| {
        std::env::var("MEME_URING_WAIT_TIMEOUT_MS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or_else(|| Duration::from_secs(30))
    })
}

/// A thin wrapper for IoUring with an internal mutex to make access Sync.
struct UnsafeSyncCell<T>(UnsafeCell<T>);

// SAFETY: We use SINGLE_ISSUER mode and ensure single-threaded access per ring
unsafe impl<T> Sync for UnsafeSyncCell<T> {}
unsafe impl<T> Send for UnsafeSyncCell<T> {}

impl<T> UnsafeSyncCell<T> {
    fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    #[inline]
    #[allow(clippy::mut_from_ref)]
    unsafe fn get_mut(&self) -> &mut T {
        unsafe { &mut *self.0.get() }
    }
}

pub enum Lane {
    Foreground,
    Background, // flush
    Compaction,
    Wal,
}

pub struct IoScheduler {
    fg: Arc<IoRingHandle>,
    bg_flush: Arc<IoRingHandle>,
    bg_compaction: Arc<IoRingHandle>,
    wal: Arc<IoRingHandle>,
}

impl IoScheduler {
    pub fn new(_force_single_issuer: bool) -> Result<Self> {
        // Ring configuration for optimal io_uring performance:
        //
        // IOPOLL: Kernel polls for completion instead of using interrupts.
        //         Only works with O_DIRECT and read/write operations.
        //         Does NOT work with fsync, so rings that need fsync cannot use IOPOLL.
        //
        // SQPOLL: Kernel thread polls submission queue, reducing syscall overhead.
        //         Works with all operations but adds CPU overhead and memory usage.
        //         Each SQPOLL ring consumes significant memlock quota (~1-2MB).
        //
        // Configuration (optimized for 8MB memlock limit):
        // - Foreground: SQPOLL + IOPOLL (lowest latency for point queries, most critical)
        // - Background Flush: Neither (linked write+fsync needs precise control)
        // - Compaction: IOPOLL only (high throughput reads, no SQPOLL to save memlock)
        // - WAL: IOPOLL only (O_DSYNC handles persistence, no SQPOLL to save memlock)
        
        // Foreground Ring: Point lookups (BlockCache) & Scans.
        // Uses SQPOLL + IOPOLL for lowest latency on reads.
        // This is the most latency-sensitive path, so we prioritize it for SQPOLL.
        let fg = Arc::new(IoRingHandle::new(RingConfig {
            entries: 256,
            cq_entries: 512,
            sqpoll: true,
            iopoll: true,
            single_issuer: true,
        })?);

        // Background Flush Ring: used by main thread only.
        // Uses SQPOLL for reduced syscall overhead.
        // No IOPOLL because:
        // 1. SST flush uses fsync for efficient batch synchronization
        // 2. rename operation is not compatible with IOPOLL
        // fsync is more efficient than O_DSYNC for large sequential writes.
        let bg_flush = Arc::new(IoRingHandle::new(RingConfig {
            entries: 512,
            cq_entries: 1024,
            sqpoll: true,
            iopoll: false,
            single_issuer: true,
        })?);

        // Compaction Ring: dedicated to compaction thread.
        // Uses SQPOLL for reduced syscall overhead.
        // No IOPOLL because compaction writes output SST files which require:
        // 1. fsync for data durability
        // 2. rename for atomic file replacement
        // Both operations are incompatible with IOPOLL.
        // Note: SQPOLL works with all operations including fsync/rename.
        let bg_compaction = Arc::new(IoRingHandle::new(RingConfig {
            entries: 512,
            cq_entries: 1024,
            sqpoll: true,
            iopoll: false,
            single_issuer: true,
        })?);

        // WAL ring: Writes with O_DIRECT + O_DSYNC (no explicit fsync needed).
        // Uses SQPOLL + IOPOLL for microsecond-level latency.
        // O_DSYNC ensures data is persisted on each write without needing fsync.
        let wal = Arc::new(IoRingHandle::new(RingConfig {
            entries: 64,
            cq_entries: 128,
            sqpoll: true,
            iopoll: true,
            single_issuer: true,
        })?);
        
        Ok(Self {
            fg,
            bg_flush,
            bg_compaction,
            wal,
        })
    }

    pub fn ring(&self, lane: Lane) -> Arc<IoRingHandle> {
        match lane {
            Lane::Foreground => self.fg.clone(),
            Lane::Background => self.bg_flush.clone(),
            Lane::Compaction => self.bg_compaction.clone(),
            Lane::Wal => self.wal.clone(),
        }
    }
}

pub struct RingConfig {
    pub entries: u32,
    pub cq_entries: u32,
    pub sqpoll: bool,
    pub iopoll: bool,
    /// If true, enable SINGLE_ISSUER/COOP_TASKRUN/DEFER_TASKRUN optimizations.
    /// Set to false for rings shared by multiple threads.
    pub single_issuer: bool,
}

const MAX_REGISTERED_BUFFERS: usize = 32;

pub struct IoRingHandle {
    ring: Mutex<IoUring>,
    id: AtomicU64,
    next_buffer_slot: AtomicUsize,
    buffers_registered: UnsafeSyncCell<bool>,
    supports_sparse: AtomicBool,
    sqpoll: bool,
    iopoll: bool,
    
    // Shadow copy of registered buffers to support legacy fallback
    shadow_iovecs: UnsafeSyncCell<Vec<libc::iovec>>,
    // Keep the dummy buffer pointer to fill holes
    dummy_buffer: *mut u8,
    
    // Whether DEFER_TASKRUN is enabled (requires special handling for wait)
    #[allow(dead_code)]
    defer_taskrun: AtomicBool,
    
    // Free list for recycled buffer slots
    free_buffer_slots: Mutex<Vec<u16>>,
}

// Safety: IoRingHandle is Send/Sync but enforces single-threaded usage via internal logic/convention
unsafe impl Send for IoRingHandle {}
unsafe impl Sync for IoRingHandle {}

#[allow(dead_code)]
unsafe fn set_no_sqarray_flag(builder: &mut io_uring::Builder) {
    unsafe {
        let builder_ptr = builder as *mut io_uring::Builder as *mut u8;
        let flags_ptr = builder_ptr.add(4) as *mut u32;
        *flags_ptr |= IORING_SETUP_NO_SQARRAY;
    }
}

impl IoRingHandle {
    pub fn new(cfg: RingConfig) -> Result<Self> {
        let mut builder = IoUring::builder();
        builder.setup_submit_all();
        
        let defer_taskrun_enabled = cfg.single_issuer && !cfg.sqpoll;
        
        if cfg.sqpoll {
            builder.setup_sqpoll(200);
        } else if cfg.single_issuer {
            // These are safe only when single-threaded access is guaranteed.
            builder.setup_single_issuer();
            builder.setup_coop_taskrun();
            builder.setup_defer_taskrun();
        }
        
        if cfg.iopoll {
            builder.setup_iopoll();
        }
        builder.setup_cqsize(cfg.cq_entries);
        
        let ring = builder.build(cfg.entries)?;
        ring.submitter().register_files_sparse(64)?;
        let _ = Self::register_ring_fd_raw(ring.as_raw_fd());
        
        // Init dummy buffer and shadow iovecs
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = std::alloc::Layout::from_size_align(page_size, page_size).unwrap();
        // We intentionally leak this memory so it remains valid for the ring's lifetime
        let dummy_buffer = unsafe { std::alloc::alloc(layout) };
        
        let mut iovecs = Vec::with_capacity(MAX_REGISTERED_BUFFERS);
        for _ in 0..MAX_REGISTERED_BUFFERS {
            iovecs.push(libc::iovec {
                iov_base: dummy_buffer as *mut _,
                iov_len: page_size,
            });
        }
        
        Ok(Self {
            ring: Mutex::new(ring),
            id: AtomicU64::new(1),
            next_buffer_slot: AtomicUsize::new(0),
            buffers_registered: UnsafeSyncCell::new(false),
            supports_sparse: AtomicBool::new(false),
            sqpoll: cfg.sqpoll,
            iopoll: cfg.iopoll,
            shadow_iovecs: UnsafeSyncCell::new(iovecs),
            dummy_buffer,
            defer_taskrun: AtomicBool::new(defer_taskrun_enabled),
            free_buffer_slots: Mutex::new(Vec::new()),
        })
    }
    
    fn register_ring_fd_raw(ring_fd: std::os::fd::RawFd) -> Result<i32> {
        #[repr(C)]
        struct IoUringRsrcUpdate {
            offset: u32,
            resv: u32,
            data: u64,
        }
        let mut update = IoUringRsrcUpdate {
            offset: u32::MAX,
            resv: 0,
            data: ring_fd as u64,
        };
        let ret = unsafe {
            libc::syscall(
                libc::SYS_io_uring_register,
                ring_fd,
                IORING_REGISTER_RING_FDS,
                &mut update as *mut IoUringRsrcUpdate,
                1u32,
            )
        };
        if ret < 0 {
            return Err(anyhow!("register_ring_fd failed: {}", std::io::Error::last_os_error()));
        }
        Ok(update.offset as i32)
    }

    #[inline]
    fn ring_lock(&self) -> std::sync::MutexGuard<'_, IoUring> {
        self.ring.lock().unwrap()
    }

    pub fn next_user_data(&self) -> u64 {
        self.id.fetch_add(1, Ordering::Relaxed)
    }

    pub fn update_fixed_file(&self, slot: u32, fd: std::os::fd::RawFd) -> Result<()> {
        let ring = self.ring_lock();
        ring.submitter().register_files_update(slot, &[fd])?;
        Ok(())
    }

    #[allow(dead_code)]
    pub unsafe fn register_buf_ring(
        &self,
        ring_addr: u64,
        ring_entries: u16,
        bgid: u16,
    ) -> Result<()> {
        let ring = self.ring_lock();
        unsafe {
            ring.submitter()
                .register_buf_ring_with_flags(ring_addr, ring_entries, bgid, 0)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn unregister_buf_ring(&self, bgid: u16) -> Result<()> {
        let ring = self.ring_lock();
        ring.submitter().unregister_buf_ring(bgid)?;
        Ok(())
    }

    #[allow(dead_code)]
    pub fn register_buffers(&self, iovecs: &[libc::iovec], _start_index: u16) -> Result<()> {
        let ring = self.ring_lock();
        unsafe {
            ring.submitter().register_buffers(iovecs)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn unregister_buffers(&self) -> Result<()> {
        let ring = self.ring_lock();
        ring.submitter().unregister_buffers()?;
        unsafe {
            *self.buffers_registered.get_mut() = false;
        }
        Ok(())
    }

    pub fn allocate_buffer_slot(&self) -> Option<u16> {
        // First try to get a recycled slot from the free list
        {
            let mut free_slots = self.free_buffer_slots.lock().unwrap();
            if let Some(slot) = free_slots.pop() {
                return Some(slot);
            }
        }
        
        // No recycled slots, allocate a new one
        let slot = self.next_buffer_slot.fetch_add(1, Ordering::Relaxed);
        if slot >= MAX_REGISTERED_BUFFERS {
            self.next_buffer_slot.fetch_sub(1, Ordering::Relaxed);
            return None;
        }
        Some(slot as u16)
    }
    
    /// Return a buffer slot to the free list for reuse
    pub fn free_buffer_slot(&self, slot: u16) {
        let mut free_slots = self.free_buffer_slots.lock().unwrap();
        free_slots.push(slot);
    }

    pub fn register_buffer_at_slot(&self, slot: u16, iovec: &libc::iovec) -> Result<()> {
        let ring = self.ring_lock();
        let shadow = unsafe { self.shadow_iovecs.get_mut() };
        
        // Update shadow with the requested buffer (allow NULL to mean "unregister")
        shadow[slot as usize] = *iovec;

        let buffers_registered = unsafe { *self.buffers_registered.get_mut() };

        if !buffers_registered {
            // First-time registration: prefer sparse BUFFERS2 (allows NULL entries).
            // struct io_uring_rsrc_register for IORING_REGISTER_BUFFERS2
            #[repr(C)]
            struct IoUringRsrcRegister {
                nr: u32,
                flags: u32,
                resv2: u64,
                data: u64,
                tags: u64,
            }
            
            const IORING_RSRC_REGISTER_SPARSE: u32 = 1 << 0;
            
            let reg = IoUringRsrcRegister {
                nr: MAX_REGISTERED_BUFFERS as u32,
                flags: IORING_RSRC_REGISTER_SPARSE,
                resv2: 0,
                data: shadow.as_ptr() as u64,
                tags: 0,
            };
            
            let ret = unsafe {
                libc::syscall(
                    libc::SYS_io_uring_register,
                    ring.as_raw_fd(),
                    IORING_REGISTER_BUFFERS2,
                    &reg as *const IoUringRsrcRegister,
                    std::mem::size_of::<IoUringRsrcRegister>() as u32,
                )
            };
            
            if ret >= 0 {
                self.supports_sparse.store(true, Ordering::Relaxed);
                unsafe { *self.buffers_registered.get_mut() = true; }
                return Ok(());
            }
            
            // Fallback to legacy BUFFERS with a dense array (replace NULL with dummy)
            // Only register the slots that are actually used (up to next_buffer_slot)
            let used_slots = self.next_buffer_slot.load(Ordering::Relaxed).max(1);
            let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
            let mut dense: Vec<libc::iovec> = shadow[..used_slots].to_vec();
            for io in dense.iter_mut() {
                if io.iov_base.is_null() {
                    io.iov_base = self.dummy_buffer as *mut _;
                    io.iov_len = page_size;
                }
            }
            unsafe {
                ring.submitter().register_buffers(&dense)?;
            }
            self.supports_sparse.store(false, Ordering::Relaxed);

            unsafe { *self.buffers_registered.get_mut() = true; }
            return Ok(());
        }

        // Try UPDATE
        #[repr(C)]
        struct IoUringRsrcUpdate2 {
            offset: u32,
            resv: u32,
            data: u64,
            tags: u64,
            nr: u32,
            resv2: u32,
        }

        if self.supports_sparse.load(Ordering::Relaxed) {
            let update = IoUringRsrcUpdate2 {
                offset: slot as u32,
                resv: 0,
                data: iovec as *const libc::iovec as u64,
                tags: 0,
                nr: 1,
                resv2: 0,
            };

            let ret = unsafe {
                libc::syscall(
                    libc::SYS_io_uring_register,
                    ring.as_raw_fd(),
                    IORING_REGISTER_BUFFERS_UPDATE,
                    &update as *const IoUringRsrcUpdate2,
                    1u32,
                )
            };

            if ret >= 0 {
                return Ok(());
            }
            // If UPDATE fails (older kernel), fall through to dense re-register.
        }

        // Fallback: re-register table using dense array (NULL -> dummy)
        // Only register up to the highest used slot + 1
        let used_slots = (slot as usize + 1).max(self.next_buffer_slot.load(Ordering::Relaxed));
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let mut dense: Vec<libc::iovec> = shadow[..used_slots].to_vec();
        for io in dense.iter_mut() {
            if io.iov_base.is_null() {
                io.iov_base = self.dummy_buffer as *mut _;
                io.iov_len = page_size;
            }
        }
        ring.submitter().unregister_buffers()?;
        unsafe {
            ring.submitter().register_buffers(&dense)?;
        }
        self.supports_sparse.store(false, Ordering::Relaxed);

        Ok(())
    }

    pub fn unregister_buffer_at_slot(&self, slot: u16) -> Result<()> {
        let null_iovec = libc::iovec {
            iov_base: std::ptr::null_mut(),
            iov_len: 0,
        };
        self.register_buffer_at_slot(slot, &null_iovec)?;
        // Return the slot to the free list for reuse
        self.free_buffer_slot(slot);
        Ok(())
    }

    pub fn run_batch_allow<F>(
        &self,
        entries: Vec<squeue::Entry>,
        allow_error: F,
    ) -> Result<Vec<Completion>>
    where
        F: FnMut(&cqueue::Entry) -> bool,
    {
        let expected = entries.len();
        self.run_batch_allow_with_expected(entries, expected, allow_error)
    }

    pub fn run_batch_allow_with_expected<F>(
        &self,
        entries: Vec<squeue::Entry>,
        expected_cqes: usize,
        mut allow_error: F,
    ) -> Result<Vec<Completion>>
    where
        F: FnMut(&cqueue::Entry) -> bool,
    {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        let mut ring = self.ring_lock();
        {
            let mut sq = ring.submission();
            for entry in entries.iter() {
                unsafe {
                    sq.push(entry)
                        .map_err(|_| anyhow!("submission queue is full"))?;
                }
            }
        }
        if expected_cqes == 0 {
            ring.submit()?;
            return Ok(Vec::new());
        }
        
        // IOPOLL mode requires special handling:
        // - Cannot use blocking wait (submit_and_wait blocks forever)
        // - Must actively poll for completions using IORING_ENTER_GETEVENTS
        if self.iopoll {
            ring.submit()?;
            let mut completions = Vec::with_capacity(expected_cqes);
            let start = std::time::Instant::now();
            
            while completions.len() < expected_cqes {
                // In IOPOLL mode, we need to call io_uring_enter with GETEVENTS
                // to poll for completions. submit_and_wait(1) does this.
                match ring.submitter().submit_and_wait(1) {
                    Ok(_) => {}
                    Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                    Err(e) if e.raw_os_error() == Some(libc::EAGAIN) => {
                        std::thread::yield_now();
                        continue;
                    }
                    Err(e) => return Err(e.into()),
                }
                
                // Drain all available completions
                let cq = ring.completion();
                for cqe in cq {
                    if cqe.result() < 0 && !allow_error(&cqe) {
                        return Err(anyhow!(
                            "io_uring completion error: {} (ud={})",
                            cqe.result(),
                            cqe.user_data()
                        ));
                    }
                    completions.push(Completion::from_entry(&cqe));
                }
                
                // Timeout protection
                if start.elapsed() > std::time::Duration::from_secs(5) {
                    return Err(anyhow!(
                        "IOPOLL timeout: expected {} completions, got {}",
                        expected_cqes,
                        completions.len()
                    ));
                }
            }
            return Ok(completions);
        }
        
        // Non-IOPOLL mode: use standard blocking wait
        ring.submit_and_wait(expected_cqes)?;
        let mut completions = Vec::with_capacity(expected_cqes);
        let mut seen = 0;
        while seen < expected_cqes {
            let cq = ring.completion();
            for cqe in cq {
                if cqe.result() < 0 && !allow_error(&cqe) {
                    return Err(anyhow!(
                        "io_uring completion error: {} (ud={})",
                        cqe.result(),
                        cqe.user_data()
                    ));
                }
                completions.push(Completion::from_entry(&cqe));
                seen += 1;
            }
        }
        Ok(completions)
    }

    pub fn run_batch(&self, entries: Vec<squeue::Entry>) -> Result<Vec<Completion>> {
        self.run_batch_allow(entries, |_| false)
    }

    pub fn run_batch_with_expected(
        &self,
        entries: Vec<squeue::Entry>,
        expected_cqes: usize,
    ) -> Result<Vec<Completion>> {
        self.run_batch_allow_with_expected(entries, expected_cqes, |_| false)
    }

    #[allow(dead_code)]
    pub fn run_and_forget(&self, entries: Vec<squeue::Entry>) -> Result<()> {
        self.run_batch(entries).map(|_| ())
    }

    pub fn run_and_forget_with_expected(
        &self,
        entries: Vec<squeue::Entry>,
        expected_cqes: usize,
    ) -> Result<()> {
        self.run_batch_with_expected(entries, expected_cqes).map(|_| ())
    }

    pub fn submit_entries(&self, entries: Vec<squeue::Entry>) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut ring = self.ring_lock();
        {
            let mut sq = ring.submission();
            for entry in entries.iter() {
                unsafe {
                    sq.push(entry)
                        .map_err(|_| anyhow!("submission queue is full"))?;
                }
            }
        }
        ring.submit()?;
        Ok(())
    }

    pub fn wait_for_completions(&self, min_complete: usize) -> Result<Vec<Completion>> {
        if min_complete == 0 {
            return Ok(Vec::new());
        }
        let mut ring = self.ring_lock();
        let debug = io_debug_enabled();
        let start = std::time::Instant::now();
        if debug {
            eprintln!("[uring] wait_for_completions min={} sqpoll={}", min_complete, self.sqpoll);
        }
        let mut completions = Vec::new();
        
        // In SQPOLL mode, we need special handling because:
        // 1. The SQ thread may be idle (sleeping after sq_thread_idle timeout)
        // 2. submit_and_wait() alone won't wake the SQ thread
        // 3. We need to use IORING_SQ_NEED_WAKEUP flag to check if wakeup is needed
        
        loop {
            let need = if completions.len() >= min_complete {
                0
            } else {
                min_complete - completions.len()
            };
            
            if need > 0 {
                if self.sqpoll {
                    // For SQPOLL mode, we need to use submit_and_wait with the number of
                    // completions we want. This will:
                    // 1. Set IORING_ENTER_GETEVENTS flag (because want > 0)
                    // 2. Set IORING_ENTER_SQ_WAKEUP flag if the SQ thread needs waking
                    // 3. Call io_uring_enter to wait for completions
                    //
                    // Note: submit_and_wait(0) would skip the syscall if SQ thread doesn't
                    // need wakeup, which is why we must pass `need` here.
                    match ring.submitter().submit_and_wait(need) {
                        Ok(_) => {}
                        Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {}
                        Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                        Err(e) if e.raw_os_error() == Some(libc::ETIME) => {
                            // Timeout - this is OK, we'll check completions and retry
                        }
                        Err(e) => return Err(e.into()),
                    }
                    
                    // Drain completions
                    let cq = ring.completion();
                    for cqe in cq {
                        if cqe.result() < 0 {
                            return Err(anyhow!(
                                "io_uring completion error: {} (ud={})",
                                cqe.result(),
                                cqe.user_data()
                            ));
                        }
                        completions.push(Completion::from_entry(&cqe));
                    }
                } else {
                    // Non-SQPOLL mode: use submit_and_wait which blocks efficiently
                    match ring.submitter().submit_and_wait(need) {
                        Ok(_) => {}
                        Err(e) if e.raw_os_error() == Some(libc::EINTR) => continue,
                        Err(e) => return Err(e.into()),
                    }
                    
                    // Drain completions
                    let cq = ring.completion();
                    for cqe in cq {
                        if cqe.result() < 0 {
                            return Err(anyhow!(
                                "io_uring completion error: {} (ud={})",
                                cqe.result(),
                                cqe.user_data()
                            ));
                        }
                        completions.push(Completion::from_entry(&cqe));
                    }
                }
            }

            if completions.len() >= min_complete {
                break;
            }
            
            if start.elapsed() > wait_timeout() {
                return Err(anyhow!(
                    "wait_for_completions timeout: requested {}, got {} (sqpoll={})",
                    min_complete,
                    completions.len(),
                    self.sqpoll
                ));
            }
        }
        
        if debug {
            eprintln!(
                "[uring] got completions count={} (requested {})",
                completions.len(),
                min_complete
            );
        }
        Ok(completions)
    }

    #[allow(dead_code)]
    pub fn poll_completions(&self) -> Vec<Completion> {
        let mut ring = self.ring_lock();
        let mut completions = Vec::new();
        let cq = ring.completion();
        for cqe in cq {
            completions.push(Completion::from_entry(&cqe));
        }
        completions
    }
    
}

impl Drop for IoRingHandle {
    fn drop(&mut self) {
        // Unregister buffers if registered
        let buffers_registered = unsafe { *self.buffers_registered.get_mut() };
        if buffers_registered {
            let ring = self.ring.lock().unwrap();
            let _ = ring.submitter().unregister_buffers();
        }
        
        // Free the dummy buffer
        let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) as usize };
        let layout = std::alloc::Layout::from_size_align(page_size, page_size).unwrap();
        unsafe {
            std::alloc::dealloc(self.dummy_buffer, layout);
        }
    }
}

pub struct Completion {
    pub result: i32,
    #[allow(dead_code)]
    pub buffer_id: Option<u16>,
    pub user_data: u64,
}

impl Completion {
    fn from_entry(entry: &cqueue::Entry) -> Self {
        let flags = entry.flags();
        let buffer_id = cqueue::buffer_select(flags);
        Self {
            result: entry.result(),
            buffer_id,
            user_data: entry.user_data(),
        }
    }
}
