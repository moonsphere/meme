mod block;
mod cache;
mod compaction;
mod lru;
mod manifest;
mod memtable;
mod scheduler;
mod sst;
mod storage;
mod util;
mod wal;

use std::{
    collections::{HashMap, HashSet},
    env,
    ops::RangeBounds,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

use anyhow::Result;

use compaction::{CompactionQueue, Compactor};
use manifest::{Manifest, SstMeta};
use memtable::{replay_wal, MemTable};
use scheduler::{IoScheduler, Lane};
use sst::{FixedBufPool, RangeIterator, RangeSpec, SstFlusher, SstHandle, SstReader, SstReaderConfig, BLOCK_SIZE};
use storage::StoragePaths;
use wal::WalWriter;

pub use sst::{PipelinedIterator, BLOCK_SIZE as SST_BLOCK_SIZE};
pub use wal::WalDurability;

pub fn run() -> Result<()> {
    let (data_root, opts) = parse_args();
    let mut db = LsmDb::open(data_root, LsmOptions {
        force_single_issuer: opts.force_single_issuer,
        ..Default::default()
    })?;

    for (k, v) in [("alpha", "one"), ("beta", "two"), ("gamma", "three")] {
        db.put(k.as_bytes(), v.as_bytes())?;
    }
    db.flush_memtable()?;
    db.compact()?;

    if let Some(val) = db.get(b"beta")? {
        println!("lookup beta -> {}", String::from_utf8_lossy(&val));
    }

    println!(
        "db at {:?} with {} SST(s)",
        db.data_root(),
        db.manifest_len()
    );
    if opts.print_stats {
        println!("cache stats: {:?}", db.cache_stats());
    }
    Ok(())
}

struct RunOptions {
    print_stats: bool,
    force_single_issuer: bool,
}

fn parse_args() -> (PathBuf, RunOptions) {
    let args = env::args().skip(1);
    let mut root: Option<PathBuf> = None;
    let mut opts = RunOptions { print_stats: false, force_single_issuer: false };

    for arg in args {
        match arg.as_str() {
            "--stats" => opts.print_stats = true,
            "--single-issuer" => opts.force_single_issuer = true,
            "--help" | "-h" => {
                println!("Usage: meme [DATA_ROOT] [--stats] [--single-issuer]");
                std::process::exit(0);
            }
            _ if root.is_none() => root = Some(PathBuf::from(arg)),
            _ => {}
        }
    }

    let data_root = root.unwrap_or_else(|| PathBuf::from("meme-data"));
    (data_root, opts)
}

pub struct LsmDb {
    paths: StoragePaths,
    scheduler: IoScheduler,
    manifest: Arc<Mutex<Manifest>>,
    wal: WalWriter,
    memtable: MemTable,
    flusher: SstFlusher,
    compaction_queue: CompactionQueue,
    flush_limit: usize,
    sst_cache: SstCache,
    block_cache: Option<Arc<crate::cache::BlockCache>>,
    /// Shared buffer pool for SST readers (avoids per-reader allocation)
    shared_buf_pool: Arc<Mutex<FixedBufPool>>,
    /// Cache of registered fixed file slots (sst_id -> fixed_slot)
    fixed_file_slots: HashMap<u64, u32>,
}

const DEFAULT_FLUSH_LIMIT: usize = 32;

pub struct LsmOptions {
    /// Number of 16KB block cache slots.
    /// Each slot requires locked memory (memlock).
    /// Default: 64 slots = 1MB (conservative for 8MB memlock limit)
    /// For production with unlimited memlock, use 4096+ slots.
    pub block_cache_slots: usize,
    pub force_single_issuer: bool,
    /// WAL durability mode.
    /// - Sync: fsync after every write (safest, slowest ~900 ops/s)
    /// - GroupCommit: batch writes and fsync periodically (balanced, ~50K-100K ops/s)
    /// - NoSync: no fsync (fastest, data may be lost on crash)
    pub wal_durability: WalDurability,
}

impl Default for LsmOptions {
    fn default() -> Self {
        Self {
            // 64 slots × 16KB = 1MB block cache
            // Conservative default to fit within 8MB memlock limit
            // (4 SQPOLL rings use ~4-6MB, leaving ~2-4MB for cache + buffers)
            block_cache_slots: 64,
            force_single_issuer: false,
            // Default to Sync for backward compatibility and safety
            wal_durability: WalDurability::Sync,
        }
    }
}

impl LsmOptions {
    /// Create options with group commit for high throughput writes
    pub fn with_group_commit() -> Self {
        Self {
            wal_durability: WalDurability::GroupCommit {
                timeout: Duration::from_millis(1),
                batch_size: 32,
            },
            ..Default::default()
        }
    }
    
    /// Create options with no sync for maximum throughput (unsafe)
    pub fn with_no_sync() -> Self {
        Self {
            wal_durability: WalDurability::NoSync,
            ..Default::default()
        }
    }
}

/// Extract major device number from st_dev (Linux extended device number encoding)
/// Modern Linux kernels use: major = ((dev >> 8) & 0xfff) | ((dev >> 32) & ~0xfff)
#[inline]
fn dev_major(dev: u64) -> u64 {
    ((dev >> 8) & 0xfff) | ((dev >> 32) & !0xfff)
}

/// Extract minor device number from st_dev (Linux extended device number encoding)
/// Modern Linux kernels use: minor = (dev & 0xff) | ((dev >> 12) & ~0xff)
#[inline]
fn dev_minor(dev: u64) -> u64 {
    (dev & 0xff) | ((dev >> 12) & !0xff)
}

/// Check if the given path is on an NVMe device.
/// IOPOLL requires NVMe or other polling-capable block devices.
fn check_nvme_device(path: &Path) -> Result<()> {
    use std::fs;
    use std::os::unix::fs::MetadataExt;
    
    // Get the device number from the path
    let metadata = fs::metadata(path).or_else(|_| {
        // If path doesn't exist yet, check parent directory
        path.parent()
            .ok_or_else(|| anyhow::anyhow!("Invalid path"))
            .and_then(|p| fs::metadata(p).map_err(Into::into))
    })?;
    
    let dev = metadata.dev();
    let major = dev_major(dev);
    
    // NVMe devices have major number 259
    // Also allow loop devices (7) and device-mapper (253) for testing
    // Reference: https://www.kernel.org/doc/Documentation/admin-guide/devices.txt
    const NVME_MAJOR: u64 = 259;
    const LOOP_MAJOR: u64 = 7;
    const DM_MAJOR: u64 = 253;
    const SCSI_MAJOR_START: u64 = 8;  // SCSI disks start at 8
    
    // Check if it's a known polling-capable device or allow for testing
    let is_nvme = major == NVME_MAJOR;
    let is_test_device = major == LOOP_MAJOR || major == DM_MAJOR;
    let is_scsi = major == SCSI_MAJOR_START;
    
    // Also check /sys/block for nvme devices
    let is_nvme_by_sysfs = check_nvme_sysfs(dev);
    
    if is_nvme || is_nvme_by_sysfs {
        return Ok(());
    }
    
    if is_test_device || is_scsi {
        eprintln!("Warning: Device (major={}) may not support IOPOLL. Performance may be degraded.", major);
        return Ok(());
    }
    
    Err(anyhow::anyhow!(
        "IOPOLL requires NVMe device. Current device major number: {}. \
         This storage engine requires NVMe for optimal performance with io_uring IOPOLL mode.",
        major
    ))
}

/// Check if device is NVMe by looking at /sys/block
fn check_nvme_sysfs(dev: u64) -> bool {
    use std::fs;
    
    let major = dev_major(dev);
    let minor = dev_minor(dev);
    
    // Try to find the block device in /sys/block
    if let Ok(entries) = fs::read_dir("/sys/block") {
        for entry in entries.flatten() {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            
            // Check if it's an nvme device
            if name_str.starts_with("nvme") {
                // Read the dev file to check major:minor
                let dev_path = entry.path().join("dev");
                if let Ok(content) = fs::read_to_string(&dev_path) {
                    let parts: Vec<&str> = content.trim().split(':').collect();
                    if parts.len() == 2
                        && let (Ok(m), Ok(n)) = (parts[0].parse::<u64>(), parts[1].parse::<u64>())
                            && m == major && n == minor {
                                return true;
                            }
                }
            }
        }
    }
    false
}

impl LsmDb {
    pub fn open(root: impl AsRef<Path>, options: LsmOptions) -> Result<Self> {
        let paths = StoragePaths::new(root.as_ref().to_path_buf())?;
        
        // Check if we're on an NVMe device (required for IOPOLL)
        check_nvme_device(&paths.root)?;
        
        let scheduler = IoScheduler::new(options.force_single_issuer)?;
        let mut manifest = Manifest::load(&paths)?;

        // Initialize Block Cache (Foreground Ring)
        let block_cache = match crate::cache::BlockCache::new(scheduler.ring(Lane::Foreground), options.block_cache_slots) {
            Ok(cache) => Some(Arc::new(cache)),
            Err(e) => {
                eprintln!("Warning: Failed to initialize block cache: {}. Performance will be degraded.", e);
                None
            }
        };

        let wal_ring = scheduler.ring(Lane::Wal);
        // WalWriter now manages its own buffer slots
        // WAL uses SQPOLL + IOPOLL with O_DIRECT + O_DSYNC (no explicit fsync needed)
        let wal = wal::WalWriter::with_durability(
            paths.wal_path(),
            wal_ring.clone(),
            0,
            true,
            options.wal_durability,
        )?;

        let mut memtable = MemTable::new();
        let buf = util::read_file(paths.wal_path())?;
        let replayed = replay_wal(&buf, &mut memtable);
        memtable.bump_seq_to(replayed);

        // Foreground flush writer (main thread)
        let flusher = SstFlusher::new(scheduler.ring(Lane::Background), 1)?;
        // Compaction writer uses its own ring to avoid contention with foreground flushes
        let compaction_flusher = SstFlusher::new(scheduler.ring(Lane::Compaction), 1)?;
        let compactor = Compactor::new(
            scheduler.ring(Lane::Compaction),
            compaction_flusher,
            3,
        );

        manifest.cleanup_missing_files(&paths)?;
        let manifest_arc = Arc::new(Mutex::new(manifest));
        let compaction_queue =
            CompactionQueue::start(compactor, manifest_arc.clone(), paths.clone());
        // Always use O_DIRECT for IOPOLL compatibility
        let mut sst_cache = SstCache::new(true);
        {
            let manifest_guard = manifest_arc.lock().unwrap();
            sst_cache.retain(manifest_guard.entries());
        }
        
        // Create shared buffer pool for SST readers
        // This avoids per-reader buffer allocation and registration overhead
        let shared_buf_pool = match FixedBufPool::new(scheduler.ring(Lane::Foreground), BLOCK_SIZE, 8) {
            Ok(pool) => Arc::new(Mutex::new(pool)),
            Err(e) => {
                eprintln!("Warning: Failed to create shared buffer pool: {}. Using per-reader pools.", e);
                // Create a minimal pool as fallback
                Arc::new(Mutex::new(FixedBufPool::new(scheduler.ring(Lane::Foreground), BLOCK_SIZE, 1)?))
            }
        };

        Ok(Self {
            paths,
            scheduler,
            manifest: manifest_arc,
            wal,
            memtable,
            flusher,
            compaction_queue,
            flush_limit: DEFAULT_FLUSH_LIMIT,
            sst_cache,
            block_cache,
            shared_buf_pool,
            fixed_file_slots: HashMap::new(),
        })
    }

    pub fn set_flush_limit(&mut self, limit: usize) {
        self.flush_limit = limit.max(1);
    }
    
    /// Set WAL durability mode at runtime
    pub fn set_wal_durability(&mut self, durability: WalDurability) {
        self.wal.set_durability(durability);
    }
    
    /// Get current WAL durability mode
    pub fn wal_durability(&self) -> WalDurability {
        self.wal.durability()
    }

    /// Put a key-value pair.
    /// Durability depends on the configured WalDurability mode:
    /// - Sync: waits for fsync after each write (~900 ops/s)
    /// - GroupCommit: batches writes and syncs periodically (~50K-100K ops/s)
    /// - NoSync: no fsync, data may be lost on crash
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = self.memtable.put(key, value);
        self.wal.append(entry)?;
        if self.memtable.len() >= self.flush_limit {
            self.flush_memtable()?;
        }
        Ok(())
    }
    
    /// Force sync any pending WAL entries to disk.
    /// Call this after a batch of puts to ensure durability.
    pub fn sync(&mut self) -> Result<()> {
        self.wal.sync()
    }
    
    /// Check if there are pending WAL entries that need to be synced
    pub fn needs_sync(&self) -> bool {
        self.wal.needs_sync()
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(val) = self.memtable.get(key) {
            return Ok(Some(val));
        }
        let mut candidates = {
            let manifest = self.manifest.lock().unwrap();
            manifest.entries_by_priority()
        };
        candidates.sort_by(|a, b| {
            a.level
                .cmp(&b.level)
                .then_with(|| b.id.cmp(&a.id))
        });
        for meta in candidates {
            if key < meta.key_min.as_slice() || key > meta.key_max.as_slice() {
                continue;
            }
            let ring = self.scheduler.ring(Lane::Foreground);
            let handle = self.sst_cache.get_handle(&meta, &self.paths)?;
            
            // Get or allocate fixed_file slot for this SST
            // This avoids repeated update_fixed_file syscalls for the same SST
            let fixed_slot = self.get_or_register_fixed_slot(&meta, &handle, &ring)?;
            
            // IOPOLL mode requires BlockCache for registered buffers
            let config = SstReaderConfig {
                iopoll_mode: self.block_cache.is_some(),
            };
            
            // Use shared buffer pool to avoid per-reader allocation
            let mut reader = SstReader::with_shared_pool(
                handle,
                ring,
                fixed_slot,
                self.shared_buf_pool.clone(),
                meta.id,
                self.block_cache.clone(),
                config,
            )?;
            let before = reader.bytes_read();
            let result = reader.get(key)?;
            if let Some(val) = result {
                let delta = reader.bytes_read() - before;
                self.sst_cache.record_bytes(delta);
                return Ok(Some(val));
            }
            let delta = reader.bytes_read() - before;
            self.sst_cache.record_bytes(delta);
        }
        Ok(None)
    }
    
    /// Get or register a fixed file slot for an SST file
    /// This caches the registration to avoid repeated syscalls
    fn get_or_register_fixed_slot(
        &mut self,
        meta: &SstMeta,
        handle: &Arc<SstHandle>,
        ring: &Arc<scheduler::IoRingHandle>,
    ) -> Result<u32> {
        use std::os::fd::AsRawFd;
        
        if let Some(&slot) = self.fixed_file_slots.get(&meta.id) {
            return Ok(slot);
        }
        
        // Allocate a new slot (use SST ID modulo to limit slot usage)
        // We use slots 2-9 for SST files (0-1 reserved for other uses)
        let slot = (meta.id as u32 % 8) + 2;
        
        // Register the file descriptor
        ring.update_fixed_file(slot, handle.file.as_raw_fd())?;
        
        // Cache the registration
        self.fixed_file_slots.insert(meta.id, slot);
        
        Ok(slot)
    }

    pub fn flush_memtable(&mut self) -> Result<()> {
        let sorted = self.memtable.drain_sorted();
        if sorted.is_empty() {
            return Ok(());
        }
        let mut entry = {
            let mut manifest = self.manifest.lock().unwrap();
            manifest.allocate_name(&sorted)
        };
        let sst_path = self.paths.sst_dir().join(&entry.file_name);
        let bounds = match self.flusher.flush(&sst_path, &sorted)? {
            Some(b) => b,
            None => return Ok(()),
        };
        entry.key_min = bounds.min;
        entry.key_max = bounds.max;
        let mut manifest = self.manifest.lock().unwrap();
        manifest.add_entry(entry);
        manifest.sync()?;
        self.sst_cache.retain(manifest.entries());
        self.wal.trim()?;
        if manifest.entries().len() > 4 {
            self.compaction_queue.schedule();
        }
        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        self.compaction_queue.schedule();
        Ok(())
    }

    pub fn compact_blocking(&mut self) -> Result<()> {
        self.compaction_queue.schedule_blocking()?;
        let manifest = self.manifest.lock().unwrap();
        self.sst_cache.retain(manifest.entries());
        Ok(())
    }

    pub fn range_iter<R>(
        &mut self,
        range: R,
        window: usize,
    ) -> Result<Option<RangeIterator>>
    where
        R: RangeBounds<Vec<u8>>,
    {
        let spec = RangeSpec::new(range);
        let entries = {
            let manifest = self.manifest.lock().unwrap();
            manifest.entries().to_vec()
        };
        let ring = self.scheduler.ring(Lane::Foreground);
        let mut iters = Vec::new();
        for meta in entries {
            if !range_overlaps(&meta, &spec) {
                continue;
            }
            let handle = self.sst_cache.get_handle(&meta, &self.paths)?;
            
            // Get or allocate fixed_file slot for this SST
            let fixed_slot = self.get_or_register_fixed_slot(&meta, &handle, &ring)?;
            
            // IOPOLL mode requires BlockCache for registered buffers
            let config = SstReaderConfig {
                iopoll_mode: self.block_cache.is_some(),
            };
            
            // Use shared buffer pool to avoid per-reader allocation
            let reader = SstReader::with_shared_pool(
                handle,
                ring.clone(),
                fixed_slot,
                self.shared_buf_pool.clone(),
                meta.id,
                self.block_cache.clone(),
                config,
            )?;
            iters.push(reader.into_pipelined_iter(window)?);
        }
        RangeIterator::from_iters(iters, spec)
    }

    pub fn data_root(&self) -> &Path {
        &self.paths.root
    }

    pub fn manifest_len(&self) -> usize {
        self.manifest.lock().unwrap().entries().len()
    }

    pub fn cache_stats(&self) -> CacheStats {
        self.sst_cache.stats()
    }
}

impl std::ops::Drop for LsmDb {
    fn drop(&mut self) {
        // CompactionQueue drop handles worker shutdown automatically.
    }
}

struct SstCache {
    map: HashMap<u64, Arc<SstHandle>>,
    stats: SstStats,
    use_direct: bool,
}

impl SstCache {
    fn new(use_direct: bool) -> Self {
        Self {
            map: HashMap::new(),
            stats: SstStats::default(),
            use_direct,
        }
    }

    fn get_handle(&mut self, meta: &SstMeta, paths: &StoragePaths) -> Result<Arc<SstHandle>> {
        if let Some(handle) = self.map.get(&meta.id) {
            self.stats.hits += 1;
            return Ok(handle.clone());
        }
        self.stats.misses += 1;
        let path = paths.sst_dir().join(&meta.file_name);
        let handle = if self.use_direct {
            // If we are using IOPOLL, we MUST use O_DIRECT.
            Arc::new(SstHandle::open_direct_strict(&path)?)
        } else {
            Arc::new(SstHandle::open(&path)?)
        };
        self.map.insert(meta.id, handle.clone());
        Ok(handle)
    }

    fn record_bytes(&mut self, bytes: u64) {
        self.stats.bytes_read += bytes;
    }

    fn retain(&mut self, entries: &[SstMeta]) {
        let valid: HashSet<u64> = entries.iter().map(|m| m.id).collect();
        self.map.retain(|id, _| valid.contains(id));
    }

    fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.stats.hits,
            misses: self.stats.misses,
            bytes_read: self.stats.bytes_read,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub bytes_read: u64,
}

#[derive(Default, Clone, Copy)]
struct SstStats {
    hits: u64,
    misses: u64,
    bytes_read: u64,
}

fn range_overlaps(meta: &SstMeta, spec: &RangeSpec) -> bool {
    if meta.key_max.is_empty() || meta.key_min.is_empty() {
        return true;
    }
    !spec.before_start(&meta.key_max) && !spec.past_end(&meta.key_min)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        block::{Block, BlockBuilder},
        manifest::Manifest,
        sst::BLOCK_SIZE,
        storage::StoragePaths,
    };
    use std::fs;
    use tempfile::TempDir;

    /// Create a temporary directory under /home/robi/mtest (user-specified real FS).
    fn disk_tempdir() -> TempDir {
        let base = std::path::Path::new("/home/robi/Code/rust/meme/mtest");
        fs::create_dir_all(base).expect("create mtest base");
        tempfile::Builder::new()
            .tempdir_in(base)
            .or_else(|_| tempfile::tempdir())
            .expect("create tempdir")
    }

    fn test_opts() -> LsmOptions {
        LsmOptions {
            // Use small cache for tests to avoid memory allocation issues
            // when running multiple tests in parallel
            block_cache_slots: 16,
            force_single_issuer: false,
            ..Default::default()
        }
    }

    #[test]
    fn memtable_get_roundtrip() {
        let mut mem = MemTable::new();
        mem.put(b"a", b"1");
        mem.put(b"b", b"2");
        assert_eq!(mem.get(b"a"), Some(b"1".to_vec()));
        assert_eq!(mem.get(b"missing"), None);
        let sorted = mem.drain_sorted();
        assert_eq!(sorted.len(), 2);
    }

    #[test]
    fn block_builder_parse_cycle() -> Result<()> {
        let entries = vec![
            (b"k1".to_vec(), b"v1".to_vec()),
            (b"k2".to_vec(), b"v2".to_vec()),
        ];
        let mut builder = BlockBuilder::new(BLOCK_SIZE);
        for (k, v) in &entries {
            assert!(builder.try_push(k, v));
        }
        let block_data = builder.finish();
        let block = Block::parse(&block_data)?;
        let decoded = block.entries()?;
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0], (b"k1".to_vec(), b"v1".to_vec()));
        assert_eq!(decoded[1], (b"k2".to_vec(), b"v2".to_vec()));
        Ok(())
    }

    #[test]
    fn manifest_roundtrip() -> Result<()> {
        let tmp = disk_tempdir();
        let paths = StoragePaths::new(tmp.path().to_path_buf())?;
        let mut manifest = Manifest::load(&paths)?;
        let entries = vec![(b"a".to_vec(), b"1".to_vec())];
        let meta = manifest.allocate_name(&entries);
        manifest.add_entry(meta);
        manifest.sync()?;
        let manifest2 = Manifest::load(&paths)?;
        assert_eq!(manifest2.entries().len(), 1);
        Ok(())
    }

    #[test]
    fn restart_and_compaction_flow() -> Result<()> {
        let tmp = disk_tempdir();
        let root = tmp.path().to_path_buf();
        {
            eprintln!("phase1 start");
            let mut db = LsmDb::open(&root, test_opts())?;
            db.set_flush_limit(1);
            db.put(b"alpha", b"one")?;
            db.flush_memtable()?;
            let wal_len = fs::metadata(root.join("wal.log"))?.len();
            assert_eq!(wal_len, 0);
            eprintln!("phase1 done");
        }

        {
            eprintln!("phase2 start");
            let mut db = LsmDb::open(&root, test_opts())?;
            let first = db.get(b"alpha")?;
            assert_eq!(first, Some(b"one".to_vec()));

            db.set_flush_limit(1);
            for i in 0..6 {
                let key = format!("k{i}");
                let val = format!("v{i}");
                db.put(key.as_bytes(), val.as_bytes())?;
                db.flush_memtable()?;
            }
            assert!(db.manifest_len() >= 4);
            db.compact_blocking()?;
            let sst_dir = root.join("sst");
            let file_count = fs::read_dir(&sst_dir)?.count();
            assert_eq!(file_count, db.manifest_len());
            eprintln!("phase2 done");
        }

        {
            eprintln!("phase3 start");
            let mut db = LsmDb::open(&root, test_opts())?;
            let second = db.get(b"alpha")?;
            assert_eq!(second, Some(b"one".to_vec()));
            for i in 0..6 {
                let key = format!("k{i}");
                let val = format!("v{i}");
                assert_eq!(db.get(key.as_bytes())?, Some(val.into_bytes()));
            }
            eprintln!("phase3 done");
        }
        Ok(())
    }

    #[test]
    fn pipelined_iterator_streams() -> Result<()> {
        let tmp = disk_tempdir();
        let root = tmp.path().to_path_buf();
        let mut db = LsmDb::open(&root, test_opts())?;
        db.set_flush_limit(100);
        let mut expected = Vec::new();
        for i in 0..50 {
            let key = format!("scan-{i:04}");
            let value = format!("val-{i:04}");
            expected.push((key.clone().into_bytes(), value.clone().into_bytes()));
            db.put(key.as_bytes(), value.as_bytes())?;
        }
        db.flush_memtable()?;
        let mut iter = db
            .range_iter(b"scan-0000".to_vec()..=b"scan-0049".to_vec(), 4)?
            .expect("range iterator");
        let mut collected = Vec::new();
        while let Some(item) = iter.next() {
            let (k, v) = item?;
            collected.push((k, v));
        }
        assert_eq!(collected, expected);
        Ok(())
    }

    /// Test WAL write and recovery
    #[test]
    fn wal_write_and_recovery() -> Result<()> {
        let tmp = disk_tempdir();
        let root = tmp.path().to_path_buf();
        
        // Write some data without flushing
        {
            let mut db = LsmDb::open(&root, test_opts())?;
            db.set_flush_limit(1000); // High limit to prevent auto-flush
            for i in 0..10 {
                let key = format!("wal-key-{i:04}");
                let val = format!("wal-val-{i:04}");
                db.put(key.as_bytes(), val.as_bytes())?;
            }
            // Don't flush - data should be in WAL only
        }
        
        // Reopen and verify data is recovered from WAL
        {
            let mut db = LsmDb::open(&root, test_opts())?;
            for i in 0..10 {
                let key = format!("wal-key-{i:04}");
                let expected_val = format!("wal-val-{i:04}");
                let actual = db.get(key.as_bytes())?;
                assert_eq!(actual, Some(expected_val.into_bytes()), "key {} not found after WAL recovery", key);
            }
        }
        Ok(())
    }

    /// Test point lookup in memtable
    #[test]
    fn point_lookup_memtable() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(1000);
        
        // Insert and lookup
        db.put(b"key1", b"value1")?;
        db.put(b"key2", b"value2")?;
        db.put(b"key3", b"value3")?;
        
        assert_eq!(db.get(b"key1")?, Some(b"value1".to_vec()));
        assert_eq!(db.get(b"key2")?, Some(b"value2".to_vec()));
        assert_eq!(db.get(b"key3")?, Some(b"value3".to_vec()));
        assert_eq!(db.get(b"nonexistent")?, None);
        
        // Update and verify
        db.put(b"key1", b"updated1")?;
        assert_eq!(db.get(b"key1")?, Some(b"updated1".to_vec()));
        
        Ok(())
    }

    /// Test point lookup in SST after flush
    #[test]
    fn point_lookup_sst() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(10);
        
        // Insert data
        for i in 0..20 {
            let key = format!("sst-key-{i:04}");
            let val = format!("sst-val-{i:04}");
            db.put(key.as_bytes(), val.as_bytes())?;
        }
        
        // Flush to SST
        db.flush_memtable()?;
        
        // Verify all keys are readable from SST
        for i in 0..20 {
            let key = format!("sst-key-{i:04}");
            let expected_val = format!("sst-val-{i:04}");
            let actual = db.get(key.as_bytes())?;
            assert_eq!(actual, Some(expected_val.into_bytes()), "key {} not found in SST", key);
        }
        
        // Verify miss
        assert_eq!(db.get(b"nonexistent")?, None);
        
        Ok(())
    }

    /// Test tombstone (delete) operation using put with empty value
    #[test]
    fn tombstone_operation() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(100);
        
        // Insert
        db.put(b"to-delete", b"value")?;
        assert_eq!(db.get(b"to-delete")?, Some(b"value".to_vec()));
        
        // "Delete" by writing empty value (tombstone pattern)
        db.put(b"to-delete", b"")?;
        // Note: This will return Some(empty) not None - true delete would need tombstone support
        let result = db.get(b"to-delete")?;
        assert_eq!(result, Some(b"".to_vec()));
        
        // Flush and verify
        db.flush_memtable()?;
        let result = db.get(b"to-delete")?;
        assert_eq!(result, Some(b"".to_vec()));
        
        Ok(())
    }

    /// Test large batch write
    #[test]
    fn large_batch_write() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(500);
        
        // Write a large batch
        let batch_size = 200;
        for i in 0..batch_size {
            let key = format!("batch-{i:06}");
            let val = format!("value-{i:06}");
            db.put(key.as_bytes(), val.as_bytes())?;
        }
        
        // Verify all written
        for i in 0..batch_size {
            let key = format!("batch-{i:06}");
            let expected = format!("value-{i:06}");
            assert_eq!(db.get(key.as_bytes())?, Some(expected.into_bytes()));
        }
        
        // Flush and verify
        db.flush_memtable()?;
        for i in 0..batch_size {
            let key = format!("batch-{i:06}");
            let expected = format!("value-{i:06}");
            assert_eq!(db.get(key.as_bytes())?, Some(expected.into_bytes()));
        }
        
        Ok(())
    }

    /// Test multiple SST files and compaction
    #[test]
    fn multiple_sst_compaction() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(5);
        
        // Create multiple SST files
        for batch in 0..4 {
            for i in 0..10 {
                let key = format!("compact-{batch}-{i:04}");
                let val = format!("value-{batch}-{i:04}");
                db.put(key.as_bytes(), val.as_bytes())?;
            }
            db.flush_memtable()?;
        }
        
        // Verify before compaction
        let before_len = db.manifest_len();
        assert!(before_len >= 4, "should have at least 4 SST files");
        
        // Compact
        db.compact_blocking()?;
        
        // Verify after compaction
        let after_len = db.manifest_len();
        assert!(after_len <= before_len, "compaction should reduce or maintain SST count");
        
        // Verify all data still accessible
        for batch in 0..4 {
            for i in 0..10 {
                let key = format!("compact-{batch}-{i:04}");
                let expected = format!("value-{batch}-{i:04}");
                let actual = db.get(key.as_bytes())?;
                assert_eq!(actual, Some(expected.into_bytes()), "key {} missing after compaction", key);
            }
        }
        
        Ok(())
    }

    /// Test range scan with various window sizes
    #[test]
    fn range_scan_windows() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(100);
        
        // Insert ordered keys
        for i in 0..30 {
            let key = format!("range-{i:04}");
            let val = format!("val-{i:04}");
            db.put(key.as_bytes(), val.as_bytes())?;
        }
        db.flush_memtable()?;
        
        // Test different window sizes
        for window in [1, 2, 4, 8] {
            let mut iter = db
                .range_iter(b"range-0000".to_vec()..=b"range-0029".to_vec(), window)?
                .expect("range iterator");
            
            let mut count = 0;
            while let Some(item) = iter.next() {
                let (k, _v) = item?;
                let expected_key = format!("range-{count:04}");
                assert_eq!(k, expected_key.into_bytes(), "wrong key at position {} with window {}", count, window);
                count += 1;
            }
            assert_eq!(count, 30, "should iterate all 30 keys with window {}", window);
        }
        
        Ok(())
    }

    /// Test concurrent-like access pattern (sequential but simulating concurrent behavior)
    #[test]
    fn mixed_read_write_pattern() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(50);
        
        // Interleaved reads and writes
        for i in 0..100 {
            let key = format!("mixed-{i:04}");
            let val = format!("value-{i:04}");
            db.put(key.as_bytes(), val.as_bytes())?;
            
            // Read some previous keys
            if i >= 5 {
                let read_key = format!("mixed-{:04}", i - 5);
                let expected = format!("value-{:04}", i - 5);
                assert_eq!(db.get(read_key.as_bytes())?, Some(expected.into_bytes()));
            }
        }
        
        // Final verification
        for i in 0..100 {
            let key = format!("mixed-{i:04}");
            let expected = format!("value-{i:04}");
            assert_eq!(db.get(key.as_bytes())?, Some(expected.into_bytes()));
        }
        
        Ok(())
    }

    /// Test empty database operations
    #[test]
    fn empty_database_operations() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        
        // Get on empty DB
        assert_eq!(db.get(b"nonexistent")?, None);
        
        // Range scan on empty DB
        let iter = db.range_iter(b"a".to_vec()..=b"z".to_vec(), 4)?;
        assert!(iter.is_none() || {
            let mut it = iter.unwrap();
            it.next().is_none()
        });
        
        Ok(())
    }

    /// Test binary keys and values
    #[test]
    fn binary_keys_values() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(100);
        
        // Binary key with null bytes
        let binary_key = vec![0x00, 0x01, 0x02, 0xFF, 0xFE];
        let binary_val = vec![0xDE, 0xAD, 0xBE, 0xEF];
        
        db.put(&binary_key, &binary_val)?;
        assert_eq!(db.get(&binary_key)?, Some(binary_val.clone()));
        
        // Flush and verify
        db.flush_memtable()?;
        assert_eq!(db.get(&binary_key)?, Some(binary_val));
        
        Ok(())
    }

    /// Test moderately large values (within block size limit)
    #[test]
    fn large_values() -> Result<()> {
        let tmp = disk_tempdir();
        let mut db = LsmDb::open(tmp.path(), test_opts())?;
        db.set_flush_limit(10);
        
        // 4KB value (within typical block size limit)
        let large_val: Vec<u8> = (0..4096).map(|i| (i % 256) as u8).collect();
        
        db.put(b"large-key", &large_val)?;
        assert_eq!(db.get(b"large-key")?, Some(large_val.clone()));
        
        // Flush and verify
        db.flush_memtable()?;
        assert_eq!(db.get(b"large-key")?, Some(large_val));
        
        Ok(())
    }
}
