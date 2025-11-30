# io_uring-backed LSM Storage Engine

## Overview

This is an experimental LSM-tree storage engine written in Rust that leverages Linux `io_uring` for high-performance asynchronous I/O. The design goals are:

- Keep all hot paths (WAL appends, memtable flush, point lookups, compaction) fully asynchronous via `io_uring`
- Minimize page-cache round-trips using registered files/buffers and SQE chaining
- Leverage SQPOLL and IOPOLL for lowest latency on critical paths
- Demonstrate on-disk structures (SST blocks, indexes, Bloom filters) optimized for io_uring data paths

## Performance Results

### vs RocksDB Production Configuration (Buffered I/O + Block Cache)

This is the most important comparison - RocksDB in its typical production configuration:

| Configuration | Latency | Throughput | Relative |
|---------------|---------|------------|----------|
| **meme (O_DIRECT + 1MB cache)** | **648µs** | **1.54 Melem/s** | **22% faster** |
| RocksDB production (buffered + 8MB cache) | 794µs | 1.26 Melem/s | baseline |

**meme with O_DIRECT + user-space cache is 22% faster than RocksDB with buffered I/O + block cache.**

### Cache Hit Performance (Sequential Access, Warmed)

| Configuration | Latency | Throughput |
|---------------|---------|------------|
| **meme (1MB cache)** | **648µs** | **1.54 Melem/s** |
| RocksDB (O_DIRECT + 8MB cache) | 790µs | 1.27 Melem/s |
| RocksDB production (buffered + 8MB cache) | 794µs | 1.26 Melem/s |
| RocksDB (buffered, no block cache) | 2.64ms | 379 Kelem/s |

### Raw Disk I/O Performance (O_DIRECT, Random Access, No Cache)

| Engine | Latency | Throughput | Relative |
|--------|---------|------------|----------|
| **meme (io_uring + IOPOLL)** | **28.1ms** | **35.5 Kelem/s** | **24% faster** |
| RocksDB (pread + O_DIRECT) | 35.0ms | 28.6 Kelem/s | baseline |

**io_uring with IOPOLL is ~24% faster than traditional pread for random disk I/O.**

### Key Insights

1. **vs RocksDB Production**: meme is 22% faster than RocksDB's typical production configuration (buffered I/O + block cache)
2. **Raw Disk I/O**: io_uring provides ~24% improvement over pread due to reduced syscall overhead and kernel polling
3. **Cache Efficiency**: meme's simpler code path and pre-parsed block cache provide consistent 22% improvement
4. **OS Page Cache Overhead**: RocksDB with only OS page cache (no block cache) is 4x slower than with block cache

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                         LsmDb                                │
├─────────────────────────────────────────────────────────────┤
│  MemTable  │  WAL Writer (Double Buffer + SQPOLL + IOPOLL)   │
├────────────┴────────────────────────────────────────────────┤
│                      IoScheduler                             │
│  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────┐│
│  │ FG Ring      │ │ BG Flush     │ │ Compaction   │ │ WAL  ││
│  │ SQPOLL+IOPOLL│ │ SQPOLL       │ │ SQPOLL       │ │SQPOLL││
│  │ Point/Scan   │ │ SST Flush    │ │ Merge+Write  │ │IOPOLL││
│  └──────────────┘ └──────────────┘ └──────────────┘ └──────┘│
├─────────────────────────────────────────────────────────────┤
│  Block Cache (Registered Buffers + Huge Pages + ReadFixed)   │
├─────────────────────────────────────────────────────────────┤
│  BufRing (Provided Buffers)                                  │
├─────────────────────────────────────────────────────────────┤
│  SST Files (O_DIRECT + Bloom + Index)                        │
└─────────────────────────────────────────────────────────────┘
```

### Ring Configuration

| Ring | SQPOLL | IOPOLL | Purpose | Notes |
|------|--------|--------|---------|-------|
| Foreground | ✅ | ✅ | Point queries, scans | Lowest latency, read-only |
| Background Flush | ✅ | ❌ | SST flush | fsync + rename incompatible with IOPOLL |
| Compaction | ✅ | ❌ | Background compaction | fsync + rename incompatible with IOPOLL |
| WAL | ✅ | ✅ | WAL writes | O_DSYNC replaces fsync, IOPOLL compatible |

### SQPOLL vs IOPOLL

These two features are independent:

- **SQPOLL**: Kernel thread polls submission queue, reducing syscall overhead. **Works with any operation** including fsync, rename, etc.
- **IOPOLL**: Kernel polls for I/O completion, bypassing interrupts. **Only works with O_DIRECT read/write operations**, not with fsync, rename, fallocate, etc.

Both can be combined for optimal performance.

## Code Structure

```
crate
├── block.rs          # Block format with restart points and prefix compression
├── buf_ring.rs       # Provided buffer ring (kernel 5.19+)
├── cache.rs          # User-space block cache with huge pages support
├── compaction.rs     # Background job queue + merge pipeline
├── lru.rs            # O(1) LRU cache implementation
├── manifest.rs       # MANIFEST persistence and SST metadata
├── memtable.rs       # Skiplist memtable and WAL entry encoding
├── scheduler.rs      # IoScheduler + IoRingHandle (4-ring architecture)
├── sst.rs            # SST block/bloom/index builders and readers
├── storage.rs        # Path management (root, wal, sst, manifest)
├── util.rs           # Shared filesystem/hash helpers
├── wal.rs            # Double-buffered WAL with O_DSYNC + IOPOLL
└── lib.rs            # LsmDb API wiring everything together
```

## Data Flow

### WAL + Memtable

- `MemTable` is a skiplist-based map storing `(key,value)` pairs in sorted order
- **Double-buffered WAL**: One buffer encodes while the other flushes, enabling CPU/IO pipelining
- **Group commit**: Batches writes up to 64KB threshold before issuing write
- **O_DSYNC mode**: WAL uses O_DSYNC instead of explicit fsync, enabling IOPOLL compatibility
- WAL writes use `WriteFixed` (registered buffers) for zero-copy I/O
- **Dedicated WAL Ring with SQPOLL + IOPOLL**: Microsecond-level submission latency

### SST Flush

- `SstFlusher` builds fixed-size blocks (default 16KiB) via `BlockBuilder`
- Flush pipeline with SQE chaining:
  1. **Fallocate**: Pre-allocates file space to avoid write-time allocation latency
  2. All blocks (4KB aligned for O_DIRECT)
  3. Block index (first key + block offset)
  4. Bloom filter bitset
  5. 32-byte footer describing index/bloom offsets
  6. **Fsync file**: Ensures data durability
  7. **Rename**: Atomic file replacement
  8. **Fsync directory**: Ensures directory entry persistence
- **Complete chain**: `fallocate → writes → fsync(file) → rename → fsync(dir)` with `IO_LINK` + `SKIP_SUCCESS`

### Point Lookups

- Each SST carries metadata loaded from footer: block index and Bloom filter
- **Block Cache**: User-space LRU cache with huge pages, uses `ReadFixed` for zero-copy
- **Pre-parsed entries**: Block entries are parsed once and cached for O(log n) binary search
- `LsmDb::get` checks memtable first, then scans SSTs by level+id priority

### Range Scans

- **PipelinedIterator**: Issues multiple outstanding block reads via buffer-rings
- **FADVISE**: Uses `POSIX_FADV_SEQUENTIAL` for sequential access hints
- Configurable prefetch window size

### Compaction

- `Compactor` merges level-0 SSTs using `PipelinedIterator` for streaming reads
- **O_DIRECT for compaction reads**: Bypasses page cache to avoid pollution
- **IOSQE_ASYNC**: Offloads large file reads to io-wq threads
- `CompactionQueue` runs in dedicated thread, decoupled from foreground work

## io_uring Features (26 Optimizations)

### Ring Setup
| Feature | Description |
|---------|-------------|
| SINGLE_ISSUER | One thread per ring, reduces locking |
| COOP_TASKRUN | Cooperative task running, reduces latency jitter |
| DEFER_TASKRUN | Defers work to io_uring_enter() |
| NO_SQARRAY | Removes SQ array indirection (kernel 6.6+) |
| SUBMIT_ALL | Batch-friendly submission |
| SQPOLL | Kernel polling thread for all 4 rings |
| IOPOLL | Kernel polls for completion (Foreground + WAL rings) |

### Registered Resources
| Feature | Description |
|---------|-------------|
| Fixed Files | `register_files_sparse(64)` avoids fd lookup |
| Fixed Buffers | `WriteFixed`/`ReadFixed` avoids page pinning |
| Buffer Ring | Provided buffers with user-space recycling |
| Ring FD | Reduces syscall overhead |
| Sparse Buffers | `IORING_REGISTER_BUFFERS2` for dynamic registration |

### Operations
| Feature | Description |
|---------|-------------|
| IO_LINK | Chains fallocate→write→fsync→rename→fsync(dir) |
| SKIP_SUCCESS | Reduces CQE count for linked ops |
| ASYNC | Offloads to io-wq for compaction |
| FADVISE | Sequential read hints |
| FALLOCATE | Pre-allocates file space to reduce tail latency |

### Kernel Features
| Feature | Description |
|---------|-------------|
| FEAT_NODROP | Guarantees no CQE drops (kernel 5.5+) |
| O_DSYNC | WAL uses O_DSYNC for IOPOLL compatibility |

## Kernel Version Requirements

| Feature | Minimum Version |
|---------|-----------------|
| Basic io_uring | 5.1 |
| IOPOLL | 5.1 |
| SQPOLL | 5.4 |
| FEAT_NODROP | 5.5 |
| Buffer Ring | 5.19 |
| SINGLE_ISSUER | 6.0 |
| DEFER_TASKRUN | 6.1 |
| NO_SQARRAY | 6.6 |

**Recommended**: >= 6.1

## System Requirements

### NVMe Device

IOPOLL mode requires NVMe or other polling-capable block devices. The engine automatically detects the device type on startup.

Supported devices:
- NVMe (major 259) - Full IOPOLL support
- SCSI (major 8) - Warning displayed, may work
- Loop devices (major 7) - For testing
- Device-mapper (major 253) - For testing

### memlock Limit

io_uring requires locked memory for ring buffers and registered buffers. The default 8MB limit may be insufficient for 4 SQPOLL rings.

```bash
# /etc/security/limits.d/99-iouring.conf
* soft memlock unlimited
* hard memlock unlimited
```

## Configuration

```rust
pub struct LsmOptions {
    /// Number of 16KB block cache slots (default: 64 = 1MB)
    /// Each slot requires locked memory (memlock).
    pub block_cache_slots: usize,
    /// Force SINGLE_ISSUER flag (default: false)
    pub force_single_issuer: bool,
}
```

### Memory Requirements

| Component | Memory Usage |
|-----------|--------------|
| 4 SQPOLL rings | ~4-6 MB |
| Block cache (default 64 slots) | 1 MB (64 × 16KB) |
| **Total** | ~5-7 MB |

## Benchmarks

Run with:
```bash
cargo bench --bench rocksdb_comparison
```

Key benchmarks:
- `point_get/meme_warmed/sequential`: Point query with warmed cache (sequential access)
- `point_get/meme_minimal_cache/random`: Raw io_uring disk I/O (random access, minimal cache)
- `point_get/rocksdb_no_cache_direct/random`: RocksDB O_DIRECT without cache (random access)
- `point_get/rocksdb_buffered_warmed/sequential`: RocksDB with OS page cache
- `point_get/rocksdb_*_cache_warmed/sequential`: RocksDB with warmed user-space cache

## Future Work

1. **Range iterators**: Level-aware scheduling for multi-level merges
2. **Bloom configuration**: Per-SST or per-level size tuning
3. **Manifest richness**: File sizes, creation seqnos, compaction priorities
4. **Compaction throttling**: Rate limiting and IO priority adjustments
5. **Metrics/observability**: Structured tracing for queue depth and latency
6. **Transactions**: Multi-put grouping and commit callbacks
7. **io-wq affinity**: `IORING_REGISTER_IOWQ_AFF` for core binding
