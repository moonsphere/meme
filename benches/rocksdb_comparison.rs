//! Benchmark comparison between meme (io_uring LSM) and RocksDB
//!
//! Run with: cargo bench --bench rocksdb_comparison
//!
//! Note: If you see "Cannot allocate memory" errors, increase memlock limit:
//!   ulimit -l unlimited
//! Or add to /etc/security/limits.d/99-iouring.conf:
//!   * soft memlock unlimited
//!   * hard memlock unlimited

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use meme::{LsmDb, LsmOptions};
use rand::prelude::*;
use rocksdb::{Options, DB};
use tempfile::TempDir;

const VALUE_SIZE: usize = 100;

fn make_key(idx: usize) -> Vec<u8> {
    format!("key-{idx:08}").into_bytes()
}

fn make_value(rng: &mut impl Rng) -> Vec<u8> {
    let mut buf = vec![0u8; VALUE_SIZE];
    rng.fill_bytes(&mut buf);
    buf
}

/// Create LsmOptions for benchmarks with 1MB cache (default, fits 8MB memlock)
fn bench_lsm_options() -> LsmOptions {
    LsmOptions::default() // 64 slots × 16KB = 1MB cache
}

/// Create LsmOptions with larger cache (requires more memlock)
/// 512 slots × 16KB = 8MB cache (matches RocksDB default)
#[allow(dead_code)]
fn bench_lsm_options_large_cache() -> LsmOptions {
    LsmOptions {
        block_cache_slots: 512, // 8MB cache
        ..Default::default()
    }
}

/// RocksDB options without block cache (O_DIRECT for fair comparison with meme)
fn rocksdb_options_no_cache() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    opts.set_use_direct_reads(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_max_background_jobs(1);
    
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.disable_cache();
    opts.set_block_based_table_factory(&block_opts);
    
    opts
}

/// RocksDB options without block cache, using page cache (buffered I/O)
fn rocksdb_options_no_cache_buffered() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    // No O_DIRECT - use OS page cache
    opts.set_max_background_jobs(1);
    
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.disable_cache();
    opts.set_block_based_table_factory(&block_opts);
    
    opts
}

/// RocksDB options with buffered I/O + 8MB block cache (realistic production config)
/// This is the most common RocksDB configuration in production:
/// - Buffered I/O (uses OS page cache)
/// - Block cache for hot data
fn rocksdb_options_buffered_with_cache() -> Options {
    use rocksdb::Cache;
    
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    // No O_DIRECT - use OS page cache (default behavior)
    opts.set_max_background_jobs(1);
    
    // 8MB block cache (typical production setting)
    let cache = Cache::new_lru_cache(8 * 1024 * 1024);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    opts.set_block_based_table_factory(&block_opts);
    
    opts
}

/// RocksDB options with 1MB block cache (fair comparison with meme default)
fn rocksdb_options_1mb_cache() -> Options {
    use rocksdb::Cache;
    
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    opts.set_use_direct_reads(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_max_background_jobs(1);
    
    // 1MB cache to match meme's default
    let cache = Cache::new_lru_cache(1 * 1024 * 1024);
    let mut block_opts = rocksdb::BlockBasedOptions::default();
    block_opts.set_block_cache(&cache);
    opts.set_block_based_table_factory(&block_opts);
    
    opts
}

/// RocksDB options with 8MB block cache (default RocksDB behavior)
fn rocksdb_options_8mb_cache() -> Options {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_write_buffer_size(4 * 1024 * 1024);
    opts.set_compression_type(rocksdb::DBCompressionType::None);
    opts.set_use_direct_reads(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_max_background_jobs(1);
    // Default block cache is 8MB
    opts
}

/// WriteOptions for sync writes (fair comparison with meme's sync WAL)
fn rocksdb_sync_write_options() -> rocksdb::WriteOptions {
    let mut opts = rocksdb::WriteOptions::default();
    opts.set_sync(true);  // Sync WAL on each write like meme does
    opts
}

fn bench_sequential_put(c: &mut Criterion) {
    let mut group = c.benchmark_group("sequential_put");
    group.sample_size(20);

    for num_keys in [1000, 5000] {
        group.throughput(Throughput::Elements(num_keys as u64));

        // meme with sync WAL (default behavior)
        group.bench_with_input(BenchmarkId::new("meme", num_keys), &num_keys, |b, &n| {
            let mut rng = StdRng::seed_from_u64(42);
            let values: Vec<Vec<u8>> = (0..n).map(|_| make_value(&mut rng)).collect();
            b.iter_batched(
                || {
                    let dir = TempDir::new().expect("tempdir");
                    let mut db = LsmDb::open(dir.path(), bench_lsm_options()).expect("open");
                    db.set_flush_limit(n * 2);
                    (dir, db, values.clone())
                },
                |(dir, mut db, vals)| {
                    for i in 0..n {
                        db.put(&make_key(i), &vals[i]).expect("put");
                    }
                    drop(db);
                    drop(dir);
                },
                criterion::BatchSize::SmallInput,
            )
        });

        // RocksDB with sync WAL (fair comparison)
        group.bench_with_input(BenchmarkId::new("rocksdb_sync", num_keys), &num_keys, |b, &n| {
            let mut rng = StdRng::seed_from_u64(42);
            let values: Vec<Vec<u8>> = (0..n).map(|_| make_value(&mut rng)).collect();
            let write_opts = rocksdb_sync_write_options();
            b.iter_batched(
                || {
                    let dir = TempDir::new().expect("tempdir");
                    let db = DB::open(&rocksdb_options_no_cache(), dir.path()).expect("open");
                    (dir, db, values.clone())
                },
                |(dir, db, vals)| {
                    for i in 0..n {
                        db.put_opt(&make_key(i), &vals[i], &write_opts).expect("put");
                    }
                    drop(db);
                    drop(dir);
                },
                criterion::BatchSize::SmallInput,
            )
        });

        // RocksDB without sync (async WAL - default RocksDB behavior)
        group.bench_with_input(BenchmarkId::new("rocksdb_async", num_keys), &num_keys, |b, &n| {
            let mut rng = StdRng::seed_from_u64(42);
            let values: Vec<Vec<u8>> = (0..n).map(|_| make_value(&mut rng)).collect();
            b.iter_batched(
                || {
                    let dir = TempDir::new().expect("tempdir");
                    let db = DB::open(&rocksdb_options_no_cache(), dir.path()).expect("open");
                    (dir, db, values.clone())
                },
                |(dir, db, vals)| {
                    for i in 0..n {
                        db.put(&make_key(i), &vals[i]).expect("put");
                    }
                    drop(db);
                    drop(dir);
                },
                criterion::BatchSize::SmallInput,
            )
        });
    }
    group.finish();
}

/// Create LsmOptions with minimal cache for raw io_uring performance testing
/// We use 1 slot (16KB) which is smaller than our test data, so most reads will miss cache
fn bench_lsm_options_minimal_cache() -> LsmOptions {
    LsmOptions {
        block_cache_slots: 1, // Minimal cache - most reads go to disk via io_uring
        ..Default::default()
    }
}

fn bench_point_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_get");
    group.sample_size(50);
    let num_keys = 1000;  // Reduced to fit in cache for fair comparison

    let meme_dir = TempDir::new().expect("tempdir");
    let mut meme_db = LsmDb::open(meme_dir.path(), bench_lsm_options()).expect("open");
    meme_db.set_flush_limit(num_keys * 2);

    let mut rng = StdRng::seed_from_u64(42);
    let keys: Vec<Vec<u8>> = (0..num_keys).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..num_keys).map(|_| make_value(&mut rng)).collect();

    for i in 0..num_keys {
        meme_db.put(&keys[i], &values[i]).expect("put");
    }
    meme_db.flush_memtable().expect("flush");

    // Warm up meme's BlockCache by reading all keys once
    for key in &keys {
        let _ = meme_db.get(key).expect("warmup get");
    }

    // Setup meme with minimal cache (raw io_uring performance - most reads miss cache)
    let meme_no_cache_dir = TempDir::new().expect("tempdir");
    let mut meme_no_cache_db = LsmDb::open(meme_no_cache_dir.path(), bench_lsm_options_minimal_cache()).expect("open");
    meme_no_cache_db.set_flush_limit(num_keys * 2);
    for i in 0..num_keys {
        meme_no_cache_db.put(&keys[i], &values[i]).expect("put");
    }
    meme_no_cache_db.flush_memtable().expect("flush");

    // Setup RocksDB without cache (O_DIRECT)
    let rocks_no_cache_dir = TempDir::new().expect("tempdir");
    let rocks_no_cache_db = DB::open(&rocksdb_options_no_cache(), rocks_no_cache_dir.path()).expect("open");
    for i in 0..num_keys {
        rocks_no_cache_db.put(&keys[i], &values[i]).expect("put");
    }
    rocks_no_cache_db.flush().expect("flush");

    // Setup RocksDB without cache (buffered I/O - uses OS page cache)
    let rocks_buffered_dir = TempDir::new().expect("tempdir");
    let rocks_buffered_db = DB::open(&rocksdb_options_no_cache_buffered(), rocks_buffered_dir.path()).expect("open");
    for i in 0..num_keys {
        rocks_buffered_db.put(&keys[i], &values[i]).expect("put");
    }
    rocks_buffered_db.flush().expect("flush");
    // Warm up OS page cache by reading all keys once
    for key in &keys {
        let _ = rocks_buffered_db.get(key).expect("warmup get");
    }

    // Setup RocksDB with buffered I/O + 8MB block cache (REALISTIC PRODUCTION CONFIG)
    // This is the most common RocksDB configuration in production
    let rocks_production_dir = TempDir::new().expect("tempdir");
    let rocks_production_db = DB::open(&rocksdb_options_buffered_with_cache(), rocks_production_dir.path()).expect("open");
    for i in 0..num_keys {
        rocks_production_db.put(&keys[i], &values[i]).expect("put");
    }
    rocks_production_db.flush().expect("flush");
    // Warm up both OS page cache and block cache
    for key in &keys {
        let _ = rocks_production_db.get(key).expect("warmup get");
    }

    // Setup RocksDB with 1MB cache (fair comparison with meme default)
    let rocks_1mb_cache_dir = TempDir::new().expect("tempdir");
    let rocks_1mb_cache_db = DB::open(&rocksdb_options_1mb_cache(), rocks_1mb_cache_dir.path()).expect("open");
    for i in 0..num_keys {
        rocks_1mb_cache_db.put(&keys[i], &values[i]).expect("put");
    }
    rocks_1mb_cache_db.flush().expect("flush");
    // Warm up RocksDB cache
    for key in &keys {
        let _ = rocks_1mb_cache_db.get(key).expect("warmup get");
    }

    // Setup RocksDB with 8MB cache (default RocksDB behavior)
    let rocks_8mb_cache_dir = TempDir::new().expect("tempdir");
    let rocks_8mb_cache_db = DB::open(&rocksdb_options_8mb_cache(), rocks_8mb_cache_dir.path()).expect("open");
    for i in 0..num_keys {
        rocks_8mb_cache_db.put(&keys[i], &values[i]).expect("put");
    }
    rocks_8mb_cache_db.flush().expect("flush");
    // Warm up RocksDB cache
    for key in &keys {
        let _ = rocks_8mb_cache_db.get(key).expect("warmup get");
    }

    // Generate random access pattern for "no cache" tests
    // This ensures we're not benefiting from sequential prefetch or cache locality
    let mut random_indices: Vec<usize> = (0..num_keys).collect();
    let mut shuffle_rng = StdRng::seed_from_u64(12345);
    random_indices.shuffle(&mut shuffle_rng);

    group.throughput(Throughput::Elements(1000));

    group.bench_function("meme_warmed/sequential", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let _ = meme_db.get(&keys[idx % num_keys]).expect("get");
                idx += 1;
            }
        })
    });

    // Raw io_uring vs raw pread comparison (minimal cache on meme side)
    // Uses RANDOM access pattern to avoid cache locality benefits
    group.bench_function("meme_minimal_cache/random", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let key_idx = random_indices[idx % num_keys];
                let _ = meme_no_cache_db.get(&keys[key_idx]).expect("get");
                idx += 1;
            }
        })
    });

    // O_DIRECT comparison (both meme and RocksDB bypass page cache)
    // Uses RANDOM access pattern to avoid any prefetch benefits
    group.bench_function("rocksdb_no_cache_direct/random", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let key_idx = random_indices[idx % num_keys];
                let _ = rocks_no_cache_db.get(&keys[key_idx]).expect("get");
                idx += 1;
            }
        })
    });

    // Buffered I/O with OS page cache warmed (no block cache)
    group.bench_function("rocksdb_buffered_warmed/sequential", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let _ = rocks_buffered_db.get(&keys[idx % num_keys]).expect("get");
                idx += 1;
            }
        })
    });

    // REALISTIC PRODUCTION CONFIG: Buffered I/O + 8MB block cache
    // This is what most RocksDB users run in production
    group.bench_function("rocksdb_production/sequential", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let _ = rocks_production_db.get(&keys[idx % num_keys]).expect("get");
                idx += 1;
            }
        })
    });

    // Random access comparison for production config
    group.bench_function("rocksdb_production/random", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let key_idx = random_indices[idx % num_keys];
                let _ = rocks_production_db.get(&keys[key_idx]).expect("get");
                idx += 1;
            }
        })
    });

    group.bench_function("rocksdb_1mb_cache_warmed/sequential", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let _ = rocks_1mb_cache_db.get(&keys[idx % num_keys]).expect("get");
                idx += 1;
            }
        })
    });

    group.bench_function("rocksdb_8mb_cache_warmed/sequential", |b| {
        let mut idx = 0;
        b.iter(|| {
            for _ in 0..1000 {
                let _ = rocks_8mb_cache_db.get(&keys[idx % num_keys]).expect("get");
                idx += 1;
            }
        })
    });

    group.finish();
}

fn bench_range_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");
    group.sample_size(20);
    let num_keys = 5000;

    let meme_dir = TempDir::new().expect("tempdir");
    let mut meme_db = LsmDb::open(meme_dir.path(), bench_lsm_options()).expect("open");
    meme_db.set_flush_limit(num_keys * 2);

    let mut rng = StdRng::seed_from_u64(42);
    let keys: Vec<Vec<u8>> = (0..num_keys).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..num_keys).map(|_| make_value(&mut rng)).collect();

    for i in 0..num_keys {
        meme_db.put(&keys[i], &values[i]).expect("put");
    }
    meme_db.flush_memtable().expect("flush");

    let rocks_dir = TempDir::new().expect("tempdir");
    let rocks_db = DB::open(&rocksdb_options_no_cache(), rocks_dir.path()).expect("open");
    for i in 0..num_keys {
        rocks_db.put(&keys[i], &values[i]).expect("put");
    }
    rocks_db.flush().expect("flush");

    for scan_size in [100, 500, 1000] {
        group.throughput(Throughput::Elements(scan_size as u64));
        let start_key = make_key(1000);
        let end_key = make_key(1000 + scan_size);

        group.bench_with_input(BenchmarkId::new("meme", scan_size), &scan_size, |b, _| {
            b.iter(|| {
                if let Some(mut iter) = meme_db
                    .range_iter(start_key.clone()..=end_key.clone(), 4)
                    .expect("iter")
                {
                    while let Some(item) = iter.next() {
                        let _ = item.expect("item");
                    }
                }
            })
        });

        group.bench_with_input(BenchmarkId::new("rocksdb", scan_size), &scan_size, |b, _| {
            b.iter(|| {
                let mut iter = rocks_db.raw_iterator();
                iter.seek(&start_key);
                while iter.valid() {
                    if let Some(k) = iter.key() {
                        if k > end_key.as_slice() {
                            break;
                        }
                        let _ = iter.value();
                    }
                    iter.next();
                }
            })
        });
    }
    group.finish();
}

fn bench_mixed_workload(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(20);
    let num_keys = 5000;

    let mut rng = StdRng::seed_from_u64(42);
    let keys: Vec<Vec<u8>> = (0..num_keys * 2).map(make_key).collect();
    let values: Vec<Vec<u8>> = (0..num_keys * 2).map(|_| make_value(&mut rng)).collect();

    group.throughput(Throughput::Elements(1000));

    group.bench_function("meme/read_80_write_20", |b| {
        let dir = TempDir::new().expect("tempdir");
        let mut db = LsmDb::open(dir.path(), bench_lsm_options()).expect("open");
        db.set_flush_limit(num_keys * 2);
        for i in 0..num_keys {
            db.put(&keys[i], &values[i]).expect("put");
        }
        let mut op = 0;
        let mut w = num_keys;
        b.iter(|| {
            for _ in 0..1000 {
                if op % 5 == 0 {
                    db.put(&keys[w % keys.len()], &values[w % values.len()]).expect("put");
                    w += 1;
                } else {
                    let _ = db.get(&keys[op % num_keys]).expect("get");
                }
                op += 1;
            }
        })
    });

    group.bench_function("rocksdb/read_80_write_20", |b| {
        let dir = TempDir::new().expect("tempdir");
        let db = DB::open(&rocksdb_options_no_cache(), dir.path()).expect("open");
        for i in 0..num_keys {
            db.put(&keys[i], &values[i]).expect("put");
        }
        let mut op = 0;
        let mut w = num_keys;
        b.iter(|| {
            for _ in 0..1000 {
                if op % 5 == 0 {
                    db.put(&keys[w % keys.len()], &values[w % values.len()]).expect("put");
                    w += 1;
                } else {
                    let _ = db.get(&keys[op % num_keys]).expect("get");
                }
                op += 1;
            }
        })
    });

    group.finish();
}

criterion_group!(
    comparison,
    bench_sequential_put,
    bench_point_get,
    bench_range_scan,
    bench_mixed_workload,
);
criterion_main!(comparison);