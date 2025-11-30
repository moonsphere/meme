use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use meme::{LsmDb, LsmOptions};
use tempfile::TempDir;

fn default_opts() -> LsmOptions {
    LsmOptions::default()
}

/// Benchmark WAL write performance (io_uring WriteFixed optimization)
fn wal_write_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("wal_write");
    group.sample_size(20); // Reduce sample size for faster benchmarks
    
    for batch_size in [10, 100, 500] {
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("batch", batch_size),
            &batch_size,
            |b, &size| {
                let dir = TempDir::new().expect("tempdir");
                let mut db = LsmDb::open(dir.path(), default_opts()).expect("open db");
                db.set_flush_limit(size * 2); // Prevent auto-flush during benchmark
                
                b.iter(|| {
                    for i in 0..size {
                        let key = format!("key-{i:06}");
                        let val = format!("val-{i:06}");
                        db.put(key.as_bytes(), val.as_bytes()).expect("put");
                    }
                })
            },
        );
    }
    group.finish();
}

/// Benchmark SST flush performance (io_uring SKIP_SUCCESS optimization)
fn sst_flush_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("sst_flush");
    
    for num_entries in [100, 500, 1000, 5000] {
        group.throughput(Throughput::Elements(num_entries as u64));
        group.bench_with_input(
            BenchmarkId::new("entries", num_entries),
            &num_entries,
            |b, &size| {
                b.iter(|| {
                    let dir = TempDir::new().expect("tempdir");
                    let mut db = LsmDb::open(dir.path(), default_opts()).expect("open db");
                    db.set_flush_limit(size * 2); // Prevent auto-flush
                    
                    // Write entries to memtable
                    for i in 0..size {
                        let key = format!("flush-{i:06}");
                        let val = format!("value-{i:06}");
                        db.put(key.as_bytes(), val.as_bytes()).expect("put");
                    }
                    
                    // Measure flush time
                    db.flush_memtable().expect("flush");
                })
            },
        );
    }
    group.finish();
}

/// Benchmark point lookup performance (io_uring provided buffers optimization)
fn point_lookup_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("point_lookup");
    
    // Setup: create a DB with data
    let dir = TempDir::new().expect("tempdir");
    let mut db = LsmDb::open(dir.path(), default_opts()).expect("open db");
    db.set_flush_limit(2000);
    
    let num_keys = 1000;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(num_keys);
    
    for i in 0..num_keys {
        let key = format!("lookup-{i:06}");
        let val = format!("value-{i:06}");
        keys.push(key.clone().into_bytes());
        db.put(key.as_bytes(), val.as_bytes()).expect("put");
    }
    db.flush_memtable().expect("flush");
    
    // Benchmark lookups in memtable
    group.bench_function("memtable_hit", |b| {
        let mut idx = 0;
        b.iter(|| {
            let key = &keys[idx % keys.len()];
            let _ = db.get(key).expect("get");
            idx += 1;
        })
    });
    
    // Benchmark lookups in SST (after flush)
    group.bench_function("sst_hit", |b| {
        let mut idx = 0;
        b.iter(|| {
            let key = &keys[idx % keys.len()];
            let _ = db.get(key).expect("get");
            idx += 1;
        })
    });
    
    // Benchmark miss lookups
    group.bench_function("miss", |b| {
        let mut idx = 0;
        b.iter(|| {
            let key = format!("nonexistent-{idx:06}");
            let _ = db.get(key.as_bytes()).expect("get");
            idx += 1;
        })
    });
    
    group.finish();
}

/// Benchmark range scan with different prefetch windows
fn range_scan_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_scan");
    group.sample_size(20); // Reduce sample size for faster benchmarks
    
    // Setup: create a DB with data
    let dir = TempDir::new().expect("tempdir");
    let mut db = LsmDb::open(dir.path(), default_opts()).expect("open db");
    db.set_flush_limit(2000);
    
    let num_keys = 500;
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(num_keys);
    
    for i in 0..num_keys {
        let key = format!("scan-{i:05}");
        let val = format!("val-{i:05}");
        keys.push(key.clone().into_bytes());
        db.put(key.as_bytes(), val.as_bytes()).expect("put");
    }
    db.flush_memtable().expect("flush");
    
    for window in [1usize, 2, 4, 8] {
        group.throughput(Throughput::Elements(num_keys as u64));
        group.bench_with_input(BenchmarkId::new("window", window), &window, |b, &win| {
            b.iter(|| {
                let mut iter = db
                    .range_iter(keys.first().unwrap().clone()..=keys.last().unwrap().clone(), win)
                    .expect("iterator build")
                    .expect("iterator");
                let mut count = 0usize;
                while let Some(item) = iter.next() {
                    let _ = item.expect("iterator item");
                    count += 1;
                }
                assert_eq!(count, keys.len());
            })
        });
    }
    group.finish();
}

/// Benchmark mixed workload (read/write)
fn mixed_workload_bench(c: &mut Criterion) {
    let mut group = c.benchmark_group("mixed_workload");
    group.sample_size(20);
    
    // 80% read, 20% write
    group.bench_function("read_heavy_80_20", |b| {
        let dir = TempDir::new().expect("tempdir");
        let mut db = LsmDb::open(dir.path(), default_opts()).expect("open db");
        db.set_flush_limit(1000);
        
        // Pre-populate
        for i in 0..500 {
            let key = format!("mixed-{i:06}");
            let val = format!("value-{i:06}");
            db.put(key.as_bytes(), val.as_bytes()).expect("put");
        }
        
        let mut op_idx = 0;
        let mut write_idx = 500;
        
        b.iter(|| {
            if op_idx % 5 == 0 {
                // Write (20%)
                let key = format!("mixed-{write_idx:06}");
                let val = format!("value-{write_idx:06}");
                db.put(key.as_bytes(), val.as_bytes()).expect("put");
                write_idx += 1;
            } else {
                // Read (80%)
                let key = format!("mixed-{:06}", op_idx % 500);
                let _ = db.get(key.as_bytes()).expect("get");
            }
            op_idx += 1;
        })
    });
    
    // 50% read, 50% write
    group.bench_function("balanced_50_50", |b| {
        let dir = TempDir::new().expect("tempdir");
        let mut db = LsmDb::open(dir.path(), default_opts()).expect("open db");
        db.set_flush_limit(1000);
        
        // Pre-populate
        for i in 0..500 {
            let key = format!("mixed-{i:06}");
            let val = format!("value-{i:06}");
            db.put(key.as_bytes(), val.as_bytes()).expect("put");
        }
        
        let mut op_idx = 0;
        let mut write_idx = 500;
        
        b.iter(|| {
            if op_idx % 2 == 0 {
                // Write (50%)
                let key = format!("mixed-{write_idx:06}");
                let val = format!("value-{write_idx:06}");
                db.put(key.as_bytes(), val.as_bytes()).expect("put");
                write_idx += 1;
            } else {
                // Read (50%)
                let key = format!("mixed-{:06}", op_idx % 500);
                let _ = db.get(key.as_bytes()).expect("get");
            }
            op_idx += 1;
        })
    });
    
    group.finish();
}

criterion_group!(
    lsm,
    wal_write_bench,
    sst_flush_bench,
    point_lookup_bench,
    range_scan_bench,
    mixed_workload_bench,
);
criterion_main!(lsm);
