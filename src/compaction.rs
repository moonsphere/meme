use std::{
    sync::{
        mpsc, Arc, Mutex,
    },
    thread,
};

use anyhow::{anyhow, Result};

use crate::{
    manifest::{Manifest, SstMeta},
    scheduler::IoRingHandle,
    sst::{
        RangeIterator, RangeSpec, SstFlusher, SstHandle, SstReader, BLOCK_SIZE,
    },
    storage::StoragePaths,
};

pub struct Compactor {
    ring: Arc<IoRingHandle>,
    flusher: SstFlusher,
    read_slot_base: u32,
}

impl Compactor {
    pub fn new(ring: Arc<IoRingHandle>, flusher: SstFlusher, read_slot_base: u32) -> Self {
        Self {
            ring,
            flusher,
            read_slot_base,
        }
    }

    pub fn maybe_compact(
        &self,
        manifest: &mut Manifest,
        paths: &StoragePaths,
    ) -> Result<()> {
        let debug = std::env::var("MEME_DEBUG").as_deref() == Ok("1");
        let mut level0: Vec<_> = manifest
            .entries()
            .iter()
            .filter(|entry| entry.level == 0)
            .cloned()
            .collect();
        if level0.len() < 2 {
            return Ok(());
        }
        if debug {
            eprintln!("[compaction] start candidates={}", level0.len());
        }
        level0.sort_by_key(|entry| entry.id);
        let batch = level0.into_iter().take(2).collect::<Vec<_>>();
        let iter = match self.merge_stream(&batch, paths)? {
            Some(iter) => iter,
            None => return Ok(()),
        };
        let mut final_entry = manifest.allocate_name(&[]);
        let out_path = paths.sst_dir().join(&final_entry.file_name);
        let bounds = match self.flusher.flush_stream(&out_path, iter)? {
            Some(b) => b,
            None => return Ok(()),
        };
        if debug {
            eprintln!(
                "[compaction] flushed {} -> {:?}",
                final_entry.file_name,
                (bounds.min.len(), bounds.max.len())
            );
        }
        final_entry.level = batch.iter().map(|e| e.level).max().unwrap_or(0) + 1;
        final_entry.key_min = bounds.min;
        final_entry.key_max = bounds.max;
        for entry in &batch {
            let _ = std::fs::remove_file(paths.sst_dir().join(&entry.file_name));
        }
        let removed: Vec<u64> = batch.iter().map(|e| e.id).collect();
        manifest.remove_ids(&removed);
        manifest.add_entry(final_entry);
        if debug {
            eprintln!("[compaction] done; removed {:?}", removed);
        }
        Ok(())
    }

    fn merge_stream(
        &self,
        entries: &[SstMeta],
        paths: &StoragePaths,
    ) -> Result<Option<RangeIterator>> {
        let mut iters = Vec::new();
        for (idx, meta) in entries.iter().enumerate() {
            let path = paths.sst_dir().join(&meta.file_name);
            // Use O_DIRECT for compaction reads to bypass page cache.
            // Compaction data is read once and won't be reused, so caching is wasteful.
            // Falls back to regular open if O_DIRECT is not supported (e.g., tmpfs in tests).
            let handle = Arc::new(SstHandle::open_direct(&path)?);
            let slot = self.read_slot_base + idx as u32;
            // Use async reads for compaction to avoid blocking the submission thread.
            // The kernel will offload large file reads to io-wq threads.
            let reader = SstReader::new(
                handle,
                self.ring.clone(),
                slot,
                BLOCK_SIZE,
                4,
                meta.id,
                None, // No block cache for compaction
            )?;
            iters.push(reader.into_pipelined_iter(4)?);
        }
        RangeIterator::from_iters(iters, RangeSpec::full())
    }
}

enum CompactionCommand {
    Run(Option<mpsc::Sender<()>>),
    Shutdown,
}

pub struct CompactionQueue {
    sender: mpsc::Sender<CompactionCommand>,
    handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl CompactionQueue {
    pub fn start(
        compactor: Compactor,
        manifest: Arc<Mutex<Manifest>>,
        paths: StoragePaths,
    ) -> Self {
        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            while let Ok(cmd) = rx.recv() {
                match cmd {
                    CompactionCommand::Run(reply) => {
                        if let Ok(mut manifest_guard) = manifest.lock()
                            && compactor
                                .maybe_compact(&mut manifest_guard, &paths)
                                .is_ok()
                            {
                                let _ = manifest_guard.sync();
                            }
                        if let Some(tx) = reply {
                            let _ = tx.send(());
                        }
                    }
                    CompactionCommand::Shutdown => break,
                }
            }
        });
        Self {
            sender: tx,
            handle: Mutex::new(Some(handle)),
        }
    }

    pub fn schedule(&self) {
        let _ = self.sender.send(CompactionCommand::Run(None));
    }

    pub fn schedule_blocking(&self) -> Result<()> {
        let (tx, rx) = mpsc::channel();
        self.sender
            .send(CompactionCommand::Run(Some(tx)))
            .map_err(|e| anyhow!("compaction queue closed: {e}"))?;
        rx.recv()
            .map_err(|e| anyhow!("compaction worker exited: {e}"))?;
        Ok(())
    }
}

impl Drop for CompactionQueue {
    fn drop(&mut self) {
        let _ = self.sender.send(CompactionCommand::Shutdown);
        if let Some(handle) = self.handle.lock().unwrap().take() {
            let _ = handle.join();
        }
    }
}
