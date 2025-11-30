use std::path::PathBuf;

use anyhow::Result;

use crate::storage::StoragePaths;

#[derive(Clone)]
pub struct SstMeta {
    pub id: u64,
    pub level: u32,
    pub file_name: String,
    pub key_min: Vec<u8>,
    pub key_max: Vec<u8>,
}

#[derive(Clone)]
pub struct Manifest {
    path: PathBuf,
    entries: Vec<SstMeta>,
    next_id: u64,
}

impl Manifest {
    pub fn load(paths: &StoragePaths) -> Result<Self> {
        let path = paths.manifest_path().to_path_buf();
        let data = std::fs::read_to_string(&path).unwrap_or_default();
        let mut entries = Vec::new();
        let mut max_id = 0;
        for line in data.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            let parts: Vec<_> = trimmed.split_whitespace().collect();
            let entry = match parts.as_slice() {
                [id, level, file, min_hex, max_hex] => {
                    let id: u64 = id.parse()?;
                    let level: u32 = level.parse()?;
                    let key_min = hex::decode(min_hex)?;
                    let key_max = hex::decode(max_hex)?;
                    SstMeta {
                        id,
                        level,
                        file_name: file.to_string(),
                        key_min,
                        key_max,
                    }
                }
                [id, file] => {
                    let id: u64 = id.parse()?;
                    SstMeta {
                        id,
                        level: 0,
                        file_name: file.to_string(),
                        key_min: Vec::new(),
                        key_max: Vec::new(),
                    }
                }
                _ => continue,
            };
            max_id = max_id.max(entry.id);
            entries.push(entry);
        }
        Ok(Self {
            path,
            entries,
            next_id: max_id + 1,
        })
    }

    pub fn cleanup_missing_files(&mut self, paths: &StoragePaths) -> Result<()> {
        self.entries
            .retain(|entry| paths.sst_dir().join(&entry.file_name).exists());
        self.sync()
    }

    pub fn entries(&self) -> &[SstMeta] {
        &self.entries
    }

    pub fn entries_by_priority(&self) -> Vec<SstMeta> {
        self.entries.clone()
    }

    pub fn allocate_name(&mut self, entries: &[(Vec<u8>, Vec<u8>)]) -> SstMeta {
        let id = self.next_id;
        self.next_id += 1;
        let key_min = entries.first().map(|(k, _)| k.clone()).unwrap_or_default();
        let key_max = entries.last().map(|(k, _)| k.clone()).unwrap_or_default();
        SstMeta {
            id,
            level: 0,
            file_name: format!("sst-{id:08}.sst"),
            key_min,
            key_max,
        }
    }

    pub fn add_entry(&mut self, entry: SstMeta) {
        self.entries.push(entry);
    }

    pub fn remove_ids(&mut self, ids: &[u64]) {
        self.entries.retain(|entry| !ids.contains(&entry.id));
    }

    pub fn sync(&self) -> Result<()> {
        let tmp = self.path.with_extension("tmp");
        let mut body = String::new();
        for entry in &self.entries {
            body.push_str(&format!(
                "{} {} {} {} {}\n",
                entry.id,
                entry.level,
                entry.file_name,
                hex::encode(&entry.key_min),
                hex::encode(&entry.key_max)
            ));
        }
        std::fs::write(&tmp, body)?;
        std::fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}
