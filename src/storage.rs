use std::{
    fs::{self, File},
    path::{Path, PathBuf},
};

use anyhow::{anyhow, Result};

#[derive(Clone)]
pub struct StoragePaths {
    pub root: PathBuf,
    wal: PathBuf,
    manifest: PathBuf,
    sst_dir: PathBuf,
}

impl StoragePaths {
    pub fn new(root: PathBuf) -> Result<Self> {
        if root.as_os_str().is_empty() {
            return Err(anyhow!("data root cannot be empty"));
        }
        fs::create_dir_all(&root)?;
        let sst_dir = root.join("sst");
        fs::create_dir_all(&sst_dir)?;
        let wal = root.join("wal.log");
        if !wal.exists() {
            File::create(&wal)?;
        }
        let manifest = root.join("MANIFEST");
        if !manifest.exists() {
            fs::write(&manifest, b"")?;
        }
        Ok(Self {
            root,
            wal,
            manifest,
            sst_dir,
        })
    }

    pub fn wal_path(&self) -> &Path {
        &self.wal
    }

    pub fn manifest_path(&self) -> &Path {
        &self.manifest
    }

    pub fn sst_dir(&self) -> &Path {
        &self.sst_dir
    }

}
