use std::{
    fs::File,
    io::Read,
    os::unix::fs::OpenOptionsExt,
    path::{Path, PathBuf},
};

use anyhow::Result;

pub fn open_o_dsync(path: &Path, truncate: bool) -> Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .create(true)
        .write(true)
        .read(true)
        .custom_flags(libc::O_DSYNC);
    if truncate {
        options.truncate(true);
    }
    Ok(options.open(path)?)
}

/// Open a file with O_DIRECT | O_DSYNC for IOPOLL-compatible writes.
///
/// Caller must ensure offsets/lengths and buffers are properly aligned.
pub fn open_o_direct_dsync(path: &Path, truncate: bool) -> Result<File> {
    let mut options = std::fs::OpenOptions::new();
    options
        .create(true)
        .write(true)
        .read(true)
        .custom_flags(libc::O_DIRECT | libc::O_DSYNC);
    if truncate {
        options.truncate(true);
    }
    Ok(options.open(path)?)
}

/// Open a file with O_DIRECT for bypassing the page cache.
///
/// This is useful for large sequential reads where the data won't be reused,
/// such as compaction reads. The caller must ensure:
/// 1. Read buffers are page-aligned (use BufRing or page-aligned allocations)
/// 2. Read offsets are block-aligned (typically 512 bytes or 4KB)
/// 3. Read sizes are multiples of the block size
///
/// Note: O_DIRECT may fail on some filesystems (e.g., tmpfs). In that case,
/// fall back to regular file opening.
pub fn open_o_direct(path: &Path) -> Result<File> {
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?)
}

pub fn tmp_name(path: &Path) -> PathBuf {
    let mut tmp = path.to_path_buf();
    let mut ext = tmp
        .extension()
        .map(|s| s.to_os_string())
        .unwrap_or_default();
    if ext.is_empty() {
        ext.push("tmp");
    } else {
        ext.push(".tmp");
    }
    tmp.set_extension(ext);
    tmp
}

pub fn simple_checksum(data: &[u8]) -> u32 {
    let mut sum = 0u32;
    for byte in data {
        sum = sum.wrapping_mul(16777619) ^ (*byte as u32);
    }
    sum
}

pub fn hash_pair(data: &[u8]) -> (u64, u64) {
    let mut h1 = 0x9e37_79b1_85eb_ca87u64;
    let mut h2 = 0xc2b2_ae3d_27d4_eb4fu64;
    for &b in data {
        h1 ^= b as u64;
        h1 = h1.wrapping_mul(0xff51_afd7_ed55_8ccdu64);
        h1 ^= h1 >> 33;
        h2 ^= (b as u64).wrapping_add(0x9e37_79b1);
        h2 = h2.wrapping_mul(0xc4ce_b9fe_1a85_ec53u64);
        h2 ^= h2 >> 29;
    }
    (h1 | 1, h2 | 1)
}

pub fn read_file(path: &Path) -> Result<Vec<u8>> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    Ok(buf)
}

pub fn path_bytes(path: &Path) -> Vec<u8> {
    use std::os::unix::ffi::OsStrExt;
    path.as_os_str().as_bytes().to_vec()
}
