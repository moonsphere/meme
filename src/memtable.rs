#[derive(Clone)]
pub struct WalEntry {
    pub seq: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WalEntry {
    pub fn encoded_len(&self) -> usize {
        8 + 4 + 4 + self.key.len() + self.value.len()
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16 + self.key.len() + self.value.len());
        buf.extend_from_slice(&self.seq.to_le_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_le_bytes());
        buf.extend_from_slice(&(self.value.len() as u32).to_le_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        buf
    }
}

pub struct MemTable {
    table: SkipList,
    next_seq: u64,
    len: usize,
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

impl MemTable {
    pub fn new() -> Self {
        Self {
            table: SkipList::new(),
            next_seq: 1,
            len: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> WalEntry {
        let seq = self.next_seq;
        self.next_seq += 1;
        if self.table.insert(key.to_vec(), value.to_vec()) {
            self.len += 1;
        }
        WalEntry {
            seq,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.table.get(key)
    }

    pub fn replay_entry(&mut self, seq: u64, key: Vec<u8>, value: Vec<u8>) {
        if self.table.insert(key, value) {
            self.len += 1;
        }
        self.bump_seq_to(seq);
    }

    pub fn bump_seq_to(&mut self, seq: u64) {
        if seq >= self.next_seq {
            self.next_seq = seq + 1;
        }
    }

    pub fn drain_sorted(&mut self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let data = self.table.iter();
        self.table.clear();
        self.len = 0;
        data
    }
}

const MAX_SKIPLIST_HEIGHT: usize = 8;

struct SkipNode {
    key: Vec<u8>,
    value: Vec<u8>,
    next: [Option<usize>; MAX_SKIPLIST_HEIGHT],
}

struct SkipList {
    head: [Option<usize>; MAX_SKIPLIST_HEIGHT],
    nodes: Vec<SkipNode>,
    height: usize,
    rng: SimpleRng,
}

impl SkipList {
    fn new() -> Self {
        Self {
            head: [None; MAX_SKIPLIST_HEIGHT],
            nodes: Vec::new(),
            height: 1,
            rng: SimpleRng::new(0x5a5a_1234),
        }
    }

    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool {
        let mut prev = [None; MAX_SKIPLIST_HEIGHT];
        let mut current = None;
        for level in (0..self.height).rev() {
            loop {
                let next = self.next_at(current, level);
                match next {
                    Some(idx) if self.nodes[idx].key.as_slice() < key.as_slice() => {
                        current = Some(idx);
                    }
                    _ => break,
                }
            }
            prev[level] = current;
        }
        if let Some(idx) = self.next_at(prev[0], 0)
            && self.nodes[idx].key == key {
                self.nodes[idx].value = value;
                return false;
            }
        let mut node = SkipNode {
            key,
            value,
            next: [None; MAX_SKIPLIST_HEIGHT],
        };
        let level = self.random_level();
        #[allow(clippy::needless_range_loop)]
        for lvl in 0..level {
            node.next[lvl] = self.next_at(prev[lvl], lvl);
            self.set_next(prev[lvl], lvl, Some(self.nodes.len()));
        }
        self.nodes.push(node);
        true
    }

    fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let mut current = None;
        for level in (0..self.height).rev() {
            loop {
                let next = self.next_at(current, level);
                match next {
                    Some(idx) if self.nodes[idx].key.as_slice() <= key => {
                        current = Some(idx);
                        if self.nodes[idx].key.as_slice() == key {
                            return Some(self.nodes[idx].value.clone());
                        }
                    }
                    _ => break,
                }
            }
        }
        None
    }

    fn iter(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
        let mut out = Vec::new();
        let mut cursor = self.head[0];
        while let Some(idx) = cursor {
            let node = &self.nodes[idx];
            out.push((node.key.clone(), node.value.clone()));
            cursor = node.next[0];
        }
        out
    }

    fn clear(&mut self) {
        self.head = [None; MAX_SKIPLIST_HEIGHT];
        self.nodes.clear();
        self.height = 1;
    }

    fn next_at(&self, idx: Option<usize>, level: usize) -> Option<usize> {
        idx.map(|i| self.nodes[i].next[level])
            .unwrap_or(self.head[level])
    }

    fn set_next(&mut self, idx: Option<usize>, level: usize, next: Option<usize>) {
        if let Some(i) = idx {
            self.nodes[i].next[level] = next;
        } else {
            self.head[level] = next;
        }
    }

    fn random_level(&mut self) -> usize {
        let mut lvl = 1;
        while lvl < MAX_SKIPLIST_HEIGHT && self.rng.next() & 1 == 0 {
            lvl += 1;
        }
        if lvl > self.height {
            self.height = lvl;
        }
        lvl
    }
}

struct SimpleRng(u64);

impl SimpleRng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 7;
        x ^= x >> 9;
        x ^= x << 8;
        self.0 = x;
        x
    }
}

pub fn replay_wal(buf: &[u8], memtable: &mut MemTable) -> u64 {
    const WAL_ALIGN: usize = 4096;
    let mut offset = 0;
    let mut last_seq = 0;
    while offset + 4 <= buf.len() {
        let frame_len = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap()) as usize;
        if frame_len == 0 {
            break;
        }
        let total_len = 4 + frame_len;
        let padded_len = (total_len + WAL_ALIGN - 1) & !(WAL_ALIGN - 1);
        if offset + padded_len > buf.len() || frame_len == 0 {
            break;
        }
        let mut cursor = offset + 4;
        let end = cursor + frame_len;
        while cursor + 16 <= end {
            let seq = u64::from_le_bytes(buf[cursor..cursor + 8].try_into().unwrap());
            cursor += 8;
            let key_len =
                u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            let value_len =
                u32::from_le_bytes(buf[cursor..cursor + 4].try_into().unwrap()) as usize;
            cursor += 4;
            if cursor + key_len + value_len > end {
                break;
            }
            let key = buf[cursor..cursor + key_len].to_vec();
            cursor += key_len;
            let value = buf[cursor..cursor + value_len].to_vec();
            cursor += value_len;
            memtable.replay_entry(seq, key, value);
            last_seq = seq;
        }
        offset += padded_len;
    }
    last_seq
}
