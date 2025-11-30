//! O(1) LRU Cache Implementation using Doubly Linked List + HashMap
//!
//! This provides constant-time operations for:
//! - get: O(1)
//! - insert: O(1)  
//! - evict: O(1)

use std::collections::HashMap;
use std::hash::Hash;

/// Node in the doubly linked list for O(1) LRU
struct LruNode<K, V> {
    key: K,
    value: V,
    prev: Option<usize>,
    next: Option<usize>,
}

/// O(1) LRU Cache using doubly linked list + HashMap
pub struct LruCache<K: Clone + Eq + Hash, V: Clone> {
    /// Storage for nodes (using indices instead of pointers for safety)
    nodes: Vec<Option<LruNode<K, V>>>,
    /// Map key -> node index
    map: HashMap<K, usize>,
    /// Free list of node indices
    free_list: Vec<usize>,
    /// Head of LRU list (least recently used)
    head: Option<usize>,
    /// Tail of LRU list (most recently used)
    tail: Option<usize>,
}

impl<K: Clone + Eq + Hash, V: Clone> LruCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        let mut nodes = Vec::with_capacity(capacity);
        let mut free_list = Vec::with_capacity(capacity);
        for i in 0..capacity {
            nodes.push(None);
            free_list.push(capacity - 1 - i);
        }
        Self {
            nodes,
            map: HashMap::with_capacity(capacity),
            free_list,
            head: None,
            tail: None,
        }
    }

    /// Get value and move to MRU position. O(1)
    pub fn get(&mut self, key: &K) -> Option<V> {
        let idx = *self.map.get(key)?;
        self.move_to_tail(idx);
        self.nodes[idx].as_ref().map(|n| n.value.clone())
    }

    /// Insert or update value. O(1)
    pub fn insert(&mut self, key: K, value: V) {
        if let Some(&idx) = self.map.get(&key) {
            if let Some(node) = self.nodes[idx].as_mut() {
                node.value = value;
            }
            self.move_to_tail(idx);
            return;
        }

        let idx = if let Some(free_idx) = self.free_list.pop() {
            free_idx
        } else {
            let evict_idx = self.head.expect("cache full but no head");
            self.remove_node(evict_idx);
            if let Some(node) = self.nodes[evict_idx].take() {
                self.map.remove(&node.key);
            }
            evict_idx
        };

        self.nodes[idx] = Some(LruNode {
            key: key.clone(),
            value,
            prev: None,
            next: None,
        });
        self.map.insert(key, idx);
        self.append_to_tail(idx);
    }

    /// Remove all entries matching a predicate. O(n)
    #[allow(dead_code)]
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&K, &V) -> bool,
    {
        let keys_to_remove: Vec<K> = self
            .map
            .keys()
            .filter(|k| {
                let idx = self.map[*k];
                if let Some(node) = &self.nodes[idx] {
                    !f(&node.key, &node.value)
                } else {
                    true
                }
            })
            .cloned()
            .collect();

        for key in keys_to_remove {
            if let Some(idx) = self.map.remove(&key) {
                self.remove_node(idx);
                self.nodes[idx] = None;
                self.free_list.push(idx);
            }
        }
    }

    fn remove_node(&mut self, idx: usize) {
        let (prev, next) = {
            let node = match self.nodes[idx].as_ref() {
                Some(n) => n,
                None => return,
            };
            (node.prev, node.next)
        };

        if let Some(prev_idx) = prev {
            if let Some(prev_node) = self.nodes[prev_idx].as_mut() {
                prev_node.next = next;
            }
        } else {
            self.head = next;
        }

        if let Some(next_idx) = next {
            if let Some(next_node) = self.nodes[next_idx].as_mut() {
                next_node.prev = prev;
            }
        } else {
            self.tail = prev;
        }

        if let Some(node) = self.nodes[idx].as_mut() {
            node.prev = None;
            node.next = None;
        }
    }

    fn append_to_tail(&mut self, idx: usize) {
        if let Some(tail_idx) = self.tail {
            if let Some(tail_node) = self.nodes[tail_idx].as_mut() {
                tail_node.next = Some(idx);
            }
            if let Some(node) = self.nodes[idx].as_mut() {
                node.prev = Some(tail_idx);
                node.next = None;
            }
            self.tail = Some(idx);
        } else {
            self.head = Some(idx);
            self.tail = Some(idx);
            if let Some(node) = self.nodes[idx].as_mut() {
                node.prev = None;
                node.next = None;
            }
        }
    }

    fn move_to_tail(&mut self, idx: usize) {
        if self.tail == Some(idx) {
            return;
        }
        self.remove_node(idx);
        self.append_to_tail(idx);
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.map.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_basic() {
        let mut cache: LruCache<i32, String> = LruCache::new(3);
        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string());

        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.get(&3), Some("three".to_string()));
    }

    #[test]
    fn test_lru_eviction() {
        let mut cache: LruCache<i32, String> = LruCache::new(2);
        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.insert(3, "three".to_string()); // Should evict 1

        assert_eq!(cache.get(&1), None);
        assert_eq!(cache.get(&2), Some("two".to_string()));
        assert_eq!(cache.get(&3), Some("three".to_string()));
    }

    #[test]
    fn test_lru_access_order() {
        let mut cache: LruCache<i32, String> = LruCache::new(2);
        cache.insert(1, "one".to_string());
        cache.insert(2, "two".to_string());
        cache.get(&1); // Access 1, making 2 the LRU
        cache.insert(3, "three".to_string()); // Should evict 2

        assert_eq!(cache.get(&1), Some("one".to_string()));
        assert_eq!(cache.get(&2), None);
        assert_eq!(cache.get(&3), Some("three".to_string()));
    }
}