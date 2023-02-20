use bytes::Buf;
use std::sync::Arc;

use super::{Block, SIZEOF_U16};

/// Iterates on a block.
pub struct BlockIterator {
    block: Arc<Block>,
    key: Vec<u8>,
    value: Vec<u8>,
    idx: usize,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: Vec::new(),
            value: Vec::new(),
            idx: 0,
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut iter = Self::new(block);
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: &[u8]) -> Self {
        let mut s = Self::create_and_seek_to_first(block);
        s.seek_to_key(key);
        s
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> &[u8] {
        &self.key
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        &self.value
    }

    /// Returns true if the iterator is valid (not end).
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty() // At end of iter, key is `clear`
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_to(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.idx += 1;
        self.seek_to(self.idx);
    }

    /// Seek to the first key that >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) {
        use std::cmp::Ordering::*;
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = (left + right) / 2;
            self.seek_to(mid);
            match self.key().cmp(key) {
                Less => left = mid + 1,
                Equal => return,
                Greater => right = mid,
            }
        }
        self.seek_to(right);
    }

    fn seek_to(&mut self, idx: usize) {
        if idx >= self.block.offsets.len() {
            self.key.clear();
            self.value.clear();
            return;
        }
        let offset = self.block.offsets[idx] as usize;
        let key_len = (&self.block.data[offset..offset + SIZEOF_U16]).get_u16() as usize;
        let key_end = offset + SIZEOF_U16 + key_len;
        self.key = self.block.data[offset + SIZEOF_U16..key_end].to_vec();
        let value_len = (&self.block.data[key_end..key_end + SIZEOF_U16]).get_u16() as usize;
        self.value =
            self.block.data[key_end + SIZEOF_U16..key_end + SIZEOF_U16 + value_len].to_vec();
        self.idx = idx;
    }
}
