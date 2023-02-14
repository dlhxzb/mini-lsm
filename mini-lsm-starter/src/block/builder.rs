use bytes::BufMut;

use super::{Block, SIZEOF_U16};

/// Builds a block.
pub struct BlockBuilder {
    block_size: usize,
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            block_size,
            data: Vec::new(),
            offsets: Vec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// ```
    /// |          data         |           offsets         |
    /// |entry|entry|entry|entry|offset|offset|offset|offset|num_of_elements|
    /// ```
    /// ```
    /// |                             entry1                            |
    /// | key_len (2B) | key (varlen) | value_len (2B) | value (varlen) | ... |
    /// ```
    #[must_use]
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> bool {
        assert!(!key.is_empty(), "key should not be empty");
        // 3 = key_len + value_len + offset
        if self.estimated_size() + key.len() + value.len() + 3 * SIZEOF_U16 > self.block_size {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key.len() as u16);
        self.data.put(key);
        self.data.put_u16(value.len() as u16);
        self.data.put(value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        assert!(!self.is_empty(), "block should not be empty");
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn estimated_size(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            self.offsets.len() * SIZEOF_U16 + self.data.len() + SIZEOF_U16
        }
    }
}
