use std::any;
use std::sync::Arc;

use anyhow::Result;

use super::SsTable;
use crate::block::BlockIterator;
use crate::iterators::StorageIterator;

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    block_iter: BlockIterator,
    block_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let block_iter = BlockIterator::create_and_seek_to_first(table.read_block(0)?);
        Ok(SsTableIterator {
            table,
            block_iter,
            block_idx: 0,
        })
    }

    /// Seek to the first key-value pair.
    pub fn seek_to_first(&mut self) -> Result<()> {
        self.block_iter = BlockIterator::create_and_seek_to_first(self.table.read_block(0)?);
        self.block_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: &[u8]) -> Result<Self> {
        Self::seek_to_key_inner(&table, key).map(|(block_idx, block_iter)| Self {
            table,
            block_iter,
            block_idx,
        })
    }

    /// Seek to the first key-value pair which >= `key`.
    pub fn seek_to_key(&mut self, key: &[u8]) -> Result<()> {
        let (block_idx, block_iter) = Self::seek_to_key_inner(&self.table, key)?;
        self.block_idx = block_idx;
        self.block_iter = block_iter;
        Ok(())
    }

    pub fn seek_to_key_inner(table: &SsTable, key: &[u8]) -> Result<(usize, BlockIterator)> {
        let mut block_idx = table.find_block_idx(key);
        let mut block_iter =
            BlockIterator::create_and_seek_to_key(table.read_block(block_idx)?, key);
        if !block_iter.is_valid() && block_idx + 1 < table.num_of_blocks() {
            block_idx += 1;
            block_iter = BlockIterator::create_and_seek_to_first(table.read_block(block_idx)?);
        }

        Ok((block_idx, block_iter))
    }
}

impl StorageIterator for SsTableIterator {
    fn value(&self) -> &[u8] {
        self.block_iter.value()
    }

    fn key(&self) -> &[u8] {
        self.block_iter.key()
    }

    fn is_valid(&self) -> bool {
        self.block_iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.block_iter.next();
        if !self.is_valid() {
            self.block_idx += 1;
            if self.block_idx < self.table.num_of_blocks() {
                self.block_iter =
                    BlockIterator::create_and_seek_to_first(self.table.read_block(self.block_idx)?);
            }
        }
        Ok(())
    }
}
