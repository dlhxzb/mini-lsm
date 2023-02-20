use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::{BufMut, Bytes};

use super::{BlockMeta, FileObject, SsTable};
use crate::block::BlockBuilder;
use crate::lsm_storage::BlockCache;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    pub(super) meta: Vec<BlockMeta>,
    block_builder: BlockBuilder,
    blocks: Vec<u8>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        assert_ne!(block_size, 0);
        Self {
            meta: Vec::new(),
            block_builder: BlockBuilder::new(block_size),
            blocks: Vec::new(),
            block_size,
        }
    }

    /// Adds a key-value pair to SSTable
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        if self.block_builder.is_empty() {
            self.meta.push(BlockMeta {
                offset: self.estimated_size(),
                first_key: Bytes::copy_from_slice(key),
            });
        }
        if !self.block_builder.add(key, value) {
            let block =
                std::mem::replace(&mut self.block_builder, BlockBuilder::new(self.block_size))
                    .build()
                    .encode();
            self.blocks.extend(block);
            self.add(key, value);
        }
    }

    /// Get the estimated size of the SSTable.
    pub fn estimated_size(&self) -> usize {
        self.blocks.len()
    }

    /// Builds the SSTable and writes it to the given path. No need to actually write to disk until
    /// chapter 4 block cache.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        if !self.block_builder.is_empty() {
            let block = self.block_builder.build().encode();
            self.blocks.extend(block);
        }

        let mut buf = self.blocks;
        let meta_offset = buf.len();
        BlockMeta::encode_block_meta(&self.meta, &mut buf);
        buf.put_u32(meta_offset as u32);
        Ok(SsTable {
            file: FileObject::create(path.as_ref(), buf)?,
            block_metas: self.meta,
            block_meta_offset: meta_offset,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
