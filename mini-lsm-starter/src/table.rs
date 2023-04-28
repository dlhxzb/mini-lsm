mod builder;
mod iterator;

use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use bytes::{Buf, BufMut, Bytes};

pub use builder::SsTableBuilder;
pub use iterator::SsTableIterator;

use crate::block::{Block, SIZEOF_U16, SIZEOF_U32};
use crate::lsm_storage::BlockCache;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: Bytes,
}

impl BlockMeta {
    /// Encode block meta to a buffer.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let estimated_size = block_meta.iter().fold(0, |acc, neta| {
            acc + SIZEOF_U16 + SIZEOF_U32 + neta.first_key.len()
        });
        buf.reserve(estimated_size);
        let original_len = buf.len();
        for meta in block_meta {
            buf.put_u32(meta.offset as u32);
            buf.put_u16(meta.first_key.len() as u16);
            buf.put_slice(&meta.first_key);
        }
        assert_eq!(estimated_size, buf.len() - original_len);
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(mut buf: impl Buf) -> Vec<BlockMeta> {
        let mut block_meta = Vec::new();
        while buf.has_remaining() {
            let offset = buf.get_u32() as usize;
            let first_key_len = buf.get_u16() as usize;
            let first_key = buf.copy_to_bytes(first_key_len);
            block_meta.push(BlockMeta { offset, first_key });
        }
        block_meta
    }
}

/// A file object.
pub struct FileObject(pub Bytes);

impl FileObject {
    pub fn read(&self, offset: usize, len: usize) -> Result<Vec<u8>> {
        Ok(self.0[offset..offset + len].to_vec())
    }

    pub fn size(&self) -> usize {
        self.0.len()
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(_path: &Path, data: Vec<u8>) -> Result<Self> {
        Ok(FileObject(data.into()))
    }

    pub fn open(_path: &Path) -> Result<Self> {
        unimplemented!()
    }
}

/// ```
/// | data block | data block | data block | data block | meta block | meta block offset (u32) |
/// ```
pub struct SsTable {
    file: FileObject,
    block_metas: Vec<BlockMeta>,
    block_meta_offset: usize,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    /// Open SSTable from a file.
    pub fn open(
        _id: usize,
        _block_cache: Option<Arc<BlockCache>>,
        file: FileObject,
    ) -> Result<Self> {
        let block_meta_offset = (&file.0[file.size() - SIZEOF_U32..]).get_u32() as usize;
        let block_metas =
            BlockMeta::decode_block_meta(&file.0[block_meta_offset..file.size() - SIZEOF_U32]);
        Ok(Self {
            file,
            block_metas,
            block_meta_offset,
        })
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        let offset = self
            .block_metas
            .get(block_idx)
            .context(format!("block_idx:{block_idx} overflow of SsTable"))?
            .offset;
        let offset_end = self
            .block_metas
            .get(block_idx + 1)
            .map(|x| x.offset)
            .unwrap_or(self.block_meta_offset);
        self.file
            .read(offset, offset_end - offset)
            .map(|v| Arc::new(Block::decode(&v)))
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, _block_idx: usize) -> Result<Arc<Block>> {
        unimplemented!()
    }

    /// Find the block that may contain `key`.
    pub fn find_block_idx(&self, key: &[u8]) -> usize {
        self.block_metas
            .partition_point(|meta| meta.first_key <= key)
            .saturating_sub(1)
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_metas.len()
    }
}

#[cfg(test)]
mod tests;
