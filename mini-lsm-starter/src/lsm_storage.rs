use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use parking_lot::{Mutex, RwLock};

use crate::block::Block;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::mem_table::MemTable;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

#[derive(Clone)]
pub struct LsmStorageInner {
    /// The current memtable.
    memtable: Arc<MemTable>,
    /// Immutable memTables, from earliest to latest.
    imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SsTables, from earliest to latest.
    l0_sstables: Vec<Arc<SsTable>>,
    /// L1 - L6 SsTables, sorted by key range.
    #[allow(dead_code)]
    levels: Vec<Vec<Arc<SsTable>>>,
    /// The next SSTable ID.
    next_sst_id: usize,
}

impl LsmStorageInner {
    fn create() -> Self {
        Self {
            memtable: Arc::new(MemTable::create()),
            imm_memtables: vec![],
            l0_sstables: vec![],
            levels: vec![],
            next_sst_id: 1,
        }
    }
}

/// The storage interface of the LSM tree.
pub struct LsmStorage {
    inner: Arc<RwLock<Arc<LsmStorageInner>>>,
    sync_lock: Mutex<()>,
    path: PathBuf,
    block_cache: Arc<BlockCache>,
}

impl LsmStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            inner: Arc::new(RwLock::new(Arc::new(LsmStorageInner::create()))),
            sync_lock: Mutex::new(()),
            path: path.as_ref().to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1 << 20)), // 4GB block cache
        })
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let inner = self.inner.read().clone();
        if let Some(value) = inner.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for table in inner.imm_memtables.iter().rev() {
            if let Some(value) = table.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        for sst in inner.l0_sstables.iter().rev() {
            let iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
            if iter.is_valid() {
                if iter.key() == key {
                    if iter.value().is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(Bytes::copy_from_slice(iter.value())));
                }
            } else {
                break;
            }
        }
        Ok(None)
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        assert!(!value.is_empty(), "value cannot be empty");
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.read().memtable.put(key, value);
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        assert!(!key.is_empty(), "key cannot be empty");
        self.inner.read().memtable.put(key, b"");
        Ok(())
    }

    /// Persist data to disk.
    ///
    /// In day 3: flush the current memtable to disk as L0 SST.
    /// In day 6: call `fsync` on WAL.
    pub fn sync(&self) -> Result<()> {
        let _sync_guard = self.sync_lock.lock();

        // Move mutable memtable to immutable memtables.
        let (memtable, sst_id) = {
            let mut inner_guard = self.inner.write();
            let inner = Arc::make_mut(&mut inner_guard); // if count>1, clone
            let memtable = std::mem::replace(&mut inner.memtable, Arc::new(MemTable::create()));
            inner.imm_memtables.push(memtable.clone());
            (memtable, inner.next_sst_id)
        };

        // Flush memtable to disk as an SST file without holding any lock
        let mut builder = SsTableBuilder::new(4096);
        memtable.flush(&mut builder)?;
        // Write to disk
        let sst = builder.build(
            sst_id,
            Some(self.block_cache.clone()),
            self.path_of_sst(sst_id),
        )?;

        let mut inner_guard = self.inner.write();
        let inner = Arc::make_mut(&mut inner_guard); // if count>1, clone
        inner.imm_memtables.pop();
        inner.l0_sstables.push(Arc::new(sst));
        inner.next_sst_id += 1;

        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let inner = self.inner.read().clone();
        let mt_iter: Vec<_> = std::iter::once(&inner.memtable)
            .chain(inner.imm_memtables.iter().rev())
            .map(|mt| {
                let iter = mt.scan(lower, upper);
                Box::new(iter)
            })
            .collect();
        let sst_iter = inner
            .l0_sstables
            .iter()
            .rev()
            .map(|sst| {
                let iter = match lower {
                    Bound::Included(key) => {
                        SsTableIterator::create_and_seek_to_key(sst.clone(), key)?
                    }
                    Bound::Excluded(key) => {
                        let mut iter = SsTableIterator::create_and_seek_to_key(sst.clone(), key)?;
                        if iter.is_valid() && iter.key() == key {
                            iter.next()?;
                        }
                        iter
                    }
                    Bound::Unbounded => SsTableIterator::create_and_seek_to_first(sst.clone())?,
                };
                Ok(Box::new(iter))
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                MergeIterator::create(mt_iter),
                MergeIterator::create(sst_iter),
            )?,
            upper.map(Bytes::copy_from_slice),
        )?))
    }

    fn path_of_sst(&self, id: usize) -> PathBuf {
        self.path.join(format!("{:05}.sst", id))
    }
}
