use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;

use crate::iterators::StorageIterator;
use crate::table::SsTableBuilder;

/// A basic mem-table based on crossbeam-skiplist
pub struct MemTable {
    map: Arc<SkipMap<Bytes, Bytes>>,
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create() -> Self {
        Self {
            map: SkipMap::new().into(),
        }
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.map
            .get(key.as_ref())
            .map(|entry| entry.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    pub fn put(&self, key: &[u8], value: &[u8]) {
        self.map
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> MemTableIterator {
        let mut iter = MemTableIterator {
            iter: self.map.range((
                lower.map(Bytes::copy_from_slice),
                upper.map(Bytes::copy_from_slice),
            )),
            item: Default::default(),
            // map: self.map.clone(),
        };
        iter.next().unwrap();
        iter
    }
    /// Flush the mem-table to SSTable.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        while let Some(entry) = self.map.pop_front() {
            builder.add(entry.key(), entry.value())
        }
        Ok(())
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

/// An iterator over a range of `SkipMap`.
pub struct MemTableIterator<'a> {
    iter: SkipMapRangeIter<'a>,
    item: (Bytes, Bytes),
    // map: Arc<SkipMap<Bytes, Bytes>>,
}

impl StorageIterator for MemTableIterator<'_> {
    fn value(&self) -> &[u8] {
        &self.item.1
    }

    fn key(&self) -> &[u8] {
        &self.item.0
    }

    fn is_valid(&self) -> bool {
        !self.item.0.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        self.item = self
            .iter
            .next()
            .map(|entry| (entry.key().clone(), entry.value().clone()))
            .unwrap_or_default();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
