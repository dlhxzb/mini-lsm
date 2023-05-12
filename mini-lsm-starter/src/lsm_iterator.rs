use std::ops::Bound;

use anyhow::Result;
use bytes::Bytes;

use crate::iterators::{merge_iterator::MergeIterator, StorageIterator};

pub struct LsmIterator {
    iter: MergeIterator,
    end_bound: Bound<Bytes>,
    is_valid: bool,
}

impl LsmIterator {
    pub fn new(iter: MergeIterator, end_bound: Bound<Bytes>) -> Result<Self> {
        let mut res = LsmIterator {
            is_valid: iter.is_valid(),
            iter,
            end_bound,
        };
        if res.is_valid && res.value().is_empty() {
            res.next()?;
        }
        Ok(res)
    }
}
impl StorageIterator for LsmIterator {
    fn is_valid(&self) -> bool {
        self.is_valid
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        while self.is_valid {
            self.iter.next()?;
            if !self.iter.is_valid() {
                self.is_valid = false;
                break;
            }
            match self.end_bound.as_ref() {
                Bound::Unbounded => {}
                Bound::Included(end) => self.is_valid = self.key() <= end.as_ref(),
                Bound::Excluded(end) => self.is_valid = self.key() < end.as_ref(),
            }
            // skip deleted item
            if !self.value().is_empty() {
                break;
            }
        }
        Ok(())
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self { iter }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn key(&self) -> &[u8] {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.is_valid() {
            self.iter.next()?;
        }
        Ok(())
    }
}
