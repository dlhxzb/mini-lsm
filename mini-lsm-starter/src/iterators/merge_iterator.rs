use std::cmp::Ordering;
use std::collections::BinaryHeap;

use anyhow::Result;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other).unwrap() == Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match self.1.key().cmp(other.1.key()) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.0.cmp(&other.0),
        }
        .reverse()
        .into()
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, perfer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let iters: BinaryHeap<HeapWrapper<I>> = iters
            .into_iter()
            .filter(|iter| iter.is_valid())
            .enumerate()
            .map(|(idx, iter)| HeapWrapper(idx, iter))
            .collect();
        Self { iters }
    }
}

impl<I: StorageIterator> StorageIterator for MergeIterator<I> {
    fn key(&self) -> &[u8] {
        self.iters
            .peek()
            .map(|wrapper| wrapper.1.key())
            .expect("Un-init HeapWrapper")
    }

    fn value(&self) -> &[u8] {
        self.iters
            .peek()
            .map(|wrapper| wrapper.1.value())
            .expect("Un-init HeapWrapper")
    }

    fn is_valid(&self) -> bool {
        !self.iters.is_empty()
    }

    fn next(&mut self) -> Result<()> {
        use std::collections::binary_heap::PeekMut;

        if let Some(current) = self.iters.peek() {
            let current_key = current.1.key().to_vec();
            while let Some(mut wrapper) = self.iters.peek_mut() {
                if wrapper.1.key() != current_key {
                    return Ok(());
                }
                // skip same key
                wrapper.1.next()?;
                // remove empty iter
                if !wrapper.1.is_valid() {
                    PeekMut::pop(wrapper);
                }
            }
        }
        Ok(())
    }
}
