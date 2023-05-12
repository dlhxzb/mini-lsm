mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

pub const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub const SIZEOF_U32: usize = std::mem::size_of::<u32>();

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted
/// key-value pairs.
pub struct Block {
    data: Vec<u8>,
    offsets: Vec<u16>,
}

impl Block {
    // TODO: Compress and checksum. flate2(miniz_oxide) or snap or brotli?
    pub fn encode(&self) -> Bytes {
        let num_of_elements = self.offsets.len();
        let mut buf = BytesMut::with_capacity((num_of_elements + 1) * SIZEOF_U16 + self.data.len());
        buf.extend_from_slice(&self.data);
        let ptr = self.offsets.as_ptr().cast::<u8>();
        // SAFETY: from_raw_parts here is safe, since offsets in len is always available
        let offsets_u8 =
            unsafe { std::slice::from_raw_parts(ptr, self.offsets.len() * SIZEOF_U16) };
        buf.extend_from_slice(offsets_u8);
        buf.put_u16(num_of_elements as u16);
        buf.into()
    }

    pub fn decode(data: &[u8]) -> Self {
        let num_of_elements = (&data[data.len() - SIZEOF_U16..]).get_u16() as usize;
        let mut s = Self {
            data: Vec::with_capacity(data.len() - num_of_elements * SIZEOF_U16 - SIZEOF_U16),
            offsets: Vec::with_capacity(num_of_elements),
        };

        let row_data_end = data.len() - SIZEOF_U16 - num_of_elements * SIZEOF_U16;
        let mut row_data = &data[..row_data_end];
        // SAFTY: will copy row_data_end into s.data, num_of_elements into s.offsets
        unsafe {
            s.data.set_len(row_data_end);
            s.offsets.set_len(num_of_elements)
        }
        row_data.copy_to_slice(&mut s.data);

        let ptr = s.offsets.as_mut_ptr().cast::<u8>();
        // SAFETY: from_raw_parts here is safe, since offsets in len is always available
        let offsets_u8 =
            unsafe { std::slice::from_raw_parts_mut(ptr, num_of_elements * SIZEOF_U16) };
        let mut row_offsets = &data[row_data_end..data.len() - SIZEOF_U16];
        row_offsets.copy_to_slice(offsets_u8);
        s
    }
}

#[cfg(test)]
mod tests;
