use bytes::{Bytes, BytesMut};

use crate::error::{DaqError, DaqResult};

/// A raw memory block with header and tail room for zero-copy protocol framing.
#[derive(Debug, Clone)]
pub struct Buffer {
    inner: BytesMut,
    head_room: usize,
    data_start: usize,
    data_end: usize,
}

impl Buffer {
    /// Create a new buffer with the given total capacity and reserved header room.
    pub fn new(capacity: usize, head_room: usize) -> Self {
        assert!(head_room <= capacity, "head_room must be <= capacity");
        let mut inner = BytesMut::zeroed(capacity);
        inner.clear();
        inner.resize(capacity, 0);
        Self {
            inner,
            head_room,
            data_start: head_room,
            data_end: head_room,
        }
    }

    /// Create a buffer from existing bytes (no header room).
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut inner = BytesMut::with_capacity(data.len());
        inner.extend_from_slice(data);
        Self {
            inner,
            head_room: 0,
            data_start: 0,
            data_end: data.len(),
        }
    }

    /// Total capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.inner.len()
    }

    /// Number of bytes of payload data currently stored.
    pub fn len(&self) -> usize {
        self.data_end - self.data_start
    }

    /// Whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Available tail room for appending data.
    pub fn tail_room(&self) -> usize {
        self.inner.len() - self.data_end
    }

    /// Available header room for prepending headers.
    pub fn head_room(&self) -> usize {
        self.data_start
    }

    /// Read the payload data.
    pub fn data(&self) -> &[u8] {
        &self.inner[self.data_start..self.data_end]
    }

    /// Append data to the tail of the buffer.
    pub fn append(&mut self, data: &[u8]) -> DaqResult<()> {
        if data.len() > self.tail_room() {
            return Err(DaqError::BufferOverflow {
                requested: data.len(),
                capacity: self.tail_room(),
            });
        }
        self.inner[self.data_end..self.data_end + data.len()].copy_from_slice(data);
        self.data_end += data.len();
        Ok(())
    }

    /// Prepend data into the header room.
    pub fn prepend(&mut self, data: &[u8]) -> DaqResult<()> {
        if data.len() > self.head_room() {
            return Err(DaqError::BufferOverflow {
                requested: data.len(),
                capacity: self.head_room(),
            });
        }
        self.data_start -= data.len();
        self.inner[self.data_start..self.data_start + data.len()].copy_from_slice(data);
        Ok(())
    }

    /// Freeze this buffer into an immutable `Bytes`.
    pub fn freeze(self) -> Bytes {
        let start = self.data_start;
        let end = self.data_end;
        Bytes::from(self.inner.freeze()).slice(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_basic() {
        let mut buf = Buffer::new(128, 16);
        assert_eq!(buf.capacity(), 128);
        assert_eq!(buf.head_room(), 16);
        assert_eq!(buf.tail_room(), 128 - 16);
        assert!(buf.is_empty());

        buf.append(b"hello").unwrap();
        assert_eq!(buf.len(), 5);
        assert_eq!(buf.data(), b"hello");

        buf.prepend(b"HDR:").unwrap();
        assert_eq!(buf.data(), b"HDR:hello");
    }

    #[test]
    fn test_buffer_overflow() {
        let mut buf = Buffer::new(8, 0);
        buf.append(b"12345678").unwrap();
        assert!(buf.append(b"x").is_err());
    }

    #[test]
    fn test_buffer_freeze() {
        let mut buf = Buffer::new(32, 4);
        buf.append(b"data").unwrap();
        let frozen = buf.freeze();
        assert_eq!(&frozen[..], b"data");
    }
}
