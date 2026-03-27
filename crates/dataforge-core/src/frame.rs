use std::collections::HashMap;

use crate::buffer::Buffer;

/// Metadata associated with a frame.
#[derive(Debug, Clone, Default)]
pub struct FrameMeta {
    pub channel: u32,
    pub flags: u32,
    pub timestamp: f64,
    pub sequence: u64,
    pub extra: HashMap<String, String>,
}

/// A frame is a chain of buffers with associated metadata.
/// Used to represent a single acquisition event or data packet.
#[derive(Debug)]
pub struct Frame {
    pub meta: FrameMeta,
    buffers: Vec<Buffer>,
}

impl Frame {
    /// Create a new empty frame with default metadata.
    pub fn new() -> Self {
        Self {
            meta: FrameMeta::default(),
            buffers: Vec::new(),
        }
    }

    /// Create a frame with the given metadata.
    pub fn with_meta(meta: FrameMeta) -> Self {
        Self {
            meta,
            buffers: Vec::new(),
        }
    }

    /// Add a buffer to the frame's buffer chain.
    pub fn push_buffer(&mut self, buf: Buffer) {
        self.buffers.push(buf);
    }

    /// Number of buffers in the chain.
    pub fn buffer_count(&self) -> usize {
        self.buffers.len()
    }

    /// Total payload size across all buffers.
    pub fn total_size(&self) -> usize {
        self.buffers.iter().map(|b| b.len()).sum()
    }

    /// Iterate over buffers.
    pub fn buffers(&self) -> &[Buffer] {
        &self.buffers
    }

    /// Consume the frame and return its buffers.
    pub fn into_buffers(self) -> Vec<Buffer> {
        self.buffers
    }
}

impl Default for Frame {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_frame_basics() {
        let mut frame = Frame::new();
        assert_eq!(frame.buffer_count(), 0);
        assert_eq!(frame.total_size(), 0);

        let mut buf = Buffer::new(64, 0);
        buf.append(b"payload1").unwrap();
        frame.push_buffer(buf);

        let mut buf2 = Buffer::new(64, 0);
        buf2.append(b"payload2").unwrap();
        frame.push_buffer(buf2);

        assert_eq!(frame.buffer_count(), 2);
        assert_eq!(frame.total_size(), 16);
    }
}
