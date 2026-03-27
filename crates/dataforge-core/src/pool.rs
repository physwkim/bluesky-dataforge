use std::sync::Arc;

use parking_lot::Mutex;

use crate::buffer::Buffer;
use crate::error::{DaqError, DaqResult};

/// Trait for buffer pool allocators.
pub trait PoolAllocator: Send + Sync {
    /// Allocate a buffer from the pool.
    fn allocate(&self) -> DaqResult<Buffer>;

    /// Return a buffer to the pool.
    fn release(&self, buf: Buffer);

    /// Number of free buffers currently available.
    fn free_count(&self) -> usize;

    /// Total number of buffers managed by the pool.
    fn total_count(&self) -> usize;
}

/// Default free-list based buffer pool.
pub struct DefaultPool {
    inner: Arc<Mutex<PoolInner>>,
    buf_capacity: usize,
    head_room: usize,
}

struct PoolInner {
    free_list: Vec<Buffer>,
    total: usize,
}

impl DefaultPool {
    /// Create a new pool pre-allocated with `count` buffers.
    pub fn new(count: usize, buf_capacity: usize, head_room: usize) -> Self {
        let mut free_list = Vec::with_capacity(count);
        for _ in 0..count {
            free_list.push(Buffer::new(buf_capacity, head_room));
        }
        Self {
            inner: Arc::new(Mutex::new(PoolInner {
                free_list,
                total: count,
            })),
            buf_capacity,
            head_room,
        }
    }
}

impl PoolAllocator for DefaultPool {
    fn allocate(&self) -> DaqResult<Buffer> {
        let mut inner = self.inner.lock();
        inner.free_list.pop().ok_or(DaqError::PoolExhausted)
    }

    fn release(&self, _buf: Buffer) {
        let mut inner = self.inner.lock();
        // Return a fresh buffer instead of the used one to avoid data leaks
        inner.free_list.push(Buffer::new(self.buf_capacity, self.head_room));
    }

    fn free_count(&self) -> usize {
        self.inner.lock().free_list.len()
    }

    fn total_count(&self) -> usize {
        self.inner.lock().total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_allocate_release() {
        let pool = DefaultPool::new(4, 256, 16);
        assert_eq!(pool.free_count(), 4);
        assert_eq!(pool.total_count(), 4);

        let buf1 = pool.allocate().unwrap();
        assert_eq!(pool.free_count(), 3);
        assert_eq!(buf1.capacity(), 256);

        pool.release(buf1);
        assert_eq!(pool.free_count(), 4);
    }

    #[test]
    fn test_pool_exhaustion() {
        let pool = DefaultPool::new(1, 64, 0);
        let _buf = pool.allocate().unwrap();
        assert!(pool.allocate().is_err());
    }
}
