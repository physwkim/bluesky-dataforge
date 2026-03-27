use thiserror::Error;

/// Core error type for the DAQ system.
#[derive(Debug, Error)]
pub enum DaqError {
    #[error("buffer underflow: requested {requested} bytes, available {available}")]
    BufferUnderflow { requested: usize, available: usize },

    #[error("buffer overflow: requested {requested} bytes, capacity {capacity}")]
    BufferOverflow { requested: usize, capacity: usize },

    #[error("pool exhausted: no free buffers available")]
    PoolExhausted,

    #[error("invalid frame: {0}")]
    InvalidFrame(String),

    #[error("stream closed")]
    StreamClosed,

    #[error("timeout after {0:?}")]
    Timeout(std::time::Duration),

    #[error("status already finished")]
    AlreadyFinished,

    #[error("{0}")]
    Other(String),
}

pub type DaqResult<T> = Result<T, DaqError>;
