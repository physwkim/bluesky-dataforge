use crate::error::DaqResult;
use crate::frame::Frame;

/// A source of frames (e.g., a detector, file reader, network socket).
pub trait StreamSource: Send {
    /// Receive the next frame, blocking until available.
    fn recv(&mut self) -> DaqResult<Frame>;

    /// Try to receive a frame without blocking.
    fn try_recv(&mut self) -> DaqResult<Option<Frame>>;

    /// Close the stream source.
    fn close(&mut self);
}

/// A sink for frames (e.g., a file writer, network sender, processor).
pub trait StreamSink: Send {
    /// Send a frame to this sink.
    fn send(&mut self, frame: Frame) -> DaqResult<()>;

    /// Flush any buffered frames.
    fn flush(&mut self) -> DaqResult<()>;

    /// Close the stream sink.
    fn close(&mut self);
}
