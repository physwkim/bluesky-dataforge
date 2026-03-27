pub mod convert;
mod async_writer;
mod status;
mod subscriber;

use pyo3::prelude::*;

/// bluesky-dataforge native module.
///
/// Provides:
/// - ForgeStatus: Rust-backed bluesky Status protocol
/// - ForgeSubscriber: Rust-accelerated document subscriber (serialization + I/O)
/// - AsyncWriter: Background array data writer for fly scan acceleration
#[pymodule]
#[pyo3(name = "_native")]
fn bluesky_dataforge(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<status::ForgeStatus>()?;
    m.add_class::<subscriber::ForgeSubscriber>()?;
    m.add_class::<async_writer::AsyncWriter>()?;
    Ok(())
}
