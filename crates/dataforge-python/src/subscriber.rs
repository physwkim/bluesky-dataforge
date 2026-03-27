use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use crate::convert::py_to_json;

/// Trait for document serialization backends.
trait DocumentWriter: Send {
    fn write(&mut self, name: &str, doc: &serde_json::Value) -> Result<(), String>;
    fn close(&mut self) -> Result<(), String>;
}

/// JSON Lines document writer — one JSON object per line.
struct JsonLinesWriter {
    writer: BufWriter<File>,
}

impl JsonLinesWriter {
    fn new(filepath: &str) -> Result<Self, String> {
        let file = File::create(filepath).map_err(|e| format!("failed to create file: {e}"))?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }
}

impl DocumentWriter for JsonLinesWriter {
    fn write(&mut self, name: &str, doc: &serde_json::Value) -> Result<(), String> {
        let envelope = serde_json::json!([name, doc]);
        serde_json::to_writer(&mut self.writer, &envelope)
            .map_err(|e| format!("serialize error: {e}"))?;
        self.writer
            .write_all(b"\n")
            .map_err(|e| format!("write error: {e}"))?;
        Ok(())
    }

    fn close(&mut self) -> Result<(), String> {
        self.writer
            .flush()
            .map_err(|e| format!("flush error: {e}"))
    }
}

/// ForgeSubscriber — Rust-accelerated document subscriber for bluesky RunEngine.
///
/// Usage:
///   sub = ForgeSubscriber("/tmp/run.jsonl")
///   RE.subscribe(sub)
///
/// Documents are serialized and written to disk in Rust with the GIL released.
#[pyclass(name = "ForgeSubscriber")]
pub struct ForgeSubscriber {
    writer: Arc<Mutex<Box<dyn DocumentWriter>>>,
    filepath: String,
}

#[pymethods]
impl ForgeSubscriber {
    #[new]
    #[pyo3(signature = (filepath, format="jsonl"))]
    fn new(filepath: &str, format: &str) -> PyResult<Self> {
        let writer: Box<dyn DocumentWriter> = match format {
            "jsonl" | "json" => Box::new(
                JsonLinesWriter::new(filepath)
                    .map_err(|e| PyRuntimeError::new_err(e))?,
            ),
            _ => {
                return Err(PyRuntimeError::new_err(format!(
                    "unsupported format: {format}"
                )))
            }
        };

        Ok(Self {
            writer: Arc::new(Mutex::new(writer)),
            filepath: filepath.to_string(),
        })
    }

    /// Called by RunEngine for each document. Callable protocol for RE.subscribe().
    fn __call__(&self, py: Python<'_>, name: &str, doc: &Bound<'_, PyAny>) -> PyResult<()> {
        // Convert Python dict to serde_json::Value (GIL held, shallow conversion)
        let json_val = py_to_json(doc)?;
        let doc_name = name.to_string();
        let writer = self.writer.clone();

        // Release GIL → Rust serialization + file I/O
        py.allow_threads(|| {
            writer
                .lock()
                .write(&doc_name, &json_val)
                .map_err(|e| pyo3::exceptions::PyIOError::new_err(e))
        })
    }

    /// Flush and close the underlying file.
    fn close(&self) -> PyResult<()> {
        self.writer
            .lock()
            .close()
            .map_err(|e| PyRuntimeError::new_err(e))
    }

    fn __repr__(&self) -> String {
        format!("ForgeSubscriber('{}')", self.filepath)
    }
}
