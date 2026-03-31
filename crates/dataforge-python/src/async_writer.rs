use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

/// A queued write item — owns its data (copied from numpy).
struct WriteItem {
    /// Raw bytes (copied from numpy array)
    data: Vec<u8>,
    /// Metadata as JSON
    metadata: serde_json::Value,
    /// Shape info for reconstruction
    shape: Vec<usize>,
    /// dtype string (e.g. "float64", "int32")
    dtype: String,
}

/// Completion token — Python can wait on this.
struct FlushToken {
    reply: mpsc::Sender<()>,
}

enum WriterMsg {
    Data(WriteItem),
    Flush(FlushToken),
    Close,
}

/// AsyncWriter — offloads array data saving to a Rust background thread.
///
/// Usage:
///   writer = AsyncWriter("/data/scan.jsonl")
///   writer.enqueue(numpy_array, {"timestamp": 1234, "uid": "..."})
///   writer.enqueue(numpy_array2, {"timestamp": 1235, "uid": "..."})
///   writer.flush()   # wait for all pending writes
///   writer.close()
///
/// `enqueue` copies the numpy array data and returns immediately.
/// The background thread handles serialization + file I/O.
#[pyclass(name = "AsyncWriter")]
pub struct AsyncWriter {
    tx: Mutex<Option<mpsc::Sender<WriterMsg>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    filepath: String,
    /// Number of items queued minus items processed.
    pending: Arc<AtomicUsize>,
    /// Errors from background write operations.
    errors: Arc<Mutex<Vec<String>>>,
}

#[pymethods]
impl AsyncWriter {
    #[new]
    #[pyo3(signature = (filepath, format="jsonl"))]
    fn new(filepath: &str, format: &str) -> PyResult<Self> {
        let path = filepath.to_string();
        let fmt = format.to_string();

        let file = File::create(&path)
            .map_err(|e| PyRuntimeError::new_err(format!("cannot create {path}: {e}")))?;
        let mut writer = BufWriter::new(file);

        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let pending = Arc::new(AtomicUsize::new(0));
        let pending_ref = pending.clone();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let errors_ref = errors.clone();

        let worker = thread::spawn(move || {
            while let Ok(msg) = rx.recv() {
                match msg {
                    WriterMsg::Data(item) => {
                        if let Err(e) = write_item(&mut writer, &item, &fmt) {
                            let msg = format!("write error: {e}");
                            eprintln!("[AsyncWriter] {msg}");
                            errors_ref.lock().push(msg);
                        }
                        pending_ref.fetch_sub(1, Ordering::Relaxed);
                    }
                    WriterMsg::Flush(token) => {
                        let _ = writer.flush();
                        let _ = token.reply.send(());
                    }
                    WriterMsg::Close => {
                        let _ = writer.flush();
                        break;
                    }
                }
            }
        });

        Ok(Self {
            tx: Mutex::new(Some(tx)),
            worker: Mutex::new(Some(worker)),
            filepath: path,
            pending,
            errors,
        })
    }

    /// Queue array data for background writing. Returns immediately.
    ///
    /// The numpy array is copied to owned memory so Python can reuse/free
    /// the array immediately.
    ///
    /// Parameters
    /// ----------
    /// data : numpy.ndarray
    ///     Array data to save. Copied to Rust-owned memory.
    /// metadata : dict, optional
    ///     Metadata dict (timestamp, uid, etc.). Converted to JSON.
    #[pyo3(signature = (data, metadata=None))]
    fn enqueue(
        &self,
        _py: Python<'_>,
        data: &Bound<'_, pyo3::PyAny>,
        metadata: Option<&Bound<'_, pyo3::PyAny>>,
    ) -> PyResult<()> {
        // Extract array info via Python attributes
        let shape: Vec<usize> = data.getattr("shape")?.extract()?;
        let dtype_str: String = data.getattr("dtype")?.str()?.to_string();

        // Copy raw bytes from numpy array — single memcpy
        let bytes_obj = data.call_method0("tobytes")?;
        let py_bytes: &[u8] = bytes_obj
            .downcast::<pyo3::types::PyBytes>()
            .map_err(|_| PyRuntimeError::new_err("tobytes() did not return bytes"))?
            .as_bytes();
        let raw_bytes = py_bytes.to_vec();

        // Convert metadata
        let meta_json = match metadata {
            Some(m) => crate::convert::py_to_json(m)?,
            None => serde_json::Value::Null,
        };

        let item = WriteItem {
            data: raw_bytes,
            metadata: meta_json,
            shape,
            dtype: dtype_str,
        };

        let tx = self.tx.lock();
        if let Some(ref sender) = *tx {
            sender
                .send(WriterMsg::Data(item))
                .map_err(|_| PyRuntimeError::new_err("writer thread gone"))?;
            self.pending.fetch_add(1, Ordering::Relaxed);
        } else {
            return Err(PyRuntimeError::new_err("writer is closed"));
        }

        Ok(())
    }

    /// Wait for all pending writes to complete.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        let tx = self.tx.lock();
        if let Some(ref sender) = *tx {
            sender
                .send(WriterMsg::Flush(FlushToken { reply: reply_tx }))
                .map_err(|_| PyRuntimeError::new_err("writer thread gone"))?;
        } else {
            // Writer already closed — nothing to flush
            return Ok(());
        }
        drop(tx);

        let rx = Mutex::new(reply_rx);
        let result = py.allow_threads(|| rx.lock().recv().is_ok());
        if result {
            let errs: Vec<String> = self.errors.lock().drain(..).collect();
            if errs.is_empty() {
                Ok(())
            } else {
                let msg = errs.join("; ");
                Err(PyRuntimeError::new_err(format!(
                    "flush completed but {} write(s) failed: {msg}",
                    errs.len()
                )))
            }
        } else {
            Err(PyRuntimeError::new_err("flush failed: worker thread gone"))
        }
    }

    /// Close the writer and wait for the background thread to finish.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let sender = self.tx.lock().take();
        if let Some(tx) = sender {
            let _ = tx.send(WriterMsg::Close);
        }
        let worker = self.worker.lock().take();
        if let Some(handle) = worker {
            py.allow_threads(|| {
                let _ = handle.join();
            });
        }
        Ok(())
    }

    /// Number of items waiting to be written (enqueued minus processed).
    #[getter]
    fn pending(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }

    fn __repr__(&self) -> String {
        format!("AsyncWriter('{}')", self.filepath)
    }
}

/// Write a single item to the output file.
fn write_item(
    writer: &mut BufWriter<File>,
    item: &WriteItem,
    format: &str,
) -> Result<(), String> {
    match format {
        "jsonl" | "json" => {
            // Write metadata + base64-encoded data as JSON line
            let encoded = base64_encode(&item.data);
            let record = serde_json::json!({
                "data": encoded,
                "shape": item.shape,
                "dtype": item.dtype,
                "metadata": item.metadata,
            });
            serde_json::to_writer(&mut *writer, &record)
                .map_err(|e| format!("json error: {e}"))?;
            writer
                .write_all(b"\n")
                .map_err(|e| format!("write error: {e}"))?;
        }
        "raw" => {
            // Write raw binary data directly
            writer
                .write_all(&item.data)
                .map_err(|e| format!("write error: {e}"))?;
        }
        _ => {
            return Err(format!("unsupported format: {format}"));
        }
    }
    Ok(())
}

/// Simple base64 encoding (no external dependency).
fn base64_encode(data: &[u8]) -> String {
    const CHARS: &[u8] =
        b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let triple = (b0 << 16) | (b1 << 8) | b2;
        result.push(CHARS[((triple >> 18) & 0x3F) as usize] as char);
        result.push(CHARS[((triple >> 12) & 0x3F) as usize] as char);
        if chunk.len() > 1 {
            result.push(CHARS[((triple >> 6) & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
        if chunk.len() > 2 {
            result.push(CHARS[(triple & 0x3F) as usize] as char);
        } else {
            result.push('=');
        }
    }
    result
}
