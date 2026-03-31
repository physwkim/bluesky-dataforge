use std::sync::mpsc;
use std::sync::Arc;
use std::thread;

use parking_lot::Mutex;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::convert::py_to_json;

enum MongoMsg {
    /// A bluesky document to insert
    Doc {
        collection: String,
        doc: serde_json::Value,
    },
    /// Flush: wait for all pending inserts to complete
    Flush(mpsc::Sender<()>),
    /// Close the writer
    Close,
}

/// AsyncMongoWriter — background MongoDB writer for bluesky documents.
///
/// Receives bluesky documents (start, descriptor, event, stop) and
/// inserts them into MongoDB in a Rust background thread using the
/// mongodb Rust driver. Python is free to continue immediately.
///
/// Usage:
///   writer = AsyncMongoWriter("mongodb://localhost:27017", "metadatastore")
///   RE.subscribe(writer)
///   RE(fly_plan(...))
///   writer.flush()
///
/// Documents are batched and inserted in bulk for efficiency.
#[pyclass(name = "AsyncMongoWriter")]
pub struct AsyncMongoWriter {
    tx: Mutex<Option<mpsc::Sender<MongoMsg>>>,
    worker: Mutex<Option<thread::JoinHandle<()>>>,
    batch_size: usize,
    /// Errors from background insert operations, collected for flush() to report.
    errors: Arc<Mutex<Vec<String>>>,
}

#[pymethods]
impl AsyncMongoWriter {
    /// Create a new AsyncMongoWriter.
    ///
    /// Parameters
    /// ----------
    /// uri : str
    ///     MongoDB connection URI (e.g. "mongodb://localhost:27017")
    /// database : str
    ///     Database name (e.g. "metadatastore")
    /// batch_size : int, optional
    ///     Number of documents to accumulate before flushing (default: 100)
    #[new]
    #[pyo3(signature = (uri, database, batch_size=100))]
    fn new(uri: &str, database: &str, batch_size: usize) -> PyResult<Self> {
        let uri = uri.to_string();
        let db_name = database.to_string();
        let batch = batch_size;

        let (tx, rx) = mpsc::channel::<MongoMsg>();
        let errors: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
        let errors_ref = errors.clone();

        let worker = thread::spawn(move || {
            // Create tokio runtime for the mongodb async driver
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime");

            rt.block_on(async move {
                let client = match mongodb::Client::with_uri_str(&uri).await {
                    Ok(c) => c,
                    Err(e) => {
                        errors_ref.lock().push(format!("connection failed: {e}"));
                        return;
                    }
                };
                let db = client.database(&db_name);

                // Accumulate documents per collection for batch insert
                let mut batches: std::collections::HashMap<String, Vec<bson::Document>> =
                    std::collections::HashMap::new();

                while let Ok(msg) = rx.recv() {
                    match msg {
                        MongoMsg::Doc { collection, doc } => {
                            let bson_doc = json_to_bson_doc(&doc);
                            let batch_vec = batches.entry(collection).or_default();
                            batch_vec.push(bson_doc);

                            // Flush batch if size reached
                            let total: usize = batches.values().map(|v| v.len()).sum();
                            if total >= batch {
                                flush_batches(&db, &mut batches, &errors_ref).await;
                            }
                        }
                        MongoMsg::Flush(reply) => {
                            flush_batches(&db, &mut batches, &errors_ref).await;
                            let _ = reply.send(());
                        }
                        MongoMsg::Close => {
                            flush_batches(&db, &mut batches, &errors_ref).await;
                            break;
                        }
                    }
                }
            });
        });

        Ok(Self {
            tx: Mutex::new(Some(tx)),
            worker: Mutex::new(Some(worker)),
            batch_size,
            errors,
        })
    }

    /// RE.subscribe(writer) compatible — receives bluesky documents.
    fn __call__(
        &self,
        _py: Python<'_>,
        name: &str,
        doc: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let json_val = py_to_json(doc)?;

        // Map bluesky document names to MongoDB collection names
        // (databroker convention)
        let collection = match name {
            "start" => "run_start",
            "stop" => "run_stop",
            "descriptor" => "event_descriptor",
            "event" | "event_page" => "event",
            "datum" | "datum_page" => "datum",
            "resource" => "resource",
            _ => name,
        }
        .to_string();

        let tx = self.tx.lock();
        if let Some(ref sender) = *tx {
            sender
                .send(MongoMsg::Doc {
                    collection,
                    doc: json_val,
                })
                .map_err(|_| PyRuntimeError::new_err("writer thread gone"))?;
        }
        Ok(())
    }

    /// Wait for all pending documents to be inserted.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        let (reply_tx, reply_rx) = mpsc::channel();
        let tx = self.tx.lock();
        if let Some(ref sender) = *tx {
            sender
                .send(MongoMsg::Flush(reply_tx))
                .map_err(|_| PyRuntimeError::new_err("writer thread gone"))?;
        } else {
            // Writer already closed — nothing to flush
            return Ok(());
        }
        drop(tx);

        let rx = Mutex::new(reply_rx);
        let ok = py.allow_threads(|| rx.lock().recv().is_ok());
        if ok {
            // Check if any errors accumulated during writes
            let errs = self.errors.lock();
            if errs.is_empty() {
                Ok(())
            } else {
                let msg = errs.join("; ");
                Err(PyRuntimeError::new_err(format!(
                    "flush completed but {} insert(s) failed: {msg}",
                    errs.len()
                )))
            }
        } else {
            Err(PyRuntimeError::new_err("flush failed: worker thread gone"))
        }
    }

    /// Close the writer and wait for background thread to finish.
    fn close(&self, py: Python<'_>) -> PyResult<()> {
        let sender = self.tx.lock().take();
        if let Some(tx) = sender {
            let _ = tx.send(MongoMsg::Close);
        }
        let worker = self.worker.lock().take();
        if let Some(handle) = worker {
            py.allow_threads(|| {
                let _ = handle.join();
            });
        }
        Ok(())
    }

    fn __repr__(&self) -> String {
        format!("AsyncMongoWriter(batch_size={})", self.batch_size)
    }
}

/// Flush all accumulated batches to MongoDB.
async fn flush_batches(
    db: &mongodb::Database,
    batches: &mut std::collections::HashMap<String, Vec<bson::Document>>,
    errors: &Arc<Mutex<Vec<String>>>,
) {
    for (coll_name, docs) in batches.drain() {
        if docs.is_empty() {
            continue;
        }
        let collection = db.collection::<bson::Document>(&coll_name);
        let count = docs.len();
        match collection.insert_many(docs).await {
            Ok(_result) => {
                tracing::debug!(
                    "[AsyncMongoWriter] inserted {} docs into {}",
                    count,
                    coll_name,
                );
            }
            Err(e) => {
                let msg = format!("insert into {coll_name} failed ({count} docs): {e}");
                eprintln!("[AsyncMongoWriter] {msg}");
                errors.lock().push(msg);
            }
        }
    }
}

/// Convert serde_json::Value to bson::Document.
fn json_to_bson_doc(val: &serde_json::Value) -> bson::Document {
    // Walk the JSON value and build a BSON document manually
    match val {
        serde_json::Value::Object(map) => {
            let mut doc = bson::Document::new();
            for (k, v) in map {
                doc.insert(k, json_to_bson(v));
            }
            doc
        }
        _ => {
            let mut doc = bson::Document::new();
            doc.insert("_value", json_to_bson(val));
            doc
        }
    }
}

fn json_to_bson(val: &serde_json::Value) -> bson::Bson {
    match val {
        serde_json::Value::Null => bson::Bson::Null,
        serde_json::Value::Bool(b) => bson::Bson::Boolean(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                bson::Bson::Int64(i)
            } else if let Some(f) = n.as_f64() {
                bson::Bson::Double(f)
            } else {
                bson::Bson::Null
            }
        }
        serde_json::Value::String(s) => bson::Bson::String(s.clone()),
        serde_json::Value::Array(arr) => {
            bson::Bson::Array(arr.iter().map(json_to_bson).collect())
        }
        serde_json::Value::Object(map) => {
            let mut doc = bson::Document::new();
            for (k, v) in map {
                doc.insert(k, json_to_bson(v));
            }
            bson::Bson::Document(doc)
        }
    }
}
