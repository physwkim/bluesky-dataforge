use std::sync::Arc;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use dataforge_core::StatusCore;

/// ForgeStatus — bluesky Status protocol implementation backed by Rust.
///
/// Implements:
///   - add_callback(callback) → None
///   - exception(timeout=0.0) → BaseException | None
///   - done (property) → bool
///   - success (property) → bool
///   - wait(timeout=None) → None
///   - set_finished() → None
///   - set_exception(exc) → None
#[pyclass(name = "ForgeStatus")]
pub struct ForgeStatus {
    pub(crate) core: Arc<StatusCore>,
    exception: Arc<parking_lot::Mutex<Option<PyObject>>>,
}

#[pymethods]
impl ForgeStatus {
    #[new]
    pub fn new() -> Self {
        Self {
            core: Arc::new(StatusCore::new()),
            exception: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Register a callback to be called when the status completes.
    /// The callback receives this status object as its argument.
    fn add_callback(&self, py: Python<'_>, callback: PyObject) -> PyResult<()> {
        let cb_ref = callback.clone_ref(py);
        let exc_store = self.exception.clone();
        let core = self.core.clone();

        // We need to pass `self` to the callback, but we can't move self.
        // Instead, create a new ForgeStatus wrapping the same core.
        self.core.add_callback(Box::new(move |_success| {
            Python::with_gil(|py| {
                let status = ForgeStatus {
                    core: core.clone(),
                    exception: exc_store.clone(),
                };
                let py_status = Py::new(py, status).unwrap();
                if let Err(e) = cb_ref.call1(py, (py_status,)) {
                    tracing::error!("Status callback error: {e}");
                }
            });
        }));

        Ok(())
    }

    /// Return the exception if the status failed, else None.
    #[pyo3(signature = (timeout=0.0))]
    fn exception(&self, py: Python<'_>, timeout: f64) -> PyResult<PyObject> {
        if !self.core.is_done() && timeout > 0.0 {
            py.allow_threads(|| {
                let _ = self.core.wait(Some(Duration::from_secs_f64(timeout)));
            });
        }
        let exc = self.exception.lock();
        match &*exc {
            Some(e) => Ok(e.clone_ref(py)),
            None => Ok(py.None()),
        }
    }

    /// Whether the operation has completed.
    #[getter]
    fn done(&self) -> bool {
        self.core.is_done()
    }

    /// Whether the operation completed successfully.
    #[getter]
    fn success(&self) -> bool {
        self.core.is_success()
    }

    /// Block until the operation completes.
    /// Releases the GIL while waiting.
    #[pyo3(signature = (timeout=None))]
    fn wait(&self, py: Python<'_>, timeout: Option<f64>) -> PyResult<()> {
        let dur = timeout.map(Duration::from_secs_f64);
        let result = py.allow_threads(|| self.core.wait(dur));

        match result {
            Ok(true) => Ok(()),
            Ok(false) => {
                let exc = self.exception.lock();
                match &*exc {
                    Some(e) => Err(PyRuntimeError::new_err(format!(
                        "Status failed: {e}"
                    ))),
                    None => Err(PyRuntimeError::new_err("Status failed")),
                }
            }
            Err(e) => Err(PyRuntimeError::new_err(format!("{e}"))),
        }
    }

    /// Mark the status as finished successfully.
    fn set_finished(&self) -> PyResult<()> {
        self.core
            .set_finished()
            .map_err(|e| PyRuntimeError::new_err(format!("{e}")))
    }

    /// Mark the status as failed with an exception.
    fn set_exception(&self, py: Python<'_>, exc: PyObject) -> PyResult<()> {
        {
            let mut store = self.exception.lock();
            *store = Some(exc.clone_ref(py));
        }
        self.core
            .set_failed()
            .map_err(|e| PyRuntimeError::new_err(format!("{e}")))
    }

    fn __repr__(&self) -> String {
        format!(
            "ForgeStatus(done={}, success={})",
            self.core.is_done(),
            self.core.is_success()
        )
    }
}

impl ForgeStatus {
    /// Create a ForgeStatus from an existing StatusCore (used internally).
    pub fn from_core(core: Arc<StatusCore>) -> Self {
        Self {
            core,
            exception: Arc::new(parking_lot::Mutex::new(None)),
        }
    }

    /// Create an already-finished ForgeStatus.
    pub fn finished() -> Self {
        let s = Self::new();
        s.core.set_finished().unwrap();
        s
    }
}
