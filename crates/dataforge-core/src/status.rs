use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};

use crate::error::{DaqError, DaqResult};

/// Python-independent status core for tracking asynchronous operation completion.
///
/// This is the Rust-side state machine that ForgeStatus (PyO3) wraps.
/// All state transitions are lock-free reads; only `set_finished`/`set_failed`
/// and callback registration require the mutex.
pub struct StatusCore {
    done: AtomicBool,
    success: AtomicBool,
    notify: Condvar,
    mutex: Mutex<()>,
    callbacks: Mutex<Vec<Box<dyn FnOnce(bool) + Send>>>,
}

impl StatusCore {
    pub fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            success: AtomicBool::new(false),
            notify: Condvar::new(),
            mutex: Mutex::new(()),
            callbacks: Mutex::new(Vec::new()),
        }
    }

    /// Whether the operation has completed (successfully or not).
    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::Acquire)
    }

    /// Whether the operation completed successfully.
    pub fn is_success(&self) -> bool {
        self.success.load(Ordering::Acquire)
    }

    /// Mark the operation as finished successfully.
    /// Wakes all waiters and fires callbacks with `true`.
    pub fn set_finished(&self) -> DaqResult<()> {
        if self.done.swap(true, Ordering::AcqRel) {
            return Err(DaqError::AlreadyFinished);
        }
        self.success.store(true, Ordering::Release);

        // Wake all waiters
        let guard = self.mutex.lock();
        self.notify.notify_all();
        drop(guard);

        self.fire_callbacks(true);
        Ok(())
    }

    /// Mark the operation as failed.
    /// Wakes all waiters and fires callbacks with `false`.
    pub fn set_failed(&self) -> DaqResult<()> {
        if self.done.swap(true, Ordering::AcqRel) {
            return Err(DaqError::AlreadyFinished);
        }
        self.success.store(false, Ordering::Release);

        let guard = self.mutex.lock();
        self.notify.notify_all();
        drop(guard);

        self.fire_callbacks(false);
        Ok(())
    }

    /// Register a callback to fire when the operation completes.
    /// If already done, fires immediately.
    pub fn add_callback(&self, cb: Box<dyn FnOnce(bool) + Send>) {
        if self.is_done() {
            cb(self.is_success());
            return;
        }

        let mut callbacks = self.callbacks.lock();
        // Double-check after acquiring lock
        if self.is_done() {
            drop(callbacks);
            cb(self.is_success());
        } else {
            callbacks.push(cb);
        }
    }

    /// Block until the operation completes or the timeout expires.
    /// Returns `Ok(true)` if successful, `Ok(false)` if failed,
    /// `Err(Timeout)` if timed out.
    pub fn wait(&self, timeout: Option<Duration>) -> DaqResult<bool> {
        if self.is_done() {
            return Ok(self.is_success());
        }

        let mut guard = self.mutex.lock();
        loop {
            if self.is_done() {
                return Ok(self.is_success());
            }
            match timeout {
                Some(dur) => {
                    let result = self.notify.wait_for(&mut guard, dur);
                    if result.timed_out() && !self.is_done() {
                        return Err(DaqError::Timeout(dur));
                    }
                }
                None => {
                    self.notify.wait(&mut guard);
                }
            }
            if self.is_done() {
                return Ok(self.is_success());
            }
        }
    }

    fn fire_callbacks(&self, success: bool) {
        let callbacks: Vec<_> = {
            let mut cbs = self.callbacks.lock();
            std::mem::take(&mut *cbs)
        };
        for cb in callbacks {
            cb(success);
        }
    }
}

impl Default for StatusCore {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_status_lifecycle() {
        let status = StatusCore::new();
        assert!(!status.is_done());
        assert!(!status.is_success());

        status.set_finished().unwrap();
        assert!(status.is_done());
        assert!(status.is_success());
    }

    #[test]
    fn test_status_double_finish() {
        let status = StatusCore::new();
        status.set_finished().unwrap();
        assert!(status.set_finished().is_err());
    }

    #[test]
    fn test_status_failure() {
        let status = StatusCore::new();
        status.set_failed().unwrap();
        assert!(status.is_done());
        assert!(!status.is_success());
    }

    #[test]
    fn test_status_callback() {
        let status = Arc::new(StatusCore::new());
        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();

        status.add_callback(Box::new(move |success| {
            assert!(success);
            called2.store(true, Ordering::Release);
        }));

        assert!(!called.load(Ordering::Acquire));
        status.set_finished().unwrap();
        assert!(called.load(Ordering::Acquire));
    }

    #[test]
    fn test_status_callback_after_done() {
        let status = StatusCore::new();
        status.set_finished().unwrap();

        let called = Arc::new(AtomicBool::new(false));
        let called2 = called.clone();
        status.add_callback(Box::new(move |_| {
            called2.store(true, Ordering::Release);
        }));
        assert!(called.load(Ordering::Acquire));
    }

    #[test]
    fn test_status_wait_threaded() {
        let status = Arc::new(StatusCore::new());
        let status2 = status.clone();

        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(10));
            status2.set_finished().unwrap();
        });

        let result = status.wait(Some(Duration::from_secs(5))).unwrap();
        assert!(result);
        handle.join().unwrap();
    }

    #[test]
    fn test_status_wait_timeout() {
        let status = StatusCore::new();
        let result = status.wait(Some(Duration::from_millis(10)));
        assert!(result.is_err());
    }
}
