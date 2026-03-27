use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;

use dataforge_core::StatusCore;

use crate::bundler::RunBundler;
use crate::dispatcher::DocumentDispatcher;
use crate::documents::{DataKey, Document, ExitStatus};
use crate::groups::GroupTracker;
use crate::msg::{Msg, MsgResult, Reading};

/// Trait for dispatching device-related messages back to Python.
pub trait DeviceDispatch: Send {
    /// Read data from a named device object.
    fn read(&self, obj_name: &str) -> Result<HashMap<String, Reading>, String>;

    /// Describe a named device object's data keys.
    fn describe(&self, obj_name: &str) -> Result<IndexMap<String, DataKey>, String>;

    /// Trigger a device, returning a StatusCore.
    fn trigger(&self, obj_name: &str) -> Result<Arc<StatusCore>, String>;

    /// Set a device value, returning a StatusCore.
    fn set(
        &self,
        obj_name: &str,
        value: serde_json::Value,
        kwargs: &HashMap<String, serde_json::Value>,
    ) -> Result<Arc<StatusCore>, String>;

    /// Stage a device.
    fn stage(&self, obj_name: &str) -> Result<(), String>;

    /// Unstage a device.
    fn unstage(&self, obj_name: &str) -> Result<(), String>;

    /// Kickoff a flyable device.
    fn kickoff(&self, obj_name: &str) -> Result<Arc<StatusCore>, String>;

    /// Complete a flyable device.
    fn complete(&self, obj_name: &str) -> Result<Arc<StatusCore>, String>;

    /// Collect events from a flyable device.
    fn collect(&self, obj_name: &str) -> Result<Vec<HashMap<String, Reading>>, String>;
}

/// Message processor — the core engine.
/// Structural Msgs are fully processed in Rust (no GIL needed).
/// Device Msgs are dispatched to Python via DeviceDispatch.
pub struct MsgProcessor {
    bundler: Option<RunBundler>,
    dispatcher: DocumentDispatcher,
    groups: GroupTracker,
    device_dispatch: Box<dyn DeviceDispatch>,
    page_batch_size: usize,
}

impl MsgProcessor {
    pub fn new(device_dispatch: Box<dyn DeviceDispatch>, page_batch_size: usize) -> Self {
        Self {
            bundler: None,
            dispatcher: DocumentDispatcher::new(),
            groups: GroupTracker::new(),
            device_dispatch,
            page_batch_size,
        }
    }

    /// Get a mutable reference to the document dispatcher for subscribing.
    pub fn dispatcher_mut(&mut self) -> &mut DocumentDispatcher {
        &mut self.dispatcher
    }

    /// Process a single message. Returns the result.
    pub fn process(&mut self, msg: Msg, group: Option<&str>) -> MsgResult {
        match msg {
            Msg::OpenRun { metadata } => self.handle_open_run(metadata),
            Msg::CloseRun {
                exit_status,
                reason,
            } => self.handle_close_run(exit_status, reason),
            Msg::Create { name } => self.handle_create(name),
            Msg::Save => self.handle_save(),
            Msg::Read { obj_name } => self.handle_read(&obj_name),
            Msg::Set {
                obj_name,
                value,
                kwargs,
            } => self.handle_set(&obj_name, value, &kwargs, group),
            Msg::Trigger { obj_name, group: msg_group } => {
                let g = msg_group.as_deref().or(group);
                self.handle_trigger(&obj_name, g)
            }
            Msg::Stage { obj_name } => self.handle_stage(&obj_name),
            Msg::Unstage { obj_name } => self.handle_unstage(&obj_name),
            Msg::Wait { group: wait_group } => self.handle_wait(&wait_group),
            Msg::Kickoff { obj_name } => self.handle_kickoff(&obj_name, group),
            Msg::Complete { obj_name } => self.handle_complete(&obj_name, group),
            Msg::Collect { obj_name } => self.handle_collect(&obj_name),
            Msg::Checkpoint => MsgResult::Ok(None),
            Msg::Sleep { duration } => {
                std::thread::sleep(std::time::Duration::from_secs_f64(duration));
                MsgResult::Ok(None)
            }
            Msg::Pause { .. } | Msg::Input { .. } | Msg::Subscribe { .. } | Msg::Unsubscribe { .. } | Msg::WaitFor { .. } => {
                MsgResult::Ok(None)
            }
        }
    }

    fn handle_open_run(&mut self, metadata: HashMap<String, serde_json::Value>) -> MsgResult {
        let mut bundler = RunBundler::new(metadata, self.page_batch_size);
        let doc = bundler.open_run();
        self.emit_doc("start", &doc);
        let uid = bundler.run_uid();
        self.bundler = Some(bundler);
        MsgResult::Ok(Some(serde_json::json!(uid.to_string())))
    }

    fn handle_close_run(
        &mut self,
        exit_status: Option<String>,
        reason: Option<String>,
    ) -> MsgResult {
        let bundler = match self.bundler.take() {
            Some(b) => b,
            None => return MsgResult::Err("no run open".into()),
        };

        let status = match exit_status.as_deref() {
            Some("abort") => ExitStatus::Abort,
            Some("fail") => ExitStatus::Fail,
            _ => ExitStatus::Success,
        };

        let mut bundler = bundler;
        let docs = bundler.close_run(status, reason);
        for (name, doc) in &docs {
            self.emit_doc(name, doc);
        }

        MsgResult::Ok(None)
    }

    fn handle_create(&mut self, name: Option<String>) -> MsgResult {
        match &mut self.bundler {
            Some(bundler) => {
                bundler.create(name);
                MsgResult::Ok(None)
            }
            None => MsgResult::Err("no run open".into()),
        }
    }

    fn handle_save(&mut self) -> MsgResult {
        // Data keys should be populated by describe() calls before save()
        let data_keys = IndexMap::new();

        let bundler = match &mut self.bundler {
            Some(b) => b,
            None => return MsgResult::Err("no run open".into()),
        };

        let docs = bundler.save(&data_keys);
        for (name, doc) in &docs {
            self.emit_doc(name, doc);
        }

        MsgResult::Ok(None)
    }

    fn handle_read(&mut self, obj_name: &str) -> MsgResult {
        match self.device_dispatch.read(obj_name) {
            Ok(readings) => {
                if let Some(bundler) = &mut self.bundler {
                    bundler.cache_reading(obj_name, &readings);
                }
                MsgResult::Ok(Some(serde_json::to_value(&readings).unwrap_or_default()))
            }
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_set(
        &mut self,
        obj_name: &str,
        value: serde_json::Value,
        kwargs: &HashMap<String, serde_json::Value>,
        group: Option<&str>,
    ) -> MsgResult {
        match self.device_dispatch.set(obj_name, value, kwargs) {
            Ok(status) => {
                if let Some(g) = group {
                    self.groups.add(g, status);
                }
                MsgResult::Ok(None)
            }
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_trigger(&mut self, obj_name: &str, group: Option<&str>) -> MsgResult {
        match self.device_dispatch.trigger(obj_name) {
            Ok(status) => {
                if let Some(g) = group {
                    self.groups.add(g, status);
                }
                MsgResult::Ok(None)
            }
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_stage(&mut self, obj_name: &str) -> MsgResult {
        match self.device_dispatch.stage(obj_name) {
            Ok(()) => MsgResult::Ok(None),
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_unstage(&mut self, obj_name: &str) -> MsgResult {
        match self.device_dispatch.unstage(obj_name) {
            Ok(()) => MsgResult::Ok(None),
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_wait(&mut self, group: &str) -> MsgResult {
        match self.groups.wait(group, None) {
            Ok(()) => MsgResult::Ok(None),
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_kickoff(&mut self, obj_name: &str, group: Option<&str>) -> MsgResult {
        match self.device_dispatch.kickoff(obj_name) {
            Ok(status) => {
                if let Some(g) = group {
                    self.groups.add(g, status);
                }
                MsgResult::Ok(None)
            }
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_complete(&mut self, obj_name: &str, group: Option<&str>) -> MsgResult {
        match self.device_dispatch.complete(obj_name) {
            Ok(status) => {
                if let Some(g) = group {
                    self.groups.add(g, status);
                }
                MsgResult::Ok(None)
            }
            Err(e) => MsgResult::Err(e),
        }
    }

    fn handle_collect(&mut self, obj_name: &str) -> MsgResult {
        match self.device_dispatch.collect(obj_name) {
            Ok(events) => MsgResult::Ok(Some(serde_json::to_value(&events).unwrap_or_default())),
            Err(e) => MsgResult::Err(e),
        }
    }

    fn emit_doc(&self, name: &str, doc: &Document) {
        self.dispatcher.dispatch(name, doc);
    }

}

#[cfg(test)]
mod tests {
    use super::*;

    struct MockDeviceDispatch;

    impl DeviceDispatch for MockDeviceDispatch {
        fn read(&self, obj_name: &str) -> Result<HashMap<String, Reading>, String> {
            Ok(HashMap::from([(
                obj_name.to_string(),
                Reading {
                    value: serde_json::json!(42.0),
                    timestamp: 1000.0,
                },
            )]))
        }

        fn describe(&self, obj_name: &str) -> Result<IndexMap<String, DataKey>, String> {
            Ok(IndexMap::from([(
                obj_name.to_string(),
                DataKey {
                    dtype: crate::documents::Dtype::Number,
                    shape: vec![],
                    source: format!("SIM:{obj_name}"),
                    external: None,
                    object_name: Some(obj_name.to_string()),
                    limits: None,
                },
            )]))
        }

        fn trigger(&self, _obj_name: &str) -> Result<Arc<StatusCore>, String> {
            let s = Arc::new(StatusCore::new());
            s.set_finished().unwrap();
            Ok(s)
        }

        fn set(
            &self,
            _obj_name: &str,
            _value: serde_json::Value,
            _kwargs: &HashMap<String, serde_json::Value>,
        ) -> Result<Arc<StatusCore>, String> {
            let s = Arc::new(StatusCore::new());
            s.set_finished().unwrap();
            Ok(s)
        }

        fn stage(&self, _obj_name: &str) -> Result<(), String> {
            Ok(())
        }
        fn unstage(&self, _obj_name: &str) -> Result<(), String> {
            Ok(())
        }
        fn kickoff(&self, _obj_name: &str) -> Result<Arc<StatusCore>, String> {
            let s = Arc::new(StatusCore::new());
            s.set_finished().unwrap();
            Ok(s)
        }
        fn complete(&self, _obj_name: &str) -> Result<Arc<StatusCore>, String> {
            let s = Arc::new(StatusCore::new());
            s.set_finished().unwrap();
            Ok(s)
        }
        fn collect(&self, _obj_name: &str) -> Result<Vec<HashMap<String, Reading>>, String> {
            Ok(vec![])
        }
    }

    #[test]
    fn test_processor_run_lifecycle() {
        let mut proc = MsgProcessor::new(Box::new(MockDeviceDispatch), 100);

        // Subscribe to track docs
        let docs = Arc::new(parking_lot::Mutex::new(Vec::new()));
        let docs2 = docs.clone();
        proc.dispatcher_mut().subscribe(Box::new(move |name, _doc| {
            docs2.lock().push(name.to_string());
        }));

        // Open run
        let result = proc.process(Msg::OpenRun { metadata: HashMap::new() }, None);
        assert!(matches!(result, MsgResult::Ok(Some(_))));

        // Create/read/save
        proc.process(Msg::Create { name: None }, None);
        proc.process(Msg::Read { obj_name: "det1".into() }, None);
        proc.process(Msg::Save, None);

        // Close run
        proc.process(
            Msg::CloseRun {
                exit_status: None,
                reason: None,
            },
            None,
        );

        let doc_names = docs.lock().clone();
        assert!(doc_names.contains(&"start".to_string()));
        assert!(doc_names.contains(&"stop".to_string()));
    }
}
