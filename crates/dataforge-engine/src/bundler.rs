use std::collections::HashMap;

use indexmap::IndexMap;
use uuid::Uuid;

use crate::documents::*;
use crate::msg::Reading;

/// State for a single descriptor stream within a run.
struct DescriptorState {
    descriptor: EventDescriptor,
    seq_counter: u64,
    page_builder: EventPageBuilder,
    event_count: u64,
}

/// RunBundler accumulates events and emits EventPages in batches.
/// This is the core optimization: instead of emitting one Event document per reading,
/// events are batched into EventPages, reducing Python callback overhead.
pub struct RunBundler {
    run_start: RunStart,
    descriptors: IndexMap<String, DescriptorState>,
    bundling: bool,
    bundle_name: Option<String>,
    read_cache: IndexMap<String, (serde_json::Value, f64)>,
    page_batch_size: usize,
    pending_docs: Vec<(String, Document)>,
}

impl RunBundler {
    pub fn new(metadata: HashMap<String, serde_json::Value>, page_batch_size: usize) -> Self {
        let plan_name = metadata
            .get("plan_name")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());
        let run_start = compose_run_start(plan_name.as_deref(), metadata);

        Self {
            run_start,
            descriptors: IndexMap::new(),
            bundling: false,
            bundle_name: None,
            read_cache: IndexMap::new(),
            page_batch_size,
            pending_docs: Vec::new(),
        }
    }

    /// Get the run UID.
    pub fn run_uid(&self) -> Uuid {
        self.run_start.uid
    }

    /// Get the RunStart document (emitted at open_run).
    pub fn open_run(&mut self) -> Document {
        Document::RunStart(self.run_start.clone())
    }

    /// Begin an event bundling context (bluesky "create").
    pub fn create(&mut self, name: Option<String>) {
        self.bundling = true;
        self.bundle_name = name;
        self.read_cache.clear();
    }

    /// Cache a reading within the current bundle.
    pub fn cache_reading(&mut self, obj_name: &str, readings: &HashMap<String, Reading>) {
        for (key, reading) in readings {
            self.read_cache
                .insert(key.clone(), (reading.value.clone(), reading.timestamp));
        }
        // Also store by object name for descriptor tracking
        let _ = obj_name;
    }

    /// Save the current bundle: compose an Event from cached readings,
    /// add it to the page accumulator, and flush if batch size reached.
    pub fn save(&mut self, data_keys: &IndexMap<String, DataKey>) -> Vec<(String, Document)> {
        if !self.bundling {
            return Vec::new();
        }

        let stream_name = self.bundle_name.clone().unwrap_or_else(|| "primary".into());
        let mut docs = std::mem::take(&mut self.pending_docs);

        // Ensure descriptor exists for this stream
        let run_uid = self.run_start.uid;
        let batch_size = self.page_batch_size;
        let state = self.descriptors.entry(stream_name.clone()).or_insert_with(|| {
            let mut desc = EventDescriptor::new(run_uid, &stream_name);
            desc.data_keys = data_keys.clone();
            let desc_uid = desc.uid;
            docs.push(("descriptor".into(), Document::Descriptor(desc.clone())));
            DescriptorState {
                descriptor: desc,
                seq_counter: 0,
                page_builder: EventPageBuilder::new(desc_uid),
                event_count: 0,
            }
        });

        // Compose event from read cache
        let mut data = IndexMap::new();
        let mut timestamps = IndexMap::new();
        for (key, (value, ts)) in &self.read_cache {
            data.insert(key.clone(), value.clone());
            timestamps.insert(key.clone(), *ts);
        }

        state.seq_counter += 1;
        let event = compose_event(state.descriptor.uid, state.seq_counter, data, timestamps);
        state.page_builder.add_event(&event);
        state.event_count += 1;

        // Flush page if batch size reached
        if state.page_builder.len() >= batch_size {
            let page = state.page_builder.build();
            docs.push(("event_page".into(), Document::EventPage(page)));
        }

        self.bundling = false;
        self.bundle_name = None;
        self.read_cache.clear();

        docs
    }

    /// Close the run: flush any remaining events and emit RunStop.
    pub fn close_run(
        &mut self,
        exit_status: ExitStatus,
        reason: Option<String>,
    ) -> Vec<(String, Document)> {
        let mut docs = Vec::new();

        // Flush any remaining events in all page builders
        for (_, state) in &mut self.descriptors {
            if !state.page_builder.is_empty() {
                let page = state.page_builder.build();
                docs.push(("event_page".into(), Document::EventPage(page)));
            }
        }

        // Compose run stop
        let num_events: HashMap<String, u64> = self
            .descriptors
            .iter()
            .map(|(name, state)| (name.clone(), state.event_count))
            .collect();

        let mut stop = RunStop::new(self.run_start.uid, exit_status);
        stop.reason = reason;
        stop.num_events = num_events;
        docs.push(("stop".into(), Document::RunStop(stop)));

        docs
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bundler_basic_flow() {
        let mut bundler = RunBundler::new(HashMap::new(), 3);

        // Open run
        let start_doc = bundler.open_run();
        assert!(matches!(start_doc, Document::RunStart(_)));

        let data_keys = IndexMap::from([(
            "det1".into(),
            DataKey {
                dtype: Dtype::Number,
                shape: vec![],
                source: "SIM:det1".into(),
                external: None,
                object_name: Some("det1".into()),
                limits: None,
            },
        )]);

        // Create/read/save cycle x3 to trigger page flush
        for i in 0..3 {
            bundler.create(None);
            let mut readings = HashMap::new();
            readings.insert(
                "det1".into(),
                Reading {
                    value: serde_json::json!(i as f64),
                    timestamp: 1000.0 + i as f64,
                },
            );
            bundler.cache_reading("det1", &readings);
            let docs = bundler.save(&data_keys);
            if i == 0 {
                // First save emits descriptor + (maybe page if batch_size=3 not reached)
                assert!(docs.iter().any(|(name, _)| name == "descriptor"));
            }
            if i == 2 {
                // Third save should trigger page flush (batch_size=3)
                assert!(docs.iter().any(|(name, _)| name == "event_page"));
            }
        }

        // Close run
        let stop_docs = bundler.close_run(ExitStatus::Success, None);
        assert!(stop_docs.iter().any(|(name, _)| name == "stop"));
    }

    #[test]
    fn test_bundler_partial_page_flushed_on_close() {
        let mut bundler = RunBundler::new(HashMap::new(), 100);
        bundler.open_run();

        let data_keys = IndexMap::from([(
            "x".into(),
            DataKey {
                dtype: Dtype::Number,
                shape: vec![],
                source: "SIM:x".into(),
                external: None,
                object_name: None,
                limits: None,
            },
        )]);

        // Add 2 events (less than batch size)
        for i in 0..2 {
            bundler.create(None);
            let mut readings = HashMap::new();
            readings.insert(
                "x".into(),
                Reading {
                    value: serde_json::json!(i),
                    timestamp: 1000.0,
                },
            );
            bundler.cache_reading("x", &readings);
            bundler.save(&data_keys);
        }

        let stop_docs = bundler.close_run(ExitStatus::Success, None);
        // Should flush partial page + stop
        assert!(stop_docs.iter().any(|(name, _)| name == "event_page"));
        assert!(stop_docs.iter().any(|(name, _)| name == "stop"));
    }
}
