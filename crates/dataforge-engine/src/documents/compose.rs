use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use indexmap::IndexMap;
use uuid::Uuid;

use super::descriptor::{DataKey, EventDescriptor};
use super::event::Event;
use super::run_start::{current_timestamp, RunStart};
use super::run_stop::{ExitStatus, RunStop};

static SCAN_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Compose a RunStart document.
pub fn compose_run_start(
    plan_name: Option<&str>,
    metadata: HashMap<String, serde_json::Value>,
) -> RunStart {
    let scan_id = SCAN_ID_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut rs = RunStart::new(scan_id);
    rs.plan_name = plan_name.map(|s| s.to_string());
    rs.metadata = metadata;
    rs
}

/// Compose an EventDescriptor for a stream.
pub fn compose_descriptor(
    run_uid: Uuid,
    stream_name: &str,
    data_keys: IndexMap<String, DataKey>,
) -> EventDescriptor {
    let mut desc = EventDescriptor::new(run_uid, stream_name);
    desc.data_keys = data_keys;
    desc
}

/// Compose a single Event.
pub fn compose_event(
    descriptor_uid: Uuid,
    seq_num: u64,
    data: IndexMap<String, serde_json::Value>,
    timestamps: IndexMap<String, f64>,
) -> Event {
    Event {
        uid: Uuid::new_v4(),
        descriptor: descriptor_uid,
        time: current_timestamp(),
        seq_num,
        data,
        timestamps,
        filled: IndexMap::new(),
    }
}

/// Compose a RunStop document.
pub fn compose_run_stop(
    run_uid: Uuid,
    exit_status: ExitStatus,
    num_events: HashMap<String, u64>,
) -> RunStop {
    let mut stop = RunStop::new(run_uid, exit_status);
    stop.num_events = num_events;
    stop
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compose_full_run() {
        let rs = compose_run_start(Some("count"), HashMap::new());
        let desc = compose_descriptor(
            rs.uid,
            "primary",
            IndexMap::from([(
                "det1".into(),
                DataKey {
                    dtype: super::super::descriptor::Dtype::Number,
                    shape: vec![],
                    source: "SIM:det1".into(),
                    external: None,
                    object_name: Some("det1".into()),
                    limits: None,
                },
            )]),
        );
        let event = compose_event(
            desc.uid,
            1,
            IndexMap::from([("det1".into(), serde_json::json!(42.0))]),
            IndexMap::from([("det1".into(), rs.time)]),
        );
        let stop = compose_run_stop(
            rs.uid,
            ExitStatus::Success,
            HashMap::from([("primary".into(), 1)]),
        );

        assert_eq!(event.descriptor, desc.uid);
        assert_eq!(stop.run_start, rs.uid);
    }
}
