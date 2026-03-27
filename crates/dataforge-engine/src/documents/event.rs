use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A single Event document (one reading from one or more signals).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub uid: Uuid,
    pub descriptor: Uuid,
    pub time: f64,
    pub seq_num: u64,
    pub data: IndexMap<String, serde_json::Value>,
    pub timestamps: IndexMap<String, f64>,
    #[serde(default)]
    pub filled: IndexMap<String, bool>,
}

/// A batched page of events — the core optimization for high-speed DAQ.
/// Instead of emitting one Event document per reading, events are accumulated
/// into pages, reducing Python callback overhead from O(events) to O(pages).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventPage {
    pub uid: Vec<Uuid>,
    pub descriptor: Uuid,
    pub time: Vec<f64>,
    pub seq_num: Vec<u64>,
    pub data: IndexMap<String, Vec<serde_json::Value>>,
    pub timestamps: IndexMap<String, Vec<f64>>,
    #[serde(default)]
    pub filled: IndexMap<String, Vec<bool>>,
}

/// Builder for accumulating individual events into an EventPage.
pub struct EventPageBuilder {
    descriptor: Uuid,
    uids: Vec<Uuid>,
    times: Vec<f64>,
    seq_nums: Vec<u64>,
    data: IndexMap<String, Vec<serde_json::Value>>,
    timestamps: IndexMap<String, Vec<f64>>,
    filled: IndexMap<String, Vec<bool>>,
}

impl EventPageBuilder {
    pub fn new(descriptor: Uuid) -> Self {
        Self {
            descriptor,
            uids: Vec::new(),
            times: Vec::new(),
            seq_nums: Vec::new(),
            data: IndexMap::new(),
            timestamps: IndexMap::new(),
            filled: IndexMap::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.uids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.uids.is_empty()
    }

    /// Add an event's data to the page.
    pub fn add_event(&mut self, event: &Event) {
        self.uids.push(event.uid);
        self.times.push(event.time);
        self.seq_nums.push(event.seq_num);

        for (key, value) in &event.data {
            self.data
                .entry(key.clone())
                .or_default()
                .push(value.clone());
        }
        for (key, ts) in &event.timestamps {
            self.timestamps.entry(key.clone()).or_default().push(*ts);
        }
        for (key, f) in &event.filled {
            self.filled.entry(key.clone()).or_default().push(*f);
        }
    }

    /// Build the accumulated EventPage and reset.
    pub fn build(&mut self) -> EventPage {
        EventPage {
            uid: std::mem::take(&mut self.uids),
            descriptor: self.descriptor,
            time: std::mem::take(&mut self.times),
            seq_num: std::mem::take(&mut self.seq_nums),
            data: std::mem::take(&mut self.data),
            timestamps: std::mem::take(&mut self.timestamps),
            filled: std::mem::take(&mut self.filled),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_event_page_builder() {
        let desc_uid = Uuid::new_v4();
        let mut builder = EventPageBuilder::new(desc_uid);

        for i in 0..5 {
            let event = Event {
                uid: Uuid::new_v4(),
                descriptor: desc_uid,
                time: 1000.0 + i as f64,
                seq_num: i + 1,
                data: IndexMap::from([("det1".into(), serde_json::json!(i as f64 * 0.1))]),
                timestamps: IndexMap::from([("det1".into(), 1000.0 + i as f64)]),
                filled: IndexMap::new(),
            };
            builder.add_event(&event);
        }

        assert_eq!(builder.len(), 5);
        let page = builder.build();
        assert_eq!(page.uid.len(), 5);
        assert_eq!(page.data["det1"].len(), 5);
        assert!(builder.is_empty());
    }

    #[test]
    fn test_event_page_serialization() {
        let page = EventPage {
            uid: vec![Uuid::new_v4()],
            descriptor: Uuid::new_v4(),
            time: vec![1234.5],
            seq_num: vec![1],
            data: IndexMap::from([("x".into(), vec![serde_json::json!(42.0)])]),
            timestamps: IndexMap::from([("x".into(), vec![1234.5])]),
            filled: IndexMap::new(),
        };
        let json = serde_json::to_string(&page).unwrap();
        let parsed: EventPage = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.seq_num, vec![1]);
    }
}
