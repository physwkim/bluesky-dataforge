use std::collections::HashMap;

use indexmap::IndexMap;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Data type enum matching bluesky's dtype specification.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum Dtype {
    String,
    Number,
    Integer,
    Boolean,
    Array,
}

/// Describes one data key within an EventDescriptor.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataKey {
    pub dtype: Dtype,
    pub shape: Vec<u64>,
    pub source: String,
    #[serde(default)]
    pub external: Option<String>,
    #[serde(default)]
    pub object_name: Option<String>,
    #[serde(default)]
    pub limits: Option<serde_json::Value>,
}

/// Configuration data for a single object.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ObjectConfig {
    #[serde(default)]
    pub data: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub timestamps: HashMap<String, f64>,
    #[serde(default)]
    pub data_keys: IndexMap<String, DataKey>,
}

/// An EventDescriptor defines the schema for events in a stream.
/// Corresponds to bluesky's EventDescriptor document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDescriptor {
    pub uid: Uuid,
    pub run_start: Uuid,
    pub time: f64,
    pub name: String,
    pub data_keys: IndexMap<String, DataKey>,
    #[serde(default)]
    pub configuration: IndexMap<String, ObjectConfig>,
    #[serde(default)]
    pub hints: DescriptorHints,
    #[serde(default)]
    pub object_keys: IndexMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DescriptorHints {
    #[serde(default)]
    pub fields: Vec<String>,
}

impl EventDescriptor {
    pub fn new(run_start: Uuid, name: impl Into<String>) -> Self {
        Self {
            uid: Uuid::new_v4(),
            run_start,
            time: super::run_start::current_timestamp(),
            name: name.into(),
            data_keys: IndexMap::new(),
            configuration: IndexMap::new(),
            hints: DescriptorHints::default(),
            object_keys: IndexMap::new(),
        }
    }

    pub fn add_data_key(&mut self, name: impl Into<String>, key: DataKey) {
        self.data_keys.insert(name.into(), key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_descriptor_serialization() {
        let mut desc = EventDescriptor::new(Uuid::new_v4(), "primary");
        desc.add_data_key(
            "det1",
            DataKey {
                dtype: Dtype::Number,
                shape: vec![],
                source: "SIM:det1".into(),
                external: None,
                object_name: Some("det1".into()),
                limits: None,
            },
        );

        let json = serde_json::to_string(&desc).unwrap();
        let parsed: EventDescriptor = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.name, "primary");
        assert_eq!(parsed.data_keys.len(), 1);
        assert_eq!(parsed.data_keys["det1"].dtype, Dtype::Number);
    }
}
