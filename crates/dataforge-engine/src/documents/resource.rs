use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A Resource document describes an external data resource (e.g., an HDF5 file).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub uid: Uuid,
    pub run_start: Uuid,
    pub spec: String,
    pub root: String,
    pub resource_path: String,
    #[serde(default)]
    pub resource_kwargs: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub path_semantics: PathSemantics,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PathSemantics {
    #[default]
    Posix,
    Windows,
}

/// A Datum document references a specific slice within a Resource.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Datum {
    pub datum_id: String,
    pub resource: Uuid,
    #[serde(default)]
    pub datum_kwargs: HashMap<String, serde_json::Value>,
}

impl Resource {
    pub fn new(run_start: Uuid, spec: impl Into<String>, resource_path: impl Into<String>) -> Self {
        Self {
            uid: Uuid::new_v4(),
            run_start,
            spec: spec.into(),
            root: String::new(),
            resource_path: resource_path.into(),
            resource_kwargs: HashMap::new(),
            path_semantics: PathSemantics::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_serialization() {
        let res = Resource::new(Uuid::new_v4(), "AD_HDF5", "/data/run001.h5");
        let json = serde_json::to_string(&res).unwrap();
        let parsed: Resource = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.spec, "AD_HDF5");
        assert_eq!(parsed.path_semantics, PathSemantics::Posix);
    }
}
