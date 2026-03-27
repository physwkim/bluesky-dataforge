use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// A RunStart document marks the beginning of a data acquisition run.
/// Corresponds to bluesky's RunStart document.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStart {
    pub uid: Uuid,
    pub time: f64,
    pub plan_name: Option<String>,
    pub plan_type: Option<String>,
    pub scan_id: u64,
    #[serde(default)]
    pub plan_args: HashMap<String, serde_json::Value>,
    #[serde(default)]
    pub hints: RunHints,
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunHints {
    #[serde(default)]
    pub dimensions: Vec<(Vec<String>, String)>,
}

impl RunStart {
    pub fn new(scan_id: u64) -> Self {
        Self {
            uid: Uuid::new_v4(),
            time: current_timestamp(),
            plan_name: None,
            plan_type: None,
            scan_id,
            plan_args: HashMap::new(),
            hints: RunHints::default(),
            metadata: HashMap::new(),
        }
    }
}

pub(crate) fn current_timestamp() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_start_serialization() {
        let rs = RunStart::new(1);
        let json = serde_json::to_string(&rs).unwrap();
        let parsed: RunStart = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.uid, rs.uid);
        assert_eq!(parsed.scan_id, 1);
    }
}
