use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Exit status of a run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ExitStatus {
    Success,
    Abort,
    Fail,
}

/// A RunStop document marks the end of a data acquisition run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunStop {
    pub uid: Uuid,
    pub run_start: Uuid,
    pub time: f64,
    pub exit_status: ExitStatus,
    #[serde(default)]
    pub reason: Option<String>,
    pub num_events: HashMap<String, u64>,
}

impl RunStop {
    pub fn new(run_start: Uuid, exit_status: ExitStatus) -> Self {
        Self {
            uid: Uuid::new_v4(),
            run_start,
            time: super::run_start::current_timestamp(),
            exit_status,
            reason: None,
            num_events: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_run_stop_serialization() {
        let stop = RunStop::new(Uuid::new_v4(), ExitStatus::Success);
        let json = serde_json::to_string(&stop).unwrap();
        let parsed: RunStop = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.exit_status, ExitStatus::Success);
    }
}
