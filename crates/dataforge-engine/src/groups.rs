use std::collections::HashMap;
use std::sync::Arc;

use dataforge_core::StatusCore;

/// Tracks groups of StatusCore objects for coordinated waiting.
/// Mirrors bluesky's group/status mechanism.
pub struct GroupTracker {
    groups: HashMap<String, Vec<Arc<StatusCore>>>,
}

impl GroupTracker {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
        }
    }

    /// Add a status to a named group.
    pub fn add(&mut self, group: &str, status: Arc<StatusCore>) {
        self.groups
            .entry(group.to_string())
            .or_default()
            .push(status);
    }

    /// Wait for all statuses in a group to complete.
    /// Returns Ok(()) if all succeeded, Err with first failure message.
    pub fn wait(&mut self, group: &str, timeout: Option<std::time::Duration>) -> Result<(), String> {
        let statuses = match self.groups.remove(group) {
            Some(s) => s,
            None => return Ok(()),
        };

        for status in &statuses {
            match status.wait(timeout) {
                Ok(true) => {}
                Ok(false) => return Err(format!("status in group '{group}' failed")),
                Err(e) => return Err(format!("waiting on group '{group}': {e}")),
            }
        }

        Ok(())
    }

    /// Check if all statuses in a group are done (non-blocking).
    pub fn is_group_done(&self, group: &str) -> bool {
        match self.groups.get(group) {
            Some(statuses) => statuses.iter().all(|s| s.is_done()),
            None => true,
        }
    }
}

impl Default for GroupTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_group_tracker_basic() {
        let mut tracker = GroupTracker::new();
        let s1 = Arc::new(StatusCore::new());
        let s2 = Arc::new(StatusCore::new());

        tracker.add("motors", s1.clone());
        tracker.add("motors", s2.clone());

        assert!(!tracker.is_group_done("motors"));

        s1.set_finished().unwrap();
        assert!(!tracker.is_group_done("motors"));

        s2.set_finished().unwrap();
        assert!(tracker.is_group_done("motors"));

        assert!(tracker.wait("motors", None).is_ok());
    }

    #[test]
    fn test_group_tracker_empty_group() {
        let mut tracker = GroupTracker::new();
        assert!(tracker.wait("nonexistent", None).is_ok());
        assert!(tracker.is_group_done("nonexistent"));
    }
}
