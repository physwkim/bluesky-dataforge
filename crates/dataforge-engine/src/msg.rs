use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// Rust representation of a bluesky Msg.
/// Structural messages (OpenRun, CloseRun, Create, Save) are fully processed in Rust.
/// Device messages (Set, Trigger, Read) are dispatched to Python via DeviceDispatch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Msg {
    // --- Structural messages (processed in Rust) ---
    OpenRun {
        #[serde(default)]
        metadata: HashMap<String, serde_json::Value>,
    },
    CloseRun {
        #[serde(default)]
        exit_status: Option<String>,
        #[serde(default)]
        reason: Option<String>,
    },
    Create {
        name: Option<String>,
    },
    Save,

    // --- Device messages (dispatched to Python) ---
    Read {
        obj_name: String,
    },
    Set {
        obj_name: String,
        value: serde_json::Value,
        #[serde(default)]
        kwargs: HashMap<String, serde_json::Value>,
    },
    Trigger {
        obj_name: String,
        #[serde(default)]
        group: Option<String>,
    },
    Stage {
        obj_name: String,
    },
    Unstage {
        obj_name: String,
    },

    // --- Status tracking ---
    Wait {
        group: String,
    },
    WaitFor {
        obj_name: String,
    },

    // --- Flyable ---
    Kickoff {
        obj_name: String,
    },
    Complete {
        obj_name: String,
    },
    Collect {
        obj_name: String,
    },

    // --- Control flow ---
    Subscribe {
        doc_type: String,
    },
    Unsubscribe {
        token: i64,
    },
    Checkpoint,
    Pause {
        #[serde(default)]
        defer: bool,
    },
    Sleep {
        duration: f64,
    },
    Input {
        prompt: Option<String>,
    },
}

/// Envelope wrapping a Msg with optional group tracking.
#[derive(Debug)]
pub struct MsgEnvelope {
    pub msg: Msg,
    pub group: Option<String>,
}

/// Result of processing a message.
#[derive(Debug)]
pub enum MsgResult {
    /// Message processed successfully, possibly with a return value.
    Ok(Option<serde_json::Value>),
    /// Message processing failed.
    Err(String),
    /// Message requires device dispatch (returned reading data).
    DeviceReading(HashMap<String, Reading>),
}

/// A single reading from a device (value + timestamp).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reading {
    pub value: serde_json::Value,
    pub timestamp: f64,
}
