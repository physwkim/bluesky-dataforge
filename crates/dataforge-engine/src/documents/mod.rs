pub mod compose;
pub mod descriptor;
pub mod event;
pub mod resource;
pub mod run_start;
pub mod run_stop;

pub use compose::{compose_descriptor, compose_event, compose_run_start, compose_run_stop};
pub use descriptor::{DataKey, DescriptorHints, Dtype, EventDescriptor, ObjectConfig};
pub use event::{Event, EventPage, EventPageBuilder};
pub use resource::{Datum, Resource};
pub use run_start::{RunHints, RunStart};
pub use run_stop::{ExitStatus, RunStop};

/// A Document is any of the bluesky document types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "doc_type")]
pub enum Document {
    RunStart(RunStart),
    Descriptor(EventDescriptor),
    Event(Event),
    EventPage(EventPage),
    RunStop(RunStop),
    Resource(Resource),
    Datum(Datum),
}

/// Document name strings as used by bluesky.
pub fn doc_name(doc: &Document) -> &'static str {
    match doc {
        Document::RunStart(_) => "start",
        Document::Descriptor(_) => "descriptor",
        Document::Event(_) => "event",
        Document::EventPage(_) => "event_page",
        Document::RunStop(_) => "stop",
        Document::Resource(_) => "resource",
        Document::Datum(_) => "datum",
    }
}
