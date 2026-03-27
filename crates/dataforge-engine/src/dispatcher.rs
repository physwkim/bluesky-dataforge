use crate::documents::Document;

/// Callback type for document subscribers.
pub type DocumentCallback = Box<dyn Fn(&str, &Document) + Send>;

/// Registry for document subscribers.
/// Manages callbacks that receive (doc_name, document) pairs.
pub struct DocumentDispatcher {
    subscribers: Vec<(i64, DocumentCallback)>,
    next_token: i64,
}

impl DocumentDispatcher {
    pub fn new() -> Self {
        Self {
            subscribers: Vec::new(),
            next_token: 0,
        }
    }

    /// Subscribe a callback, returning a token for unsubscription.
    pub fn subscribe(&mut self, callback: DocumentCallback) -> i64 {
        let token = self.next_token;
        self.next_token += 1;
        self.subscribers.push((token, callback));
        token
    }

    /// Unsubscribe by token.
    pub fn unsubscribe(&mut self, token: i64) -> bool {
        let len_before = self.subscribers.len();
        self.subscribers.retain(|(t, _)| *t != token);
        self.subscribers.len() < len_before
    }

    /// Dispatch a document to all subscribers.
    pub fn dispatch(&self, name: &str, doc: &Document) {
        for (_, cb) in &self.subscribers {
            cb(name, doc);
        }
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
}

impl Default for DocumentDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::documents::RunStart;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[test]
    fn test_dispatcher_subscribe_dispatch() {
        let mut dispatcher = DocumentDispatcher::new();
        let count = Arc::new(AtomicUsize::new(0));
        let count2 = count.clone();

        let token = dispatcher.subscribe(Box::new(move |name, _doc| {
            assert_eq!(name, "start");
            count2.fetch_add(1, Ordering::Relaxed);
        }));

        let rs = RunStart::new(1);
        dispatcher.dispatch("start", &Document::RunStart(rs));
        assert_eq!(count.load(Ordering::Relaxed), 1);

        dispatcher.unsubscribe(token);
        assert_eq!(dispatcher.subscriber_count(), 0);
    }
}
