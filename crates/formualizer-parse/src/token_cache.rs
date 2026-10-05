//! FIFO lexical cache: constant-time hits, incremental payload accounting.
use crate::TokenSpan;
use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};
pub(crate) struct TokenCache {
    entries: HashMap<Arc<str>, Arc<[TokenSpan]>>,
    order: VecDeque<Arc<str>>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}
impl Default for TokenCache {
    fn default() -> Self {
        Self::new(16_384, 8 * 1024 * 1024)
    }
}
impl TokenCache {
    pub(crate) fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }
    fn payload(source: &str, tokens: &[TokenSpan]) -> usize {
        source.len().saturating_add(std::mem::size_of_val(tokens))
    }
    pub(crate) fn get(&self, source: &str) -> Option<(Arc<str>, Arc<[TokenSpan]>)> {
        self.entries
            .get_key_value(source)
            .map(|(s, t)| (Arc::clone(s), Arc::clone(t)))
    }
    pub(crate) fn insert(&mut self, source: Arc<str>, tokens: Arc<[TokenSpan]>) {
        let bytes = Self::payload(&source, &tokens);
        if self.max_entries == 0
            || bytes > self.max_bytes
            || self.entries.contains_key(source.as_ref())
        {
            return;
        }
        while self.entries.len() >= self.max_entries || self.bytes > self.max_bytes - bytes {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            if let Some(tokens) = self.entries.remove(old.as_ref()) {
                self.bytes -= Self::payload(&old, &tokens);
            }
        }
        self.bytes += bytes;
        self.order.push_back(Arc::clone(&source));
        self.entries.insert(source, tokens);
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn exact_payload_boundary() {
        let span = TokenSpan {
            token_type: crate::TokenType::Literal,
            subtype: crate::TokenSubType::None,
            start: 0,
            end: 1,
        };
        let payload = 1 + std::mem::size_of::<TokenSpan>();
        let mut c = TokenCache::new(10, payload);
        c.insert(Arc::from("a"), Arc::from([span]));
        assert_eq!(c.bytes, payload);
        assert!(c.get("a").is_some());
        c.insert(Arc::from("b"), Arc::from([span]));
        assert!(c.get("a").is_none());
        assert!(c.get("b").is_some());
        c.insert(Arc::from("oversized"), Arc::from([span]));
        assert!(c.get("b").is_some());
        assert_eq!(c.bytes, payload);
    }
    #[test]
    fn fifo_and_disabled() {
        let mut c = TokenCache::new(2, 100);
        for s in ["a", "b"] {
            c.insert(Arc::from(s), Arc::from([]));
        }
        assert!(c.get("a").is_some());
        c.insert(Arc::from("c"), Arc::from([]));
        assert!(c.get("a").is_none());
        assert_eq!(c.bytes, 2);
        let mut c = TokenCache::new(0, 100);
        c.insert(Arc::from("a"), Arc::from([]));
        assert!(c.get("a").is_none());
        let mut c = TokenCache::new(10, 1);
        c.insert(Arc::from("ab"), Arc::from([]));
        assert!(c.get("ab").is_none());
    }
}
