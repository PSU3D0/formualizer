use crate::function::Function;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;

// Use a simple tuple as the key - DashMap will handle the hashing correctly
static REG: Lazy<DashMap<(String, String), Arc<dyn Function>>> = Lazy::new(DashMap::new);

pub fn register_function(f: Arc<dyn Function>) {
    let key = (f.namespace().to_string(), f.name().to_string());
    REG.insert(key, f);
}

pub fn get(ns: &str, name: &str) -> Option<Arc<dyn Function>> {
    // DashMap allows looking up with borrowed forms automatically
    REG.get(&(ns.to_string(), name.to_string()))
        .map(|v| Arc::clone(v.value()))
}
