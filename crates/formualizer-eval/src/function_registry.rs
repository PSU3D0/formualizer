use once_cell::sync::Lazy;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use crate::traits::Function;

/// 2-tuple key: (namespace, name)
pub type FnKey = (&'static str, &'static str);

static REG: Lazy<RwLock<HashMap<FnKey, Arc<dyn Function>>>> =
    Lazy::new(|| RwLock::new(HashMap::new()));

pub fn register(f: Arc<dyn Function>) {
    REG.write().unwrap().insert((f.namespace(), f.name()), f);
}

/// Lookup helper used by the interpreter.
pub fn get(namespace: &str, name: &str) -> Option<Arc<dyn Function>> {
    REG.read().ok()?.get(&(namespace, name)).cloned()
}
