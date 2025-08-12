use crate::function::Function;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::Arc;

// Case-insensitive registry keyed by (NAMESPACE, NAME) in uppercase
static REG: Lazy<DashMap<(String, String), Arc<dyn Function>>> = Lazy::new(DashMap::new);

// Optional alias map: (NS, ALIAS) -> (NS, CANONICAL_NAME), all uppercase
static ALIASES: Lazy<DashMap<(String, String), (String, String)>> = Lazy::new(DashMap::new);

#[inline]
fn norm<S: AsRef<str>>(s: S) -> String {
    s.as_ref().to_uppercase()
}

pub fn register_function(f: Arc<dyn Function>) {
    let ns = norm(f.namespace());
    let name = norm(f.name());
    let key = (ns.clone(), name.clone());
    // Insert canonical
    REG.insert(key.clone(), Arc::clone(&f));
    // Register aliases
    for &alias in f.aliases() {
        if alias.eq_ignore_ascii_case(&name) {
            continue;
        }
        let akey = (ns.clone(), norm(alias));
        ALIASES.insert(akey, key.clone());
    }
}

pub fn get(ns: &str, name: &str) -> Option<Arc<dyn Function>> {
    let key = (norm(ns), norm(name));
    if let Some(v) = REG.get(&key) {
        return Some(Arc::clone(v.value()));
    }
    // Resolve alias → canonical, then look up
    if let Some(canon) = ALIASES.get(&key) {
        if let Some(v) = REG.get(canon.value()) {
            return Some(Arc::clone(v.value()));
        }
    }
    None
}

/// Register an alias name for an existing function. All names are normalized to uppercase.
pub fn register_alias(ns: &str, alias: &str, target_ns: &str, target_name: &str) {
    let akey = (norm(ns), norm(alias));
    let tkey = (norm(target_ns), norm(target_name));
    ALIASES.insert(akey, tkey);
}
