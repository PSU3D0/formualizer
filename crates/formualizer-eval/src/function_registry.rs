use crate::traits::Function;
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::{
    borrow::Borrow,
    hash::Hash,
    sync::Arc,
};

#[derive(Eq, PartialEq, Hash)]
struct FnKey {
    ns: &'static str,
    name: &'static str,
}

/// Borrowed form used only at lookup time
#[derive(Eq, PartialEq, Hash)]
struct FnKeyRef<'a> {
    ns: &'a str,
    name: &'a str,
}

impl<'a> Borrow<FnKeyRef<'a>> for FnKey {
    fn borrow(&self) -> &FnKeyRef<'a> {
        // SAFETY: FnKey and FnKeyRef have identical layout, and 'static outlives any 'a.
        unsafe { &*(self as *const FnKey as *const FnKeyRef<'a>) }
    }
}

impl<'a> From<&'a FnKey> for &'a FnKeyRef<'a> {
    fn from(key: &'a FnKey) -> Self {
        key.borrow()
    }
}

static REG: Lazy<DashMap<FnKey, Arc<dyn Function>>> = Lazy::new(DashMap::new);

pub fn register(f: Arc<dyn Function>) {
    REG.insert(
        FnKey {
            ns: f.namespace(),
            name: f.name(),
        },
        f,
    );
}
pub fn get(ns: &str, name: &str) -> Option<Arc<dyn Function>> {
    REG.get(&FnKeyRef { ns, name })
        .map(|v| Arc::clone(v.value()))
}
