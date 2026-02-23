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

// Known Excel function prefixes that should be stripped for compatibility
const EXCEL_PREFIXES: &[&str] = &["_XLFN.", "_XLL.", "_XLWS."];

fn resolve_registered(key: &(String, String)) -> Option<Arc<dyn Function>> {
    // Try direct lookup
    if let Some(v) = REG.get(key) {
        return Some(Arc::clone(v.value()));
    }

    // Try existing alias
    if let Some(canon) = ALIASES.get(key)
        && let Some(v) = REG.get(canon.value())
    {
        return Some(Arc::clone(v.value()));
    }

    None
}

pub fn get(ns: &str, name: &str) -> Option<Arc<dyn Function>> {
    let ns_norm = norm(ns);
    let normalized_name = norm(name);
    let key = (ns_norm.clone(), normalized_name.clone());

    if let Some(v) = resolve_registered(&key) {
        return Some(v);
    }

    // Try repeatedly stripping known Excel prefixes and cache discovered aliases.
    //
    // This handles formulas like:
    //   _xlfn.SUM(...)
    //   _xlfn._xlws.FILTER(...)
    // without mutating original formula text/AST.
    let mut candidate = normalized_name.as_str();
    loop {
        let mut stripped_any = false;
        for prefix in EXCEL_PREFIXES {
            if let Some(rest) = candidate.strip_prefix(prefix) {
                candidate = rest;
                stripped_any = true;

                let stripped_key = (ns_norm.clone(), candidate.to_string());
                if let Some(v) = resolve_registered(&stripped_key) {
                    // Cache this discovery as an alias for future lookups.
                    ALIASES.insert(key.clone(), stripped_key);
                    return Some(v);
                }

                break;
            }
        }

        if !stripped_any {
            break;
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

/// Snapshot canonical registered functions (namespace, name, function object).
///
/// Keys are normalized uppercase. Aliases are not included in this list.
pub fn snapshot_registered() -> Vec<(String, String, Arc<dyn Function>)> {
    REG.iter()
        .map(|entry| {
            let ((ns, name), func) = entry.pair();
            (ns.clone(), name.clone(), Arc::clone(func))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::FnCaps;

    struct TestFn {
        ns: &'static str,
        name: &'static str,
        aliases: &'static [&'static str],
    }

    impl Function for TestFn {
        fn caps(&self) -> FnCaps {
            FnCaps::PURE
        }

        fn name(&self) -> &'static str {
            self.name
        }

        fn namespace(&self) -> &'static str {
            self.ns
        }

        fn aliases(&self) -> &'static [&'static str] {
            self.aliases
        }

        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            Ok(crate::traits::CalcValue::Scalar(
                formualizer_common::LiteralValue::Number(1.0),
            ))
        }
    }

    #[test]
    fn resolves_single_excel_prefix() {
        let ns = "__REG_PREFIX_SINGLE__";
        register_function(Arc::new(TestFn {
            ns,
            name: "SUM",
            aliases: &[],
        }));

        let f = get(ns, "_xlfn.sum").expect("function should resolve");
        assert_eq!(f.name(), "SUM");
    }

    #[test]
    fn resolves_chained_excel_prefixes() {
        let ns = "__REG_PREFIX_CHAINED__";
        register_function(Arc::new(TestFn {
            ns,
            name: "FILTER",
            aliases: &[],
        }));

        let f = get(ns, "_xlfn._xlws.filter").expect("function should resolve");
        assert_eq!(f.name(), "FILTER");
    }

    #[test]
    fn resolves_chained_prefixes_with_alias_target() {
        let ns = "__REG_PREFIX_ALIAS__";
        register_function(Arc::new(TestFn {
            ns,
            name: "MODERN",
            aliases: &["LEGACY"],
        }));

        let f = get(ns, "_xlfn._xlws.legacy").expect("function should resolve");
        assert_eq!(f.name(), "MODERN");
    }

    #[test]
    fn direct_prefixed_registration_wins_before_compat_stripping() {
        let ns = "__REG_DIRECT_PREFIX__";
        register_function(Arc::new(TestFn {
            ns,
            name: "SUM",
            aliases: &[],
        }));
        register_function(Arc::new(TestFn {
            ns,
            name: "_XLFN.SUM",
            aliases: &[],
        }));

        let f = get(ns, "_xlfn.sum").expect("function should resolve");
        assert_eq!(f.name(), "_XLFN.SUM");
    }
}
