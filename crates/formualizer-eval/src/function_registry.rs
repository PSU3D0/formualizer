use crate::function::{FnCaps, Function};
use crate::function_contract::{
    FunctionDependencySemantics, FunctionEnvironmentSemantics, FunctionEvaluationSemantics,
    FunctionResultSemantics, FunctionSemanticContract,
};
use once_cell::sync::Lazy;
use std::collections::{HashMap, VecDeque};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

type RegistryKey = (String, String);

#[derive(Clone)]
struct RegistryEntry {
    function: Arc<dyn Function>,
    generation: u64,
    trusted_builtin: bool,
    semantics: SemanticContractResolution,
    semantics_by_arity: Arc<RwLock<HashMap<usize, SemanticContractResolution>>>,
}

#[derive(Clone)]
struct AliasEntry {
    target: RegistryKey,
    owner: Option<(RegistryKey, u64)>,
}

struct RegistryState {
    registrations: HashMap<RegistryKey, RegistryEntry>,
    aliases: HashMap<RegistryKey, AliasEntry>,
    semantic_epoch: u64,
    semantic_changes: VecDeque<(u64, Vec<RegistryKey>)>,
}

impl Default for RegistryState {
    fn default() -> Self {
        Self {
            registrations: HashMap::new(),
            aliases: HashMap::new(),
            semantic_epoch: 1,
            semantic_changes: VecDeque::new(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SemanticConformanceIssue {
    CapabilityPanicked,
    DependencyContractPanicked,
    SemanticContractPanicked,
    ArityMetadataPanicked,
    VariadicMetadataPanicked,
    AliasMetadataPanicked,
    ArgumentSchemaPanicked,
    AritySchemaMismatch,
    DynamicDependencyMismatch,
    ShortCircuitMismatch,
    ReferenceResultMismatch,
    LocalEnvironmentMismatch,
    SpillResultMismatch,
}

#[derive(Clone, Debug)]
pub struct SemanticContractResolution {
    pub contract: Option<FunctionSemanticContract>,
    pub generation: u64,
    pub trusted_builtin: bool,
    pub issues: Vec<SemanticConformanceIssue>,
}

impl SemanticContractResolution {
    pub fn conforms(&self) -> bool {
        self.contract.is_some() && self.issues.is_empty()
    }
}

#[derive(Clone)]
pub struct ResolvedFunction {
    pub namespace: String,
    pub canonical_name: String,
    pub function: Arc<dyn Function>,
    pub semantics: SemanticContractResolution,
}

static REGISTRY: Lazy<RwLock<RegistryState>> = Lazy::new(|| RwLock::new(RegistryState::default()));
static NEXT_GENERATION: AtomicU64 = AtomicU64::new(1);

#[inline]
fn norm<S: AsRef<str>>(s: S) -> String {
    s.as_ref().to_uppercase()
}

pub fn semantic_epoch() -> u64 {
    REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .semantic_epoch
}

pub(crate) struct SemanticEpochReadGuard(std::sync::RwLockReadGuard<'static, RegistryState>);

impl SemanticEpochReadGuard {
    pub(crate) fn epoch(&self) -> u64 {
        self.0.semantic_epoch
    }
}

pub(crate) fn semantic_epoch_read_guard() -> SemanticEpochReadGuard {
    SemanticEpochReadGuard(
        REGISTRY
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner()),
    )
}

pub(crate) struct SemanticChanges {
    pub(crate) epoch: u64,
    pub(crate) complete: bool,
    pub(crate) keys: Vec<(String, String)>,
}

fn publish_semantic_change(state: &mut RegistryState, keys: impl IntoIterator<Item = RegistryKey>) {
    state.semantic_epoch = state.semantic_epoch.saturating_add(1);
    let epoch = state.semantic_epoch;
    state
        .semantic_changes
        .push_back((epoch, keys.into_iter().collect()));
    if state.semantic_changes.len() > 1_024 {
        state.semantic_changes.pop_front();
    }
}

pub(crate) fn semantic_changes_since(epoch: u64) -> SemanticChanges {
    let state = REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let complete = state
        .semantic_changes
        .front()
        .is_none_or(|(oldest, _)| epoch.saturating_add(1) >= *oldest);
    let keys = state
        .semantic_changes
        .iter()
        .filter(|(changed_epoch, _)| *changed_epoch > epoch)
        .flat_map(|(_, keys)| keys.iter().cloned())
        .collect();
    SemanticChanges {
        epoch: state.semantic_epoch,
        complete,
        keys,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegistrationError {
    NameMetadataPanicked,
    NamespaceMetadataPanicked,
}

pub fn try_register_function(function: Arc<dyn Function>) -> Result<(), RegistrationError> {
    register(function, false)
}

pub fn register_function(function: Arc<dyn Function>) {
    let _ = try_register_function(function);
}
pub(crate) fn register_builtin(function: Arc<dyn Function>) {
    register(function, true).expect("builtin name and namespace metadata must not panic");
}

fn register(function: Arc<dyn Function>, trusted_builtin: bool) -> Result<(), RegistrationError> {
    let namespace = catch_unwind(AssertUnwindSafe(|| function.namespace()))
        .map_err(|_| RegistrationError::NamespaceMetadataPanicked)?;
    let name = catch_unwind(AssertUnwindSafe(|| function.name()))
        .map_err(|_| RegistrationError::NameMetadataPanicked)?;
    let key = (norm(namespace), norm(name));
    let generation = NEXT_GENERATION.fetch_add(1, Ordering::Relaxed);
    let aliases = catch_unwind(AssertUnwindSafe(|| function.aliases().to_vec()));
    let min_args = catch_unwind(AssertUnwindSafe(|| function.min_args()));
    let initial_arity = min_args.as_ref().copied().unwrap_or(0);
    let mut semantics = match min_args {
        Ok(arity) => inspect_semantics(&function, trusted_builtin, generation, arity),
        Err(_) => failed_resolution(
            generation,
            trusted_builtin,
            SemanticConformanceIssue::ArityMetadataPanicked,
        ),
    };
    let aliases = match aliases {
        Ok(aliases) => aliases,
        Err(_) => {
            semantics
                .issues
                .push(SemanticConformanceIssue::AliasMetadataPanicked);
            semantics.contract = None;
            Vec::new()
        }
    };

    let mut state = REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if trusted_builtin
        && state
            .registrations
            .get(&key)
            .is_some_and(|entry| entry.trusted_builtin)
    {
        return Ok(());
    }
    let previous = state
        .registrations
        .get(&key)
        .map(|entry| (entry.generation, entry.trusted_builtin));
    let mut changed_spellings = Vec::new();
    if let Some((previous_generation, _)) = previous {
        changed_spellings.extend(
            state
                .aliases
                .iter()
                .filter(|(_, alias)| {
                    alias.owner.as_ref() == Some(&(key.clone(), previous_generation))
                })
                .map(|(alias_key, _)| alias_key.clone()),
        );
        state
            .aliases
            .retain(|_, alias| alias.owner.as_ref() != Some(&(key.clone(), previous_generation)));
    }
    state.registrations.insert(
        key.clone(),
        RegistryEntry {
            function: Arc::clone(&function),
            generation,
            trusted_builtin,
            semantics: semantics.clone(),
            semantics_by_arity: Arc::new(RwLock::new(HashMap::from([(initial_arity, semantics)]))),
        },
    );
    for alias in aliases {
        if !alias.eq_ignore_ascii_case(&key.1) {
            let alias_key = (key.0.clone(), norm(alias));
            changed_spellings.push(alias_key.clone());
            state.aliases.insert(
                alias_key,
                AliasEntry {
                    target: key.clone(),
                    owner: Some((key.clone(), generation)),
                },
            );
        }
    }
    if previous.is_some_and(|(_, was_trusted)| was_trusted)
        || (previous.is_some() && trusted_builtin)
    {
        changed_spellings.push(key);
        publish_semantic_change(&mut state, changed_spellings);
    }
    Ok(())
}

fn failed_resolution(
    generation: u64,
    trusted_builtin: bool,
    issue: SemanticConformanceIssue,
) -> SemanticContractResolution {
    SemanticContractResolution {
        contract: None,
        generation,
        trusted_builtin,
        issues: vec![issue],
    }
}

fn inspect_semantics(
    function: &Arc<dyn Function>,
    trusted_builtin: bool,
    generation: u64,
    arity: usize,
) -> SemanticContractResolution {
    let mut issues = Vec::new();
    let caps = inspected(
        &mut issues,
        SemanticConformanceIssue::CapabilityPanicked,
        || function.caps(),
    )
    .unwrap_or_else(FnCaps::empty);
    let precision = inspected(
        &mut issues,
        SemanticConformanceIssue::DependencyContractPanicked,
        || function.dependency_contract(arity),
    )
    .flatten();
    let explicit = inspected(
        &mut issues,
        SemanticConformanceIssue::SemanticContractPanicked,
        || function.semantic_contract(arity),
    )
    .flatten();
    let schema = inspected(
        &mut issues,
        SemanticConformanceIssue::ArgumentSchemaPanicked,
        || function.arg_schema(),
    );
    let min_args = inspected(
        &mut issues,
        SemanticConformanceIssue::ArityMetadataPanicked,
        || function.min_args(),
    );
    let variadic = inspected(
        &mut issues,
        SemanticConformanceIssue::VariadicMetadataPanicked,
        || function.variadic(),
    );
    if let (Some(schema), Some(min_args), Some(variadic)) = (schema, min_args, variadic)
        && !schema_allows_arity(schema, min_args, variadic, arity, !trusted_builtin)
    {
        issues.push(SemanticConformanceIssue::AritySchemaMismatch);
    }
    let contract =
        explicit.or_else(|| trusted_builtin.then(|| trusted_contract_from_caps(caps, precision)));
    if let Some(contract) = contract {
        check_capability(
            &mut issues,
            caps.contains(FnCaps::DYNAMIC_DEPENDENCY),
            contract.dependency == FunctionDependencySemantics::Dynamic,
            SemanticConformanceIssue::DynamicDependencyMismatch,
        );
        check_capability(
            &mut issues,
            caps.contains(FnCaps::SHORT_CIRCUIT),
            contract.evaluation == FunctionEvaluationSemantics::ShortCircuit,
            SemanticConformanceIssue::ShortCircuitMismatch,
        );
        check_capability(
            &mut issues,
            caps.contains(FnCaps::RETURNS_REFERENCE),
            contract.result.may_return_reference(),
            SemanticConformanceIssue::ReferenceResultMismatch,
        );
        check_capability(
            &mut issues,
            caps.contains(FnCaps::LOCAL_ENVIRONMENT),
            contract.environment == FunctionEnvironmentSemantics::LocalBindings,
            SemanticConformanceIssue::LocalEnvironmentMismatch,
        );
        check_capability(
            &mut issues,
            caps.contains(FnCaps::MAY_SPILL),
            contract.result.may_spill(),
            SemanticConformanceIssue::SpillResultMismatch,
        );
    }
    SemanticContractResolution {
        contract: issues.is_empty().then_some(contract).flatten(),
        generation,
        trusted_builtin,
        issues,
    }
}

fn schema_allows_arity(
    schema: &[crate::args::ArgSchema],
    min_args: usize,
    variadic: bool,
    arity: usize,
    strict_required_count: bool,
) -> bool {
    if schema.is_empty() {
        return min_args == 0 && arity == 0;
    }

    let mut optional_seen = false;
    let mut required_count = 0usize;
    let mut repeating = None;
    for (index, argument) in schema.iter().enumerate() {
        if argument.required {
            if optional_seen {
                return false;
            }
            required_count += 1;
        } else {
            optional_seen = true;
        }
        if let Some(width) = argument.repeating {
            if width == 0 || repeating.is_some() || index + 1 != schema.len() {
                return false;
            }
            repeating = Some(width);
        }
    }
    let represented_minimum = min_args.min(schema.len());
    if (strict_required_count && required_count != min_args)
        || (!strict_required_count && required_count < represented_minimum)
        || schema
            .iter()
            .take(represented_minimum)
            .any(|argument| !argument.required)
        || (!variadic && schema.len() > 1 && min_args > schema.len())
        || arity < min_args
    {
        return false;
    }
    if let Some(width) = repeating {
        if width > schema.len() {
            return false;
        }
        let fixed_prefix = schema.len() - width;
        return arity >= schema.len() && (arity - fixed_prefix).is_multiple_of(width);
    }
    if variadic {
        return true;
    }
    arity <= schema.len().max(min_args)
}

fn inspected<T>(
    issues: &mut Vec<SemanticConformanceIssue>,
    issue: SemanticConformanceIssue,
    inspect: impl FnOnce() -> T,
) -> Option<T> {
    match catch_unwind(AssertUnwindSafe(inspect)) {
        Ok(value) => Some(value),
        Err(_) => {
            issues.push(issue);
            None
        }
    }
}

fn check_capability(
    issues: &mut Vec<SemanticConformanceIssue>,
    capability: bool,
    semantic: bool,
    issue: SemanticConformanceIssue,
) {
    if capability != semantic {
        issues.push(issue);
    }
}

fn trusted_contract_from_caps(
    caps: FnCaps,
    precision: Option<crate::function_contract::FunctionDependencyContract>,
) -> FunctionSemanticContract {
    let mut contract = FunctionSemanticContract::trusted_builtin_default(precision);
    if caps.contains(FnCaps::DYNAMIC_DEPENDENCY) {
        contract.dependency = FunctionDependencySemantics::Dynamic;
    }
    if caps.contains(FnCaps::SHORT_CIRCUIT) {
        contract.evaluation = FunctionEvaluationSemantics::ShortCircuit;
    }
    contract.result = FunctionResultSemantics::from_capabilities(
        caps.contains(FnCaps::RETURNS_REFERENCE),
        caps.contains(FnCaps::MAY_SPILL),
    );
    if caps.contains(FnCaps::LOCAL_ENVIRONMENT) {
        contract.environment = FunctionEnvironmentSemantics::LocalBindings;
    }
    contract
}

const EXCEL_PREFIXES: &[&str] = &["_XLFN.", "_XLL.", "_XLWS."];

fn resolve_registered(
    state: &RegistryState,
    key: &RegistryKey,
) -> Option<(RegistryKey, RegistryEntry)> {
    if let Some(entry) = state.registrations.get(key) {
        return Some((key.clone(), entry.clone()));
    }
    let alias = state.aliases.get(key)?;
    state
        .registrations
        .get(&alias.target)
        .map(|entry| (alias.target.clone(), entry.clone()))
}

fn resolve_entry(ns: &str, name: &str) -> Option<(RegistryKey, RegistryEntry)> {
    let ns = norm(ns);
    let normalized_name = norm(name);
    let key = (ns.clone(), normalized_name.clone());
    let mut state = REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    if let Some(entry) = resolve_registered(&state, &key) {
        return Some(entry);
    }
    let mut candidate = normalized_name.as_str();
    loop {
        let mut stripped_any = false;
        for prefix in EXCEL_PREFIXES {
            if let Some(rest) = candidate.strip_prefix(prefix) {
                candidate = rest;
                stripped_any = true;
                let stripped_key = (ns.clone(), candidate.to_string());
                if let Some((canonical, entry)) = resolve_registered(&state, &stripped_key) {
                    state.aliases.insert(
                        key.clone(),
                        AliasEntry {
                            target: canonical.clone(),
                            owner: Some((canonical.clone(), entry.generation)),
                        },
                    );
                    return Some((canonical, entry));
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

pub fn get(ns: &str, name: &str) -> Option<Arc<dyn Function>> {
    resolve_entry(ns, name).map(|(_, entry)| entry.function)
}
pub fn resolve(ns: &str, name: &str) -> Option<ResolvedFunction> {
    resolve_entry(ns, name).map(to_resolved)
}

pub fn resolve_with_epoch(ns: &str, name: &str) -> Option<(u64, ResolvedFunction)> {
    let key = (norm(ns), norm(name));
    let state = REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    resolve_registered(&state, &key).map(|entry| (state.semantic_epoch, to_resolved(entry)))
}
pub fn resolve_for_arity(ns: &str, name: &str, arity: usize) -> Option<ResolvedFunction> {
    resolve_entry(ns, name).map(|((namespace, canonical_name), entry)| {
        let semantics = {
            let cached = entry
                .semantics_by_arity
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .get(&arity)
                .cloned();
            cached.unwrap_or_else(|| {
                let inspected = inspect_semantics(
                    &entry.function,
                    entry.trusted_builtin,
                    entry.generation,
                    arity,
                );
                entry
                    .semantics_by_arity
                    .write()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .entry(arity)
                    .or_insert_with(|| inspected.clone())
                    .clone()
            })
        };
        ResolvedFunction {
            semantics,
            namespace,
            canonical_name,
            function: entry.function,
        }
    })
}
fn to_resolved(
    ((namespace, canonical_name), entry): (RegistryKey, RegistryEntry),
) -> ResolvedFunction {
    ResolvedFunction {
        namespace,
        canonical_name,
        function: entry.function,
        semantics: entry.semantics,
    }
}

pub fn register_alias(ns: &str, alias: &str, target_ns: &str, target_name: &str) {
    let mut state = REGISTRY
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let alias_key = (norm(ns), norm(alias));
    let target = (norm(target_ns), norm(target_name));
    let old_target = state
        .aliases
        .get(&alias_key)
        .map(|entry| entry.target.clone());
    if old_target.as_ref() == Some(&target) {
        return;
    }
    state.aliases.insert(
        alias_key.clone(),
        AliasEntry {
            target: target.clone(),
            owner: None,
        },
    );
    let mut changed = vec![alias_key, target];
    if let Some(old_target) = old_target {
        changed.push(old_target);
    }
    publish_semantic_change(&mut state, changed);
}

pub fn snapshot_registered() -> Vec<(String, String, Arc<dyn Function>)> {
    let state = REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state
        .registrations
        .iter()
        .map(|((ns, name), entry)| (ns.clone(), name.clone(), Arc::clone(&entry.function)))
        .collect()
}
pub fn snapshot_semantics() -> Vec<ResolvedFunction> {
    let state = REGISTRY
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    state
        .registrations
        .iter()
        .map(|((namespace, canonical_name), entry)| ResolvedFunction {
            namespace: namespace.clone(),
            canonical_name: canonical_name.clone(),
            function: Arc::clone(&entry.function),
            semantics: entry.semantics.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestFn {
        ns: &'static str,
        name: &'static str,
        aliases: &'static [&'static str],
    }

    impl Function for TestFn {
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
    fn resolves_prefixes_aliases_and_direct_registration() {
        let ns = "__REG_PREFIX__";
        register_function(Arc::new(TestFn {
            ns,
            name: "FILTER",
            aliases: &["LEGACY"],
        }));
        assert_eq!(get(ns, "_xlfn._xlws.legacy").unwrap().name(), "FILTER");
        register_function(Arc::new(TestFn {
            ns,
            name: "_XLFN.FILTER",
            aliases: &[],
        }));
        assert_eq!(get(ns, "_xlfn.filter").unwrap().name(), "_XLFN.FILTER");
    }

    #[test]
    fn trusted_replacement_records_removed_owned_alias_spelling() {
        let namespace = "__REG_STALE_ALIAS__";
        register_builtin(Arc::new(TestFn {
            ns: namespace,
            name: "TARGET",
            aliases: &["STALE_OWNED_ALIAS"],
        }));
        let before = semantic_epoch();
        register_function(Arc::new(TestFn {
            ns: namespace,
            name: "TARGET",
            aliases: &["NEW_OWNED_ALIAS"],
        }));
        let changes = semantic_changes_since(before);
        assert!(
            changes
                .keys
                .contains(&(namespace.to_string(), "STALE_OWNED_ALIAS".to_string()))
        );
        assert!(
            changes
                .keys
                .contains(&(namespace.to_string(), "NEW_OWNED_ALIAS".to_string()))
        );
    }

    #[test]
    fn alias_redirect_publishes_old_new_targets_and_spelling_atomically() {
        crate::builtins::load_builtins();
        register_alias("", "__ALIAS_EPOCH_FIXTURE__", "", "SUM");
        let before = semantic_epoch();
        register_alias("", "__ALIAS_EPOCH_FIXTURE__", "", "ABS");
        let changes = semantic_changes_since(before);
        assert!(changes.epoch > before);
        assert!(changes.complete);
        for expected in [
            (String::new(), "__ALIAS_EPOCH_FIXTURE__".to_string()),
            (String::new(), "SUM".to_string()),
            (String::new(), "ABS".to_string()),
        ] {
            assert!(changes.keys.contains(&expected), "missing {expected:?}");
        }
    }

    #[test]
    fn replacement_advances_semantic_generation() {
        let ns = "__REG_GENERATION__";
        register_function(Arc::new(TestFn {
            ns,
            name: "F",
            aliases: &[],
        }));
        let first = resolve(ns, "F").unwrap().semantics.generation;
        register_function(Arc::new(TestFn {
            ns,
            name: "F",
            aliases: &[],
        }));
        let second = resolve(ns, "F").unwrap().semantics.generation;
        assert!(second > first);
    }

    struct PanickingSchemaFn;

    impl Function for PanickingSchemaFn {
        fn name(&self) -> &'static str {
            "PANICKING_SCHEMA"
        }
        fn semantic_contract(&self, _arity: usize) -> Option<FunctionSemanticContract> {
            Some(FunctionSemanticContract::trusted_builtin_default(None))
        }
        fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
            panic!("bad schema")
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            unreachable!()
        }
    }

    #[test]
    fn schema_panic_as_sole_defect_is_non_panicking_and_fails_closed() {
        register_function(Arc::new(PanickingSchemaFn));
        let semantics = resolve("", "PANICKING_SCHEMA").unwrap().semantics;
        assert!(semantics.contract.is_none());
        assert!(
            semantics
                .issues
                .contains(&SemanticConformanceIssue::ArgumentSchemaPanicked)
        );
        assert_eq!(
            semantics.issues,
            vec![SemanticConformanceIssue::ArgumentSchemaPanicked]
        );
        assert!(!semantics.conforms());
    }

    #[test]
    fn every_registered_builtin_has_a_conforming_semantic_contract() {
        crate::builtins::load_builtins();
        let builtins: Vec<_> = snapshot_semantics()
            .into_iter()
            .filter(|entry| entry.semantics.trusted_builtin)
            .collect();
        assert!(builtins.len() > 100);
        let rejected: Vec<_> = builtins
            .iter()
            .filter(|entry| !entry.semantics.conforms())
            .map(|entry| {
                (
                    &entry.namespace,
                    &entry.canonical_name,
                    &entry.semantics.issues,
                )
            })
            .collect();
        assert!(rejected.is_empty(), "non-conforming builtins: {rejected:?}");
    }

    #[test]
    fn semantic_contract_is_context_and_arity_aware() {
        crate::builtins::lookup::register_builtins();
        let row_without_arg = resolve_for_arity("", "ROW", 0).unwrap();
        let row_with_arg = resolve_for_arity("", "ROW", 1).unwrap();
        assert_eq!(
            row_without_arg.semantics.contract.unwrap().context,
            crate::function_contract::FunctionContextDependence::PlacementDependent
        );
        assert_eq!(
            row_with_arg.semantics.contract.unwrap().context,
            crate::function_contract::FunctionContextDependence::None
        );
    }

    struct ExplicitSafeCustomFn;

    impl Function for ExplicitSafeCustomFn {
        fn name(&self) -> &'static str {
            "EXPLICIT_SAFE_CUSTOM"
        }
        fn semantic_contract(&self, _arity: usize) -> Option<FunctionSemanticContract> {
            Some(FunctionSemanticContract::trusted_builtin_default(None))
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            unreachable!()
        }
    }

    #[test]
    fn explicit_custom_semantics_can_conform_without_becoming_trusted() {
        register_function(Arc::new(ExplicitSafeCustomFn));
        let semantics = resolve_for_arity("", "EXPLICIT_SAFE_CUSTOM", 0)
            .unwrap()
            .semantics;
        assert!(!semantics.trusted_builtin);
        assert!(semantics.conforms());
    }

    #[test]
    fn concurrent_replacements_leave_only_final_owned_alias() {
        let ns = "__REG_CONCURRENT__";
        register_function(Arc::new(TestFn {
            ns,
            name: "TARGET",
            aliases: &["INITIAL"],
        }));
        let mut workers = Vec::new();
        for alias in ["A", "B", "C", "D"] {
            workers.push(std::thread::spawn(move || {
                for _ in 0..100 {
                    let aliases: &'static [&'static str] = Box::leak(Box::new([alias]));
                    register_function(Arc::new(TestFn {
                        ns,
                        name: "TARGET",
                        aliases,
                    }));
                    assert_eq!(get(ns, "TARGET").unwrap().name(), "TARGET");
                }
            }));
        }
        for worker in workers {
            worker.join().unwrap();
        }
        register_function(Arc::new(TestFn {
            ns,
            name: "TARGET",
            aliases: &["FINAL"],
        }));
        for stale in ["INITIAL", "A", "B", "C", "D"] {
            assert!(get(ns, stale).is_none());
        }
        assert!(get(ns, "FINAL").is_some());
    }

    #[test]
    fn independent_exception_inventory_matches_builtin_caps_and_context() {
        crate::builtins::load_builtins();
        for name in [
            "RAND",
            "RANDBETWEEN",
            "RANDARRAY",
            "TODAY",
            "NOW",
            "OFFSET",
            "INDIRECT",
        ] {
            assert!(
                get("", name).unwrap().caps().contains(FnCaps::VOLATILE),
                "{name}"
            );
        }
        for name in ["OFFSET", "INDIRECT"] {
            assert!(
                get("", name)
                    .unwrap()
                    .caps()
                    .contains(FnCaps::DYNAMIC_DEPENDENCY),
                "{name}"
            );
        }
        for name in ["INDEX", "OFFSET", "INDIRECT", "CHOOSE"] {
            assert!(
                get("", name)
                    .unwrap()
                    .caps()
                    .contains(FnCaps::RETURNS_REFERENCE),
                "{name}"
            );
        }
        for name in ["LET", "LAMBDA"] {
            assert!(
                get("", name)
                    .unwrap()
                    .caps()
                    .contains(FnCaps::LOCAL_ENVIRONMENT),
                "{name}"
            );
        }
        for name in [
            "IF",
            "IFERROR",
            "IFNA",
            "IFS",
            "SWITCH",
            "CHOOSE",
            "FILTER",
            "UNIQUE",
            "SEQUENCE",
            "TRANSPOSE",
            "TAKE",
            "DROP",
            "SORT",
            "SORTBY",
            "RANDARRAY",
            "HSTACK",
            "VSTACK",
            "TOCOL",
            "TOROW",
            "CHOOSECOLS",
            "CHOOSEROWS",
            "FREQUENCY",
            "LINEST",
            "TREND",
            "GROWTH",
            "LOGEST",
            "MODE.MULT",
            "TEXTSPLIT",
        ] {
            assert!(
                get("", name).unwrap().caps().contains(FnCaps::MAY_SPILL),
                "{name}"
            );
        }
        const SHORT_CIRCUIT: &[&str] = &[
            "IF", "IFERROR", "IFNA", "IFS", "SWITCH", "CHOOSE", "LET", "LAMBDA", "AND", "OR",
        ];
        let observed_short_circuit: std::collections::BTreeSet<_> = snapshot_registered()
            .into_iter()
            .filter(|(namespace, _, function)| {
                namespace.is_empty() && function.caps().contains(FnCaps::SHORT_CIRCUIT)
            })
            .map(|(_, name, _)| name)
            .collect();
        let expected_short_circuit: std::collections::BTreeSet<_> = SHORT_CIRCUIT
            .iter()
            .map(|name| (*name).to_string())
            .collect();
        assert_eq!(observed_short_circuit, expected_short_circuit);
        for name in SHORT_CIRCUIT {
            let contract = resolve_for_arity("", name, get("", name).unwrap().min_args())
                .unwrap()
                .semantics
                .contract
                .unwrap();
            assert_eq!(
                contract.evaluation,
                FunctionEvaluationSemantics::ShortCircuit,
                "{name}"
            );
        }
        assert_eq!(
            resolve_for_arity("", "CHOOSE", 2)
                .unwrap()
                .semantics
                .contract
                .unwrap()
                .result,
            FunctionResultSemantics::MayReturnReferenceAndSpill
        );
        for name in ["ROW", "COLUMN"] {
            let contract = resolve_for_arity("", name, 0)
                .unwrap()
                .semantics
                .contract
                .unwrap();
            assert_eq!(
                contract.context,
                crate::function_contract::FunctionContextDependence::PlacementDependent,
                "{name}"
            );
            assert_eq!(
                resolve_for_arity("", name, 1)
                    .unwrap()
                    .semantics
                    .contract
                    .unwrap()
                    .context,
                crate::function_contract::FunctionContextDependence::None,
                "{name} with argument"
            );
        }
        for name in ["ISFORMULA", "FORMULATEXT", "SHEET", "SHEETS"] {
            let contract = resolve_for_arity("", name, get("", name).unwrap().min_args())
                .unwrap()
                .semantics
                .contract
                .unwrap();
            assert_eq!(
                contract.context,
                crate::function_contract::FunctionContextDependence::WorkbookMetadata,
                "{name}"
            );
        }
    }

    struct NamePanicFn;
    impl Function for NamePanicFn {
        fn name(&self) -> &'static str {
            panic!("name")
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            unreachable!()
        }
    }
    struct NamespacePanicFn;
    impl Function for NamespacePanicFn {
        fn name(&self) -> &'static str {
            "NS_PANIC"
        }
        fn namespace(&self) -> &'static str {
            panic!("namespace")
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            unreachable!()
        }
    }

    #[test]
    fn canonical_metadata_panics_decline_registration_without_unwinding() {
        assert_eq!(
            try_register_function(Arc::new(NamePanicFn)),
            Err(RegistrationError::NameMetadataPanicked)
        );
        assert_eq!(
            try_register_function(Arc::new(NamespacePanicFn)),
            Err(RegistrationError::NamespaceMetadataPanicked)
        );
        register_function(Arc::new(NamePanicFn));
        register_function(Arc::new(NamespacePanicFn));
        assert!(get("", "NS_PANIC").is_none());
    }

    #[derive(Clone, Copy)]
    enum BadSchemaKind {
        TooLarge,
        Repeating,
        MinDisagreement,
        RequiredCount,
        TooManyRequired,
        RepeatWidth,
    }
    struct BadSchemaFn {
        name: &'static str,
        kind: BadSchemaKind,
    }
    impl Function for BadSchemaFn {
        fn name(&self) -> &'static str {
            self.name
        }
        fn min_args(&self) -> usize {
            if matches!(
                self.kind,
                BadSchemaKind::MinDisagreement
                    | BadSchemaKind::RequiredCount
                    | BadSchemaKind::RepeatWidth
            ) {
                2
            } else {
                1
            }
        }
        fn variadic(&self) -> bool {
            matches!(self.kind, BadSchemaKind::RepeatWidth)
        }
        fn semantic_contract(&self, _arity: usize) -> Option<FunctionSemanticContract> {
            Some(FunctionSemanticContract::trusted_builtin_default(None))
        }
        fn arg_schema(&self) -> &'static [crate::args::ArgSchema] {
            static ONE: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
                std::sync::LazyLock::new(|| vec![crate::args::ArgSchema::any()]);
            static BAD_REPEAT: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
                std::sync::LazyLock::new(|| {
                    let mut arg = crate::args::ArgSchema::any();
                    arg.repeating = Some(0);
                    vec![arg]
                });
            static REQUIRED_COUNT: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
                std::sync::LazyLock::new(|| {
                    let mut optional = crate::args::ArgSchema::any();
                    optional.required = false;
                    vec![crate::args::ArgSchema::any(), optional]
                });
            static REPEAT_WIDTH: std::sync::LazyLock<Vec<crate::args::ArgSchema>> =
                std::sync::LazyLock::new(|| {
                    let first = crate::args::ArgSchema::any();
                    let mut second = crate::args::ArgSchema::any();
                    second.repeating = Some(2);
                    vec![first, second]
                });
            match self.kind {
                BadSchemaKind::TooLarge => &ONE,
                BadSchemaKind::Repeating => &BAD_REPEAT,
                BadSchemaKind::MinDisagreement => &[],
                BadSchemaKind::RequiredCount => &REQUIRED_COUNT,
                BadSchemaKind::TooManyRequired => &REPEAT_WIDTH,
                BadSchemaKind::RepeatWidth => &REPEAT_WIDTH,
            }
        }
        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [crate::traits::ArgumentHandle<'a, 'b>],
            _ctx: &dyn crate::traits::FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, formualizer_common::ExcelError> {
            unreachable!()
        }
    }

    #[test]
    fn malformed_arity_and_schema_contracts_fail_closed() {
        for (name, kind, arity) in [
            ("TOO_LARGE", BadSchemaKind::TooLarge, 2),
            ("BAD_REPEAT", BadSchemaKind::Repeating, 1),
            ("MIN_DISAGREEMENT", BadSchemaKind::MinDisagreement, 2),
            ("REQUIRED_COUNT", BadSchemaKind::RequiredCount, 2),
            ("TOO_MANY_REQUIRED", BadSchemaKind::TooManyRequired, 1),
            ("REPEAT_WIDTH", BadSchemaKind::RepeatWidth, 3),
        ] {
            register_function(Arc::new(BadSchemaFn { name, kind }));
            let semantics = resolve_for_arity("", name, arity).unwrap().semantics;
            assert!(semantics.contract.is_none(), "{name}");
            assert!(
                semantics
                    .issues
                    .contains(&SemanticConformanceIssue::AritySchemaMismatch),
                "{name}: {:?}",
                semantics.issues
            );
        }
    }

    #[test]
    fn valid_optional_and_width_n_repeating_schemas_conform() {
        let required = crate::args::ArgSchema::any();
        let mut optional = crate::args::ArgSchema::any();
        optional.required = false;
        assert!(schema_allows_arity(
            &[required.clone(), optional],
            1,
            false,
            2,
            true
        ));

        let mut repeat_end = crate::args::ArgSchema::any();
        repeat_end.repeating = Some(2);
        let repeating = [required, repeat_end];
        assert!(schema_allows_arity(&repeating, 2, true, 4, true));
        assert!(!schema_allows_arity(&repeating, 2, true, 3, true));
    }

    #[test]
    fn replacement_readers_observe_generation_and_epoch_atomically() {
        let ns = "__REG_SNAPSHOT_RACE__";
        register_builtin(Arc::new(TestFn {
            ns,
            name: "TARGET",
            aliases: &[],
        }));
        let (initial_epoch, initial) = resolve_with_epoch(ns, "TARGET").unwrap();
        let initial_generation = initial.semantics.generation;
        let barrier = Arc::new(std::sync::Barrier::new(5));
        let mut readers = Vec::new();
        for _ in 0..4 {
            let barrier = Arc::clone(&barrier);
            readers.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..1_000 {
                    let (epoch, resolved) = resolve_with_epoch(ns, "TARGET").unwrap();
                    if resolved.semantics.generation != initial_generation {
                        assert!(epoch > initial_epoch);
                    }
                }
            }));
        }
        barrier.wait();
        register_function(Arc::new(TestFn {
            ns,
            name: "TARGET",
            aliases: &[],
        }));
        for reader in readers {
            reader.join().unwrap();
        }
        let (epoch, resolved) = resolve_with_epoch(ns, "TARGET").unwrap();
        assert!(epoch > initial_epoch);
        assert!(resolved.semantics.generation > initial_generation);
    }
}
