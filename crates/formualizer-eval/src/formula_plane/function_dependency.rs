//! FormulaPlane-local function dependency contracts for FP4.B.
//!
//! This module is passive infrastructure. It classifies normalized function
//! names into dependency contracts for later FormulaPlane summary/reporting
//! work, but it does not change evaluation, graph, dirty propagation, loader,
//! or public API behavior.

use crate::args::ShapeKind;
use crate::function::FnCaps;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FunctionDependencyClass {
    StaticScalarAllArgs,
    StaticReduction,
    CriteriaAggregation,
    MaskConditional,
    LookupStaticRange,
    DynamicDependency,
    Volatile,
    ReferenceReturning,
    LocalEnvironment,
    ArrayOrSpill,
    OpaqueScalar,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FunctionSupportStatus {
    /// The name/arity can produce a supported summary if all argument shapes are
    /// supported by the later summary analyzer.
    Supported,
    /// The function is recognized for taxonomy/reporting but does not emit a
    /// supported dependency summary in FP4.B.
    ClassifiedOnly,
    /// The function is recognized as an explicit fallback/reject case.
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ArgumentDependencyRole {
    ScalarValue,
    FiniteRangeValue,
    CriteriaRange,
    CriteriaExpression,
    ReductionValue,
    LazyBranch,
    LookupKey,
    LookupTable,
    LookupResultSelector,
    ByReference,
    LocalBindingName,
    LocalBindingValue,
    LambdaBody,
    IgnoredLiteral,
    Unsupported,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FunctionArgContract {
    AllArgs(ArgumentDependencyRole),
    VariadicReduction,
    Fixed(&'static [ArgumentDependencyRole]),
    CriteriaPairs {
        value_range: Option<usize>,
        first_pair: usize,
    },
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum FunctionDependencyRejectReason {
    UnknownFunction,
    DynamicDependency,
    VolatileFunction,
    ReferenceReturningFunction,
    LocalEnvironmentFunction,
    ArrayOrSpillFunction,
    UnsupportedFunctionClass,
    InvalidArity,
    InvalidCriteriaPairing,
    UnsupportedArgumentRole,
    FunctionContractDrift,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FunctionDependencyContract {
    pub(crate) canonical_name: String,
    pub(crate) class: FunctionDependencyClass,
    pub(crate) support_status: FunctionSupportStatus,
    pub(crate) arg_roles: FunctionArgContract,
    pub(crate) reject_reasons: Vec<FunctionDependencyRejectReason>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FunctionRegistryDiagnosticStatus {
    Present,
    Missing,
    RegistryUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FunctionArgSchemaDiagnosticStatus {
    Available,
    Missing,
    Unavailable,
    NotRequested,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FunctionArgSchemaDiagnostic {
    pub(crate) required: bool,
    pub(crate) by_ref: bool,
    pub(crate) shape: ShapeKind,
    pub(crate) max: Option<usize>,
    pub(crate) repeating: Option<usize>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FunctionRegistryDiagnosticInput {
    pub(crate) registry_status: FunctionRegistryDiagnosticStatus,
    pub(crate) namespace: Option<String>,
    pub(crate) registered_name: Option<String>,
    pub(crate) aliases: Vec<String>,
    pub(crate) caps: Option<FnCaps>,
    pub(crate) call_arity: Option<usize>,
    pub(crate) min_args: Option<usize>,
    pub(crate) max_args: Option<usize>,
    pub(crate) variadic: Option<bool>,
    pub(crate) arg_schema_status: FunctionArgSchemaDiagnosticStatus,
    pub(crate) arg_schema: Option<Vec<FunctionArgSchemaDiagnostic>>,
    pub(crate) eval_reference_available: Option<bool>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum FunctionContractDriftKind {
    MissingRegistryEntry,
    RegistryUnavailable,
    RegistryNameMismatch,
    CapabilityMismatch,
    ReferenceCapabilityMismatch,
    ArityMismatch,
    SchemaUnavailable,
    SchemaDrift,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FunctionContractDrift {
    pub(crate) canonical_name: String,
    pub(crate) kind: FunctionContractDriftKind,
    pub(crate) detail: String,
}

const NO_ARG_ROLES: [ArgumentDependencyRole; 0] = [];
const IF_ARG_ROLES: [ArgumentDependencyRole; 3] = [
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::LazyBranch,
    ArgumentDependencyRole::LazyBranch,
];
const ERROR_BRANCH_ARG_ROLES: [ArgumentDependencyRole; 2] = [
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::LazyBranch,
];
const VLOOKUP_ARG_ROLES: [ArgumentDependencyRole; 4] = [
    ArgumentDependencyRole::LookupKey,
    ArgumentDependencyRole::LookupTable,
    ArgumentDependencyRole::LookupResultSelector,
    ArgumentDependencyRole::ScalarValue,
];
const XLOOKUP_ARG_ROLES: [ArgumentDependencyRole; 3] = [
    ArgumentDependencyRole::LookupKey,
    ArgumentDependencyRole::LookupTable,
    ArgumentDependencyRole::LookupResultSelector,
];
const MATCH_ARG_ROLES: [ArgumentDependencyRole; 3] = [
    ArgumentDependencyRole::LookupKey,
    ArgumentDependencyRole::LookupTable,
    ArgumentDependencyRole::ScalarValue,
];
const INDEX_ARG_ROLES: [ArgumentDependencyRole; 3] = [
    ArgumentDependencyRole::ByReference,
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::ScalarValue,
];
const CHOOSE_ARG_ROLES: [ArgumentDependencyRole; 2] = [
    ArgumentDependencyRole::LookupResultSelector,
    ArgumentDependencyRole::ByReference,
];
const INDIRECT_ARG_ROLES: [ArgumentDependencyRole; 2] = [
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::IgnoredLiteral,
];
const OFFSET_ARG_ROLES: [ArgumentDependencyRole; 5] = [
    ArgumentDependencyRole::ByReference,
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::ScalarValue,
];
const LET_ARG_ROLES: [ArgumentDependencyRole; 3] = [
    ArgumentDependencyRole::LocalBindingName,
    ArgumentDependencyRole::LocalBindingValue,
    ArgumentDependencyRole::LambdaBody,
];
const LAMBDA_ARG_ROLES: [ArgumentDependencyRole; 2] = [
    ArgumentDependencyRole::LocalBindingName,
    ArgumentDependencyRole::LambdaBody,
];
const RANDBETWEEN_ARG_ROLES: [ArgumentDependencyRole; 2] = [
    ArgumentDependencyRole::ScalarValue,
    ArgumentDependencyRole::ScalarValue,
];

const STATIC_SCALAR_ALL_ARGS: &[&str] = &[
    "ABS",
    "ISBLANK",
    "ISERR",
    "ISERROR",
    "ISLOGICAL",
    "ISNA",
    "ISNONTEXT",
    "ISNUMBER",
    "ISTEXT",
    "N",
    "NOT",
    "T",
];

const STATIC_REDUCTIONS: &[&str] = &["AVERAGE", "COUNT", "COUNTA", "MAX", "MIN", "PRODUCT", "SUM"];

const CRITERIA_AGGREGATIONS: &[&str] = &[
    "AVERAGEIF",
    "AVERAGEIFS",
    "COUNTIF",
    "COUNTIFS",
    "SUMIF",
    "SUMIFS",
];

const MASK_CONDITIONALS: &[&str] = &["IF", "IFERROR", "IFNA", "IFS", "SWITCH"];

const LOOKUP_STATIC_RANGES: &[&str] = &["HLOOKUP", "MATCH", "VLOOKUP", "XLOOKUP"];

const REFERENCE_RETURNING_FUNCTIONS: &[&str] = &["CHOOSE", "INDEX"];

const DYNAMIC_DEPENDENCY_FUNCTIONS: &[&str] = &["INDIRECT", "OFFSET"];

const VOLATILE_FUNCTIONS: &[&str] = &["NOW", "RAND", "RANDBETWEEN", "TODAY"];

const LOCAL_ENVIRONMENT_FUNCTIONS: &[&str] = &["LAMBDA", "LET"];

const ARRAY_OR_SPILL_FUNCTIONS: &[&str] = &[
    "FILTER",
    "RANDARRAY",
    "SEQUENCE",
    "SORT",
    "SORTBY",
    "TEXTSPLIT",
    "UNIQUE",
];

const KNOWN_BUILTIN_NAMES: &[&str] = &[
    "ABS",
    "ACCRINT",
    "ACCRINTM",
    "ACOS",
    "ACOSH",
    "ACOT",
    "ACOTH",
    "ADDRESS",
    "AGGREGATE",
    "AND",
    "ARABIC",
    "ARRAYTOTEXT",
    "ASIN",
    "ASINH",
    "ATAN",
    "ATAN2",
    "ATANH",
    "AVEDEV",
    "AVERAGE",
    "AVERAGEA",
    "AVERAGEIF",
    "AVERAGEIFS",
    "BASE",
    "BETA.DIST",
    "BETA.INV",
    "BIN2DEC",
    "BIN2HEX",
    "BIN2OCT",
    "BINOM.DIST",
    "BINOM.DIST.RANGE",
    "BINOM.INV",
    "BITAND",
    "BITLSHIFT",
    "BITOR",
    "BITRSHIFT",
    "BITXOR",
    "CEILING",
    "CEILING.MATH",
    "CEILING.PRECISE",
    "CHAR",
    "CHISQ.DIST",
    "CHISQ.DIST.RT",
    "CHISQ.INV",
    "CHISQ.INV.RT",
    "CHISQ.TEST",
    "CHOOSE",
    "CHOOSECOLS",
    "CHOOSEROWS",
    "CLEAN",
    "CODE",
    "COLUMN",
    "COLUMNS",
    "COMBIN",
    "COMBINA",
    "COMPLEX",
    "CONCAT",
    "CONCATENATE",
    "CONFIDENCE.NORM",
    "CONFIDENCE.T",
    "CONVERT",
    "CORREL",
    "COS",
    "COSH",
    "COT",
    "COTH",
    "COUNT",
    "COUNTA",
    "COUNTBLANK",
    "COUNTIF",
    "COUNTIFS",
    "COUNTING",
    "COVARIANCE.P",
    "COVARIANCE.S",
    "CSC",
    "CSCH",
    "CUMIPMT",
    "CUMPRINC",
    "DATE",
    "DATEDIF",
    "DATEVALUE",
    "DAVERAGE",
    "DAY",
    "DAYS",
    "DAYS360",
    "DB",
    "DCOUNT",
    "DCOUNTA",
    "DDB",
    "DEC2BIN",
    "DEC2HEX",
    "DEC2OCT",
    "DECIMAL",
    "DEGREES",
    "DELTA",
    "DEVSQ",
    "DGET",
    "DMAX",
    "DMIN",
    "DOLLAR",
    "DOLLARDE",
    "DOLLARFR",
    "DPRODUCT",
    "DROP",
    "DSTDEV",
    "DSTDEVP",
    "DSUM",
    "DVAR",
    "DVARP",
    "EDATE",
    "EFFECT",
    "EOMONTH",
    "ERF",
    "ERF.PRECISE",
    "ERFC",
    "ERROR.TYPE",
    "ERRORFN",
    "EVEN",
    "EXACT",
    "EXP",
    "EXPON.DIST",
    "F.DIST",
    "F.DIST.RT",
    "F.INV",
    "F.INV.RT",
    "F.TEST",
    "FACT",
    "FACTDOUBLE",
    "FALSE",
    "FILTER",
    "FIND",
    "FISHER",
    "FISHERINV",
    "FIXED",
    "FLOOR",
    "FLOOR.MATH",
    "FLOOR.PRECISE",
    "FORECAST.LINEAR",
    "FREQUENCY",
    "FV",
    "GAMMA",
    "GAMMA.DIST",
    "GAMMA.INV",
    "GAMMALN",
    "GAMMALN.PRECISE",
    "GAUSS",
    "GCD",
    "GEOMEAN",
    "GESTEP",
    "GROUPBY",
    "GROWTH",
    "HARMEAN",
    "HEX2BIN",
    "HEX2DEC",
    "HEX2OCT",
    "HLOOKUP",
    "HOUR",
    "HSTACK",
    "HYPGEOM.DIST",
    "IF",
    "IFERROR",
    "IFNA",
    "IFS",
    "IMABS",
    "IMAGINARY",
    "IMARGUMENT",
    "IMCONJUGATE",
    "IMCOS",
    "IMDIV",
    "IMEXP",
    "IMLN",
    "IMLOG10",
    "IMLOG2",
    "IMPOWER",
    "IMPRODUCT",
    "IMREAL",
    "IMSIN",
    "IMSQRT",
    "IMSUB",
    "IMSUM",
    "INDEX",
    "INDIRECT",
    "INT",
    "INTERCEPT",
    "IPMT",
    "IRR",
    "ISBLANK",
    "ISERR",
    "ISERROR",
    "ISEVEN",
    "ISFORMULA",
    "ISLOGICAL",
    "ISNA",
    "ISNONTEXT",
    "ISNUMBER",
    "ISO.CEILING",
    "ISODD",
    "ISOWEEKNUM",
    "ISPMT",
    "ISTEXT",
    "KURT",
    "LAMBDA",
    "LARGE",
    "LCM",
    "LEFT",
    "LEN",
    "LET",
    "LINEST",
    "LN",
    "LOG",
    "LOG10",
    "LOGEST",
    "LOGNORM.DIST",
    "LOGNORM.INV",
    "LOOKUP",
    "LOWER",
    "MATCH",
    "MAX",
    "MAXA",
    "MAXIFS",
    "MEDIAN",
    "MID",
    "MIN",
    "MINA",
    "MINIFS",
    "MINUTE",
    "MIRR",
    "MOD",
    "MODE.MULT",
    "MODE.SNGL",
    "MONTH",
    "MROUND",
    "MULTINOMIAL",
    "N",
    "NA",
    "NEGBINOM.DIST",
    "NETWORKDAYS",
    "NETWORKDAYS.INTL",
    "NOMINAL",
    "NORM.DIST",
    "NORM.INV",
    "NORM.S.DIST",
    "NORM.S.INV",
    "NOT",
    "NOW",
    "NPER",
    "NPV",
    "OCT2BIN",
    "OCT2DEC",
    "OCT2HEX",
    "ODD",
    "OFFSET",
    "OR",
    "PDURATION",
    "PEARSON",
    "PERCENTILE.EXC",
    "PERCENTILE.INC",
    "PERCENTRANK.EXC",
    "PERCENTRANK.INC",
    "PERMUT",
    "PHI",
    "PI",
    "PIVOTBY",
    "PMT",
    "POISSON.DIST",
    "POWER",
    "PPMT",
    "PRICE",
    "PRODUCT",
    "PROPER",
    "PV",
    "QUARTILE.EXC",
    "QUARTILE.INC",
    "QUOTIENT",
    "RADIANS",
    "RAND",
    "RANDARRAY",
    "RANDBETWEEN",
    "RANK.AVG",
    "RANK.EQ",
    "RATE",
    "REPLACE",
    "REPT",
    "RIGHT",
    "ROMAN",
    "ROUND",
    "ROUNDDOWN",
    "ROUNDUP",
    "ROW",
    "ROWS",
    "RRI",
    "RSQ",
    "SEARCH",
    "SEC",
    "SECH",
    "SECOND",
    "SEQUENCE",
    "SERIESSUM",
    "SIGN",
    "SIN",
    "SINH",
    "SKEW",
    "SKEW.P",
    "SLN",
    "SLOPE",
    "SMALL",
    "SORT",
    "SORTBY",
    "SQRT",
    "SQRTPI",
    "STANDARDIZE",
    "STDEV.P",
    "STDEV.S",
    "STDEVA",
    "STDEVPA",
    "STEYX",
    "SUBSTITUTE",
    "SUBTOTAL",
    "SUM",
    "SUMIF",
    "SUMIFS",
    "SUMPRODUCT",
    "SUMSQ",
    "SUMX2MY2",
    "SUMX2PY2",
    "SUMXMY2",
    "SWITCH",
    "SYD",
    "T",
    "T.DIST",
    "T.DIST.2T",
    "T.DIST.RT",
    "T.INV",
    "T.INV.2T",
    "T.TEST",
    "TAKE",
    "TAN",
    "TANH",
    "TBILLEQ",
    "TBILLPRICE",
    "TBILLYIELD",
    "TEXT",
    "TEXTAFTER",
    "TEXTBEFORE",
    "TEXTJOIN",
    "TEXTSPLIT",
    "THROWNAME",
    "TIME",
    "TIMEVALUE",
    "TODAY",
    "TRANSPOSE",
    "TREND",
    "TRIM",
    "TRIMMEAN",
    "TRUE",
    "TRUNC",
    "TYPE",
    "UNICHAR",
    "UNICODE",
    "UNIQUE",
    "UPPER",
    "VALUE",
    "VALUETOTEXT",
    "VAR.P",
    "VAR.S",
    "VARA",
    "VARPA",
    "VLOOKUP",
    "VSTACK",
    "WEEKDAY",
    "WEEKNUM",
    "WEIBULL.DIST",
    "WORKDAY",
    "WORKDAY.INTL",
    "XIRR",
    "XLOOKUP",
    "XMATCH",
    "XNPV",
    "XOR",
    "YEAR",
    "YEARFRAC",
    "YIELD",
    "Z.TEST",
];

pub(crate) fn dependency_contract_for_function(
    canonical_name: &str,
    arity: usize,
) -> FunctionDependencyContract {
    let name = normalize_function_name(canonical_name);

    if STATIC_SCALAR_ALL_ARGS.contains(&name.as_str()) {
        return static_scalar_contract(name, arity);
    }
    if STATIC_REDUCTIONS.contains(&name.as_str()) {
        return static_reduction_contract(name, arity);
    }
    if CRITERIA_AGGREGATIONS.contains(&name.as_str()) {
        return criteria_aggregation_contract(name, arity);
    }
    if MASK_CONDITIONALS.contains(&name.as_str()) {
        return contract(
            name.clone(),
            FunctionDependencyClass::MaskConditional,
            FunctionSupportStatus::ClassifiedOnly,
            mask_conditional_arg_contract(&name),
            vec![FunctionDependencyRejectReason::UnsupportedFunctionClass],
        );
    }
    if LOOKUP_STATIC_RANGES.contains(&name.as_str()) {
        return contract(
            name.clone(),
            FunctionDependencyClass::LookupStaticRange,
            FunctionSupportStatus::ClassifiedOnly,
            lookup_arg_contract(&name),
            vec![FunctionDependencyRejectReason::UnsupportedFunctionClass],
        );
    }
    if REFERENCE_RETURNING_FUNCTIONS.contains(&name.as_str()) {
        return contract(
            name.clone(),
            FunctionDependencyClass::ReferenceReturning,
            FunctionSupportStatus::Rejected,
            reference_returning_arg_contract(&name),
            vec![FunctionDependencyRejectReason::ReferenceReturningFunction],
        );
    }
    if DYNAMIC_DEPENDENCY_FUNCTIONS.contains(&name.as_str()) {
        return contract(
            name.clone(),
            FunctionDependencyClass::DynamicDependency,
            FunctionSupportStatus::Rejected,
            dynamic_dependency_arg_contract(&name),
            vec![FunctionDependencyRejectReason::DynamicDependency],
        );
    }
    if VOLATILE_FUNCTIONS.contains(&name.as_str()) {
        return contract(
            name.clone(),
            FunctionDependencyClass::Volatile,
            FunctionSupportStatus::Rejected,
            volatile_arg_contract(&name),
            vec![FunctionDependencyRejectReason::VolatileFunction],
        );
    }
    if LOCAL_ENVIRONMENT_FUNCTIONS.contains(&name.as_str()) {
        return contract(
            name.clone(),
            FunctionDependencyClass::LocalEnvironment,
            FunctionSupportStatus::Rejected,
            local_environment_arg_contract(&name),
            vec![FunctionDependencyRejectReason::LocalEnvironmentFunction],
        );
    }
    if ARRAY_OR_SPILL_FUNCTIONS.contains(&name.as_str()) {
        return contract(
            name,
            FunctionDependencyClass::ArrayOrSpill,
            FunctionSupportStatus::Rejected,
            FunctionArgContract::Unsupported,
            vec![FunctionDependencyRejectReason::ArrayOrSpillFunction],
        );
    }
    if KNOWN_BUILTIN_NAMES.contains(&name.as_str()) {
        return contract(
            name,
            FunctionDependencyClass::Unsupported,
            FunctionSupportStatus::Rejected,
            FunctionArgContract::Unsupported,
            vec![FunctionDependencyRejectReason::UnsupportedFunctionClass],
        );
    }

    contract(
        name,
        FunctionDependencyClass::OpaqueScalar,
        FunctionSupportStatus::Rejected,
        FunctionArgContract::AllArgs(ArgumentDependencyRole::Unsupported),
        vec![FunctionDependencyRejectReason::UnknownFunction],
    )
}

pub(crate) fn normalize_function_name(name: &str) -> String {
    let mut normalized = name.trim().to_ascii_uppercase();
    loop {
        let stripped = ["_XLFN.", "_XLL.", "_XLWS."]
            .iter()
            .find_map(|prefix| normalized.strip_prefix(prefix).map(str::to_string));
        if let Some(stripped) = stripped {
            normalized = stripped;
        } else {
            return normalized;
        }
    }
}

pub(crate) fn dependency_contract_drift(
    contract: &FunctionDependencyContract,
    registry: FunctionRegistryDiagnosticInput,
) -> Vec<FunctionContractDrift> {
    let mut drifts = Vec::new();

    match registry.registry_status {
        FunctionRegistryDiagnosticStatus::Present => {}
        FunctionRegistryDiagnosticStatus::Missing => {
            drifts.push(drift(
                contract,
                FunctionContractDriftKind::MissingRegistryEntry,
                "registry has no entry for the classified function",
            ));
            return drifts;
        }
        FunctionRegistryDiagnosticStatus::RegistryUnavailable => {
            drifts.push(drift(
                contract,
                FunctionContractDriftKind::RegistryUnavailable,
                "registry facts are unavailable or builtins were not loaded",
            ));
            return drifts;
        }
    }

    if let Some(registered_name) = &registry.registered_name {
        let registered = normalize_function_name(registered_name);
        let alias_matches = registry
            .aliases
            .iter()
            .any(|alias| normalize_function_name(alias) == contract.canonical_name);
        if registered != contract.canonical_name && !alias_matches {
            drifts.push(drift(
                contract,
                FunctionContractDriftKind::RegistryNameMismatch,
                format!(
                    "registry name {registered} and aliases do not match contract {}",
                    contract.canonical_name
                ),
            ));
        }
    }

    if let Some(caps) = registry.caps {
        check_capability(
            contract,
            caps,
            FnCaps::VOLATILE,
            matches!(contract.class, FunctionDependencyClass::Volatile),
            "VOLATILE",
            &mut drifts,
        );
        check_capability(
            contract,
            caps,
            FnCaps::DYNAMIC_DEPENDENCY,
            matches!(contract.class, FunctionDependencyClass::DynamicDependency),
            "DYNAMIC_DEPENDENCY",
            &mut drifts,
        );
        check_capability(
            contract,
            caps,
            FnCaps::RETURNS_REFERENCE,
            matches!(
                contract.class,
                FunctionDependencyClass::ReferenceReturning
                    | FunctionDependencyClass::DynamicDependency
            ),
            "RETURNS_REFERENCE",
            &mut drifts,
        );
        check_capability(
            contract,
            caps,
            FnCaps::REDUCTION,
            matches!(
                contract.class,
                FunctionDependencyClass::StaticReduction
                    | FunctionDependencyClass::CriteriaAggregation
            ),
            "REDUCTION",
            &mut drifts,
        );
        check_capability(
            contract,
            caps,
            FnCaps::LOOKUP,
            matches!(contract.class, FunctionDependencyClass::LookupStaticRange),
            "LOOKUP",
            &mut drifts,
        );
        check_capability(
            contract,
            caps,
            FnCaps::SHORT_CIRCUIT,
            matches!(
                contract.class,
                FunctionDependencyClass::MaskConditional
                    | FunctionDependencyClass::LocalEnvironment
            ),
            "SHORT_CIRCUIT",
            &mut drifts,
        );

        if let Some(eval_reference_available) = registry.eval_reference_available {
            let caps_reference = caps.contains(FnCaps::RETURNS_REFERENCE);
            if caps_reference != eval_reference_available {
                drifts.push(drift(
                    contract,
                    FunctionContractDriftKind::ReferenceCapabilityMismatch,
                    format!(
                        "RETURNS_REFERENCE cap is {caps_reference} but eval_reference availability is {eval_reference_available}"
                    ),
                ));
            }
        }
    }

    if let Some(eval_reference_available) = registry.eval_reference_available {
        let expected_reference = matches!(
            contract.class,
            FunctionDependencyClass::ReferenceReturning
                | FunctionDependencyClass::DynamicDependency
        );
        if eval_reference_available && !expected_reference {
            drifts.push(drift(
                contract,
                FunctionContractDriftKind::ReferenceCapabilityMismatch,
                "eval_reference is available for a non-reference contract class",
            ));
        } else if !eval_reference_available && expected_reference {
            drifts.push(drift(
                contract,
                FunctionContractDriftKind::ReferenceCapabilityMismatch,
                "eval_reference is unavailable for a reference-capable contract class",
            ));
        }
    }

    if let Some(call_arity) = registry.call_arity {
        if let Some(min_args) = registry.min_args {
            if call_arity < min_args {
                drifts.push(drift(
                    contract,
                    FunctionContractDriftKind::ArityMismatch,
                    format!("call arity {call_arity} is below registry min_args {min_args}"),
                ));
            }
        }
        if let Some(max_args) = registry.max_args {
            if registry.variadic != Some(true) && call_arity > max_args {
                drifts.push(drift(
                    contract,
                    FunctionContractDriftKind::ArityMismatch,
                    format!("call arity {call_arity} is above registry max_args {max_args}"),
                ));
            }
        }
    }

    match registry.arg_schema_status {
        FunctionArgSchemaDiagnosticStatus::Available => {
            if let Some(schema) = &registry.arg_schema {
                check_schema(contract, schema, &mut drifts);
            }
        }
        FunctionArgSchemaDiagnosticStatus::Missing => drifts.push(drift(
            contract,
            FunctionContractDriftKind::SchemaUnavailable,
            "registry has no argument schema facts",
        )),
        FunctionArgSchemaDiagnosticStatus::Unavailable => drifts.push(drift(
            contract,
            FunctionContractDriftKind::SchemaUnavailable,
            "argument schema facts are unavailable without calling Function::arg_schema()",
        )),
        FunctionArgSchemaDiagnosticStatus::NotRequested => {}
    }

    drifts
}

impl FunctionRegistryDiagnosticInput {
    pub(crate) fn missing_registry() -> Self {
        Self {
            registry_status: FunctionRegistryDiagnosticStatus::Missing,
            namespace: None,
            registered_name: None,
            aliases: Vec::new(),
            caps: None,
            call_arity: None,
            min_args: None,
            max_args: None,
            variadic: None,
            arg_schema_status: FunctionArgSchemaDiagnosticStatus::Missing,
            arg_schema: None,
            eval_reference_available: None,
        }
    }

    pub(crate) fn registry_unavailable() -> Self {
        Self {
            registry_status: FunctionRegistryDiagnosticStatus::RegistryUnavailable,
            namespace: None,
            registered_name: None,
            aliases: Vec::new(),
            caps: None,
            call_arity: None,
            min_args: None,
            max_args: None,
            variadic: None,
            arg_schema_status: FunctionArgSchemaDiagnosticStatus::NotRequested,
            arg_schema: None,
            eval_reference_available: None,
        }
    }

    pub(crate) fn present(registered_name: impl Into<String>) -> Self {
        Self {
            registry_status: FunctionRegistryDiagnosticStatus::Present,
            namespace: None,
            registered_name: Some(registered_name.into()),
            aliases: Vec::new(),
            caps: None,
            call_arity: None,
            min_args: None,
            max_args: None,
            variadic: None,
            arg_schema_status: FunctionArgSchemaDiagnosticStatus::NotRequested,
            arg_schema: None,
            eval_reference_available: None,
        }
    }
}

fn static_scalar_contract(name: String, arity: usize) -> FunctionDependencyContract {
    if arity >= 1 {
        contract(
            name,
            FunctionDependencyClass::StaticScalarAllArgs,
            FunctionSupportStatus::Supported,
            FunctionArgContract::AllArgs(ArgumentDependencyRole::ScalarValue),
            Vec::new(),
        )
    } else {
        contract(
            name,
            FunctionDependencyClass::StaticScalarAllArgs,
            FunctionSupportStatus::Rejected,
            FunctionArgContract::AllArgs(ArgumentDependencyRole::ScalarValue),
            vec![FunctionDependencyRejectReason::InvalidArity],
        )
    }
}

fn static_reduction_contract(name: String, arity: usize) -> FunctionDependencyContract {
    if arity >= 1 {
        contract(
            name,
            FunctionDependencyClass::StaticReduction,
            FunctionSupportStatus::Supported,
            FunctionArgContract::VariadicReduction,
            Vec::new(),
        )
    } else {
        contract(
            name,
            FunctionDependencyClass::StaticReduction,
            FunctionSupportStatus::Rejected,
            FunctionArgContract::VariadicReduction,
            vec![FunctionDependencyRejectReason::InvalidArity],
        )
    }
}

fn criteria_aggregation_contract(name: String, arity: usize) -> FunctionDependencyContract {
    let (arg_roles, reject_reason) = match name.as_str() {
        "COUNTIF" => (
            FunctionArgContract::CriteriaPairs {
                value_range: None,
                first_pair: 0,
            },
            if arity == 2 {
                None
            } else {
                Some(FunctionDependencyRejectReason::InvalidArity)
            },
        ),
        "COUNTIFS" => (
            FunctionArgContract::CriteriaPairs {
                value_range: None,
                first_pair: 0,
            },
            if arity < 2 {
                Some(FunctionDependencyRejectReason::InvalidArity)
            } else if arity % 2 != 0 {
                Some(FunctionDependencyRejectReason::InvalidCriteriaPairing)
            } else {
                None
            },
        ),
        "SUMIF" | "AVERAGEIF" => (
            FunctionArgContract::CriteriaPairs {
                value_range: if arity >= 3 { Some(2) } else { None },
                first_pair: 0,
            },
            if matches!(arity, 2 | 3) {
                None
            } else {
                Some(FunctionDependencyRejectReason::InvalidArity)
            },
        ),
        "SUMIFS" | "AVERAGEIFS" => (
            FunctionArgContract::CriteriaPairs {
                value_range: Some(0),
                first_pair: 1,
            },
            if arity < 3 {
                Some(FunctionDependencyRejectReason::InvalidArity)
            } else if arity % 2 == 0 {
                Some(FunctionDependencyRejectReason::InvalidCriteriaPairing)
            } else {
                None
            },
        ),
        _ => unreachable!("criteria aggregation names are pre-filtered"),
    };

    match reject_reason {
        Some(reason) => contract(
            name,
            FunctionDependencyClass::CriteriaAggregation,
            FunctionSupportStatus::Rejected,
            arg_roles,
            vec![reason],
        ),
        None => contract(
            name,
            FunctionDependencyClass::CriteriaAggregation,
            FunctionSupportStatus::Supported,
            arg_roles,
            Vec::new(),
        ),
    }
}

fn mask_conditional_arg_contract(name: &str) -> FunctionArgContract {
    match name {
        "IF" => FunctionArgContract::Fixed(&IF_ARG_ROLES),
        "IFERROR" | "IFNA" => FunctionArgContract::Fixed(&ERROR_BRANCH_ARG_ROLES),
        _ => FunctionArgContract::Unsupported,
    }
}

fn lookup_arg_contract(name: &str) -> FunctionArgContract {
    match name {
        "VLOOKUP" | "HLOOKUP" => FunctionArgContract::Fixed(&VLOOKUP_ARG_ROLES),
        "XLOOKUP" => FunctionArgContract::Fixed(&XLOOKUP_ARG_ROLES),
        "MATCH" => FunctionArgContract::Fixed(&MATCH_ARG_ROLES),
        _ => FunctionArgContract::Unsupported,
    }
}

fn reference_returning_arg_contract(name: &str) -> FunctionArgContract {
    match name {
        "INDEX" => FunctionArgContract::Fixed(&INDEX_ARG_ROLES),
        "CHOOSE" => FunctionArgContract::Fixed(&CHOOSE_ARG_ROLES),
        _ => FunctionArgContract::Unsupported,
    }
}

fn dynamic_dependency_arg_contract(name: &str) -> FunctionArgContract {
    match name {
        "INDIRECT" => FunctionArgContract::Fixed(&INDIRECT_ARG_ROLES),
        "OFFSET" => FunctionArgContract::Fixed(&OFFSET_ARG_ROLES),
        _ => FunctionArgContract::Unsupported,
    }
}

fn volatile_arg_contract(name: &str) -> FunctionArgContract {
    match name {
        "NOW" | "RAND" | "TODAY" => FunctionArgContract::Fixed(&NO_ARG_ROLES),
        "RANDBETWEEN" => FunctionArgContract::Fixed(&RANDBETWEEN_ARG_ROLES),
        _ => FunctionArgContract::Unsupported,
    }
}

fn local_environment_arg_contract(name: &str) -> FunctionArgContract {
    match name {
        "LET" => FunctionArgContract::Fixed(&LET_ARG_ROLES),
        "LAMBDA" => FunctionArgContract::Fixed(&LAMBDA_ARG_ROLES),
        _ => FunctionArgContract::Unsupported,
    }
}

fn contract(
    canonical_name: String,
    class: FunctionDependencyClass,
    support_status: FunctionSupportStatus,
    arg_roles: FunctionArgContract,
    reject_reasons: Vec<FunctionDependencyRejectReason>,
) -> FunctionDependencyContract {
    FunctionDependencyContract {
        canonical_name,
        class,
        support_status,
        arg_roles,
        reject_reasons,
    }
}

fn check_capability(
    contract: &FunctionDependencyContract,
    caps: FnCaps,
    cap: FnCaps,
    expected: bool,
    label: &'static str,
    drifts: &mut Vec<FunctionContractDrift>,
) {
    let actual = caps.contains(cap);
    if actual != expected {
        drifts.push(drift(
            contract,
            FunctionContractDriftKind::CapabilityMismatch,
            format!("FnCaps::{label} is {actual} but contract expectation is {expected}"),
        ));
    }
}

fn check_schema(
    contract: &FunctionDependencyContract,
    schema: &[FunctionArgSchemaDiagnostic],
    drifts: &mut Vec<FunctionContractDrift>,
) {
    if schema.iter().any(|arg| arg.by_ref)
        && !contract_has_role(contract, ArgumentDependencyRole::ByReference)
    {
        drifts.push(drift(
            contract,
            FunctionContractDriftKind::SchemaDrift,
            "argument schema has by-ref arguments but the contract has no by-reference role",
        ));
    }

    if schema.iter().any(|arg| arg.shape == ShapeKind::Array)
        && !matches!(contract.class, FunctionDependencyClass::ArrayOrSpill)
    {
        drifts.push(drift(
            contract,
            FunctionContractDriftKind::SchemaDrift,
            "argument schema accepts arrays but the contract is not ArrayOrSpill",
        ));
    }
}

fn contract_has_role(contract: &FunctionDependencyContract, role: ArgumentDependencyRole) -> bool {
    match &contract.arg_roles {
        FunctionArgContract::AllArgs(arg_role) => *arg_role == role,
        FunctionArgContract::VariadicReduction => role == ArgumentDependencyRole::ReductionValue,
        FunctionArgContract::Fixed(roles) => roles.contains(&role),
        FunctionArgContract::CriteriaPairs { .. } => matches!(
            role,
            ArgumentDependencyRole::CriteriaRange
                | ArgumentDependencyRole::CriteriaExpression
                | ArgumentDependencyRole::FiniteRangeValue
        ),
        FunctionArgContract::Unsupported => role == ArgumentDependencyRole::Unsupported,
    }
}

fn drift(
    contract: &FunctionDependencyContract,
    kind: FunctionContractDriftKind,
    detail: impl Into<String>,
) -> FunctionContractDrift {
    FunctionContractDrift {
        canonical_name: contract.canonical_name.clone(),
        kind,
        detail: detail.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_contract(
        name: &str,
        arity: usize,
        class: FunctionDependencyClass,
        status: FunctionSupportStatus,
        reasons: &[FunctionDependencyRejectReason],
    ) -> FunctionDependencyContract {
        let contract = dependency_contract_for_function(name, arity);
        assert_eq!(contract.canonical_name, normalize_function_name(name));
        assert_eq!(contract.class, class, "class for {name}/{arity}");
        assert_eq!(
            contract.support_status, status,
            "support status for {name}/{arity}"
        );
        assert_eq!(
            contract.reject_reasons, reasons,
            "reject reasons for {name}/{arity}"
        );
        contract
    }

    #[test]
    fn formula_plane_function_dependency_classifies_supported_scalar_contracts() {
        for name in STATIC_SCALAR_ALL_ARGS {
            let contract = assert_contract(
                name,
                1,
                FunctionDependencyClass::StaticScalarAllArgs,
                FunctionSupportStatus::Supported,
                &[],
            );
            assert_eq!(
                contract.arg_roles,
                FunctionArgContract::AllArgs(ArgumentDependencyRole::ScalarValue)
            );
        }

        assert_contract(
            "ABS",
            0,
            FunctionDependencyClass::StaticScalarAllArgs,
            FunctionSupportStatus::Rejected,
            &[FunctionDependencyRejectReason::InvalidArity],
        );
    }

    #[test]
    fn formula_plane_function_dependency_classifies_supported_reduction_contracts() {
        for name in STATIC_REDUCTIONS {
            let contract = assert_contract(
                name,
                1,
                FunctionDependencyClass::StaticReduction,
                FunctionSupportStatus::Supported,
                &[],
            );
            assert_eq!(contract.arg_roles, FunctionArgContract::VariadicReduction);
        }

        assert_contract(
            "SUM",
            0,
            FunctionDependencyClass::StaticReduction,
            FunctionSupportStatus::Rejected,
            &[FunctionDependencyRejectReason::InvalidArity],
        );
    }

    #[test]
    fn formula_plane_function_dependency_classifies_criteria_aggregations() {
        for (name, arity, value_range, first_pair) in [
            ("COUNTIF", 2, None, 0),
            ("COUNTIFS", 2, None, 0),
            ("COUNTIFS", 4, None, 0),
            ("SUMIF", 2, None, 0),
            ("SUMIF", 3, Some(2), 0),
            ("SUMIFS", 3, Some(0), 1),
            ("SUMIFS", 5, Some(0), 1),
            ("AVERAGEIF", 2, None, 0),
            ("AVERAGEIF", 3, Some(2), 0),
            ("AVERAGEIFS", 3, Some(0), 1),
            ("AVERAGEIFS", 5, Some(0), 1),
        ] {
            let contract = assert_contract(
                name,
                arity,
                FunctionDependencyClass::CriteriaAggregation,
                FunctionSupportStatus::Supported,
                &[],
            );
            assert_eq!(
                contract.arg_roles,
                FunctionArgContract::CriteriaPairs {
                    value_range,
                    first_pair
                },
                "arg contract for {name}/{arity}"
            );
        }
    }

    #[test]
    fn formula_plane_function_dependency_classifies_deferred_and_rejected_contracts() {
        for name in MASK_CONDITIONALS {
            assert_contract(
                name,
                3,
                FunctionDependencyClass::MaskConditional,
                FunctionSupportStatus::ClassifiedOnly,
                &[FunctionDependencyRejectReason::UnsupportedFunctionClass],
            );
        }
        for name in LOOKUP_STATIC_RANGES {
            assert_contract(
                name,
                3,
                FunctionDependencyClass::LookupStaticRange,
                FunctionSupportStatus::ClassifiedOnly,
                &[FunctionDependencyRejectReason::UnsupportedFunctionClass],
            );
        }
        for name in REFERENCE_RETURNING_FUNCTIONS {
            assert_contract(
                name,
                2,
                FunctionDependencyClass::ReferenceReturning,
                FunctionSupportStatus::Rejected,
                &[FunctionDependencyRejectReason::ReferenceReturningFunction],
            );
        }
        for name in DYNAMIC_DEPENDENCY_FUNCTIONS {
            assert_contract(
                name,
                1,
                FunctionDependencyClass::DynamicDependency,
                FunctionSupportStatus::Rejected,
                &[FunctionDependencyRejectReason::DynamicDependency],
            );
        }
        for name in VOLATILE_FUNCTIONS {
            assert_contract(
                name,
                0,
                FunctionDependencyClass::Volatile,
                FunctionSupportStatus::Rejected,
                &[FunctionDependencyRejectReason::VolatileFunction],
            );
        }
        for name in LOCAL_ENVIRONMENT_FUNCTIONS {
            assert_contract(
                name,
                2,
                FunctionDependencyClass::LocalEnvironment,
                FunctionSupportStatus::Rejected,
                &[FunctionDependencyRejectReason::LocalEnvironmentFunction],
            );
        }
        for name in ARRAY_OR_SPILL_FUNCTIONS {
            assert_contract(
                name,
                1,
                FunctionDependencyClass::ArrayOrSpill,
                FunctionSupportStatus::Rejected,
                &[FunctionDependencyRejectReason::ArrayOrSpillFunction],
            );
        }
    }

    #[test]
    fn formula_plane_function_dependency_normalizes_excel_prefixes() {
        let prefixed = assert_contract(
            "_xlfn.SUM",
            1,
            FunctionDependencyClass::StaticReduction,
            FunctionSupportStatus::Supported,
            &[],
        );
        assert_eq!(prefixed.canonical_name, "SUM");

        let chained = assert_contract(
            "_xlfn._xlws.FILTER",
            1,
            FunctionDependencyClass::ArrayOrSpill,
            FunctionSupportStatus::Rejected,
            &[FunctionDependencyRejectReason::ArrayOrSpillFunction],
        );
        assert_eq!(chained.canonical_name, "FILTER");
    }

    #[test]
    fn formula_plane_function_dependency_rejects_invalid_reduction_and_criteria_arities() {
        for (name, arity, reason) in [
            ("SUM", 0, FunctionDependencyRejectReason::InvalidArity),
            ("COUNTIF", 1, FunctionDependencyRejectReason::InvalidArity),
            ("COUNTIF", 3, FunctionDependencyRejectReason::InvalidArity),
            ("COUNTIFS", 1, FunctionDependencyRejectReason::InvalidArity),
            (
                "COUNTIFS",
                3,
                FunctionDependencyRejectReason::InvalidCriteriaPairing,
            ),
            ("SUMIF", 1, FunctionDependencyRejectReason::InvalidArity),
            ("SUMIF", 4, FunctionDependencyRejectReason::InvalidArity),
            ("SUMIFS", 2, FunctionDependencyRejectReason::InvalidArity),
            (
                "SUMIFS",
                4,
                FunctionDependencyRejectReason::InvalidCriteriaPairing,
            ),
            ("AVERAGEIF", 1, FunctionDependencyRejectReason::InvalidArity),
            ("AVERAGEIF", 4, FunctionDependencyRejectReason::InvalidArity),
            (
                "AVERAGEIFS",
                2,
                FunctionDependencyRejectReason::InvalidArity,
            ),
            (
                "AVERAGEIFS",
                4,
                FunctionDependencyRejectReason::InvalidCriteriaPairing,
            ),
        ] {
            let expected_class = if name == "SUM" {
                FunctionDependencyClass::StaticReduction
            } else {
                FunctionDependencyClass::CriteriaAggregation
            };
            assert_contract(
                name,
                arity,
                expected_class,
                FunctionSupportStatus::Rejected,
                &[reason],
            );
        }
    }

    #[test]
    fn formula_plane_function_dependency_distinguishes_known_unsupported_from_unknown() {
        assert_contract(
            "SIN",
            1,
            FunctionDependencyClass::Unsupported,
            FunctionSupportStatus::Rejected,
            &[FunctionDependencyRejectReason::UnsupportedFunctionClass],
        );
        assert_contract(
            "CUSTOMFN",
            1,
            FunctionDependencyClass::OpaqueScalar,
            FunctionSupportStatus::Rejected,
            &[FunctionDependencyRejectReason::UnknownFunction],
        );
    }

    #[test]
    fn formula_plane_function_dependency_drift_handles_missing_registry_safely() {
        let contract = dependency_contract_for_function("SUM", 1);
        let before = contract.clone();
        let drifts = dependency_contract_drift(
            &contract,
            FunctionRegistryDiagnosticInput::missing_registry(),
        );

        assert_eq!(contract, before);
        assert_eq!(
            drifts.iter().map(|drift| drift.kind).collect::<Vec<_>>(),
            vec![FunctionContractDriftKind::MissingRegistryEntry]
        );
    }

    #[test]
    fn formula_plane_function_dependency_drift_handles_schema_unavailable_safely() {
        let contract = dependency_contract_for_function("SUM", 1);
        let mut input = FunctionRegistryDiagnosticInput::present("SUM");
        input.caps = Some(FnCaps::PURE | FnCaps::REDUCTION);
        input.call_arity = Some(1);
        input.min_args = Some(1);
        input.variadic = Some(true);
        input.arg_schema_status = FunctionArgSchemaDiagnosticStatus::Unavailable;

        let drifts = dependency_contract_drift(&contract, input);

        assert!(drifts.iter().any(|drift| {
            drift.kind == FunctionContractDriftKind::SchemaUnavailable
                && drift.canonical_name == "SUM"
        }));
        assert_eq!(contract.support_status, FunctionSupportStatus::Supported);
        assert!(contract.reject_reasons.is_empty());
    }

    #[test]
    fn formula_plane_function_dependency_drift_is_report_only() {
        let contract = dependency_contract_for_function("ABS", 1);
        let before = contract.clone();
        let mut input = FunctionRegistryDiagnosticInput::present("ABS");
        input.caps = Some(FnCaps::VOLATILE | FnCaps::RETURNS_REFERENCE);
        input.eval_reference_available = Some(false);
        input.arg_schema_status = FunctionArgSchemaDiagnosticStatus::Available;
        input.arg_schema = Some(vec![FunctionArgSchemaDiagnostic {
            required: true,
            by_ref: true,
            shape: ShapeKind::Scalar,
            max: None,
            repeating: None,
        }]);

        let drifts = dependency_contract_drift(&contract, input);

        assert!(!drifts.is_empty());
        assert_eq!(contract, before);
        assert_eq!(contract.support_status, FunctionSupportStatus::Supported);
        assert!(
            !contract
                .reject_reasons
                .contains(&FunctionDependencyRejectReason::FunctionContractDrift)
        );
    }

    #[test]
    fn formula_plane_function_dependency_drift_handles_registry_unavailable() {
        let contract = dependency_contract_for_function("SUM", 1);
        let drifts = dependency_contract_drift(
            &contract,
            FunctionRegistryDiagnosticInput::registry_unavailable(),
        );

        assert_eq!(
            drifts.iter().map(|drift| drift.kind).collect::<Vec<_>>(),
            vec![FunctionContractDriftKind::RegistryUnavailable]
        );
        assert_eq!(contract.support_status, FunctionSupportStatus::Supported);
    }
}
