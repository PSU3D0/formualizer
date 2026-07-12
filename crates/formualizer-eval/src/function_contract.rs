//! Function-owned dependency and semantic contracts.
//!
//! Dependency precision remains optional. Semantic contracts classify call-site
//! behavior without making function names an eligibility authority.

use crate::function::FnCaps;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionDependencySemantics {
    RecursiveSyntacticArgs,
    Dynamic,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionEvaluationSemantics {
    Eager,
    ShortCircuit,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionResultSemantics {
    ScalarValue,
    MayReturnReference,
    MaySpill,
    MayReturnReferenceAndSpill,
    Unknown,
}

impl FunctionResultSemantics {
    pub fn from_capabilities(may_return_reference: bool, may_spill: bool) -> Self {
        match (may_return_reference, may_spill) {
            (false, false) => Self::ScalarValue,
            (true, false) => Self::MayReturnReference,
            (false, true) => Self::MaySpill,
            (true, true) => Self::MayReturnReferenceAndSpill,
        }
    }

    pub fn may_return_reference(self) -> bool {
        matches!(
            self,
            Self::MayReturnReference | Self::MayReturnReferenceAndSpill
        )
    }

    pub fn may_spill(self) -> bool {
        matches!(self, Self::MaySpill | Self::MayReturnReferenceAndSpill)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionEnvironmentSemantics {
    None,
    LocalBindings,
    Unknown,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionContextDependence {
    None,
    PlacementDependent,
    WorkbookMetadata,
    LocaleOrConfiguration,
    Unsupported,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FunctionSemanticContract {
    pub dependency: FunctionDependencySemantics,
    pub evaluation: FunctionEvaluationSemantics,
    pub result: FunctionResultSemantics,
    pub environment: FunctionEnvironmentSemantics,
    pub context: FunctionContextDependence,
    pub precision: Option<FunctionDependencyContract>,
}

impl FunctionSemanticContract {
    pub fn trusted_builtin_default(precision: Option<FunctionDependencyContract>) -> Self {
        Self {
            dependency: FunctionDependencySemantics::RecursiveSyntacticArgs,
            evaluation: FunctionEvaluationSemantics::Eager,
            result: FunctionResultSemantics::ScalarValue,
            environment: FunctionEnvironmentSemantics::None,
            context: FunctionContextDependence::None,
            precision,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) struct FunctionSemanticIdentity {
    pub(crate) namespace: String,
    pub(crate) canonical_name: String,
    pub(crate) generation: u64,
    pub(crate) caps: FnCaps,
    pub(crate) contract: FunctionSemanticContract,
    pub(crate) argument_by_ref: Vec<bool>,
}

impl FunctionSemanticIdentity {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        push_bytes(&mut out, self.namespace.as_bytes());
        push_bytes(&mut out, self.canonical_name.as_bytes());
        out.extend_from_slice(&self.generation.to_le_bytes());
        out.extend_from_slice(&self.caps.bits().to_le_bytes());
        out.push(self.contract.dependency as u8);
        out.push(self.contract.evaluation as u8);
        out.push(self.contract.result as u8);
        out.push(self.contract.environment as u8);
        out.push(self.contract.context as u8);
        match self.contract.precision {
            Some(precision) => {
                out.push(1);
                encode_precision(&mut out, precision);
            }
            None => out.push(0),
        }
        out.extend_from_slice(&(self.argument_by_ref.len() as u64).to_le_bytes());
        out.extend(self.argument_by_ref.iter().map(|value| u8::from(*value)));
        out
    }
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    out.extend_from_slice(bytes);
}

fn encode_precision(out: &mut Vec<u8>, precision: FunctionDependencyContract) {
    out.push(precision.class as u8);
    match precision.arity {
        FunctionArityRule::Exactly(value) => encode_arity(out, 0, value),
        FunctionArityRule::AtLeast(value) => encode_arity(out, 1, value),
        FunctionArityRule::OneOf(values) => {
            out.push(2);
            out.extend_from_slice(&(values.len() as u64).to_le_bytes());
            for value in values {
                out.extend_from_slice(&(*value as u64).to_le_bytes());
            }
        }
        FunctionArityRule::EvenAtLeast(value) => encode_arity(out, 3, value),
        FunctionArityRule::OddAtLeast(value) => encode_arity(out, 4, value),
    }
    match precision.arguments {
        FunctionArgumentDependencyContract::AllArgs(role) => encode_role(out, 0, role),
        FunctionArgumentDependencyContract::Variadic(role) => encode_role(out, 1, role),
        FunctionArgumentDependencyContract::CriteriaPairs(criteria) => {
            out.push(2);
            match criteria.value_range {
                CriteriaValueRange::None => out.push(0),
                CriteriaValueRange::Fixed(index) => encode_arity(out, 1, index),
                CriteriaValueRange::Optional {
                    provided_index,
                    fallback_criteria_range_index,
                } => {
                    encode_arity(out, 2, provided_index);
                    out.extend_from_slice(&(fallback_criteria_range_index as u64).to_le_bytes());
                }
            }
            out.extend_from_slice(&(criteria.first_criteria_pair as u64).to_le_bytes());
        }
        FunctionArgumentDependencyContract::LocalBindingPairs => out.push(3),
        FunctionArgumentDependencyContract::LambdaParameters => out.push(4),
    }
}

fn encode_arity(out: &mut Vec<u8>, tag: u8, value: usize) {
    out.push(tag);
    out.extend_from_slice(&(value as u64).to_le_bytes());
}

fn encode_role(out: &mut Vec<u8>, tag: u8, role: FunctionArgumentDependencyRole) {
    out.push(tag);
    out.push(role as u8);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionDependencyClass {
    /// Dependencies are the union of all scalar/value arguments.
    StaticScalarAllArgs,
    /// Dependencies are the union of finite scalar/range reduction inputs.
    StaticReduction,
    /// Dependencies are finite criteria ranges, optional value ranges, and
    /// dependencies of criteria expressions.
    CriteriaAggregation,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionArgumentDependencyRole {
    ScalarValue,
    FiniteRangeValue,
    ReductionValue,
    CriteriaRange,
    CriteriaExpression,
    ValueRange,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionArityRule {
    Exactly(usize),
    AtLeast(usize),
    OneOf(&'static [usize]),
    EvenAtLeast(usize),
    OddAtLeast(usize),
}

impl FunctionArityRule {
    pub fn allows(self, arity: usize) -> bool {
        match self {
            Self::Exactly(expected) => arity == expected,
            Self::AtLeast(min) => arity >= min,
            Self::OneOf(allowed) => allowed.contains(&arity),
            Self::EvenAtLeast(min) => arity >= min && arity.is_multiple_of(2),
            Self::OddAtLeast(min) => arity >= min && !arity.is_multiple_of(2),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CriteriaValueRange {
    /// No separate value range; the function only contributes criteria ranges
    /// and criteria-expression dependencies.
    None,
    /// A fixed argument index is the value/sum/average range.
    Fixed(usize),
    /// The value range is optional. If omitted, the criteria range at
    /// `fallback_criteria_range_index` is also the value range.
    Optional {
        provided_index: usize,
        fallback_criteria_range_index: usize,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CriteriaAggregationDependencyContract {
    pub value_range: CriteriaValueRange,
    pub first_criteria_pair: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionArgumentDependencyContract {
    AllArgs(FunctionArgumentDependencyRole),
    Variadic(FunctionArgumentDependencyRole),
    CriteriaPairs(CriteriaAggregationDependencyContract),
    LocalBindingPairs,
    LambdaParameters,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct FunctionDependencyContract {
    pub class: FunctionDependencyClass,
    pub arity: FunctionArityRule,
    pub arguments: FunctionArgumentDependencyContract,
}

impl FunctionDependencyContract {
    pub fn static_scalar_all_args(arity: usize) -> Option<Self> {
        Self {
            class: FunctionDependencyClass::StaticScalarAllArgs,
            arity: FunctionArityRule::Exactly(1),
            arguments: FunctionArgumentDependencyContract::AllArgs(
                FunctionArgumentDependencyRole::ScalarValue,
            ),
        }
        .for_arity(arity)
    }

    pub fn static_reduction(arity: usize, min_args: usize) -> Option<Self> {
        Self {
            class: FunctionDependencyClass::StaticReduction,
            arity: FunctionArityRule::AtLeast(min_args),
            arguments: FunctionArgumentDependencyContract::Variadic(
                FunctionArgumentDependencyRole::ReductionValue,
            ),
        }
        .for_arity(arity)
    }

    pub fn criteria_aggregation(
        arity: usize,
        arity_rule: FunctionArityRule,
        value_range: CriteriaValueRange,
        first_criteria_pair: usize,
    ) -> Option<Self> {
        Self {
            class: FunctionDependencyClass::CriteriaAggregation,
            arity: arity_rule,
            arguments: FunctionArgumentDependencyContract::CriteriaPairs(
                CriteriaAggregationDependencyContract {
                    value_range,
                    first_criteria_pair,
                },
            ),
        }
        .for_arity(arity)
    }

    pub fn for_arity(self, arity: usize) -> Option<Self> {
        self.arity.allows(arity).then_some(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::Function;
    use crate::traits::{ArgumentHandle, FunctionContext};
    use formualizer_common::ExcelError;

    struct NoOptInFn;

    impl Function for NoOptInFn {
        fn name(&self) -> &'static str {
            "NO_OPT_IN"
        }

        fn eval<'a, 'b, 'c>(
            &self,
            _args: &'c [ArgumentHandle<'a, 'b>],
            _ctx: &dyn FunctionContext<'b>,
        ) -> Result<crate::traits::CalcValue<'b>, ExcelError> {
            unreachable!("contract tests never evaluate")
        }
    }

    #[test]
    fn default_function_dependency_contract_is_conservative_none() {
        let function = NoOptInFn;

        assert_eq!(function.dependency_contract(0), None);
        assert_eq!(function.dependency_contract(1), None);
        assert_eq!(function.dependency_contract(3), None);
    }

    #[test]
    fn arity_rules_are_explicit_and_bounded() {
        assert!(FunctionArityRule::Exactly(1).allows(1));
        assert!(!FunctionArityRule::Exactly(1).allows(0));
        assert!(FunctionArityRule::AtLeast(0).allows(0));
        assert!(FunctionArityRule::AtLeast(1).allows(3));
        assert!(!FunctionArityRule::AtLeast(2).allows(1));
        assert!(FunctionArityRule::OneOf(&[2, 3]).allows(3));
        assert!(!FunctionArityRule::OneOf(&[2, 3]).allows(4));
        assert!(FunctionArityRule::EvenAtLeast(2).allows(4));
        assert!(!FunctionArityRule::EvenAtLeast(2).allows(3));
        assert!(FunctionArityRule::OddAtLeast(3).allows(5));
        assert!(!FunctionArityRule::OddAtLeast(3).allows(4));
    }

    #[test]
    fn constructors_return_none_for_unsupported_arities() {
        assert!(FunctionDependencyContract::static_scalar_all_args(1).is_some());
        assert_eq!(FunctionDependencyContract::static_scalar_all_args(2), None);

        assert!(FunctionDependencyContract::static_reduction(0, 0).is_some());
        assert_eq!(FunctionDependencyContract::static_reduction(0, 1), None);

        assert!(
            FunctionDependencyContract::criteria_aggregation(
                4,
                FunctionArityRule::EvenAtLeast(2),
                CriteriaValueRange::None,
                0,
            )
            .is_some()
        );
        assert_eq!(
            FunctionDependencyContract::criteria_aggregation(
                3,
                FunctionArityRule::EvenAtLeast(2),
                CriteriaValueRange::None,
                0,
            ),
            None
        );
    }

    #[test]
    fn selected_builtin_opt_ins_are_colocated_and_arity_gated() {
        use crate::builtins::math::aggregate::{AverageFn, SumFn};
        use crate::builtins::math::criteria_aggregates::{CountIfsFn, SumIfFn, SumIfsFn};
        use crate::builtins::math::numeric::AbsFn;

        let abs = AbsFn;
        assert_eq!(
            abs.dependency_contract(1).map(|contract| contract.class),
            Some(FunctionDependencyClass::StaticScalarAllArgs)
        );
        assert_eq!(abs.dependency_contract(2), None);

        let sum = SumFn;
        assert_eq!(
            sum.dependency_contract(0).map(|contract| contract.class),
            Some(FunctionDependencyClass::StaticReduction)
        );

        let average = AverageFn;
        assert_eq!(average.dependency_contract(0), None);
        assert_eq!(
            average
                .dependency_contract(1)
                .map(|contract| contract.class),
            Some(FunctionDependencyClass::StaticReduction)
        );

        let countifs = CountIfsFn;
        assert!(countifs.dependency_contract(2).is_some());
        assert!(countifs.dependency_contract(4).is_some());
        assert_eq!(countifs.dependency_contract(3), None);

        let sumif = SumIfFn;
        let contract = sumif.dependency_contract(3).expect("SUMIF arity 3");
        assert_eq!(contract.class, FunctionDependencyClass::CriteriaAggregation);
        assert_eq!(
            contract.arguments,
            FunctionArgumentDependencyContract::CriteriaPairs(
                CriteriaAggregationDependencyContract {
                    value_range: CriteriaValueRange::Optional {
                        provided_index: 2,
                        fallback_criteria_range_index: 0,
                    },
                    first_criteria_pair: 0,
                }
            )
        );

        let sumifs = SumIfsFn;
        assert!(sumifs.dependency_contract(3).is_some());
        assert!(sumifs.dependency_contract(5).is_some());
        assert_eq!(sumifs.dependency_contract(4), None);
    }
}
