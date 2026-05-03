//! Optional function-owned dependency contracts.
//!
//! These contracts describe how a function contributes dependencies for passive
//! planning/FormulaPlane analysis. They are deliberately additive: functions
//! that do not opt in keep the default conservative behavior and receive no
//! dependency-summary optimization.

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
