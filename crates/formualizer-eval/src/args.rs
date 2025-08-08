use crate::traits::ArgumentHandle;
use crate::traits::EvaluationContext;
use formualizer_common::{ArgKind, ExcelError, ExcelErrorKind, LiteralValue};
use smallvec::{SmallVec, smallvec};
use std::borrow::Cow;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ShapeKind {
    Scalar,
    Range,
    Array,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum CoercionPolicy {
    None,
    NumberStrict,
    NumberLenientText,
    Logical,
    Criteria,
    DateTimeSerial,
}

#[derive(Clone, Debug)]
pub struct ArgSchema {
    pub kinds: SmallVec<[ArgKind; 2]>,
    pub required: bool,
    pub by_ref: bool,
    pub shape: ShapeKind,
    pub coercion: CoercionPolicy,
    pub max: Option<usize>,
    pub repeating: Option<usize>,
    pub default: Option<LiteralValue>,
}

impl ArgSchema {
    pub fn any() -> Self {
        Self {
            kinds: smallvec![ArgKind::Any],
            required: true,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::None,
            max: None,
            repeating: None,
            default: None,
        }
    }

    pub fn number_lenient_scalar() -> Self {
        Self {
            kinds: smallvec![ArgKind::Number],
            required: true,
            by_ref: false,
            shape: ShapeKind::Scalar,
            coercion: CoercionPolicy::NumberLenientText,
            max: None,
            repeating: None,
            default: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CriteriaPredicate {
    Eq(LiteralValue),
    Ne(LiteralValue),
    Gt(f64),
    Ge(f64),
    Lt(f64),
    Le(f64),
    TextLike {
        pattern: String,
        case_insensitive: bool,
    },
    IsBlank,
    IsNumber,
    IsText,
    IsLogical,
}

#[derive(Debug)]
pub enum PreparedArg<'a> {
    Value(Cow<'a, LiteralValue>),
    Range(crate::engine::range_stream::RangeStorage<'a>),
    Reference(formualizer_core::parser::ReferenceType),
    Predicate(CriteriaPredicate),
}

pub struct PreparedArgs<'a> {
    pub items: Vec<PreparedArg<'a>>,
}

pub struct ValidationOptions {
    pub warn_only: bool,
}

impl Default for ValidationOptions {
    fn default() -> Self {
        Self { warn_only: false }
    }
}

// Legacy adapter removed in clean break.

pub fn validate_and_prepare<'a, 'b>(
    args: &'a [ArgumentHandle<'a, 'b>],
    schema: &[ArgSchema],
    _ctx: &dyn EvaluationContext,
    options: ValidationOptions,
) -> Result<PreparedArgs<'a>, ExcelError> {
    // Arity: simple rule – if schema.len() == 1, allow variadic repetition; else match up to schema.len()
    if schema.is_empty() {
        return Ok(PreparedArgs { items: Vec::new() });
    }

    let mut items: Vec<PreparedArg<'a>> = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        let spec = if schema.len() == 1 {
            &schema[0]
        } else if idx < schema.len() {
            &schema[idx]
        } else {
            // Extra args without repeating rule → error or warn
            if options.warn_only {
                continue;
            } else {
                return Err(
                    ExcelError::new(ExcelErrorKind::Value).with_message("Too many arguments")
                );
            }
        };

        // By-ref argument: require a reference (AST literal or function-returned)
        if spec.by_ref {
            match arg.as_reference_or_eval() {
                Ok(r) => {
                    items.push(PreparedArg::Reference(r));
                    continue;
                }
                Err(e) => {
                    if options.warn_only {
                        continue;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        // Shape handling
        match spec.shape {
            ShapeKind::Scalar => {
                // Collapse to scalar if needed
                match arg.value() {
                    Ok(v) => items.push(PreparedArg::Value(v)),
                    Err(e) => items.push(PreparedArg::Value(Cow::Owned(LiteralValue::Error(e)))),
                }
            }
            ShapeKind::Range | ShapeKind::Array => {
                match arg.range_storage() {
                    Ok(r) => items.push(PreparedArg::Range(r)),
                    Err(_e) => {
                        // Excel-compatible: functions that accept ranges typically also accept scalars.
                        // Fall back to treating the argument as a scalar value, even in strict mode.
                        match arg.value() {
                            Ok(v) => items.push(PreparedArg::Value(v)),
                            Err(e2) => {
                                items.push(PreparedArg::Value(Cow::Owned(LiteralValue::Error(e2))))
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(PreparedArgs { items })
}
