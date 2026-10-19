use super::*;
use crate::traits::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct UnversionedProvider {
    calls: Arc<AtomicUsize>,
    value: Arc<AtomicUsize>,
}
impl FunctionProvider for UnversionedProvider {
    fn get_function(&self, ns: &str, name: &str) -> Option<Arc<dyn crate::function::Function>> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        if name.eq_ignore_ascii_case("UNVERSIONED_SNAPSHOT") {
            let value = self.value.load(Ordering::Relaxed);
            return (value != 0)
                .then(|| Arc::new(SnapshotValue(value)) as Arc<dyn crate::function::Function>);
        }
        crate::function_registry::get(ns, name)
    }
}
impl ReferenceResolver for UnversionedProvider {
    fn resolve_cell_reference(
        &self,
        _: Option<&str>,
        _: u32,
        _: u32,
    ) -> Result<LiteralValue, ExcelError> {
        Ok(LiteralValue::Empty)
    }
}
impl RangeResolver for UnversionedProvider {
    fn resolve_range_reference(
        &self,
        _: Option<&str>,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u32>,
        _: Option<u32>,
    ) -> Result<Box<dyn Range>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::Ref))
    }
}
impl NamedRangeResolver for UnversionedProvider {
    fn resolve_named_range_reference(&self, _: &str) -> Result<Vec<Vec<LiteralValue>>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::Name))
    }
}
impl TableResolver for UnversionedProvider {
    fn resolve_table_reference(
        &self,
        _: &formualizer_parse::parser::TableReference,
    ) -> Result<Box<dyn Table>, ExcelError> {
        Err(ExcelError::new(ExcelErrorKind::Ref))
    }
}
struct SnapshotValue(usize);
impl crate::function::Function for SnapshotValue {
    fn name(&self) -> &'static str {
        "UNVERSIONED_SNAPSHOT"
    }
    fn eval<'a, 'b, 'c>(
        &self,
        _: &'c [ArgumentHandle<'a, 'b>],
        _: &dyn FunctionContext<'b>,
    ) -> Result<CalcValue<'b>, ExcelError> {
        Ok(CalcValue::Scalar(LiteralValue::Number(self.0 as f64)))
    }
}

#[test]
fn unversioned_family_snapshot_refreshes_missing_replaced_and_removed()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = UnversionedProvider::default();
    let value = provider.value.clone();
    let mut e = Engine::new(
        provider,
        EvalConfig {
            enable_parallel: false,
            ..EvalConfig::default()
        },
    );
    for row in 1..=64 {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(0.0))?;
        e.set_cell_formula(
            "Sheet1",
            row,
            2,
            formualizer_parse::parser::parse(format!("=UNVERSIONED_SNAPSHOT()+A{row}"))?,
        )?;
    }
    e.set_cell_formula(
        "Sheet1",
        1,
        8,
        formualizer_parse::parser::parse("=UNVERSIONED_SNAPSHOT()+A1+10")?,
    )?;
    for (pass, result) in [0, 42, 84, 0].into_iter().enumerate() {
        value.store(result, Ordering::Relaxed);
        for row in 1..=64 {
            e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(pass as f64))?;
        }
        e.evaluate_all()?;
        for row in 1..=64 {
            if result == 0 {
                assert!(
                    matches!(e.get_cell_value("Sheet1", row, 2), Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Name)
                );
            } else {
                assert_eq!(
                    e.get_cell_value("Sheet1", row, 2),
                    Some(LiteralValue::Number(result as f64 + pass as f64))
                );
            }
        }
        if result == 0 {
            assert!(
                matches!(e.get_cell_value("Sheet1", 1, 8), Some(LiteralValue::Error(err)) if err.kind == ExcelErrorKind::Name)
            );
        } else {
            assert_eq!(
                e.get_cell_value("Sheet1", 1, 8),
                Some(LiteralValue::Number(result as f64 + pass as f64 + 10.0))
            );
        }
    }
    Ok(())
}
impl SourceResolver for UnversionedProvider {}
impl Resolver for UnversionedProvider {}
impl EvaluationContext for UnversionedProvider {}

#[test]
fn unversioned_family_snapshot_avoids_member_provider_calls()
-> Result<(), Box<dyn std::error::Error>> {
    let provider = UnversionedProvider::default();
    let calls = provider.calls.clone();
    let mut e = Engine::new(
        provider,
        EvalConfig {
            enable_parallel: false,
            ..EvalConfig::default()
        },
    );
    for row in 1..=64 {
        e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(1.0))?;
        // walk (LEFT), lift, criteria, invariant and nested argument evaluation.
        for (col, text) in [
            (2, format!("=LEFT(A{row}&\"x\",1)")),
            (3, format!("=ABS(A{row})+1")),
            (4, format!("=COUNTIF($A$1:$A$64,A{row})")),
            (5, format!("=IF(A{row}>0,SUM($A$1:$A$64),0)")),
        ] {
            e.set_cell_formula("Sheet1", row, col, formualizer_parse::parser::parse(text)?)?;
        }
    }
    e.set_cell_formula(
        "Sheet1",
        1,
        8,
        formualizer_parse::parser::parse("=ABS(A1)+10")?,
    )?;
    for input in [2.0, 3.0] {
        for row in 1..=64 {
            e.set_cell_value("Sheet1", row, 1, LiteralValue::Number(input))?;
        }
        calls.store(0, Ordering::Relaxed);
        e.evaluate_all()?;
        let count = calls.load(Ordering::Relaxed);
        // None revisions must refresh each request, but never each member.
        assert!(count > 0 && count < 64, "provider calls={count}");
        assert!(e.family_members_for_test() > 0);
        for row in 1..=64 {
            assert_eq!(
                e.get_cell_value("Sheet1", row, 2),
                Some(LiteralValue::Text(format!("{input}")))
            );
            assert_eq!(
                e.get_cell_value("Sheet1", row, 3),
                Some(LiteralValue::Number(input + 1.0))
            );
            assert_eq!(
                e.get_cell_value("Sheet1", row, 4),
                Some(LiteralValue::Number(64.0))
            );
            assert_eq!(
                e.get_cell_value("Sheet1", row, 5),
                Some(LiteralValue::Number(64.0 * input))
            );
        }
        assert_eq!(
            e.get_cell_value("Sheet1", 1, 8),
            Some(LiteralValue::Number(input + 10.0))
        );
    }
    Ok(())
}
