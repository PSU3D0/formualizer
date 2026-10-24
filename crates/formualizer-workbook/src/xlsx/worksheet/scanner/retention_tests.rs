//! Output requirements are independent from publication admission/index strategy.
use super::*;
use crate::xlsx::options::ScanRetention;

#[derive(Default)]
struct Collected<'a> {
    bytes: &'a [u8],
    literals: Vec<(u32, u32, String)>,
    formulas: Vec<(u32, u32, u64, String)>,
}
impl emission::Sink for Collected<'_> {
    fn literal(&mut self, literal: emission::Literal<'_>) -> Result<(), IoError> {
        let emission::Value::Text(value) = literal.value else {
            panic!("expected text literal")
        };
        self.literals
            .push((literal.row, literal.col, value.into_owned()));
        assert_eq!(&self.bytes[literal.raw_type_span.unwrap()], b"str");
        Ok(())
    }
    fn formula(&mut self, formula: emission::Formula<'_>) -> Result<(), IoError> {
        assert_eq!(formula.offset, formula.envelope.cell.start);
        assert!(self.bytes[formula.envelope.formula.clone()].starts_with(b"<f"));
        assert_eq!(
            &self.bytes[formula.envelope.raw_formula_type.unwrap()],
            b"shared"
        );
        assert_eq!(
            &self.bytes[formula.envelope.raw_shared_index.unwrap()],
            b"2"
        );
        assert_eq!(&self.bytes[formula.envelope.raw_cache.unwrap()], b"1");
        let text = match formula.kind {
            emission::FormulaKind::SharedAnchor {
                shared_index,
                declared_range,
                text,
            } => {
                assert_eq!(shared_index, Some(2));
                assert_eq!(
                    declared_range,
                    SourceRect {
                        first_row: 1,
                        first_col: 1,
                        last_row: 2,
                        last_col: 1
                    }
                );
                assert_eq!(
                    &self.bytes[formula.envelope.raw_declared_range.unwrap()],
                    b"A1:A2"
                );
                text.to_owned()
            }
            emission::FormulaKind::SharedDescendant { shared_index } => {
                assert_eq!(shared_index, Some(2));
                assert!(formula.envelope.raw_declared_range.is_none());
                String::new()
            }
            _ => panic!("expected shared source event"),
        };
        self.formulas
            .push((formula.row, formula.col, formula.sequence, text));
        Ok(())
    }
}

#[test]
fn every_output_requirement_can_be_selected_independently() {
    let bytes = br#"<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><dimension ref="A1:B2"/><sheetData><row r="1"><c r="A1"><f t="shared" si="2" ref="A1:A2">A1+1</f><v>1</v></c><c r="B1" t="str"><v>x</v></c></row><row r="2"><c r="A2"><f t="shared" si="2"/><v>1</v></c></row></sheetData></worksheet>"#;
    let options = ReadOptions::from(&XlsxRecalculateOptions::default());
    for mask in 0..16 {
        let retention = ScanRetention {
            values: mask & 1 != 0,
            formula_events: mask & 2 != 0,
            marks: mask & 4 != 0,
            geometry: mask & 8 != 0,
        };
        let request = ScanRequest {
            retention,
            strategy: ScanStrategy::Indexed,
        };
        let mut sink = Collected {
            bytes,
            ..Default::default()
        };
        let (mut observed, mut logical) = (0, 0);
        let Scanned::Done(scan) = scan_with(
            bytes,
            &options,
            request,
            &mut observed,
            &mut logical,
            &mut sink,
        )
        .unwrap() else {
            panic!("no dynamic metadata in fixture")
        };
        assert_eq!((observed, logical), (3, 4));
        assert_eq!(sink.literals.len(), usize::from(retention.values));
        assert_eq!(
            sink.formulas.len(),
            if retention.formula_events { 2 } else { 0 }
        );
        if retention.formula_events {
            assert_eq!(
                sink.formulas,
                [(1, 1, 0, "A1+1".to_owned()), (2, 1, 1, String::new())]
            );
        }
        assert_eq!(scan.cells.len(), if retention.marks { 2 } else { 0 });
        assert_eq!(scan.index.is_some(), retention.geometry);
    }
}
