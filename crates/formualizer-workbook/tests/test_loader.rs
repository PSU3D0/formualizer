#[cfg(feature = "json")]
mod json_stream {
    use formualizer_common::RangeAddress;
    use formualizer_eval::engine::ingest::EngineLoadStream;
    use formualizer_eval::engine::named_range::NamedDefinition;
    use formualizer_eval::engine::{Engine, EvalConfig};
    use formualizer_workbook::traits::NamedRangeScope;
    use formualizer_workbook::{CellData, JsonAdapter, NamedRange, SpreadsheetWriter};

    fn create_test_engine() -> Engine<formualizer_eval::test_workbook::TestWorkbook> {
        let ctx = formualizer_eval::test_workbook::TestWorkbook::new();
        Engine::new(ctx, EvalConfig::default())
    }

    #[test]
    fn json_stream_registers_named_ranges() {
        let mut adapter = JsonAdapter::new();
        adapter.create_sheet("Sheet1").unwrap();
        adapter.create_sheet("Sheet2").unwrap();
        adapter
            .write_cell("Sheet2", 1, 1, CellData::from_value(0.0))
            .unwrap();

        adapter.set_named_ranges(
            "Sheet1",
            vec![
                NamedRange {
                    name: "GlobalName".into(),
                    scope: NamedRangeScope::Workbook,
                    address: RangeAddress::new("Sheet1", 1, 1, 1, 1).unwrap(),
                },
                NamedRange {
                    name: "LocalName".into(),
                    scope: NamedRangeScope::Sheet,
                    address: RangeAddress::new("Sheet1", 2, 1, 2, 2).unwrap(),
                },
            ],
        );

        let mut engine = create_test_engine();
        adapter.stream_into_engine(&mut engine).unwrap();

        let sheet_id = engine.sheet_id("Sheet1").expect("sheet present");
        let global = engine
            .resolve_name_entry("GlobalName", sheet_id)
            .map(|nr| &nr.definition)
            .expect("global name registered");

        match global {
            NamedDefinition::Cell(cell) => {
                assert_eq!(engine.sheet_name(cell.sheet_id), "Sheet1");
                assert_eq!(format!("{}", cell.coord), "$A$1");
            }
            other => panic!("expected cell definition, got {other:?}"),
        }

        let local = engine
            .resolve_name_entry("LocalName", sheet_id)
            .map(|nr| &nr.definition)
            .expect("local name registered");

        match local {
            NamedDefinition::Range(range) => {
                assert_eq!(engine.sheet_name(range.start.sheet_id), "Sheet1");
                assert_eq!(format!("{}", range.start.coord), "$A$2");
                assert_eq!(format!("{}", range.end.coord), "$B$2");
            }
            other => panic!("expected range definition, got {other:?}"),
        }
    }
}
