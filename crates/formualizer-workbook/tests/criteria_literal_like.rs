use formualizer_workbook::{LiteralValue, Workbook, WorkbookConfig};

#[test]
fn criteria_keep_sql_like_punctuation_literal_in_base_and_overlay_lanes() {
    let data = [
        "a%b", "axxxb", "a_b", "aQb", "1_0", "1x0", "a%xyz", "ab", r"a\b", r"a\xyz", "a*b", "a?b",
        "a~b", "A%B",
    ];
    let cases: &[(&str, &[usize])] = &[
        ("a%b", &[0, 13]),
        ("1_0", &[4]),
        ("a_b", &[2]),
        ("<>a%b", &[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
        ("a%*", &[0, 6, 13]),
        ("a_?", &[2]),
        (r"a\b", &[8]),
        (r"a\*", &[8, 9]),
        ("a~*b", &[10]),
        ("a~?b", &[11]),
        ("a~~b", &[12]),
        ("a?b", &[0, 2, 3, 8, 10, 11, 12, 13]),
        ("<>a%*", &[1, 2, 3, 4, 5, 7, 8, 9, 10, 11, 12]),
    ];
    {
        let mut wb = Workbook::new_with_config(WorkbookConfig::ephemeral());
        wb.add_sheet("Data").unwrap();
        wb.add_sheet("Results").unwrap();
        for (index, text) in data.iter().enumerate() {
            wb.set_value(
                "Data",
                index as u32 + 1,
                1,
                LiteralValue::Text((*text).into()),
            )
            .unwrap();
            wb.set_value(
                "Data",
                index as u32 + 1,
                2,
                LiteralValue::Number((index + 1) as f64),
            )
            .unwrap();
        }
        // Install identical values as immutable Arrow base lanes before the edit.
        let mut ingest =
            formualizer_eval::engine::arrow_ingest::ArrowBulkIngestBuilder::new(wb.engine_mut());
        ingest.add_sheet("Data", 2, 1024);
        for (index, text) in data.iter().enumerate() {
            ingest
                .append_row(
                    "Data",
                    &[
                        LiteralValue::Text((*text).into()),
                        LiteralValue::Number((index + 1) as f64),
                    ],
                )
                .unwrap();
        }
        ingest.finish().unwrap();
        let mut formulas = Vec::new();
        for &(criterion, matches) in cases {
            let count = matches.len() as f64;
            let sum = matches.iter().map(|index| (index + 1) as f64).sum::<f64>();
            let max = matches.iter().map(|index| index + 1).max().unwrap() as f64;
            formulas.extend([
                (format!("COUNTIF(Data!A1:A14,\"{criterion}\")"), count),
                (
                    format!("COUNTIFS(Data!A1:A14,\"{criterion}\",Data!B1:B14,\">0\")"),
                    count,
                ),
                (
                    format!("SUMIF(Data!A1:A14,\"{criterion}\",Data!B1:B14)"),
                    sum,
                ),
                (
                    format!("AVERAGEIF(Data!A1:A14,\"{criterion}\",Data!B1:B14)"),
                    sum / count,
                ),
                (
                    format!("MAXIFS(Data!B1:B14,Data!A1:A14,\"{criterion}\")"),
                    max,
                ),
            ]);
        }
        for edited in [false, true] {
            if edited {
                // Edit a non-matching cell in the criteria range to exercise overlay reads.
                wb.set_value("Data", 6, 1, LiteralValue::Text("unrelated".into()))
                    .unwrap();
            }
            for (index, (formula, _)) in formulas.iter().enumerate() {
                wb.set_formula("Results", index as u32 + 1, 1, formula)
                    .unwrap();
            }
            wb.evaluate_all().unwrap();
            for (index, (formula, expected)) in formulas.iter().enumerate() {
                assert_eq!(
                    wb.get_value("Results", index as u32 + 1, 1),
                    Some(LiteralValue::Number(*expected)),
                    "edited={edited}: {formula}"
                );
            }
        }
    }
}
