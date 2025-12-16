use crate::arrow_store::{ArrowSheet, IngestBuilder};
use crate::engine::Engine;
use crate::traits::EvaluationContext;
use chrono::Timelike;
use formualizer_common::{ExcelError, LiteralValue};
use rustc_hash::FxHashMap;

#[derive(Debug, Clone, Default)]
pub struct ArrowBulkIngestSummary {
    pub sheets: usize,
    pub total_rows: usize,
}

/// Bulk Arrow ingest builder for Phase A base values.
pub struct ArrowBulkIngestBuilder<'e, R: EvaluationContext> {
    engine: &'e mut Engine<R>,
    builders: FxHashMap<String, IngestBuilder>,
    rows: FxHashMap<String, usize>,
}

impl<'e, R: EvaluationContext> ArrowBulkIngestBuilder<'e, R> {
    pub fn new(engine: &'e mut Engine<R>) -> Self {
        Self {
            engine,
            builders: FxHashMap::default(),
            rows: FxHashMap::default(),
        }
    }

    /// Add a sheet ingest target. Creates or replaces any existing Arrow sheet on finish.
    pub fn add_sheet(&mut self, name: &str, ncols: usize, chunk_rows: usize) {
        let ib = IngestBuilder::new(name, ncols, chunk_rows, self.engine.config.date_system);
        self.builders.insert(name.to_string(), ib);
        self.rows.insert(name.to_string(), 0);
        self.engine.sheet_id_mut(name);
    }

    /// Append a single row of values for the given sheet (0-based col order, length == ncols).
    pub fn append_row(&mut self, name: &str, row: &[LiteralValue]) -> Result<(), ExcelError> {
        let ib = self
            .builders
            .get_mut(name)
            .expect("sheet must be added before append_row");
        ib.append_row(row)?;
        *self.rows.get_mut(name).unwrap() += 1;
        Ok(())
    }

    /// Finish all sheet builders, installing ArrowSheets into the engine store.
    pub fn finish(mut self) -> Result<ArrowBulkIngestSummary, ExcelError> {
        let mut sheets: Vec<(String, ArrowSheet)> = Vec::with_capacity(self.builders.len());
        for (name, builder) in self.builders.drain() {
            let sheet = builder.finish();
            sheets.push((name, sheet));
        }
        // Insert or replace by name
        for (name, sheet) in sheets {
            let store = self.engine.sheet_store_mut();
            if let Some(pos) = store.sheets.iter().position(|s| s.name.as_ref() == name) {
                store.sheets[pos] = sheet;
            } else {
                store.sheets.push(sheet);
            }
        }
        let total_rows = self.rows.values().copied().sum();
        Ok(ArrowBulkIngestSummary {
            sheets: self.rows.len(),
            total_rows,
        })
    }
}

/// Bulk Arrow update builder for Phase C. Chooses overlay vs rebuild per chunk.
pub struct ArrowBulkUpdateBuilder<'e, R: EvaluationContext> {
    engine: &'e mut Engine<R>,
    // sheet -> col0 -> row0 -> value
    updates: FxHashMap<String, FxHashMap<usize, FxHashMap<usize, LiteralValue>>>,
}

impl<'e, R: EvaluationContext> ArrowBulkUpdateBuilder<'e, R> {
    pub fn new(engine: &'e mut Engine<R>) -> Self {
        Self {
            engine,
            updates: FxHashMap::default(),
        }
    }

    pub fn update_cell(&mut self, sheet: &str, row: u32, col: u32, value: LiteralValue) {
        let s = self.updates.entry(sheet.to_string()).or_default();
        let c = s.entry(col.saturating_sub(1) as usize).or_default();
        c.insert(row.saturating_sub(1) as usize, value);
    }

    pub fn finish(mut self) -> Result<usize, ExcelError> {
        use std::sync::Arc;
        let date_system = self.engine.config.date_system;
        let mut total = 0usize;
        for (sheet_name, by_col) in self.updates.drain() {
            let maybe_sheet = self.engine.sheet_store_mut().sheet_mut(&sheet_name);
            if maybe_sheet.is_none() {
                continue;
            }
            let sheet = maybe_sheet.unwrap();
            for (col0, rows_map) in by_col {
                total += rows_map.len();
                if col0 >= sheet.columns.len() {
                    continue;
                }
                // Partition by chunk
                let mut by_chunk: FxHashMap<usize, Vec<(usize, LiteralValue)>> =
                    FxHashMap::default();
                for (row0, v) in rows_map {
                    if row0 >= sheet.nrows as usize {
                        sheet.ensure_row_capacity(row0 + 1);
                    }
                    if let Some((ch_idx, in_off)) = sheet.chunk_of_row(row0) {
                        by_chunk.entry(ch_idx).or_default().push((in_off, v));
                    }
                }
                for (ch_idx, mut items) in by_chunk {
                    let Some(ch) = sheet.ensure_column_chunk_mut(col0, ch_idx) else {
                        continue;
                    };
                    let len = ch.type_tag.len();
                    // heuristic: rebuild if > 2% or > 1024 updates in this chunk
                    let rebuild = items.len() > len / 50 || items.len() > 1024;
                    if !rebuild {
                        // overlay
                        for (off, v) in items {
                            let ov = match v {
                                LiteralValue::Empty => crate::arrow_store::OverlayValue::Empty,
                                LiteralValue::Int(i) => {
                                    crate::arrow_store::OverlayValue::Number(i as f64)
                                }
                                LiteralValue::Number(n) => {
                                    crate::arrow_store::OverlayValue::Number(n)
                                }
                                LiteralValue::Boolean(b) => {
                                    crate::arrow_store::OverlayValue::Boolean(b)
                                }
                                LiteralValue::Text(s) => {
                                    crate::arrow_store::OverlayValue::Text(Arc::from(s))
                                }
                                LiteralValue::Error(e) => crate::arrow_store::OverlayValue::Error(
                                    crate::arrow_store::map_error_code(e.kind),
                                ),
                                LiteralValue::Date(d) => {
                                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                                        date_system,
                                        &dt,
                                    );
                                    crate::arrow_store::OverlayValue::Number(serial)
                                }
                                LiteralValue::DateTime(dt) => {
                                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                                        date_system,
                                        &dt,
                                    );
                                    crate::arrow_store::OverlayValue::Number(serial)
                                }
                                LiteralValue::Time(t) => {
                                    let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                                    crate::arrow_store::OverlayValue::Number(serial)
                                }
                                LiteralValue::Duration(d) => {
                                    let serial = d.num_seconds() as f64 / 86_400.0;
                                    crate::arrow_store::OverlayValue::Number(serial)
                                }
                                LiteralValue::Pending => crate::arrow_store::OverlayValue::Pending,
                                LiteralValue::Array(_) => crate::arrow_store::OverlayValue::Error(
                                    crate::arrow_store::map_error_code(
                                        formualizer_common::ExcelErrorKind::Value,
                                    ),
                                ),
                            };
                            ch.overlay.set(off, ov);
                        }
                    } else {
                        // rebuild chunk with updates applied
                        use arrow_array::Array as _;
                        use arrow_array::builder::{
                            BooleanBuilder, Float64Builder, StringBuilder, UInt8Builder,
                        };
                        items.sort_by_key(|(o, _)| *o);
                        let mut tag_b = UInt8Builder::with_capacity(len);
                        let mut nb = Float64Builder::with_capacity(len);
                        let mut bb = BooleanBuilder::with_capacity(len);
                        let mut sb = StringBuilder::with_capacity(len, len * 8);
                        let mut eb = UInt8Builder::with_capacity(len);
                        let mut non_num = 0usize;
                        let mut non_bool = 0usize;
                        let mut non_text = 0usize;
                        let mut non_err = 0usize;
                        let mut it = items.into_iter().peekable();
                        for i in 0..len {
                            let upd = if it.peek().map(|(o, _)| *o == i).unwrap_or(false) {
                                Some(it.next().unwrap().1)
                            } else {
                                None
                            };
                            let val = if let Some(v) = upd {
                                v
                            } else {
                                // read from base tag/lane
                                let t = crate::arrow_store::TypeTag::from_u8(ch.type_tag.value(i));
                                match t {
                                    crate::arrow_store::TypeTag::Empty => LiteralValue::Empty,
                                    crate::arrow_store::TypeTag::Number
                                    | crate::arrow_store::TypeTag::DateTime
                                    | crate::arrow_store::TypeTag::Duration => {
                                        if let Some(a) = &ch.numbers {
                                            let fa = a
                                                .as_any()
                                                .downcast_ref::<arrow_array::Float64Array>()
                                                .unwrap();
                                            if fa.is_null(i) {
                                                LiteralValue::Empty
                                            } else {
                                                LiteralValue::Number(fa.value(i))
                                            }
                                        } else {
                                            LiteralValue::Empty
                                        }
                                    }
                                    crate::arrow_store::TypeTag::Boolean => {
                                        if let Some(a) = &ch.booleans {
                                            let ba = a
                                                .as_any()
                                                .downcast_ref::<arrow_array::BooleanArray>()
                                                .unwrap();
                                            if ba.is_null(i) {
                                                LiteralValue::Empty
                                            } else {
                                                LiteralValue::Boolean(ba.value(i))
                                            }
                                        } else {
                                            LiteralValue::Empty
                                        }
                                    }
                                    crate::arrow_store::TypeTag::Text => {
                                        if let Some(a) = &ch.text {
                                            let sa = a
                                                .as_any()
                                                .downcast_ref::<arrow_array::StringArray>()
                                                .unwrap();
                                            if sa.is_null(i) {
                                                LiteralValue::Empty
                                            } else {
                                                LiteralValue::Text(sa.value(i).to_string())
                                            }
                                        } else {
                                            LiteralValue::Empty
                                        }
                                    }
                                    crate::arrow_store::TypeTag::Error => {
                                        if let Some(a) = &ch.errors {
                                            let ea = a
                                                .as_any()
                                                .downcast_ref::<arrow_array::UInt8Array>()
                                                .unwrap();
                                            if ea.is_null(i) {
                                                LiteralValue::Empty
                                            } else {
                                                LiteralValue::Error(ExcelError::new(
                                                    crate::arrow_store::unmap_error_code(
                                                        ea.value(i),
                                                    ),
                                                ))
                                            }
                                        } else {
                                            LiteralValue::Empty
                                        }
                                    }
                                    crate::arrow_store::TypeTag::Pending => LiteralValue::Pending,
                                }
                            };
                            match val {
                                LiteralValue::Empty => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Empty as u8);
                                    nb.append_null();
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Int(i) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Number as u8);
                                    nb.append_value(i as f64);
                                    non_num += 1;
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Number(n) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Number as u8);
                                    nb.append_value(n);
                                    non_num += 1;
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Boolean(b) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Boolean as u8);
                                    nb.append_null();
                                    bb.append_value(b);
                                    non_bool += 1;
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Text(s) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Text as u8);
                                    nb.append_null();
                                    bb.append_null();
                                    sb.append_value(&s);
                                    non_text += 1;
                                    eb.append_null();
                                }
                                LiteralValue::Error(e) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Error as u8);
                                    nb.append_null();
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_value(crate::arrow_store::map_error_code(e.kind));
                                    non_err += 1;
                                }
                                LiteralValue::Date(d) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Number as u8);
                                    let dt = d.and_hms_opt(0, 0, 0).unwrap();
                                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                                        date_system,
                                        &dt,
                                    );
                                    nb.append_value(serial);
                                    non_num += 1;
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::DateTime(dt) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Number as u8);
                                    let serial = crate::builtins::datetime::datetime_to_serial_for(
                                        date_system,
                                        &dt,
                                    );
                                    nb.append_value(serial);
                                    non_num += 1;
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Time(t) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Number as u8);
                                    let serial = t.num_seconds_from_midnight() as f64 / 86_400.0;
                                    nb.append_value(serial);
                                    non_num += 1;
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Duration(d) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Number as u8);
                                    let serial = d.num_seconds() as f64 / 86_400.0;
                                    nb.append_value(serial);
                                    non_num += 1;
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Pending => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Pending as u8);
                                    nb.append_null();
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_null();
                                }
                                LiteralValue::Array(_) => {
                                    tag_b.append_value(crate::arrow_store::TypeTag::Error as u8);
                                    nb.append_null();
                                    bb.append_null();
                                    sb.append_null();
                                    eb.append_value(crate::arrow_store::map_error_code(
                                        formualizer_common::ExcelErrorKind::Value,
                                    ));
                                    non_err += 1;
                                }
                            }
                        }
                        ch.type_tag = Arc::new(tag_b.finish());
                        ch.numbers = if non_num == 0 {
                            None
                        } else {
                            Some(Arc::new(nb.finish()))
                        };
                        ch.booleans = if non_bool == 0 {
                            None
                        } else {
                            Some(Arc::new(bb.finish()))
                        };
                        ch.text = if non_text == 0 {
                            None
                        } else {
                            Some(Arc::new(sb.finish()))
                        };
                        ch.errors = if non_err == 0 {
                            None
                        } else {
                            Some(Arc::new(eb.finish()))
                        };
                        ch.meta.len = len;
                        ch.meta.non_null_num = non_num;
                        ch.meta.non_null_bool = non_bool;
                        ch.meta.non_null_text = non_text;
                        ch.meta.non_null_err = non_err;
                        ch.overlay.clear();
                    }
                }
            }
        }
        // Advance snapshot and mark edited
        self.engine.mark_data_edited();
        Ok(total)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::EvalConfig;
    use crate::test_workbook::TestWorkbook;

    #[test]
    fn arrow_bulk_ingest_basic() {
        let mut engine = Engine::new(TestWorkbook::default(), EvalConfig::default());
        let mut ab = engine.begin_bulk_ingest_arrow();
        ab.add_sheet("S", 3, 2);
        ab.append_row(
            "S",
            &[
                LiteralValue::Number(1.0),
                LiteralValue::Text("a".into()),
                LiteralValue::Empty,
            ],
        )
        .unwrap();
        ab.append_row(
            "S",
            &[
                LiteralValue::Boolean(true),
                LiteralValue::Text("".into()),
                LiteralValue::Error(formualizer_common::ExcelError::new_value()),
            ],
        )
        .unwrap();
        let summary = ab.finish().unwrap();
        assert_eq!(summary.sheets, 1);
        assert_eq!(summary.total_rows, 2);

        let sheet = engine
            .sheet_store()
            .sheet("S")
            .expect("arrow sheet present");
        assert_eq!(sheet.columns.len(), 3);
        assert_eq!(sheet.nrows, 2);
        // Validate chunking (chunk_rows=2 => single chunk)
        for col in &sheet.columns {
            assert_eq!(col.chunks.len(), 1);
            assert_eq!(col.chunks[0].len(), 2);
        }
    }
}
