use crate::binding::{
    BoundPort, PortBinding, RangeBinding, RecordBinding, RecordFieldBinding, ScalarBinding,
    TableBinding,
};
use crate::value::{PortValue, TableRow, TableValue};
use formualizer_common::{DateSystem, LiteralValue};
use regex::Regex;
use serde_json::Value as JsonValue;
use sheetport_spec::{Constraints, ValueType};
use std::collections::BTreeMap;

/// Detailed information about why a value failed validation.
#[derive(Debug, Clone)]
pub struct ConstraintViolation {
    pub port: String,
    pub path: String,
    pub message: String,
}

impl ConstraintViolation {
    pub fn new(
        port: impl Into<String>,
        path: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            port: port.into(),
            path: path.into(),
            message: message.into(),
        }
    }
}

/// Scope for validation. Partial is used when only updated values are provided.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValidationScope {
    Full,
    Partial,
}

pub fn validate_port_value(
    binding: &PortBinding,
    value: &PortValue,
    scope: ValidationScope,
) -> Result<(), Vec<ConstraintViolation>> {
    if !binding.required && value.is_empty() {
        return Ok(());
    }

    let mut violations = Vec::new();
    match (&binding.kind, value) {
        (BoundPort::Scalar(scalar), PortValue::Scalar(lit)) => {
            validate_scalar(binding, scalar, lit, scope, &mut violations);
        }
        (BoundPort::Record(record), PortValue::Record(map)) => {
            validate_record(binding, record, map, scope, &mut violations);
        }
        (BoundPort::Range(range), PortValue::Range(rows)) => {
            validate_range(binding, range, rows, &mut violations);
        }
        (BoundPort::Table(table), PortValue::Table(table_value)) => {
            validate_table(binding, table, table_value, &mut violations);
        }
        _ => violations.push(ConstraintViolation::new(
            &binding.id,
            binding.id.clone(),
            "value shape does not match manifest declaration",
        )),
    }

    if violations.is_empty() {
        Ok(())
    } else {
        Err(violations)
    }
}

fn validate_scalar(
    binding: &PortBinding,
    scalar: &ScalarBinding,
    value: &LiteralValue,
    scope: ValidationScope,
    violations: &mut Vec<ConstraintViolation>,
) {
    let path = binding.id.clone();
    validate_literal(
        &binding.id,
        &path,
        scalar.value_type,
        binding.constraints.as_ref(),
        value,
        scope,
        violations,
    );
}

fn validate_record(
    binding: &PortBinding,
    record: &RecordBinding,
    map: &BTreeMap<String, LiteralValue>,
    scope: ValidationScope,
    violations: &mut Vec<ConstraintViolation>,
) {
    match scope {
        ValidationScope::Full => {
            for (field_name, field_binding) in &record.fields {
                let path = format!("{}.{}", binding.id, field_name);
                match map.get(field_name) {
                    Some(value) => validate_record_field(
                        binding,
                        field_binding,
                        value,
                        scope,
                        &path,
                        violations,
                    ),
                    None => violations.push(ConstraintViolation::new(
                        &binding.id,
                        path,
                        "record field missing in resolved value",
                    )),
                }
            }
        }
        ValidationScope::Partial => {
            for (field_name, value) in map {
                match record.fields.get(field_name) {
                    Some(field_binding) => {
                        let path = format!("{}.{}", binding.id, field_name);
                        validate_record_field(
                            binding,
                            field_binding,
                            value,
                            scope,
                            &path,
                            violations,
                        );
                    }
                    None => violations.push(ConstraintViolation::new(
                        &binding.id,
                        format!("{}.{}", binding.id, field_name),
                        "record field is not declared in manifest",
                    )),
                }
            }
        }
    }
}

fn validate_record_field(
    binding: &PortBinding,
    field_binding: &RecordFieldBinding,
    value: &LiteralValue,
    scope: ValidationScope,
    path: &str,
    violations: &mut Vec<ConstraintViolation>,
) {
    validate_literal(
        &binding.id,
        path,
        field_binding.value_type,
        field_binding.constraints.as_ref(),
        value,
        scope,
        violations,
    );
}

fn validate_range(
    binding: &PortBinding,
    range: &RangeBinding,
    rows: &[Vec<LiteralValue>],
    violations: &mut Vec<ConstraintViolation>,
) {
    for (row_idx, row) in rows.iter().enumerate() {
        for (col_idx, cell) in row.iter().enumerate() {
            let path = format!("{}[r{},c{}]", binding.id, row_idx + 1, col_idx + 1);
            validate_literal(
                &binding.id,
                &path,
                range.cell_type,
                binding.constraints.as_ref(),
                cell,
                ValidationScope::Full,
                violations,
            );
        }
    }
}

fn validate_table(
    binding: &PortBinding,
    table: &TableBinding,
    table_value: &TableValue,
    violations: &mut Vec<ConstraintViolation>,
) {
    for (row_idx, row) in table_value.rows.iter().enumerate() {
        validate_table_row(binding, table, row, row_idx, violations);
    }
}

fn validate_table_row(
    binding: &PortBinding,
    table: &TableBinding,
    row: &TableRow,
    row_idx: usize,
    violations: &mut Vec<ConstraintViolation>,
) {
    for column_name in row.values.keys() {
        if !table.columns.iter().any(|c| c.name == *column_name) {
            violations.push(ConstraintViolation::new(
                &binding.id,
                format!("{}[{}].{}", binding.id, row_idx, column_name),
                "column is not defined in manifest",
            ));
        }
    }

    for column in &table.columns {
        let path = format!("{}[{}].{}", binding.id, row_idx, column.name);
        match row.values.get(&column.name) {
            Some(value) => validate_literal(
                &binding.id,
                &path,
                column.value_type,
                binding.constraints.as_ref(),
                value,
                ValidationScope::Full,
                violations,
            ),
            None => violations.push(ConstraintViolation::new(
                &binding.id,
                path,
                "table row missing column value",
            )),
        }
    }
}

fn validate_literal(
    port_id: &str,
    path: &str,
    value_type: ValueType,
    constraints: Option<&Constraints>,
    value: &LiteralValue,
    scope: ValidationScope,
    violations: &mut Vec<ConstraintViolation>,
) {
    if is_empty(value) {
        let nullable = constraints.and_then(|c| c.nullable).unwrap_or(false);
        if !nullable {
            violations.push(ConstraintViolation::new(
                port_id,
                path.to_string(),
                "value may not be empty",
            ));
        }
        return;
    }

    if let Err(message) = ensure_type(value_type, value) {
        violations.push(ConstraintViolation::new(port_id, path.to_string(), message));
        return;
    }

    if let Some(constraints) = constraints
        && let Err(message) = enforce_constraints(value_type, value, constraints)
    {
        violations.push(ConstraintViolation::new(port_id, path.to_string(), message));
    }

    if scope == ValidationScope::Partial && is_empty(value) {
        // Already handled above; included for completeness.
    }
}

fn ensure_type(value_type: ValueType, value: &LiteralValue) -> Result<(), String> {
    match value_type {
        ValueType::String => {
            if matches!(value, LiteralValue::Text(_)) {
                Ok(())
            } else {
                Err("expected string value".into())
            }
        }
        ValueType::Number => {
            if matches!(value, LiteralValue::Number(_) | LiteralValue::Int(_)) {
                Ok(())
            } else {
                Err("expected numeric value".into())
            }
        }
        ValueType::Integer => match value {
            LiteralValue::Int(_) => Ok(()),
            LiteralValue::Number(n) if (*n - n.trunc()).abs() < f64::EPSILON => Ok(()),
            _ => Err("expected integer value".into()),
        },
        ValueType::Boolean => {
            if matches!(value, LiteralValue::Boolean(_)) {
                Ok(())
            } else {
                Err("expected boolean value".into())
            }
        }
        ValueType::Date => {
            if matches!(value, LiteralValue::Date(_) | LiteralValue::DateTime(_)) {
                Ok(())
            } else {
                Err("expected date value".into())
            }
        }
        ValueType::Datetime => {
            if matches!(value, LiteralValue::DateTime(_)) {
                Ok(())
            } else {
                Err("expected datetime value".into())
            }
        }
    }
}

fn enforce_constraints(
    value_type: ValueType,
    value: &LiteralValue,
    constraints: &Constraints,
) -> Result<(), String> {
    if let Some(min) = constraints.min {
        let v = to_f64(value)
            .ok_or_else(|| "value must be numeric to apply `min` constraint".to_string())?;
        if v < min {
            return Err(format!("value {v} is below minimum {min}"));
        }
    }

    if let Some(max) = constraints.max {
        let v = to_f64(value)
            .ok_or_else(|| "value must be numeric to apply `max` constraint".to_string())?;
        if v > max {
            return Err(format!("value {v} exceeds maximum {max}"));
        }
    }

    if let Some(pattern) = &constraints.pattern {
        let string = to_string_value(value, value_type)
            .ok_or_else(|| "value must be a string to apply `pattern` constraint".to_string())?;
        let regex = Regex::new(pattern).map_err(|err| format!("invalid regex pattern: {err}"))?;
        if !regex.is_match(&string) {
            return Err(format!(
                "value `{string}` does not match pattern `{pattern}`"
            ));
        }
    }

    if let Some(enum_values) = &constraints.r#enum {
        let literal_json = literal_to_json(value)
            .ok_or_else(|| "value cannot be compared against enumeration entries".to_string())?;
        if !enum_values
            .iter()
            .any(|candidate| candidate == &literal_json)
        {
            return Err("value is not an allowed enumeration option".to_string());
        }
    }

    Ok(())
}

fn to_f64(value: &LiteralValue) -> Option<f64> {
    match value {
        LiteralValue::Number(n) => Some(*n),
        LiteralValue::Int(i) => Some(*i as f64),
        _ => None,
    }
}

fn to_string_value(value: &LiteralValue, value_type: ValueType) -> Option<String> {
    match (value_type, value) {
        (_, LiteralValue::Text(s)) => Some(s.clone()),
        (_, LiteralValue::Number(n)) => Some(n.to_string()),
        (_, LiteralValue::Int(i)) => Some(i.to_string()),
        (_, LiteralValue::Boolean(b)) => Some(b.to_string()),
        (ValueType::Date, LiteralValue::Date(d)) => Some(d.to_string()),
        (ValueType::Date, LiteralValue::DateTime(dt)) => Some(dt.date().to_string()),
        (ValueType::Datetime, LiteralValue::DateTime(dt)) => Some(dt.to_string()),
        _ => None,
    }
}

fn literal_to_json(value: &LiteralValue) -> Option<JsonValue> {
    match value {
        LiteralValue::Text(s) => Some(JsonValue::String(s.clone())),
        LiteralValue::Number(n) => serde_json::Number::from_f64(*n).map(JsonValue::Number),
        LiteralValue::Int(i) => Some(JsonValue::Number((*i).into())),
        LiteralValue::Boolean(b) => Some(JsonValue::Bool(*b)),
        LiteralValue::Date(d) => Some(JsonValue::String(d.to_string())),
        LiteralValue::DateTime(dt) => Some(JsonValue::String(dt.to_string())),
        _ => None,
    }
}

fn is_empty(value: &LiteralValue) -> bool {
    matches!(value, LiteralValue::Empty)
}

/// Coerce temporal/serial values to the manifest-declared type at the port boundary.
///
/// SheetPort is a versioned protocol: a port declared `number`/`integer` must keep
/// receiving numeric serials regardless of the engine's `TemporalEgress` policy, and a
/// port declared `date`/`datetime` must receive native temporals even when the engine
/// egresses raw serials. The manifest is the contract; engine egress is a default that
/// applies only where the manifest does not speak.
pub(crate) fn coerce_port_value_to_declared(
    binding: &PortBinding,
    value: PortValue,
    date_system: DateSystem,
) -> PortValue {
    match (&binding.kind, value) {
        (BoundPort::Scalar(scalar), PortValue::Scalar(v)) => PortValue::Scalar(
            coerce_literal_to_declared(scalar.value_type, v, date_system),
        ),
        (BoundPort::Record(record), PortValue::Record(mut map)) => {
            for (field_name, field_binding) in &record.fields {
                if let Some(v) = map.remove(field_name) {
                    map.insert(
                        field_name.clone(),
                        coerce_literal_to_declared(field_binding.value_type, v, date_system),
                    );
                }
            }
            PortValue::Record(map)
        }
        (BoundPort::Range(range), PortValue::Range(rows)) => PortValue::Range(
            rows.into_iter()
                .map(|row| {
                    row.into_iter()
                        .map(|v| coerce_literal_to_declared(range.cell_type, v, date_system))
                        .collect()
                })
                .collect(),
        ),
        (BoundPort::Table(table), PortValue::Table(mut tv)) => {
            for row in &mut tv.rows {
                for column in &table.columns {
                    if let Some(v) = row.values.remove(&column.name) {
                        row.values.insert(
                            column.name.clone(),
                            coerce_literal_to_declared(column.value_type, v, date_system),
                        );
                    }
                }
            }
            PortValue::Table(tv)
        }
        (_, value) => value,
    }
}

fn coerce_literal_to_declared(
    value_type: ValueType,
    value: LiteralValue,
    system: DateSystem,
) -> LiteralValue {
    use formualizer_common::{
        date_to_serial_for, datetime_to_serial_for, time_to_fraction, try_serial_to_date_for,
        try_serial_to_datetime_for,
    };
    match value_type {
        ValueType::Number | ValueType::Integer => match value {
            LiteralValue::Date(d) => LiteralValue::Number(date_to_serial_for(system, &d)),
            LiteralValue::DateTime(dt) => LiteralValue::Number(datetime_to_serial_for(system, &dt)),
            LiteralValue::Time(t) => LiteralValue::Number(time_to_fraction(&t)),
            LiteralValue::Duration(d) => LiteralValue::Number(
                d.num_nanoseconds()
                    .map(|n| n as f64 / (86_400.0 * 1_000_000_000.0))
                    .unwrap_or_else(|| d.num_seconds() as f64 / 86_400.0),
            ),
            other => other,
        },
        ValueType::Date => match value {
            LiteralValue::Number(n) => try_serial_to_date_for(system, n)
                .map(LiteralValue::Date)
                .unwrap_or(LiteralValue::Number(n)),
            other => other,
        },
        ValueType::Datetime => match value {
            LiteralValue::Number(n) => try_serial_to_datetime_for(system, n)
                .map(LiteralValue::DateTime)
                .unwrap_or(LiteralValue::Number(n)),
            other => other,
        },
        ValueType::String | ValueType::Boolean => value,
    }
}

#[cfg(test)]
mod coerce_tests {
    use super::*;
    use chrono::{NaiveDate, NaiveDateTime};

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).unwrap()
    }

    #[test]
    fn numeric_port_receives_serial_from_native_date() {
        let v = coerce_literal_to_declared(
            ValueType::Number,
            LiteralValue::Date(d(2026, 8, 20)),
            DateSystem::Excel1900,
        );
        let LiteralValue::Number(serial) = v else {
            panic!("expected serial, got {v:?}");
        };
        assert_eq!(
            formualizer_common::try_serial_to_date_for(DateSystem::Excel1900, serial).unwrap(),
            d(2026, 8, 20)
        );
    }

    #[test]
    fn numeric_port_receives_serial_from_native_datetime() {
        let dt = NaiveDateTime::new(
            d(2026, 8, 20),
            chrono::NaiveTime::from_hms_opt(6, 0, 0).unwrap(),
        );
        let v = coerce_literal_to_declared(
            ValueType::Number,
            LiteralValue::DateTime(dt),
            DateSystem::Excel1900,
        );
        let LiteralValue::Number(serial) = v else {
            panic!("expected serial, got {v:?}");
        };
        assert!(
            (serial.fract() - 0.25).abs() < 1e-9,
            "6:00 is a quarter day, got {serial}"
        );
    }

    #[test]
    fn date_port_receives_native_from_serial() {
        let serial = formualizer_common::date_to_serial_for(DateSystem::Excel1900, &d(2026, 8, 20));
        let v = coerce_literal_to_declared(
            ValueType::Date,
            LiteralValue::Number(serial),
            DateSystem::Excel1900,
        );
        assert_eq!(v, LiteralValue::Date(d(2026, 8, 20)));
    }

    #[test]
    fn non_temporal_values_pass_through_unchanged() {
        for vt in [ValueType::Number, ValueType::Date, ValueType::String] {
            assert_eq!(
                coerce_literal_to_declared(
                    vt,
                    LiteralValue::Text("x".into()),
                    DateSystem::Excel1900
                ),
                LiteralValue::Text("x".into())
            );
        }
        assert_eq!(
            coerce_literal_to_declared(
                ValueType::Number,
                LiteralValue::Int(3),
                DateSystem::Excel1900
            ),
            LiteralValue::Int(3)
        );
    }
}
