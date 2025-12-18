use crate::workbook::{Workbook, js_to_literal, literal_to_js};
use formualizer_common::{LiteralValue, RangeAddress};
use formualizer_sheetport::{
    BoundPort, ConstraintViolation, EvalOptions, ManifestBindings, PortBinding, PortValue,
    TableBinding, TableRow, TableValue,
};
use sheetport_spec::{Direction, Manifest, ManifestIssue};
use std::collections::{BTreeMap, BTreeSet};
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct SheetPortSession {
    workbook: Workbook,
    bindings: ManifestBindings,
}

#[wasm_bindgen]
impl SheetPortSession {
    #[wasm_bindgen(js_name = "fromManifestYaml")]
    pub fn from_manifest_yaml(
        manifest_yaml: String,
        workbook: &Workbook,
    ) -> Result<SheetPortSession, JsValue> {
        let manifest = Manifest::from_yaml_str(&manifest_yaml)
            .map_err(|err| js_error(format!("manifest parse failed: {err}")))?;
        SheetPortSession::from_manifest(manifest, workbook)
    }

    #[wasm_bindgen(js_name = "manifest")]
    pub fn manifest(&self) -> Result<JsValue, JsValue> {
        serde_wasm_bindgen::to_value(self.bindings.manifest())
            .map_err(|err| js_error(format!("manifest serialization failed: {err}")))
    }

    #[wasm_bindgen(js_name = "describePorts")]
    pub fn describe_ports(&self) -> Result<JsValue, JsValue> {
        let list = js_sys::Array::new();
        for binding in self.bindings.bindings() {
            let entry = js_sys::Object::new();
            set(&entry, "id", JsValue::from_str(&binding.id))?;
            set(
                &entry,
                "direction",
                JsValue::from_str(match binding.direction {
                    Direction::In => "in",
                    Direction::Out => "out",
                }),
            )?;
            set(&entry, "required", JsValue::from_bool(binding.required))?;
            set(
                &entry,
                "partitionKey",
                JsValue::from_bool(binding.partition_key),
            )?;
            if let Some(desc) = &binding.description {
                set(&entry, "description", JsValue::from_str(desc))?;
            }
            set(
                &entry,
                "shape",
                JsValue::from_str(match binding.kind {
                    BoundPort::Scalar(_) => "scalar",
                    BoundPort::Record(_) => "record",
                    BoundPort::Range(_) => "range",
                    BoundPort::Table(_) => "table",
                }),
            )?;
            if let Some(constraints) = &binding.constraints {
                let value = serde_wasm_bindgen::to_value(constraints)
                    .map_err(|err| js_error(format!("constraint serialization failed: {err}")))?;
                set(&entry, "constraints", value)?;
            }
            if let Some(units) = &binding.units {
                let value = serde_wasm_bindgen::to_value(units)
                    .map_err(|err| js_error(format!("units serialization failed: {err}")))?;
                set(&entry, "units", value)?;
            }
            if let Some(default) = &binding.resolved_default {
                let value = port_value_to_js(default)?;
                set(&entry, "default", value)?;
            } else if let Some(raw) = &binding.default {
                let value = serde_wasm_bindgen::to_value(raw)
                    .map_err(|err| js_error(format!("default serialization failed: {err}")))?;
                set(&entry, "default", value)?;
            }
            set(&entry, "location", location_summary(binding)?)?;
            list.push(&entry);
        }
        Ok(list.into())
    }

    #[wasm_bindgen(js_name = "readInputs")]
    pub fn read_inputs(&mut self) -> Result<JsValue, JsValue> {
        let snapshot = self.with_sheetport(|sheetport| sheetport.read_inputs())?;
        snapshot_to_js(snapshot.inner())
    }

    #[wasm_bindgen(js_name = "readOutputs")]
    pub fn read_outputs(&mut self) -> Result<JsValue, JsValue> {
        let snapshot = self.with_sheetport(|sheetport| sheetport.read_outputs())?;
        snapshot_to_js(snapshot.inner())
    }

    #[wasm_bindgen(js_name = "writeInputs")]
    pub fn write_inputs(&mut self, updates: JsValue) -> Result<(), JsValue> {
        let update = js_to_input_update(&self.bindings, &updates)?;
        self.with_sheetport(move |sheetport| sheetport.write_inputs(update))
            .map(|_| ())
    }

    #[wasm_bindgen(js_name = "evaluateOnce")]
    pub fn evaluate_once(&mut self, options: JsValue) -> Result<JsValue, JsValue> {
        let eval_options = parse_eval_options(options)?;
        let outputs =
            self.with_sheetport(move |sheetport| sheetport.evaluate_once(eval_options))?;
        snapshot_to_js(outputs.inner())
    }
}

impl SheetPortSession {
    fn from_manifest(manifest: Manifest, workbook: &Workbook) -> Result<Self, JsValue> {
        let bindings = bind_manifest(workbook, manifest)?;
        Ok(Self {
            workbook: workbook.clone(),
            bindings,
        })
    }

    fn with_sheetport<F, T>(&mut self, f: F) -> Result<T, JsValue>
    where
        F: FnOnce(
            &mut formualizer_sheetport::SheetPort,
        ) -> Result<T, formualizer_sheetport::SheetPortError>,
    {
        let arc = self.workbook.inner_arc();
        let mut guard = arc
            .write()
            .map_err(|_| js_error("failed to lock workbook"))?;
        let bindings_clone = self.bindings.clone();
        let mut sheetport =
            formualizer_sheetport::SheetPort::from_bindings(&mut guard, bindings_clone)
                .map_err(sheetport_error_to_js)?;
        match f(&mut sheetport) {
            Ok(value) => {
                let (_, bindings) = sheetport.into_parts();
                self.bindings = bindings;
                Ok(value)
            }
            Err(err) => Err(sheetport_error_to_js(err)),
        }
    }
}

fn bind_manifest(workbook: &Workbook, manifest: Manifest) -> Result<ManifestBindings, JsValue> {
    let arc = workbook.inner_arc();
    let mut guard = arc
        .write()
        .map_err(|_| js_error("failed to lock workbook"))?;
    let sheetport = formualizer_sheetport::SheetPort::new(&mut guard, manifest)
        .map_err(sheetport_error_to_js)?;
    let (_, bindings) = sheetport.into_parts();
    Ok(bindings)
}

fn snapshot_to_js(map: &BTreeMap<String, PortValue>) -> Result<JsValue, JsValue> {
    let obj = js_sys::Object::new();
    for (port_id, value) in map {
        let js = port_value_to_js(value)?;
        set(&obj, port_id, js)?;
    }
    Ok(obj.into())
}

fn port_value_to_js(value: &PortValue) -> Result<JsValue, JsValue> {
    Ok(match value {
        PortValue::Scalar(literal) => literal_to_js(literal.clone()),
        PortValue::Record(fields) => {
            let obj = js_sys::Object::new();
            for (field, literal) in fields {
                set(&obj, field, literal_to_js(literal.clone()))?;
            }
            obj.into()
        }
        PortValue::Range(rows) => {
            let outer = js_sys::Array::new();
            for row in rows {
                let arr = js_sys::Array::new();
                for cell in row {
                    arr.push(&literal_to_js(cell.clone()));
                }
                outer.push(&arr);
            }
            outer.into()
        }
        PortValue::Table(table) => table_value_to_js(table)?,
    })
}

fn table_value_to_js(table: &TableValue) -> Result<JsValue, JsValue> {
    let outer = js_sys::Array::new();
    for row in &table.rows {
        let obj = js_sys::Object::new();
        for (column, literal) in &row.values {
            set(&obj, column, literal_to_js(literal.clone()))?;
        }
        outer.push(&obj);
    }
    Ok(outer.into())
}

fn js_to_input_update(
    bindings: &ManifestBindings,
    value: &JsValue,
) -> Result<formualizer_sheetport::InputUpdate, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(formualizer_sheetport::InputUpdate::default());
    }
    let obj = value
        .clone()
        .dyn_into::<js_sys::Object>()
        .map_err(|_| js_error("input updates must be an object"))?;
    let keys = js_sys::Object::keys(&obj);
    let mut update = formualizer_sheetport::InputUpdate::default();
    for key in keys.iter() {
        let port = key
            .as_string()
            .ok_or_else(|| js_error("input update keys must be strings"))?;
        let binding = bindings
            .get(&port)
            .ok_or_else(|| js_error(format!("unknown port `{port}`")))?;
        let js_value = js_sys::Reflect::get(&obj, &JsValue::from_str(&port))
            .map_err(|err| js_error(format!("failed to read update for `{port}`: {err:?}`")))?;
        let port_value = js_to_port_value(binding, &js_value)?;
        update.insert(port, port_value);
    }
    Ok(update)
}

fn js_to_port_value(binding: &PortBinding, value: &JsValue) -> Result<PortValue, JsValue> {
    if value.is_null() || value.is_undefined() {
        return Ok(match &binding.kind {
            BoundPort::Scalar(_) => PortValue::Scalar(LiteralValue::Empty),
            BoundPort::Record(_) => PortValue::Record(BTreeMap::new()),
            BoundPort::Range(_) => PortValue::Range(Vec::new()),
            BoundPort::Table(_) => PortValue::Table(TableValue::default()),
        });
    }

    match &binding.kind {
        BoundPort::Scalar(_) => Ok(PortValue::Scalar(js_to_literal(value))),
        BoundPort::Record(record) => {
            let obj = value
                .clone()
                .dyn_into::<js_sys::Object>()
                .map_err(|_| js_error("record inputs must be objects"))?;
            let keys = js_sys::Object::keys(&obj);
            let mut map = BTreeMap::new();
            for key in keys.iter() {
                let field = key
                    .as_string()
                    .ok_or_else(|| js_error("record field names must be strings"))?;
                if !record.fields.contains_key(&field) {
                    return Err(js_error(format!(
                        "record update references unknown field `{field}`"
                    )));
                }
                let js_value =
                    js_sys::Reflect::get(&obj, &JsValue::from_str(&field)).map_err(|err| {
                        js_error(format!("failed to read record field `{field}`: {err:?}"))
                    })?;
                map.insert(field, js_to_literal(&js_value));
            }
            Ok(PortValue::Record(map))
        }
        BoundPort::Range(_) => {
            let outer = value
                .clone()
                .dyn_into::<js_sys::Array>()
                .map_err(|_| js_error("range inputs must be arrays of rows"))?;
            let mut rows = Vec::with_capacity(outer.length() as usize);
            let mut expected_width: Option<usize> = None;
            for (idx, row_value) in outer.iter().enumerate() {
                let row_arr = row_value
                    .dyn_into::<js_sys::Array>()
                    .map_err(|_| js_error(format!("range row {} must be an array", idx + 1)))?;
                let mut row: Vec<LiteralValue> = Vec::with_capacity(row_arr.length() as usize);
                for cell in row_arr.iter() {
                    row.push(js_to_literal(&cell));
                }
                if let Some(width) = expected_width {
                    if width != row.len() {
                        return Err(js_error(format!(
                            "range rows must be rectangular (row {} has {}, expected {})",
                            idx + 1,
                            row.len(),
                            width
                        )));
                    }
                } else {
                    expected_width = Some(row.len());
                }
                rows.push(row);
            }
            Ok(PortValue::Range(rows))
        }
        BoundPort::Table(table) => Ok(PortValue::Table(js_to_table_value(table, value)?)),
    }
}

fn js_to_table_value(table: &TableBinding, value: &JsValue) -> Result<TableValue, JsValue> {
    let rows = value
        .clone()
        .dyn_into::<js_sys::Array>()
        .map_err(|_| js_error("table inputs must be arrays of row objects"))?;
    let mut collected = Vec::with_capacity(rows.length() as usize);
    let allowed: BTreeSet<&str> = table.columns.iter().map(|col| col.name.as_str()).collect();
    for (idx, row_value) in rows.iter().enumerate() {
        let object = row_value
            .dyn_into::<js_sys::Object>()
            .map_err(|_| js_error(format!("table row {} must be an object", idx + 1)))?;
        let keys = js_sys::Object::keys(&object);
        let mut map = BTreeMap::new();
        for key in keys.iter() {
            let column = key
                .as_string()
                .ok_or_else(|| js_error("table column names must be strings"))?;
            if !allowed.contains(column.as_str()) {
                return Err(js_error(format!(
                    "table update references unknown column `{column}`"
                )));
            }
            let cell = js_sys::Reflect::get(&object, &JsValue::from_str(&column))
                .map_err(|err| js_error(format!("failed to read column `{column}`: {err:?}")))?;
            map.insert(column, js_to_literal(&cell));
        }
        collected.push(TableRow::new(map));
    }
    Ok(TableValue::new(collected))
}

fn location_summary(binding: &PortBinding) -> Result<JsValue, JsValue> {
    let obj = js_sys::Object::new();
    match &binding.kind {
        BoundPort::Scalar(scalar) => {
            set(&obj, "kind", JsValue::from_str("scalar"))?;
            set(&obj, "selector", scalar_location_to_js(&scalar.location)?)?;
        }
        BoundPort::Record(record) => {
            set(&obj, "kind", JsValue::from_str("record"))?;
            set(&obj, "selector", area_location_to_js(&record.location)?)?;
        }
        BoundPort::Range(range) => {
            set(&obj, "kind", JsValue::from_str("range"))?;
            set(&obj, "selector", area_location_to_js(&range.location)?)?;
        }
        BoundPort::Table(table) => {
            set(&obj, "kind", JsValue::from_str("table"))?;
            set(&obj, "selector", table_location_to_js(&table.location)?)?;
            let columns = js_sys::Array::new();
            for column in &table.columns {
                let col = js_sys::Object::new();
                set(&col, "name", JsValue::from_str(&column.name))?;
                set(
                    &col,
                    "valueType",
                    JsValue::from_str(&format!("{:?}", column.value_type).to_lowercase()),
                )?;
                if let Some(hint) = &column.column_hint {
                    set(&col, "column", JsValue::from_str(hint))?;
                }
                if let Some(format) = &column.format {
                    set(&col, "format", JsValue::from_str(format))?;
                }
                if let Some(units) = &column.units {
                    let value = serde_wasm_bindgen::to_value(units)
                        .map_err(|err| js_error(format!("units serialization failed: {err}")))?;
                    set(&col, "units", value)?;
                }
                columns.push(&col);
            }
            if !table.keys.is_empty() {
                let keys = js_sys::Array::new();
                for key in &table.keys {
                    keys.push(&JsValue::from_str(key));
                }
                set(&obj, "keys", keys.into())?;
            }
            set(&obj, "columns", columns.into())?;
        }
    }
    Ok(obj.into())
}

fn scalar_location_to_js(
    location: &formualizer_sheetport::ScalarLocation,
) -> Result<JsValue, JsValue> {
    match location {
        formualizer_sheetport::ScalarLocation::Cell(addr) => Ok(range_address_to_js(addr)),
        formualizer_sheetport::ScalarLocation::Name(name) => {
            let obj = js_sys::Object::new();
            set(&obj, "name", JsValue::from_str(name))?;
            Ok(obj.into())
        }
        formualizer_sheetport::ScalarLocation::StructRef(reference) => {
            let obj = js_sys::Object::new();
            set(&obj, "structRef", JsValue::from_str(reference))?;
            Ok(obj.into())
        }
    }
}

fn area_location_to_js(location: &formualizer_sheetport::AreaLocation) -> Result<JsValue, JsValue> {
    match location {
        formualizer_sheetport::AreaLocation::Range(addr) => Ok(range_address_to_js(addr)),
        formualizer_sheetport::AreaLocation::Name(name) => {
            let obj = js_sys::Object::new();
            set(&obj, "name", JsValue::from_str(name))?;
            Ok(obj.into())
        }
        formualizer_sheetport::AreaLocation::StructRef(reference) => {
            let obj = js_sys::Object::new();
            set(&obj, "structRef", JsValue::from_str(reference))?;
            Ok(obj.into())
        }
        formualizer_sheetport::AreaLocation::Layout(layout) => serde_wasm_bindgen::to_value(layout)
            .map_err(|err| js_error(format!("layout serialization failed: {err}"))),
    }
}

fn table_location_to_js(
    location: &formualizer_sheetport::TableLocation,
) -> Result<JsValue, JsValue> {
    match location {
        formualizer_sheetport::TableLocation::Table(selector) => {
            serde_wasm_bindgen::to_value(selector)
                .map_err(|err| js_error(format!("table selector serialization failed: {err}")))
        }
        formualizer_sheetport::TableLocation::Layout(layout) => {
            serde_wasm_bindgen::to_value(layout)
                .map_err(|err| js_error(format!("layout serialization failed: {err}")))
        }
    }
}

fn range_address_to_js(addr: &RangeAddress) -> JsValue {
    let obj = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("sheet"),
        &JsValue::from_str(&addr.sheet),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("startRow"),
        &JsValue::from_f64(addr.start_row as f64),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("startCol"),
        &JsValue::from_f64(addr.start_col as f64),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("endRow"),
        &JsValue::from_f64(addr.end_row as f64),
    );
    let _ = js_sys::Reflect::set(
        &obj,
        &JsValue::from_str("endCol"),
        &JsValue::from_f64(addr.end_col as f64),
    );
    obj.into()
}

fn parse_eval_options(options: JsValue) -> Result<EvalOptions, JsValue> {
    let mut eval = EvalOptions::default();
    if options.is_null() || options.is_undefined() {
        return Ok(eval);
    }
    let obj = options
        .clone()
        .dyn_into::<js_sys::Object>()
        .map_err(|_| js_error("evaluateOnce options must be an object"))?;
    if let Some(value) = get_optional_bool(&obj, "freezeVolatile")? {
        eval.freeze_volatile = value;
    }
    if let Some(value) = get_optional_number(&obj, "rngSeed")? {
        if value < 0.0 {
            return Err(js_error("rngSeed must be non-negative"));
        }
        eval.rng_seed = Some(value as u64);
    }
    Ok(eval)
}

fn get_optional_bool(obj: &js_sys::Object, key: &str) -> Result<Option<bool>, JsValue> {
    let value = js_sys::Reflect::get(obj, &JsValue::from_str(key))
        .map_err(|err| js_error(format!("failed to read `{key}`: {err:?}")))?;
    if value.is_undefined() || value.is_null() {
        Ok(None)
    } else {
        Ok(Some(value.as_bool().ok_or_else(|| {
            js_error(format!("`{key}` must be a boolean"))
        })?))
    }
}

fn get_optional_number(obj: &js_sys::Object, key: &str) -> Result<Option<f64>, JsValue> {
    let value = js_sys::Reflect::get(obj, &JsValue::from_str(key))
        .map_err(|err| js_error(format!("failed to read `{key}`: {err:?}")))?;
    if value.is_undefined() || value.is_null() {
        Ok(None)
    } else {
        Ok(Some(value.as_f64().ok_or_else(|| {
            js_error(format!("`{key}` must be a number"))
        })?))
    }
}

fn set(target: &js_sys::Object, key: impl AsRef<str>, value: JsValue) -> Result<(), JsValue> {
    js_sys::Reflect::set(target, &JsValue::from_str(key.as_ref()), &value)
        .map(|_| ())
        .map_err(|err| js_error(format!("failed to set `{}`: {err:?}", key.as_ref())))
}

fn js_error(message: impl AsRef<str>) -> JsValue {
    JsValue::from(js_sys::Error::new(message.as_ref()))
}

fn sheetport_error_to_js(err: formualizer_sheetport::SheetPortError) -> JsValue {
    let error = js_sys::Error::new(&err.to_string());
    let object = error.unchecked_ref::<js_sys::Object>();
    let kind = match &err {
        formualizer_sheetport::SheetPortError::InvalidManifest { .. } => "InvalidManifest",
        formualizer_sheetport::SheetPortError::UnsupportedSelector { .. } => "UnsupportedSelector",
        formualizer_sheetport::SheetPortError::InvalidReference { .. } => "InvalidReference",
        formualizer_sheetport::SheetPortError::MissingSheet { .. } => "MissingSheet",
        formualizer_sheetport::SheetPortError::InvariantViolation { .. } => "InvariantViolation",
        formualizer_sheetport::SheetPortError::ConstraintViolation { .. } => "ConstraintViolation",
        formualizer_sheetport::SheetPortError::Engine { .. } => "Engine",
        formualizer_sheetport::SheetPortError::Workbook { .. } => "Workbook",
    };
    let _ = js_sys::Reflect::set(object, &JsValue::from_str("kind"), &JsValue::from_str(kind));
    match err {
        formualizer_sheetport::SheetPortError::InvalidManifest { issues } => {
            let list = js_sys::Array::new();
            for ManifestIssue { path, message } in issues {
                let entry = js_sys::Object::new();
                let _ = js_sys::Reflect::set(
                    &entry,
                    &JsValue::from_str("path"),
                    &JsValue::from_str(&path),
                );
                let _ = js_sys::Reflect::set(
                    &entry,
                    &JsValue::from_str("message"),
                    &JsValue::from_str(&message),
                );
                list.push(&entry);
            }
            let _ = js_sys::Reflect::set(object, &JsValue::from_str("issues"), &list.into());
        }
        formualizer_sheetport::SheetPortError::ConstraintViolation { violations } => {
            let list = js_sys::Array::new();
            for violation in violations {
                list.push(&constraint_violation_to_js(&violation));
            }
            let _ = js_sys::Reflect::set(object, &JsValue::from_str("violations"), &list.into());
        }
        formualizer_sheetport::SheetPortError::UnsupportedSelector { port, reason } => {
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("port"),
                &JsValue::from_str(&port),
            );
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("reason"),
                &JsValue::from_str(&reason),
            );
        }
        formualizer_sheetport::SheetPortError::InvalidReference {
            port,
            reference,
            details,
        } => {
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("port"),
                &JsValue::from_str(&port),
            );
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("reference"),
                &JsValue::from_str(&reference),
            );
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("details"),
                &JsValue::from_str(&details),
            );
        }
        formualizer_sheetport::SheetPortError::MissingSheet { port, sheet } => {
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("port"),
                &JsValue::from_str(&port),
            );
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("sheet"),
                &JsValue::from_str(&sheet),
            );
        }
        formualizer_sheetport::SheetPortError::InvariantViolation { port, message } => {
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("port"),
                &JsValue::from_str(&port),
            );
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("details"),
                &JsValue::from_str(&message),
            );
        }
        formualizer_sheetport::SheetPortError::Engine { source } => {
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("source"),
                &JsValue::from_str(&source.to_string()),
            );
        }
        formualizer_sheetport::SheetPortError::Workbook { source } => {
            let _ = js_sys::Reflect::set(
                object,
                &JsValue::from_str("source"),
                &JsValue::from_str(&source.to_string()),
            );
        }
    }
    error.into()
}

fn constraint_violation_to_js(violation: &ConstraintViolation) -> JsValue {
    let entry = js_sys::Object::new();
    let _ = js_sys::Reflect::set(
        &entry,
        &JsValue::from_str("port"),
        &JsValue::from_str(&violation.port),
    );
    let _ = js_sys::Reflect::set(
        &entry,
        &JsValue::from_str("path"),
        &JsValue::from_str(&violation.path),
    );
    let _ = js_sys::Reflect::set(
        &entry,
        &JsValue::from_str("message"),
        &JsValue::from_str(&violation.message),
    );
    entry.into()
}
