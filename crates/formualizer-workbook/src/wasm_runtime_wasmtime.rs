#![cfg(all(feature = "wasm_runtime_wasmtime", not(target_arch = "wasm32")))]

use crate::workbook::{WasmModuleManifest, WasmRuntimeHint, WasmUdfRuntime};
use formualizer_common::{
    LiteralValue,
    error::{ExcelError, ExcelErrorKind},
};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use wasmtime::{Engine, ExternType, Module, Val, ValType};

#[derive(Serialize)]
struct WasmInvokeRequest {
    codec_version: u32,
    args: Vec<LiteralValue>,
}

#[derive(Deserialize)]
struct WasmInvokeResponse {
    ok: bool,
    #[serde(default)]
    value: Option<LiteralValue>,
    #[serde(default)]
    error: Option<WasmInvokeError>,
}

#[derive(Deserialize)]
struct WasmInvokeError {
    kind: Option<ExcelErrorKind>,
    message: Option<String>,
}

#[derive(Default)]
pub(crate) struct WasmtimeWasmRuntime {
    engine: Engine,
    modules: RwLock<BTreeMap<String, Module>>,
}

impl WasmtimeWasmRuntime {
    fn get_module(&self, module_id: &str) -> Result<Module, ExcelError> {
        self.modules.read().get(module_id).cloned().ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name).with_message(format!(
                "WASM module {module_id} is not registered in runtime"
            ))
        })
    }

    fn coerce_arg(value: &LiteralValue, ty: &ValType) -> Result<Val, ExcelError> {
        fn as_f64(value: &LiteralValue) -> Option<f64> {
            match value {
                LiteralValue::Number(n) => Some(*n),
                LiteralValue::Int(i) => Some(*i as f64),
                LiteralValue::Boolean(b) => Some(if *b { 1.0 } else { 0.0 }),
                _ => None,
            }
        }

        match ty {
            ValType::I32 => {
                let n = as_f64(value).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Cannot coerce argument to i32")
                })?;
                if !n.is_finite() || n < i32::MIN as f64 || n > i32::MAX as f64 {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Argument out of i32 range"));
                }
                Ok(Val::I32(n as i32))
            }
            ValType::I64 => {
                let n = as_f64(value).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Cannot coerce argument to i64")
                })?;
                if !n.is_finite() || n < i64::MIN as f64 || n > i64::MAX as f64 {
                    return Err(ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Argument out of i64 range"));
                }
                Ok(Val::I64(n as i64))
            }
            ValType::F32 => {
                let n = as_f64(value).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Cannot coerce argument to f32")
                })?;
                Ok(Val::F32((n as f32).to_bits()))
            }
            ValType::F64 => {
                let n = as_f64(value).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("Cannot coerce argument to f64")
                })?;
                Ok(Val::F64(n.to_bits()))
            }
            _ => Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("Unsupported WASM argument type")),
        }
    }

    fn decode_result(val: &Val) -> Result<LiteralValue, ExcelError> {
        match val {
            Val::I32(v) => Ok(LiteralValue::Int(i64::from(*v))),
            Val::I64(v) => Ok(LiteralValue::Int(*v)),
            Val::F32(bits) => Ok(LiteralValue::Number(f32::from_bits(*bits) as f64)),
            Val::F64(bits) => Ok(LiteralValue::Number(f64::from_bits(*bits))),
            _ => {
                Err(ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message("Unsupported WASM result type"))
            }
        }
    }

    fn invoke_abi_json(
        &self,
        module_id: &str,
        export_name: &str,
        codec_version: u32,
        args: &[LiteralValue],
    ) -> Result<Option<LiteralValue>, ExcelError> {
        let module = self.get_module(module_id)?;
        let mut store = wasmtime::Store::new(&self.engine, ());
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("WASM instantiation failed: {err}"))
        })?;

        let memory = instance.get_memory(&mut store, "memory").ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::NImpl).with_message("Missing memory export")
        })?;

        let alloc = instance
            .get_typed_func::<i32, i32>(&mut store, "fz_alloc")
            .map_err(|_| {
                ExcelError::new(ExcelErrorKind::NImpl).with_message("Missing fz_alloc export")
            })?;

        let free = instance
            .get_typed_func::<(i32, i32), ()>(&mut store, "fz_free")
            .map_err(|_| {
                ExcelError::new(ExcelErrorKind::NImpl).with_message("Missing fz_free export")
            })?;

        let invoke = instance
            .get_typed_func::<(i32, i32), i64>(&mut store, export_name)
            .map_err(|_| {
                ExcelError::new(ExcelErrorKind::NImpl)
                    .with_message(format!("Missing typed ABI export: {export_name}"))
            })?;

        let request = serde_json::to_vec(&WasmInvokeRequest {
            codec_version,
            args: args.to_vec(),
        })
        .map_err(|err| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Failed to encode WASM invoke request: {err}"))
        })?;

        let req_len_i32 = i32::try_from(request.len()).map_err(|_| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message("WASM invoke request too large for i32 length")
        })?;

        let req_ptr = alloc.call(&mut store, req_len_i32).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Value).with_message(format!("WASM alloc failed: {err}"))
        })?;

        memory
            .write(
                &mut store,
                usize::try_from(req_ptr).map_err(|_| {
                    ExcelError::new(ExcelErrorKind::Value)
                        .with_message("WASM alloc returned negative pointer")
                })?,
                &request,
            )
            .map_err(|err| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Failed writing WASM request memory: {err}"))
            })?;

        let out = invoke
            .call(&mut store, (req_ptr, req_len_i32))
            .map_err(|err| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("WASM invoke trap: {err}"))
            })?;

        let out_ptr = (out as u64 & 0xFFFF_FFFF) as u32;
        let out_len = ((out as u64 >> 32) & 0xFFFF_FFFF) as u32;

        let mut response_bytes = vec![0u8; out_len as usize];
        memory
            .read(&store, out_ptr as usize, &mut response_bytes)
            .map_err(|err| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Failed reading WASM response memory: {err}"))
            })?;

        let _ = free.call(&mut store, (req_ptr, req_len_i32));
        let _ = free.call(
            &mut store,
            (
                i32::try_from(out_ptr).unwrap_or(i32::MAX),
                i32::try_from(out_len).unwrap_or(i32::MAX),
            ),
        );

        let response =
            serde_json::from_slice::<WasmInvokeResponse>(&response_bytes).map_err(|err| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("Failed decoding WASM response JSON: {err}"))
            })?;

        if response.ok {
            return Ok(response.value);
        }

        let error = response.error.unwrap_or(WasmInvokeError {
            kind: Some(ExcelErrorKind::Error),
            message: Some("Unknown WASM error".to_string()),
        });
        Err(
            ExcelError::new(error.kind.unwrap_or(ExcelErrorKind::Error)).with_message(
                error
                    .message
                    .unwrap_or_else(|| "WASM plugin error".to_string()),
            ),
        )
    }
}

impl WasmUdfRuntime for WasmtimeWasmRuntime {
    fn can_bind_functions(&self) -> bool {
        true
    }

    fn validate_module(
        &self,
        module_id: &str,
        wasm_bytes: &[u8],
        manifest: &WasmModuleManifest,
    ) -> Result<(), ExcelError> {
        let module = Module::new(&self.engine, wasm_bytes).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("Invalid WASM module for {module_id}: {err}"))
        })?;

        for function in &manifest.functions {
            let matches_export = module.exports().any(|export| {
                export.name() == function.export_name && matches!(export.ty(), ExternType::Func(_))
            });
            if !matches_export {
                return Err(ExcelError::new(ExcelErrorKind::Name).with_message(format!(
                    "WASM export {} is not present as a function in module {}",
                    function.export_name, module_id
                )));
            }
        }

        self.modules.write().insert(module_id.to_string(), module);
        Ok(())
    }

    fn invoke(
        &self,
        module_id: &str,
        export_name: &str,
        _function_name: &str,
        codec_version: u32,
        args: &[LiteralValue],
        _runtime_hint: Option<&WasmRuntimeHint>,
    ) -> Result<LiteralValue, ExcelError> {
        // Prefer ABI-style invocation first if available in module.
        match self.invoke_abi_json(module_id, export_name, codec_version, args) {
            Ok(Some(value)) => return Ok(value),
            Ok(None) => {}
            Err(err) if err.kind == ExcelErrorKind::NImpl => {}
            Err(err) => return Err(err),
        }

        let module = self.get_module(module_id)?;
        let mut store = wasmtime::Store::new(&self.engine, ());
        let instance = wasmtime::Instance::new(&mut store, &module, &[]).map_err(|err| {
            ExcelError::new(ExcelErrorKind::Value)
                .with_message(format!("WASM instantiation failed: {err}"))
        })?;

        let func = instance.get_func(&mut store, export_name).ok_or_else(|| {
            ExcelError::new(ExcelErrorKind::Name).with_message(format!(
                "WASM export {export_name} not found in module {module_id}"
            ))
        })?;

        let ty = func.ty(&store);
        let params_tys: Vec<ValType> = ty.params().collect();
        if params_tys.len() != args.len() {
            return Err(ExcelError::new(ExcelErrorKind::Value).with_message(format!(
                "WASM export {export_name} expects {} argument(s), got {}",
                params_tys.len(),
                args.len()
            )));
        }

        let params = params_tys
            .iter()
            .zip(args)
            .map(|(ty, value)| Self::coerce_arg(value, ty))
            .collect::<Result<Vec<_>, _>>()?;

        let results_tys: Vec<ValType> = ty.results().collect();
        let mut results = results_tys
            .iter()
            .map(|ty| {
                Val::default_for_ty(ty).ok_or_else(|| {
                    ExcelError::new(ExcelErrorKind::NImpl)
                        .with_message("Unsupported WASM result type")
                })
            })
            .collect::<Result<Vec<_>, _>>()?;

        func.call(&mut store, &params, &mut results)
            .map_err(|err| {
                ExcelError::new(ExcelErrorKind::Value)
                    .with_message(format!("WASM invoke trap: {err}"))
            })?;

        match results.len() {
            0 => Ok(LiteralValue::Empty),
            1 => Self::decode_result(&results[0]),
            _ => Err(ExcelError::new(ExcelErrorKind::NImpl)
                .with_message("WASM exports with multiple return values are not yet supported")),
        }
    }
}

pub(crate) fn new_wasmtime_runtime() -> WasmtimeWasmRuntime {
    WasmtimeWasmRuntime::default()
}
