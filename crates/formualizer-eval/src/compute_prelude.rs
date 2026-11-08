//! Arrow compute affordances restricted to the engine's stored lane types.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, BooleanArray, PrimitiveArray, StringArray, types::ArrowPrimitiveType};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, NullBufferBuilder};
use arrow_schema::ArrowError;

pub use arrow_array::ArrayRef;
pub use arrow_schema::DataType;
// The cast and filter exports have no internal callers; preserve their public API.
pub use arrow_cast::cast::cast_with_options;
pub use arrow_select::concat::concat as concat_arrays;
pub use arrow_select::filter::filter as filter_array;
pub use arrow_select::zip::zip as zip_select;

pub mod boolean {
    pub use arrow_arith::boolean::{and_kleene, not, or_kleene};
}

pub mod cmp {
    pub use arrow_ord::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
}

pub mod comparison {
    pub use arrow_string::like::{like, starts_with};
    pub use arrow_string::regexp::{regexp_is_match, regexp_is_match_scalar};
}

/// Temporal casting affordance retained for public source compatibility.
///
/// The engine does not call this helper; keep its existing semantics for callers
/// of the public compute prelude without adding a runtime engine cast path.
pub fn cast_temporal_if_needed(arr: &ArrayRef, target: &DataType) -> ArrayRef {
    use arrow_cast::cast::CastOptions;
    if matches!(
        target,
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(_, _)
    ) {
        cast_with_options(
            arr,
            target,
            &CastOptions {
                safe: false,
                format_options: Default::default(),
            },
        )
        .expect("temporal cast")
    } else {
        arr.clone()
    }
}

/// Concatenate engine lanes without Arrow's dictionary/interleave/take dispatch.
///
/// Supported lanes are `Float64`, `Boolean`, `Utf8`, `UInt8`, and `UInt32`.
/// Values and validity are copied in bulk, including non-byte-aligned bitmap
/// slices. Other Arrow types are deliberately unsupported: the store must lower
/// them to one of these five lane types first. General Arrow callers can keep
/// using [`concat_arrays`].
///
/// # Errors
/// Returns an error for empty input, mismatched or unsupported lane types, or
/// an overflowing total length / UTF-8 byte offset.
pub(crate) fn concat_engine_arrays(arrays: &[&dyn Array]) -> Result<ArrayRef, ArrowError> {
    let first = arrays.first().ok_or_else(|| {
        ArrowError::InvalidArgumentError("concat requires at least one array".into())
    })?;
    let data_type = first.data_type();
    let len = arrays.iter().try_fold(0usize, |len, array| {
        if array.data_type() != data_type {
            return Err(ArrowError::InvalidArgumentError(
                "concat requires arrays of the same type".into(),
            ));
        }
        len.checked_add(array.len())
            .ok_or_else(|| ArrowError::ComputeError("concat length overflow".into()))
    })?;
    if !matches!(
        data_type,
        DataType::Float64 | DataType::Boolean | DataType::Utf8 | DataType::UInt8 | DataType::UInt32
    ) {
        return Err(ArrowError::InvalidArgumentError(format!(
            "unsupported engine concat type: {data_type}"
        )));
    }
    if arrays.len() == 1 {
        return Ok(first.slice(0, len));
    }
    if *data_type == DataType::Utf8 {
        return concat_strings(arrays, len);
    }
    let nulls = concat_nulls(arrays, len);
    match data_type {
        DataType::Float64 => Ok(concat_primitives::<arrow_array::types::Float64Type>(
            arrays, len, nulls,
        )),
        DataType::UInt8 => Ok(concat_primitives::<arrow_array::types::UInt8Type>(
            arrays, len, nulls,
        )),
        DataType::UInt32 => Ok(concat_primitives::<arrow_array::types::UInt32Type>(
            arrays, len, nulls,
        )),
        DataType::Boolean => {
            let mut values = BooleanBufferBuilder::new(len);
            for array in arrays {
                let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                values.append_buffer(array.values());
            }
            Ok(Arc::new(BooleanArray::new(values.finish(), nulls)))
        }
        _ => unreachable!("validated engine lane type"),
    }
}

fn concat_nulls(arrays: &[&dyn Array], len: usize) -> Option<NullBuffer> {
    if arrays.iter().all(|array| array.null_count() == 0) {
        return None;
    }
    let mut nulls = NullBufferBuilder::new(len);
    for array in arrays {
        match array.nulls() {
            Some(buffer) => nulls.append_buffer(buffer),
            None => nulls.append_n_non_nulls(array.len()),
        }
    }
    nulls.finish()
}

fn concat_primitives<T: ArrowPrimitiveType>(
    arrays: &[&dyn Array],
    len: usize,
    nulls: Option<NullBuffer>,
) -> ArrayRef {
    let mut values = Vec::with_capacity(len);
    for array in arrays {
        let array = array.as_any().downcast_ref::<PrimitiveArray<T>>().unwrap();
        values.extend_from_slice(array.values());
    }
    Arc::new(PrimitiveArray::<T>::new(values.into(), nulls))
}

fn concat_strings(arrays: &[&dyn Array], len: usize) -> Result<ArrayRef, ArrowError> {
    // Only copy the byte ranges covered by the slices, not their backing arrays.
    let byte_len = arrays
        .iter()
        .try_fold(0usize, |total, array| {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            let offsets = array.value_offsets();
            total.checked_add((offsets[array.len()] - offsets[0]) as usize)
        })
        .filter(|len| *len <= i32::MAX as usize)
        .ok_or_else(|| ArrowError::ComputeError("concat Utf8 offset overflow".into()))?;
    // The typed builder rebases offsets and bulk-copies bytes/validity without
    // rescanning already valid UTF-8 or entering generic compute dispatch.
    let mut builder = StringBuilder::with_capacity(len, byte_len);
    for array in arrays {
        builder.append_array(array.as_any().downcast_ref::<StringArray>().unwrap())?;
    }
    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::concat_engine_arrays as concat_arrays;
    use super::*;
    use arrow_array::{Float64Array, UInt8Array, UInt32Array};
    use arrow_buffer::OffsetBuffer;
    use proptest::prelude::*;

    fn assert_oracle(chunks: &[ArrayRef]) {
        let arrays: Vec<&dyn Array> = chunks.iter().map(|array| array.as_ref()).collect();
        let actual = concat_arrays(&arrays).unwrap();
        let expected = arrow_select::concat::concat(&arrays).unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
    }

    // Exercise independently sliced input buffers, empty chunks, null runs, and
    // unaligned bitmap offsets. Chunk boundaries differ from byte boundaries.
    fn sliced_chunks(array: ArrayRef, offset: usize, width: usize) -> Vec<ArrayRef> {
        let end = array.len();
        vec![
            array.slice(offset, width),
            array.slice(end, 0),
            array.slice(0, offset),
            array.slice(offset + width, end - offset - width),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(128))]
        #[test]
        fn typed_concat_matches_arrow(
            values in prop::collection::vec(prop::option::of(any::<u32>()), 80..180),
            offset in 1usize..16,
            width in 0usize..64,
        ) {
            let floats = Float64Array::from(values.iter().map(|v| v.map(|n| (n as f64) / 7.0)).collect::<Vec<_>>());
            let booleans = BooleanArray::from(values.iter().map(|v| v.map(|n| n % 3 == 0)).collect::<Vec<_>>());
            let bytes = UInt8Array::from(values.iter().map(|v| v.map(|n| n as u8)).collect::<Vec<_>>());
            let integers = UInt32Array::from(values.clone());
            let text: Vec<Option<String>> = values.iter().map(|v| v.map(|n| match n % 4 {
                0 => String::new(),
                1 => format!("é🦀{n}"),
                _ => n.to_string(),
            })).collect();
            let strings = StringArray::from(text);
            for array in [
                Arc::new(floats) as ArrayRef,
                Arc::new(booleans),
                Arc::new(bytes),
                Arc::new(integers),
                Arc::new(strings),
            ] {
                assert_oracle(&sliced_chunks(array, offset, width));
            }
        }
    }

    #[test]
    fn public_concat_retains_support_for_non_engine_types() {
        let values = arrow_array::Int16Array::from(vec![Some(7), None, Some(-3)]);
        let first = values.slice(0, 1);
        let second = values.slice(1, 2);
        let chunks = [&first as &dyn Array, &second as &dyn Array];
        let actual = super::concat_arrays(&chunks).unwrap();
        assert_eq!(actual.to_data(), values.to_data());
        assert!(super::concat_engine_arrays(&chunks).is_err());
    }

    #[test]
    fn empty_single_and_all_null_lanes_match_arrow() {
        for data_type in [
            DataType::Float64,
            DataType::Boolean,
            DataType::UInt8,
            DataType::UInt32,
            DataType::Utf8,
        ] {
            let nulls = arrow_array::new_null_array(&data_type, 19);
            let empty = arrow_array::new_empty_array(&data_type);
            assert_oracle(&[empty.clone(), empty.clone()]);
            assert_oracle(&[nulls.slice(3, 13)]);
            assert_oracle(&[empty.clone(), nulls.slice(1, 9), nulls.slice(2, 7), empty]);
        }
    }

    #[test]
    fn non_null_and_mixed_validity_match_arrow() {
        for data_type in [
            DataType::Float64,
            DataType::Boolean,
            DataType::UInt8,
            DataType::UInt32,
            DataType::Utf8,
        ] {
            let valid: ArrayRef = match data_type {
                DataType::Float64 => Arc::new(Float64Array::from(vec![1.0; 19])),
                DataType::Boolean => Arc::new(BooleanArray::from(vec![true; 19])),
                DataType::UInt8 => Arc::new(UInt8Array::from(vec![7; 19])),
                DataType::UInt32 => Arc::new(UInt32Array::from(vec![99; 19])),
                _ => Arc::new(StringArray::from(vec!["é🦀"; 19])),
            };
            assert_oracle(&[valid.slice(1, 7), valid.slice(3, 9)]);
            assert_oracle(&[
                valid.slice(1, 7),
                arrow_array::new_null_array(&data_type, 9),
                valid.slice(3, 9),
            ]);
        }
    }

    #[test]
    fn string_slices_preserve_null_payload_and_rebase_offsets() {
        let array: ArrayRef = Arc::new(StringArray::new(
            OffsetBuffer::new(vec![0i32, 6, 10, 15, 15, 21].into()),
            b"prefixNULLhelloSUFFIX".to_vec().into(),
            Some(NullBuffer::from(vec![true, false, true, true, true])),
        ));
        assert_oracle(&[array.slice(1, 3), array.slice(2, 2), array.slice(5, 0)]);
        let single = concat_arrays(&[array.as_ref()]).unwrap();
        let original = array.as_any().downcast_ref::<StringArray>().unwrap();
        let single = single.as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(original.value_data().as_ptr(), single.value_data().as_ptr());
    }

    #[test]
    fn preserves_float_bits() {
        let bits = [
            0x7ff8000000000123,
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
        ];
        let array = Float64Array::from(bits.map(f64::from_bits).to_vec());
        let actual = concat_arrays(&[&array.slice(1, 3), &array.slice(0, 1)]).unwrap();
        let actual = actual.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(
            actual
                .values()
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>(),
            vec![bits[1], bits[2], bits[3], bits[0]]
        );
    }

    #[test]
    fn rejects_empty_mixed_and_unsupported_types() {
        assert!(concat_arrays(&[]).is_err());
        assert!(
            concat_arrays(&[&Float64Array::from(vec![1.0]), &UInt8Array::from(vec![1])]).is_err()
        );
        let unsupported = arrow_array::Int64Array::from(vec![1]);
        assert!(concat_arrays(&[&unsupported]).is_err());
        assert!(concat_arrays(&[&unsupported, &unsupported]).is_err());
    }
}
