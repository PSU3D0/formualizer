//! Compatibility with the original inline error's serde representation.
#![cfg(feature = "serde")]

use formualizer_common::{
    ErrorContext, ExcelError, ExcelErrorExtra, ExcelErrorKind, PlanStaleReason,
    PreparationStaleReason, ResourceExhaustionDetail, ResourceExhaustionReason,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename = "ExcelError")]
struct InlineError {
    kind: ExcelErrorKind,
    message: Option<String>,
    context: Option<ErrorContext>,
    extra: ExcelErrorExtra,
}

#[test]
fn error_wire_format_is_unchanged_for_every_payload() {
    for extra in [
        ExcelErrorExtra::None,
        ExcelErrorExtra::Spill {
            expected_rows: 2,
            expected_cols: 3,
        },
        ExcelErrorExtra::Resource {
            detail: Box::new(ResourceExhaustionDetail {
                reason: ResourceExhaustionReason::ScratchMemory,
                limit: 42,
                observed: 43,
                request_id: Some(7),
            }),
        },
        ExcelErrorExtra::PreparationStale {
            reason: PreparationStaleReason::Graph,
        },
        ExcelErrorExtra::PlanStale {
            reason: PlanStaleReason::Provider,
        },
    ] {
        for error in [
            ExcelError::new_value().with_extra(extra.clone()),
            ExcelError::new_value()
                .with_message("")
                .with_extra(extra.clone()),
            ExcelError::new_value()
                .with_message("explanation")
                .with_location(1, 2)
                .with_origin(Some("Sheet1".into()), 3, 4)
                .with_extra(extra),
        ] {
            let inline = InlineError {
                kind: error.kind,
                message: error.message().map(str::to_owned),
                context: error.context().cloned(),
                extra: error.extra().clone(),
            };
            let encoded = serde_json::to_string(&error).unwrap();
            assert_eq!(encoded, serde_json::to_string(&inline).unwrap());
            assert_eq!(error, serde_json::from_str::<ExcelError>(&encoded).unwrap());
            assert_eq!(
                inline,
                serde_json::from_str::<InlineError>(&encoded).unwrap()
            );
            // Sequence serializers/deserializers must keep the original field order, too.
            let sequence =
                serde_json::json!([inline.kind, inline.message, inline.context, inline.extra]);
            assert_eq!(
                error,
                serde_json::from_value::<ExcelError>(sequence).unwrap()
            );
        }
    }
    assert_eq!(
        serde_json::to_string(&ExcelError::new_div()).unwrap(),
        r##"{"kind":"Div","message":null,"context":null,"extra":"None"}"##,
    );
    assert_eq!(
        serde_json::from_str::<ExcelError>(r##"{"kind":"Div","extra":"None"}"##).unwrap(),
        ExcelError::new_div(),
    );
}
