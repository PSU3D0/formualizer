//! Legacy reader raw-envelope decisions, independent of XML header parsing.
//! The scanner records rare exceptions; staging preserves the original phase order.
use super::*;

#[derive(Debug)]
pub(in crate::xlsx) enum Refusal {
    Coordinate(u8),
    CellType(Box<str>),
    SharedIndex(&'static str),
}
impl Refusal {
    pub fn error(&self) -> IoError {
        let error = match self {
            Self::Coordinate(byte) => crate::xlsx::types::XlsxError::Alphanumeric(*byte),
            Self::CellType(kind) => crate::xlsx::types::XlsxError::CellTAttribute(kind.to_string()),
            Self::SharedIndex(reason) => crate::xlsx::types::XlsxError::Unexpected(reason),
        };
        IoError::XlsxRead(crate::xlsx::types::XlsxReadError::Xlsx(error))
    }
}

#[derive(Debug)]
pub(in crate::xlsx) struct Evidence {
    pub shared: bool,
    pub refusal: Option<Refusal>,
}
impl Evidence {
    pub fn validate(&self) -> Result<(), IoError> {
        self.refusal
            .as_ref()
            .map_or(Ok(()), |error| Err(error.error()))
    }
}
fn coordinate(raw: &[u8]) -> Result<(), Refusal> {
    raw.iter()
        .find(|byte| !byte.is_ascii_alphanumeric())
        .map_or(Ok(()), |byte| Err(Refusal::Coordinate(*byte)))
}
fn raw<'a>(bytes: &'a [u8], span: &Option<Range<usize>>) -> Option<&'a [u8]> {
    span.as_ref().map(|span| &bytes[span.clone()])
}

/// Common envelopes need no retained allocation. Only a raw/decoded identity
/// mismatch or a deferred refusal is carried with a publication mark.
pub(in crate::xlsx) fn inspect(
    event: &events::Formula<'_>,
    bytes: &[u8],
    cache_readable: bool,
    cell_kind: Option<CellKind>,
) -> Option<Box<Evidence>> {
    let envelope = &event.envelope;
    let shared = raw(bytes, &envelope.raw_formula_type) == Some(b"shared");
    let decoded_shared = matches!(
        event.kind,
        events::FormulaKind::SharedAnchor { .. } | events::FormulaKind::SharedDescendant { .. }
    );
    let refusal = validate(event, bytes, shared, cache_readable, cell_kind).err();
    (shared != decoded_shared || refusal.is_some()).then(|| Box::new(Evidence { shared, refusal }))
}
fn validate(
    event: &events::Formula<'_>,
    bytes: &[u8],
    shared: bool,
    cache_readable: bool,
    cell_kind: Option<CellKind>,
) -> Result<(), Refusal> {
    let envelope = &event.envelope;
    if let Some(raw) = raw(bytes, &envelope.raw_coordinate) {
        coordinate(raw)?;
    }
    let cleared = !cache_readable || matches!(cell_kind.as_deref(), Some("s" | "inlineStr" | "d"));
    let kind_kept = !cleared || cell_kind.as_deref() == Some("n");
    let cache_error = if kind_kept && (cleared || envelope.raw_cache.is_some()) {
        raw(bytes, &envelope.raw_cell_type)
            .filter(|kind| {
                !matches!(
                    *kind,
                    b"s" | b"b" | b"d" | b"e" | b"str" | b"n" | b"inlineStr" | b"is"
                )
            })
            .map(|kind| {
                Refusal::CellType(String::from_utf8_lossy(kind).into_owned().into_boxed_str())
            })
    } else {
        None
    };
    let formula_result = if shared {
        (|| {
            let index = raw(bytes, &envelope.raw_shared_index).ok_or(Refusal::SharedIndex(
                "si attribute is mandatory if it is shared",
            ))?;
            if index.is_empty()
                || !index.iter().all(u8::is_ascii_digit)
                || std::str::from_utf8(index)
                    .ok()
                    .and_then(|n| n.parse::<usize>().ok())
                    .is_none()
            {
                return Err(Refusal::SharedIndex("si attribute must be a number"));
            }
            if let Some(reference) = raw(bytes, &envelope.raw_declared_range) {
                for coord in reference.split(|byte| *byte == b':') {
                    coordinate(coord)?;
                }
            }
            Ok(())
        })()
    } else {
        Ok(())
    };
    match (formula_result, cache_error) {
        (Err(_), Some(cache))
            if envelope
                .raw_cache
                .as_ref()
                .is_some_and(|span| span.start < envelope.formula.start) =>
        {
            Err(cache)
        }
        (Err(formula), _) => Err(formula),
        (Ok(()), Some(cache)) => Err(cache),
        (Ok(()), None) => Ok(()),
    }
}
