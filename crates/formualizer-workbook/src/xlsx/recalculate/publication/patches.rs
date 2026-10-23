//! Non-overlapping source-byte edits and bounded output assembly.
//! Edits refer to immutable source spans, not transformed evaluation formulas.
use super::super::{Cache, IoError, sheet, unsupported};
use std::io::{Cursor, Seek, SeekFrom, Write};
use std::ops::Range;

pub(in crate::xlsx) struct Patch {
    pub(in crate::xlsx) span: Range<usize>,
    pub(in crate::xlsx) replacement: Vec<u8>,
}
pub(in crate::xlsx::recalculate) fn cache_patches(
    xml: &[u8],
    cell: &sheet::FormulaMark,
    value: &Cache,
    patches: &mut Vec<Patch>,
) {
    let wanted = value.kind();
    let current = cell.kind.as_deref().filter(|t| *t != "n");
    if wanted != current {
        let (span, replacement) = match &cell.kind_span {
            Some(span) => (
                span.clone(),
                wanted
                    .map(|t| format!("t=\"{t}\"").into_bytes())
                    .unwrap_or_default(),
            ),
            None => (
                cell.open_end - 1..cell.open_end - 1,
                wanted
                    .map(|t| format!(" t=\"{t}\"").into_bytes())
                    .unwrap_or_default(),
            ),
        };
        patches.push(Patch { span, replacement });
    }
    if let Some(span) = &cell.inline {
        patches.push(Patch {
            span: span.clone(),
            replacement: Vec::new(),
        });
    }
    let prefix = cell.prefix(xml);
    let name = format!("{prefix}v");
    let text = value.text();
    let replacement = if let Some(v) = &cell.value {
        let mut out = if v.empty {
            xml[v.span.start..v.span.end - 2].to_vec()
        } else {
            xml[v.span.start..v.open_end - 1].to_vec()
        };
        if matches!(value, Cache::Empty) {
            out.extend_from_slice(b"/>");
        } else {
            out.push(b'>');
            out.extend_from_slice(text.as_bytes());
            if v.empty {
                out.extend_from_slice(format!("</{}>", v.qualified(xml)).as_bytes());
            } else {
                out.extend_from_slice(&xml[v.close_start..v.span.end]);
            }
        }
        out
    } else if matches!(value, Cache::Empty) {
        format!("<{name}/>").into_bytes()
    } else {
        format!("<{name}>{text}</{name}>").into_bytes()
    };
    patches.push(Patch {
        span: cell
            .value
            .as_ref()
            .map(|v| v.span.clone())
            .unwrap_or(cell.formula_end..cell.formula_end),
        replacement,
    });
}
pub(in crate::xlsx) fn apply_patches(
    bytes: &[u8],
    mut patches: Vec<Patch>,
    limit: usize,
) -> Result<Vec<u8>, IoError> {
    // An insertion sorts before a replacement starting at the same offset.
    patches.sort_by_key(|p| (p.span.start, p.span.end));
    let mut length = bytes.len();
    let mut previous = 0;
    for patch in &patches {
        if patch.span.start < previous
            || patch.span.end > bytes.len()
            || patch.span.start > patch.span.end
        {
            return Err(unsupported("overlapping cache edit spans", "worksheet"));
        }
        previous = patch.span.end;
        length = length
            .checked_sub(patch.span.len())
            .and_then(|n| n.checked_add(patch.replacement.len()))
            .ok_or_else(|| unsupported("cache output size overflow", "worksheet"))?;
        if length > limit {
            return Err(unsupported("worksheet output byte limit", "worksheet"));
        }
    }
    // One append pass, rather than repeatedly shifting the remainder of XML.
    let mut out = Vec::with_capacity(length);
    previous = 0;
    for patch in patches {
        out.extend_from_slice(&bytes[previous..patch.span.start]);
        out.extend_from_slice(&patch.replacement);
        previous = patch.span.end;
    }
    out.extend_from_slice(&bytes[previous..]);
    Ok(out)
}
pub(in crate::xlsx) struct BoundedOutput {
    pub(in crate::xlsx) cursor: Cursor<Vec<u8>>,
    pub(in crate::xlsx) limit: usize,
}
impl Write for BoundedOutput {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if self
            .cursor
            .position()
            .checked_add(bytes.len() as u64)
            .is_none_or(|n| n > self.limit as u64)
        {
            return Err(std::io::Error::other("XLSX output byte limit"));
        }
        self.cursor.write(bytes)
    }
    fn flush(&mut self) -> std::io::Result<()> {
        self.cursor.flush()
    }
}
impl Seek for BoundedOutput {
    fn seek(&mut self, from: SeekFrom) -> std::io::Result<u64> {
        self.cursor.seek(from)
    }
}

pub(in crate::xlsx) fn remove(span: &Range<usize>) -> Patch {
    Patch {
        span: span.clone(),
        replacement: Vec::new(),
    }
}
