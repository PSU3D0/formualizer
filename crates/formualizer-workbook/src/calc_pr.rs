//! XLSX `<calcPr>` round-trip (spec §9, RFC #113).
//!
//! `xl/workbook.xml` carries workbook-level calculation properties:
//!
//! ```xml
//! <calcPr calcMode="auto" iterate="1" iterateCount="100"
//!         iterateDelta="0.001" fullCalcOnLoad="1"/>
//! ```
//!
//! This module is the single source of truth for both directions:
//!
//! - **Load**: [`parse_calc_pr`] extracts a [`CalcSettings`] from the raw
//!   `workbook.xml` bytes (the `.xlsx` is a zip; the calamine and umya backends
//!   already depend on `zip` + `quick_xml`, so we reuse those rather than
//!   adding a heavy dependency). [`apply_calc_settings_to_cycle`] maps the
//!   parsed settings onto the engine's [`CycleConfig`].
//! - **Save**: umya hard-codes `<calcPr calcId="122211"/>` and exposes no API
//!   for the iterate attributes, so [`rewrite_calc_pr_in_zip`] post-processes
//!   the umya-written `.xlsx` bytes, rewriting only the `<calcPr>` element in
//!   `xl/workbook.xml` while leaving everything else byte-identical.
//!
//! ## Detection-handling decision (spec §9 implication)
//!
//! Spec §9 says "Loading never changes `detection`". Taken literally that would
//! produce `CyclePolicy::Iterate` with the *default* `CycleDetection::Static`,
//! which is a config error that panics at engine construction
//! ([`CycleConfig::validate`]). To keep `Workbook::from_reader` ergonomic and
//! non-panicking we resolve the implication per the task's first option: when a
//! file enables iteration we set `detection: Runtime` as well (iteration is
//! meaningless under `Static`, and the engine's own `CycleConfig::iterate`
//! helper makes exactly this coupling). A file that does *not* enable iteration
//! leaves the cycle config completely untouched, so the caller's chosen
//! `detection` (including a deliberate `Static` compat switch) is preserved.

use crate::traits::CalcSettings;
use formualizer_eval::engine::{CycleConfig, CyclePolicy};

/// Parse the `<calcPr>` element from raw `xl/workbook.xml` bytes.
///
/// Returns `None` when there is no `<calcPr>` element at all (so callers leave
/// the engine config untouched). A present-but-empty `<calcPr/>` yields a
/// default [`CalcSettings`] (`iterate = false`).
#[cfg(any(feature = "calamine", feature = "umya", feature = "experimental-fzcp"))]
pub fn parse_calc_pr(workbook_xml: &[u8]) -> Option<CalcSettings> {
    use quick_xml::Reader as XmlReader;
    use quick_xml::events::Event;
    use quick_xml::name::QName;

    let mut xml = XmlReader::from_reader(workbook_xml);
    xml.config_mut().trim_text(true);
    let mut buf = Vec::new();

    loop {
        buf.clear();
        match xml.read_event_into(&mut buf) {
            // `<calcPr .../>` is normally self-closing (Empty); accept Start too
            // for robustness against producers that emit `<calcPr></calcPr>`.
            Ok(Event::Empty(ref e)) | Ok(Event::Start(ref e))
                if e.local_name().as_ref() == b"calcPr" =>
            {
                let decoder = xml.decoder();
                let mut settings = CalcSettings::default();
                for attr in e.attributes().filter_map(Result::ok) {
                    let value = match attr.decode_and_unescape_value(decoder) {
                        Ok(v) => v.into_owned(),
                        Err(_) => continue,
                    };
                    match attr.key {
                        QName(b"iterate") => settings.iterate = parse_xml_bool(&value),
                        QName(b"iterateCount") => settings.iterate_count = value.parse().ok(),
                        QName(b"iterateDelta") => settings.iterate_delta = value.parse().ok(),
                        QName(b"calcMode") => settings.calc_mode = Some(value),
                        QName(b"fullCalcOnLoad") => {
                            settings.full_calc_on_load = Some(parse_xml_bool(&value))
                        }
                        _ => {}
                    }
                }
                return Some(settings);
            }
            Ok(Event::Eof) => return None,
            Err(_) => return None,
            _ => {}
        }
    }
}

/// OOXML boolean attribute parsing: `"1"`/`"true"`/`"on"` → `true`.
fn parse_xml_bool(value: &str) -> bool {
    matches!(value.trim(), "1" | "true" | "True" | "TRUE" | "on")
}

/// Apply parsed [`CalcSettings`] onto an engine [`CycleConfig`] (spec §9 load
/// mapping). See the module docs for the detection-handling decision.
///
/// - `iterate=true`: `policy = Iterate { max_iterations: iterateCount ?? 100,
///   max_change: iterateDelta ?? 0.001 }` and `detection = Runtime`.
/// - `iterate=false` (or absent): the config is returned unchanged.
pub fn apply_calc_settings_to_cycle(settings: &CalcSettings, cycle: CycleConfig) -> CycleConfig {
    if !settings.iterate {
        return cycle;
    }
    let max_iterations = settings
        .iterate_count
        .filter(|n| *n >= 1)
        .unwrap_or(CyclePolicy::EXCEL_DEFAULT_MAX_ITERATIONS);
    let max_change = settings
        .iterate_delta
        .filter(|d| d.is_finite() && *d >= 0.0)
        .unwrap_or(CyclePolicy::EXCEL_DEFAULT_MAX_CHANGE);
    // `CycleConfig::iterate` sets detection = Runtime, resolving the §9
    // "loading never changes detection" implication (see module docs).
    CycleConfig::iterate(max_iterations, max_change)
}

/// Derive the writable `<calcPr>` iterate attributes from the engine's active
/// [`CycleConfig`] (spec §9 save mapping). Only `iterate`/`iterateCount`/
/// `iterateDelta` are produced here; `calcMode`/`fullCalcOnLoad`/`calcId` are
/// preserved separately during the zip rewrite.
pub fn calc_settings_from_cycle(cycle: &CycleConfig) -> CalcSettings {
    match cycle.policy {
        CyclePolicy::Iterate {
            max_iterations,
            max_change,
        } => CalcSettings {
            iterate: true,
            iterate_count: Some(max_iterations),
            iterate_delta: Some(max_change),
            ..Default::default()
        },
        CyclePolicy::Error => CalcSettings {
            iterate: false,
            ..Default::default()
        },
    }
}

/// Serialize a `<calcPr>` element reflecting `settings`, preserving
/// round-trip-only attributes (`calcMode`, `fullCalcOnLoad`, `calcId`).
///
/// `calc_id` carries the `calcId` attribute from the file being rewritten so
/// the save path keeps whatever the writer emitted.
fn render_calc_pr(settings: &CalcSettings, calc_id: Option<&str>) -> String {
    let mut out = String::from("<calcPr");
    if let Some(id) = calc_id {
        out.push_str(&format!(" calcId=\"{}\"", xml_escape_attr(id)));
    }
    if let Some(mode) = &settings.calc_mode {
        out.push_str(&format!(" calcMode=\"{}\"", xml_escape_attr(mode)));
    }
    out.push_str(&format!(
        " iterate=\"{}\"",
        if settings.iterate { 1 } else { 0 }
    ));
    if let Some(count) = settings.iterate_count {
        out.push_str(&format!(" iterateCount=\"{count}\""));
    }
    if let Some(delta) = settings.iterate_delta {
        out.push_str(&format!(" iterateDelta=\"{delta}\""));
    }
    if let Some(full) = settings.full_calc_on_load {
        out.push_str(&format!(" fullCalcOnLoad=\"{}\"", if full { 1 } else { 0 }));
    }
    out.push_str("/>");
    out
}

fn xml_escape_attr(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// Extract the value of an attribute from a single XML element tag body.
fn extract_tag_attr(tag: &str, key: &str) -> Option<String> {
    for quote in ['"', '\''] {
        let needle = format!("{key}={quote}");
        if let Some(pos) = tag.find(&needle) {
            let start = pos + needle.len();
            if let Some(end) = tag[start..].find(quote) {
                return Some(tag[start..start + end].to_string());
            }
        }
    }
    None
}

/// Rewrite (or insert) the `<calcPr>` element inside `workbook.xml` text so it
/// reflects `settings`. Round-trip-only attributes (`calcMode`,
/// `fullCalcOnLoad`, `calcId`) present in the existing element are preserved
/// unless `settings` overrides them.
pub fn rewrite_calc_pr_in_workbook_xml(xml: &str, settings: &CalcSettings) -> String {
    if let Some(start) = xml.find("<calcPr") {
        // Find the end of the element: `/>` for self-closing or `</calcPr>`.
        let after = &xml[start..];
        let (elem_len, existing_tag): (usize, &str) = if let Some(self_close) = after.find("/>") {
            // Only treat as self-closing if `/>` comes before any `>`.
            let gt = after.find('>').unwrap_or(usize::MAX);
            if self_close < gt {
                (self_close + 2, &after[..self_close])
            } else {
                let close = after.find("</calcPr>").map(|i| i + "</calcPr>".len());
                match close {
                    Some(len) => (len, &after[..after.find('>').unwrap_or(0)]),
                    None => (after.find('>').map(|i| i + 1).unwrap_or(after.len()), after),
                }
            }
        } else {
            let close = after.find("</calcPr>").map(|i| i + "</calcPr>".len());
            match close {
                Some(len) => (len, &after[..after.find('>').unwrap_or(0)]),
                None => (after.find('>').map(|i| i + 1).unwrap_or(after.len()), after),
            }
        };

        // Merge preserved attributes from the existing element when the active
        // settings did not capture them.
        let mut merged = settings.clone();
        if merged.calc_mode.is_none() {
            merged.calc_mode = extract_tag_attr(existing_tag, "calcMode");
        }
        if merged.full_calc_on_load.is_none() {
            merged.full_calc_on_load =
                extract_tag_attr(existing_tag, "fullCalcOnLoad").map(|v| parse_xml_bool(&v));
        }
        let calc_id = extract_tag_attr(existing_tag, "calcId");
        let rendered = render_calc_pr(&merged, calc_id.as_deref());

        let mut result = String::with_capacity(xml.len() + rendered.len());
        result.push_str(&xml[..start]);
        result.push_str(&rendered);
        result.push_str(&xml[start + elem_len..]);
        result
    } else {
        // No existing calcPr: insert before </workbook>.
        let rendered = render_calc_pr(settings, None);
        if let Some(close) = xml.rfind("</workbook>") {
            let mut result = String::with_capacity(xml.len() + rendered.len());
            result.push_str(&xml[..close]);
            result.push_str(&rendered);
            result.push_str(&xml[close..]);
            result
        } else {
            xml.to_string()
        }
    }
}

/// Post-process an `.xlsx` zip byte buffer: rewrite `xl/workbook.xml`'s
/// `<calcPr>` to reflect `settings`, leaving every other zip entry untouched.
///
/// This is the umya save path (the umya writer cannot express the iterate
/// attributes). Returns the original bytes unchanged if the archive has no
/// `xl/workbook.xml`.
#[cfg(feature = "umya")]
pub fn rewrite_calc_pr_in_zip(
    bytes: &[u8],
    settings: &CalcSettings,
) -> Result<Vec<u8>, std::io::Error> {
    use std::io::{Cursor, Read, Write};
    use zip::write::SimpleFileOptions;

    let mut archive = zip::ZipArchive::new(Cursor::new(bytes))
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

    let mut out_buf = Vec::with_capacity(bytes.len() + 64);
    {
        let mut writer = zip::ZipWriter::new(Cursor::new(&mut out_buf));
        for i in 0..archive.len() {
            let mut entry = archive
                .by_index(i)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            let name = entry.name().to_string();
            let options = SimpleFileOptions::default()
                .compression_method(entry.compression())
                .last_modified_time(entry.last_modified().unwrap_or_default());

            if name == "xl/workbook.xml" {
                let mut xml = String::new();
                entry.read_to_string(&mut xml)?;
                let rewritten = rewrite_calc_pr_in_workbook_xml(&xml, settings);
                writer
                    .start_file(name, options)
                    .map_err(std::io::Error::other)?;
                writer.write_all(rewritten.as_bytes())?;
            } else {
                writer.raw_copy_file(entry).map_err(std::io::Error::other)?;
            }
        }
        writer.finish().map_err(std::io::Error::other)?;
    }
    Ok(out_buf)
}

#[cfg(test)]
mod tests {
    use super::*;
    use formualizer_eval::engine::{CycleDetection, CyclePolicy};

    #[test]
    fn parse_iterate_on_with_custom_values() {
        let xml = br#"<?xml version="1.0"?>
            <workbook><calcPr calcMode="auto" iterate="1" iterateCount="42"
            iterateDelta="0.5" fullCalcOnLoad="1"/></workbook>"#;
        let s = parse_calc_pr(xml).expect("calcPr present");
        assert!(s.iterate);
        assert_eq!(s.iterate_count, Some(42));
        assert_eq!(s.iterate_delta, Some(0.5));
        assert_eq!(s.calc_mode.as_deref(), Some("auto"));
        assert_eq!(s.full_calc_on_load, Some(true));
    }

    #[test]
    fn parse_iterate_true_text() {
        let xml = br#"<workbook><calcPr iterate="true"/></workbook>"#;
        let s = parse_calc_pr(xml).unwrap();
        assert!(s.iterate);
    }

    #[test]
    fn parse_iterate_zero() {
        let xml = br#"<workbook><calcPr calcId="1" iterate="0"/></workbook>"#;
        let s = parse_calc_pr(xml).unwrap();
        assert!(!s.iterate);
    }

    #[test]
    fn parse_no_calc_pr_is_none() {
        let xml = br#"<workbook><sheets/></workbook>"#;
        assert!(parse_calc_pr(xml).is_none());
    }

    #[test]
    fn apply_iterate_on_sets_runtime_and_knobs() {
        let s = CalcSettings {
            iterate: true,
            iterate_count: Some(42),
            iterate_delta: Some(0.5),
            ..Default::default()
        };
        let cfg = apply_calc_settings_to_cycle(&s, CycleConfig::default());
        assert_eq!(cfg.detection, CycleDetection::Runtime);
        assert_eq!(
            cfg.policy,
            CyclePolicy::Iterate {
                max_iterations: 42,
                max_change: 0.5
            }
        );
        cfg.validate().expect("loaded config must be valid");
    }

    #[test]
    fn apply_iterate_on_defaults_to_excel_knobs() {
        let s = CalcSettings {
            iterate: true,
            ..Default::default()
        };
        let cfg = apply_calc_settings_to_cycle(&s, CycleConfig::default());
        assert_eq!(
            cfg.policy,
            CyclePolicy::Iterate {
                max_iterations: 100,
                max_change: 0.001
            }
        );
    }

    #[test]
    fn apply_iterate_off_leaves_config_untouched() {
        let s = CalcSettings::default();
        let base = CycleConfig {
            detection: CycleDetection::Static,
            policy: CyclePolicy::Error,
        };
        let cfg = apply_calc_settings_to_cycle(&s, base);
        assert_eq!(cfg, base);
    }

    #[test]
    fn rewrite_replaces_existing_calc_pr_preserving_calc_id_and_mode() {
        let xml =
            r#"<workbook xmlns="x"><sheets/><calcPr calcId="122211" calcMode="auto"/></workbook>"#;
        let settings = CalcSettings {
            iterate: true,
            iterate_count: Some(7),
            iterate_delta: Some(0.01),
            ..Default::default()
        };
        let out = rewrite_calc_pr_in_workbook_xml(xml, &settings);
        assert!(out.contains("iterate=\"1\""), "{out}");
        assert!(out.contains("iterateCount=\"7\""), "{out}");
        assert!(out.contains("iterateDelta=\"0.01\""), "{out}");
        assert!(out.contains("calcId=\"122211\""), "{out}");
        assert!(out.contains("calcMode=\"auto\""), "{out}");
        // Re-parse to confirm round-trip self-consistency.
        let reparsed = parse_calc_pr(out.as_bytes()).unwrap();
        assert!(reparsed.iterate);
        assert_eq!(reparsed.iterate_count, Some(7));
        assert_eq!(reparsed.calc_mode.as_deref(), Some("auto"));
    }

    #[test]
    fn rewrite_inserts_calc_pr_when_absent() {
        let xml = r#"<workbook><sheets/></workbook>"#;
        let settings = CalcSettings {
            iterate: true,
            iterate_count: Some(5),
            iterate_delta: Some(0.002),
            ..Default::default()
        };
        let out = rewrite_calc_pr_in_workbook_xml(xml, &settings);
        let reparsed = parse_calc_pr(out.as_bytes()).unwrap();
        assert!(reparsed.iterate);
        assert_eq!(reparsed.iterate_count, Some(5));
    }
}
