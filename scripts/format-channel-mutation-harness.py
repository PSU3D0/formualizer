#!/usr/bin/env python3
import os
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
ARROW = ROOT / "crates/formualizer-eval/src/arrow_store/mod.rs"
INTERPRETER = ROOT / "crates/formualizer-eval/src/interpreter.rs"
EVAL = ROOT / "crates/formualizer-eval/src/engine/eval.rs"
FUNCTION = ROOT / "crates/formualizer-eval/src/function.rs"
TRAITS = ROOT / "crates/formualizer-eval/src/traits.rs"
CALAMINE = ROOT / "crates/formualizer-workbook/src/backends/calamine.rs"
NUMFMT = ROOT / "crates/formualizer-common/src/numfmt.rs"
FORMAT = ROOT / "crates/formualizer-eval/src/format.rs"

MUTANTS = [
    ("M1_runs_get_offbyone", ARROW, [(".partition_point(|end| (*end as usize) <= offset);", ".partition_point(|end| (*end as usize) < offset);")]),
    ("M2_lane_always_none", ARROW, [("    pub fn from_ids(ids: &[u16]) -> Option<Self> {\n        if ids.iter().all(|id| *id == FormatId::GENERAL.0) {", "    pub fn from_ids(ids: &[u16]) -> Option<Self> {\n        if true { return None; }\n        if ids.iter().all(|id| *id == FormatId::GENERAL.0) {")]),
    ("M3_slice_ignores_offset", ARROW, [("        let ids: Vec<_> = (offset..offset.saturating_add(len))\n            .map(|i| self.get(i).0)\n            .collect();", "        let ids: Vec<_> = (0..len).map(|i| self.get(i).0).collect();")]),
    ("M4_date_plus_time_is_date", INTERPRETER, [("            | ('+', Some(FormatClass::Time), Some(FormatClass::Date)) => {\n                Some(crate::format::FormatId::DATETIME)\n            }", "            | ('+', Some(FormatClass::Time), Some(FormatClass::Date)) => {\n                Some(crate::format::FormatId::DATE)\n            }")]),
    ("M5_is_plain_always_true", INTERPRETER, [("        let is_plain = |class: &Option<FormatClass>| {\n            matches!(\n                class,\n                None | Some(FormatClass::General | FormatClass::Number { .. })\n            )\n        };", "        let is_plain = |class: &Option<FormatClass>| { let _ = class; true };")]),
    ("M6_serial_optout_ignored", EVAL, [("        if policy == crate::engine::TemporalEgress::Serial {\n            return value;\n        }", "        if false && policy == crate::engine::TemporalEgress::Serial {\n            return value;\n        }")]),
    ("M7_time_class_yields_date", EVAL, [("            Some(FormatClass::Time) => {\n                let seconds = (serial.rem_euclid(1.0) * 86_400.0).round() as u32 % 86_400;", "            Some(FormatClass::Time) => {\n                return formualizer_common::try_serial_to_date_for(date_system, serial)\n                    .map(LiteralValue::Date)\n                    .unwrap_or(LiteralValue::Number(serial));\n                #[allow(unreachable_code)]\n                let seconds = (serial.rem_euclid(1.0) * 86_400.0).round() as u32 % 86_400;")]),
    ("M8_format_id_no_general_filter", ARROW, [("            .or_else(|| ch.computed_overlay.get_format(in_off))\n            .filter(|id| *id != FormatId::GENERAL)", "            .or_else(|| ch.computed_overlay.get_format(in_off))")]),
    ("M9_base_lane_beats_user_overlay", ARROW, [("        ch.overlay\n            .get_format(in_off)\n            .or_else(|| ch.format.as_ref().map(|runs| runs.get(in_off)))", "        ch.format.as_ref().map(|runs| runs.get(in_off))\n            .or_else(|| ch.overlay.get_format(in_off))")]),
    ("M10_no_derived_fallback", EVAL, [("        arrow.or_else(|| {\n            let sheet_id = self.graph.sheet_id(sheet)?;", "        if true { return arrow; }\n        #[allow(unreachable_code)]\n        arrow.or_else(|| {\n            let sheet_id = self.graph.sheet_id(sheet)?;")]),
    ("M11_calamine_all_date", CALAMINE, [("        DataRef::DateTime(dt) if dt.is_duration() => {\n            Some(formualizer_eval::format::FormatId::DURATION)\n        }\n        DataRef::DateTime(dt) if (0.0..1.0).contains(&dt.as_f64()) => {\n            Some(formualizer_eval::format::FormatId::TIME)\n        }\n        DataRef::DateTime(dt) if dt.as_f64().fract().abs() > f64::EPSILON => {\n            Some(formualizer_eval::format::FormatId::DATETIME)\n        }", "")]),
    ("M12_ingest_time_as_date", ARROW, [("                LiteralValue::Time(_) => FormatId::TIME.0,", "                LiteralValue::Time(_) => FormatId::DATE.0,")]),
    ("M13_dispatch_drops_propagation", FUNCTION, [("        self.eval(args, ctx)\n            .map(|result| self.apply_format_propagation(result))\n    }\n}", "        self.eval(args, ctx)\n    }\n}")]),
    ("M14_with_format_keeps_general", TRAITS, [("        let format = format.filter(|id| *id != crate::format::FormatId::GENERAL);\n", "")]),
    ("M15_grow_fills_with_date", ARROW, [("            ids.resize(new_len, FormatId::GENERAL.0);", "            ids.resize(new_len, FormatId::DATE.0);")]),
    ("M16_duration_class_removed", NUMFMT, [("    if visible.contains(\"[h]\") || visible.contains(\"[m]\") || visible.contains(\"[s]\") {\n        return FormatClass::Duration;\n    }", "")]),
    ("M17_intern_returns_general", FORMAT, [("        let Ok(raw_id) = u16::try_from(self.formats.len()) else {\n            eprintln!(\n                \"number-format registry exhausted at {} entries; saturating `{}` to General\",\n                self.formats.len(),\n                parsed.code()\n            );\n            return FormatId::GENERAL;\n        };\n        let id = FormatId(raw_id);", "        let id = FormatId::GENERAL;")]),
    ("M18_skip_structural_derived_purge", EVAL, [
        ("            .retain(|cell, _| cell.sheet_id != sheet_id || cell.coord.row() < start0);", "            .retain(|_, _| true);"),
        ("            .retain(|cell, _| cell.sheet_id != sheet_id || cell.coord.col() < start0);", "            .retain(|_, _| true);"),
    ]),
    ("M19_skip_changelog_invalidation", EVAL, [("        self.clear_logged_cell_format_states(&new_events);", "        // mutation: changelog invalidation disabled")]),
]

TEST_COMMAND = [
    "cargo", "test", "--workspace", "--lib", "--test", "api_compat_datetime",
    "--test", "calamine", "--test", "json_date_system_engine",
]


def main() -> int:
    if subprocess.run(["git", "diff", "--quiet"], cwd=ROOT).returncode != 0 or subprocess.run(
        ["git", "diff", "--cached", "--quiet"], cwd=ROOT
    ).returncode != 0:
        raise SystemExit("format-channel mutation harness requires a clean checkout")

    paths = {path for _, path, _ in MUTANTS}
    originals = {path: path.read_text() for path in paths}
    results = []
    try:
        for name, path, replacements in MUTANTS:
            for source, original in originals.items():
                source.write_text(original)
            text = path.read_text()
            for old, new in replacements:
                if old not in text:
                    raise RuntimeError(f"{name}: mutation pattern did not match {path}")
                text = text.replace(old, new, 1)
            path.write_text(text)
            log_path = Path("/tmp") / f"formualizer-format-channel-{name}.log"
            env = os.environ.copy()
            env["PYO3_PYTHON"] = "/usr/bin/python3.12"
            with log_path.open("w") as log:
                result = subprocess.run(TEST_COMMAND, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
            log_text = log_path.read_text()
            killers = [
                line.removeprefix("test ").removesuffix(" ... FAILED")
                for line in log_text.splitlines()
                if line.startswith("test ") and line.endswith(" ... FAILED")
            ]
            status = "KILLED" if result.returncode != 0 and killers else "SURVIVED"
            results.append((name, status, killers[:6], log_path))
            print(f"{name}: {status}" + (f" by {', '.join(killers[:6])}" if killers else ""))
    finally:
        for path, original in originals.items():
            path.write_text(original)

    survived = [result for result in results if result[1] != "KILLED"]
    print(f"matrix: {len(results) - len(survived)}/{len(results)} killed")
    if survived:
        for name, _, _, log_path in survived:
            print(f"inspect {name}: {log_path}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
