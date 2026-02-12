from __future__ import annotations

import math
import os
import posixpath
import re
import tempfile
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
from xml.etree import ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS_REL = "http://schemas.openxmlformats.org/officeDocument/2006/relationships"
NS_PKG_REL = "http://schemas.openxmlformats.org/package/2006/relationships"
SHEET_REL_TYPE = (
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet"
)

CELL_REF_RE = re.compile(r"^\$?([A-Za-z]+)\$?([0-9]+)$")

ERROR_TOKEN_MAP = {
    "Div0": "#DIV/0!",
    "Div": "#DIV/0!",
    "NA": "#N/A",
    "Na": "#N/A",
    "Name": "#NAME?",
    "Null": "#NULL!",
    "Num": "#NUM!",
    "Ref": "#REF!",
    "Value": "#VALUE!",
}


def recalculate_file(path: os.PathLike[str] | str, output: os.PathLike[str] | str | None = None) -> Dict[str, Any]:
    """Evaluate formula cells in an XLSX and write cached values to sheet XML.

    Parameters
    ----------
    path:
        Input workbook path.
    output:
        Optional output path. When omitted, the workbook is updated in-place.
    """

    in_path = Path(path)
    out_path = Path(output) if output is not None else in_path
    in_place = out_path.resolve() == in_path.resolve()

    # Register default SpreadsheetML namespace so rewritten sheet XML keeps stable tags.
    ET.register_namespace("", NS_MAIN)

    from .formualizer_py import Workbook  # lazy import to avoid import order issues

    wb = Workbook.from_path(str(in_path))
    summary: Dict[str, Any] = {"evaluated": 0, "errors": 0, "sheets": {}}
    modified_entries: Dict[str, bytes] = {}

    with zipfile.ZipFile(in_path, "r") as zin:
        sheet_path_by_name = _sheet_path_map(zin)

        for sheet_name, sheet_part in sheet_path_by_name.items():
            sheet_xml = zin.read(sheet_part)
            root = ET.fromstring(sheet_xml)
            sheet_eval = 0
            sheet_err = 0

            for cell in root.iter(f"{{{NS_MAIN}}}c"):
                formula = cell.find(f"{{{NS_MAIN}}}f")
                if formula is None:
                    continue

                ref = cell.attrib.get("r")
                if not ref:
                    continue

                coord = _parse_a1(ref)
                if coord is None:
                    continue
                row, col = coord
                sheet_eval += 1

                value = _evaluate_cell_with_xlfn_fallback(wb, sheet_name, row, col)
                if _is_error_dict(value):
                    sheet_err += 1
                _write_cached_value(cell, value)

            summary["sheets"][sheet_name] = {"evaluated": sheet_eval, "errors": sheet_err}
            summary["evaluated"] += sheet_eval
            summary["errors"] += sheet_err

            if sheet_eval > 0:
                modified_entries[sheet_part] = ET.tostring(
                    root, encoding="utf-8", xml_declaration=True
                )

    _rewrite_zip(in_path, out_path, modified_entries, in_place=in_place)
    return summary


def _sheet_path_map(zin: zipfile.ZipFile) -> Dict[str, str]:
    workbook_root = ET.fromstring(zin.read("xl/workbook.xml"))
    rels_root = ET.fromstring(zin.read("xl/_rels/workbook.xml.rels"))

    rel_targets: Dict[str, str] = {}
    for rel in rels_root.findall(f"{{{NS_PKG_REL}}}Relationship"):
        if rel.attrib.get("Type") != SHEET_REL_TYPE:
            continue
        rid = rel.attrib.get("Id")
        target = rel.attrib.get("Target")
        if rid and target:
            rel_targets[rid] = _resolve_target(target)

    mapping: Dict[str, str] = {}
    for sheet in workbook_root.findall(f".//{{{NS_MAIN}}}sheet"):
        name = sheet.attrib.get("name")
        rid = sheet.attrib.get(f"{{{NS_REL}}}id")
        if not name or not rid:
            continue
        part = rel_targets.get(rid)
        if part:
            mapping[name] = part
    return mapping


def _resolve_target(target: str) -> str:
    if target.startswith("/"):
        return target.lstrip("/")
    return posixpath.normpath(posixpath.join("xl", target))


def _parse_a1(cell_ref: str) -> Optional[Tuple[int, int]]:
    m = CELL_REF_RE.match(cell_ref)
    if not m:
        return None
    col_letters, row_str = m.groups()
    col = _col_to_index(col_letters)
    row = int(row_str)
    return row, col


def _col_to_index(col_letters: str) -> int:
    col = 0
    for ch in col_letters.upper():
        col = col * 26 + (ord(ch) - ord("A") + 1)
    return col


def _evaluate_cell_with_xlfn_fallback(wb: Any, sheet: str, row: int, col: int) -> Any:
    try:
        value = wb.evaluate_cell(sheet, row, col)
    except Exception as exc:
        return {"type": "Error", "kind": "Value", "message": str(exc)}

    if not _is_xlfn_name_error(value):
        return value

    original_formula = wb.get_formula(sheet, row, col)
    if not original_formula:
        return value

    normalized = _strip_xlfn_prefixes(original_formula)
    if normalized == original_formula:
        return value

    try:
        wb.set_formula(sheet, row, col, normalized)
        value = wb.evaluate_cell(sheet, row, col)
    except Exception as exc:
        value = {"type": "Error", "kind": "Value", "message": str(exc)}
    finally:
        try:
            wb.set_formula(sheet, row, col, original_formula)
        except Exception:
            pass

    return value


def _is_xlfn_name_error(value: Any) -> bool:
    if not _is_error_dict(value):
        return False
    if value.get("kind") != "Name":
        return False
    message = str(value.get("message", "")).lower()
    return "unknown function:" in message and "_xlfn." in message


def _is_error_dict(value: Any) -> bool:
    return isinstance(value, dict) and value.get("type") == "Error"


def _strip_xlfn_prefixes(formula: str) -> str:
    has_eq = formula.startswith("=")
    body = formula[1:] if has_eq else formula
    prefix1 = "_xlfn._xlws."
    prefix2 = "_xlfn."

    out: list[str] = []
    i = 0
    in_string = False
    while i < len(body):
        ch = body[i]
        if ch == '"':
            if in_string and i + 1 < len(body) and body[i + 1] == '"':
                out.append('""')
                i += 2
                continue
            in_string = not in_string
            out.append(ch)
            i += 1
            continue

        if not in_string:
            lower_rest = body[i:].lower()
            if lower_rest.startswith(prefix1):
                i += len(prefix1)
                continue
            if lower_rest.startswith(prefix2):
                i += len(prefix2)
                continue

        out.append(ch)
        i += 1

    normalized = "".join(out)
    return f"={normalized}" if has_eq else normalized


def _write_cached_value(cell: ET.Element, value: Any) -> None:
    payload_type, payload_value = _to_xml_payload(value)

    # Formula cells should store cached result in <v>. Remove inline strings if present.
    for inline_str in list(cell.findall(f"{{{NS_MAIN}}}is")):
        cell.remove(inline_str)

    v_elem = cell.find(f"{{{NS_MAIN}}}v")
    if payload_value is None:
        if v_elem is not None:
            cell.remove(v_elem)
    else:
        if v_elem is None:
            v_elem = ET.SubElement(cell, f"{{{NS_MAIN}}}v")
        v_elem.text = payload_value

    if payload_type is None:
        cell.attrib.pop("t", None)
    else:
        cell.set("t", payload_type)


def _to_xml_payload(value: Any) -> Tuple[Optional[str], Optional[str]]:
    if _is_error_dict(value):
        kind = str(value.get("kind", "Value"))
        return "e", ERROR_TOKEN_MAP.get(kind, "#VALUE!")

    if isinstance(value, bool):
        return "b", "1" if value else "0"

    if isinstance(value, (int, float)) and not isinstance(value, bool):
        numeric = float(value)
        if not math.isfinite(numeric):
            return "e", "#NUM!"
        return None, _format_number(numeric)

    if value is None:
        return None, None

    if isinstance(value, str):
        return "str", value

    return "str", str(value)


def _format_number(value: float) -> str:
    if value.is_integer():
        return str(int(value))
    return format(value, ".15g")


def _rewrite_zip(
    in_path: Path,
    out_path: Path,
    modified_entries: Dict[str, bytes],
    *,
    in_place: bool,
) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if in_place:
        fd, temp_name = tempfile.mkstemp(
            suffix=".xlsx.tmp", prefix=f"{in_path.stem}.", dir=str(in_path.parent)
        )
        os.close(fd)
        temp_path = Path(temp_name)
        try:
            _copy_zip_with_modifications(in_path, temp_path, modified_entries)
            temp_path.replace(in_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()
    else:
        _copy_zip_with_modifications(in_path, out_path, modified_entries)


def _copy_zip_with_modifications(
    source: Path, destination: Path, modified_entries: Dict[str, bytes]
) -> None:
    with zipfile.ZipFile(source, "r") as zin, zipfile.ZipFile(destination, "w") as zout:
        for info in zin.infolist():
            data = modified_entries.get(info.filename)
            if data is None:
                data = zin.read(info.filename)
            zout.writestr(info, data)
