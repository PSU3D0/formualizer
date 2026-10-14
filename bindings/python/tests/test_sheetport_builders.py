import runpy
import subprocess
import sys
from collections import UserDict
from pathlib import Path

import pytest

from formualizer import SheetPortManifestError, SheetPortSession, Workbook
from formualizer.sheetport import Input, Manifest, Output


def test_examples():
    root = Path(__file__).resolve().parents[1]
    for path in (root / "examples").glob("sheetport_*.py"):
        runpy.run_path(str(path), run_name="__main__")


def test_doc_sync():
    root = Path(__file__).resolve().parents[3]
    subprocess.run(
        [sys.executable, str(root / "scripts/check-doc-examples.py")], check=True
    )


def test_canonical_and_yaml_parity():
    wb = Workbook()
    wb.add_sheet("Sheet1")
    manifest = Manifest(
        "sample",
        ports=[
            Input("x", "Sheet1!A1", type="number"),
            Output("y", "Sheet1!A2", type="number"),
        ],
    )
    session = SheetPortSession(wb, manifest)
    yaml_session = SheetPortSession.from_manifest_yaml(manifest.to_yaml(), wb)
    assert session.manifest == yaml_session.manifest
    canonical = SheetPortSession(wb, UserDict(manifest.to_dict()))
    assert canonical.manifest == session.manifest
    hand_yaml = """spec: fio
spec_version: 0.3.0
manifest: {id: sample, name: sample}
ports:
- {id: x, dir: in, shape: scalar, location: {a1: 'Sheet1!A1'}, schema: {type: number}}
- {id: y, dir: out, shape: scalar, location: {a1: 'Sheet1!A2'}, schema: {type: number}}
"""
    assert (
        SheetPortSession.from_manifest_yaml(hand_yaml, wb).manifest == session.manifest
    )
    assert (
        Manifest.from_yaml(manifest.to_yaml()).to_dict()
        == manifest.to_dict()
        == session.manifest
    )


def test_helpers_snapshot_canonical_mappings_and_sequences():
    constraints = UserDict({"min": 0})
    port = Input("x", "Sheet1!A1", type="number", constraints=constraints)
    manifest = Manifest("sample", ports=(port,), tags=("example",))
    constraints["min"] = 10
    value = manifest.to_dict()
    assert value["manifest"]["tags"] == ["example"]
    assert value["ports"][0]["constraints"]["min"] == 0
    value["ports"][0]["constraints"]["min"] = 20
    assert manifest.to_dict()["ports"][0]["constraints"]["min"] == 0


def test_invalid_parity():
    wb = Workbook()
    manifest = Manifest("bad id", ports=[])
    with pytest.raises(SheetPortManifestError) as direct:
        SheetPortSession(wb, manifest.to_dict())
    with pytest.raises(SheetPortManifestError) as yaml:
        SheetPortSession.from_manifest_yaml(manifest.to_yaml(), wb)
    assert direct.value.args == yaml.value.args
