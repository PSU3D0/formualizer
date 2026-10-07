"""Exercise the candidate native CLI, including real module-entry processes."""

import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import openpyxl
import pytest

from formualizer import cli

try:
    from .test_xlsx_source_spills import fixture
except ImportError:  # pytest's default (prepend) import mode
    from test_xlsx_source_spills import fixture


def invoke(*args):
    # Inherit PYTHONPATH: never accidentally test an installed release wheel.
    return subprocess.run(
        [sys.executable, "-m", "formualizer", *map(str, args)],
        capture_output=True,
        timeout=15,
        check=False,
    )


def report(process, code, status):
    assert process.returncode == code, process.stderr
    assert process.stderr == b""
    assert len(process.stdout.splitlines()) == 1
    result = json.loads(process.stdout)
    assert result["schema"] == "formualizer.recalc/1"
    assert result["status"] == status
    return result


def test_write_check_unchanged_and_spill(tmp_path):
    path = tmp_path / "book.xlsx"
    before = fixture()
    path.write_bytes(before)
    mtime = path.stat().st_mtime_ns
    report(invoke("recalc", path, "--check", "--json"), 3, "stale")
    assert path.read_bytes() == before
    assert path.stat().st_mtime_ns == mtime
    assert report(invoke("recalc", path, "--json"), 0, "written")["written"]
    book = openpyxl.load_workbook(path, data_only=True)
    assert [book.active[f"C{i}"].value for i in range(2, 5)] == [1, 2, 3]
    assert book.active["C9"].value == 6
    book.close()
    current = path.read_bytes()
    mtime = path.stat().st_mtime_ns
    report(invoke("recalc", path, "--json"), 0, "unchanged")
    report(invoke("recalc", path, "--check", "--json"), 0, "current")
    assert path.read_bytes() == current
    assert path.stat().st_mtime_ns == mtime


def test_output(tmp_path):
    source, output = tmp_path / "source.xlsx", tmp_path / "output.xlsx"
    original = fixture()
    source.write_bytes(original)
    mtime = source.stat().st_mtime_ns
    report(invoke("recalc", source, "-o", output, "--json"), 0, "written")
    assert output.exists()
    assert source.read_bytes() == original
    assert source.stat().st_mtime_ns == mtime
    report(invoke("recalc", output, "--check", "--json"), 0, "current")


def test_refusal(tmp_path):
    source = tmp_path / "merged.xlsx"
    original = fixture(merge=True)
    source.write_bytes(original)
    result = report(invoke("recalc", source, "--json"), 2, "refused")
    assert "merged" in result["refusal"]["feature"]
    assert not result["written"]
    assert source.read_bytes() == original


def test_non_zip(tmp_path):
    source = tmp_path / "invalid.xlsx"
    source.write_bytes(b"not zip")
    report(invoke("recalc", source, "--json"), 1, "error")


@pytest.mark.parametrize("args", [("recalc",), ("recalc", "--unknown")])
def test_usage(args):
    human = invoke(*args)
    assert human.returncode == 64
    assert human.stdout == b""
    assert b"\nUsage:" in human.stderr
    report(invoke(*args, "--json"), 64, "error")


def test_version():
    result = invoke("--version")
    assert result.returncode == 0
    assert result.stdout.startswith(b"formualizer ")
    assert result.stderr == b""


def test_main_bytes_and_code(capfdbinary):
    expected = cli._native._run_cli(
        ["formualizer", "recalc", "--json"], cli._native._CliCancelToken()
    )
    assert cli.main(["recalc", "--json"]) == expected[0] == 64
    captured = capfdbinary.readouterr()
    assert (captured.out, captured.err) == expected[1:]


def test_cancelled_main(tmp_path, monkeypatch, capfdbinary):
    path = tmp_path / "cancel.xlsx"
    original = fixture()
    path.write_bytes(original)
    mtime = path.stat().st_mtime_ns
    token = cli._native._CliCancelToken()
    token.cancel()
    monkeypatch.setattr(cli._native, "_CliCancelToken", lambda: token)
    assert cli.main(["recalc", str(path), "--json"]) == 130
    captured = capfdbinary.readouterr()
    assert json.loads(captured.out)["status"] == "interrupted"
    assert captured.err == b""
    assert path.read_bytes() == original
    assert path.stat().st_mtime_ns == mtime


@pytest.mark.skipif(os.name != "posix", reason="POSIX SIGINT delivery")
@pytest.mark.timeout(10)
def test_sigint(tmp_path):
    path = tmp_path / "interrupt.xlsx"
    original = fixture()
    path.write_bytes(original)
    mtime = path.stat().st_mtime_ns
    # Gate before native entry, so signal delivery cannot race a fast workbook.
    # Actual CLI execution then observes the token cancelled by the real handler.
    script = """
import sys
from threading import Event, Thread
from formualizer import cli
class ReadyThread(Thread):
    announced = False
    def join(self, timeout=None):
        if not self.announced:
            self.announced = True
            print("ready", file=sys.stderr, flush=True)
        return super().join(timeout)
cli.Thread = ReadyThread
native_run = cli._native._run_cli
native_token = cli._native._CliCancelToken
gate = Event()
class Token:
    def __init__(self):
        self.native = native_token()
    def cancel(self):
        self.native.cancel()
        gate.set()
def run(argv, token):
    if not gate.wait(5):
        raise RuntimeError("no interrupt received")
    return native_run(argv, token.native)
cli._native._CliCancelToken = Token
cli._native._run_cli = run
sys.exit(cli.main(["recalc", sys.argv[1], "--json"]))
"""
    with subprocess.Popen(
        [sys.executable, "-c", script, str(path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as process:
        try:
            assert process.stderr.readline() == b"ready\n"
            process.send_signal(signal.SIGINT)
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode == 130
            assert stderr == b""
            assert json.loads(stdout)["status"] == "interrupted"
        finally:
            if process.poll() is None:
                process.kill()
                process.wait()
    assert path.read_bytes() == original
    assert path.stat().st_mtime_ns == mtime


def test_unavailable(monkeypatch, capsys):
    monkeypatch.delattr(cli._native, "_run_cli")
    assert cli.main(["--version"]) == 1
    assert capsys.readouterr().err == "CLI not available on this platform\n"


def test_candidate_import():
    import formualizer

    # The candidate route supplies one temporary package root.
    if os.environ.get("PYTHONPATH"):
        roots = os.environ["PYTHONPATH"].split(os.pathsep)
        assert any(Path(formualizer.__file__).is_relative_to(root) for root in roots)
