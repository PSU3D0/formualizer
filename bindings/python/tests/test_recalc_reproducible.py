"""Reproducible source recalculation: fixed clock, seed and the replay echo."""

from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from io import BytesIO

import openpyxl
import pytest

import formualizer as fz

DEFAULT_SEED = 0xF0F0_D0D0_AAAA_5555
EPOCH = datetime(1899, 12, 30)


def volatile_book() -> bytes:
    book = openpyxl.Workbook()
    sheet = book.active
    sheet["A1"] = "=TODAY()"
    sheet["A2"] = "=NOW()"
    sheet["A3"] = "=RAND()"
    sheet["A4"] = "=RANDBETWEEN(1,1000000000)"
    out = BytesIO()
    book.save(out)
    return out.getvalue()


def cached(payload: bytes) -> list:
    book = openpyxl.load_workbook(BytesIO(payload), data_only=True)
    values = [book.active[f"A{i}"].value for i in range(1, 5)]
    book.close()
    return values


def serial(local: datetime) -> float:
    return (local - EPOCH) / timedelta(days=1)


def assert_clock(payload: bytes, local: datetime) -> None:
    today, now, *_ = cached(payload)
    assert today == serial(local.replace(hour=0, minute=0, second=0))
    assert now == pytest.approx(serial(local), abs=1e-9)


@pytest.mark.parametrize(
    ("timestamp", "zone", "local", "label"),
    [
        (
            datetime(2026, 3, 1, 23, 30, tzinfo=timezone.utc),
            None,
            datetime(2026, 3, 1, 23, 30),
            "UTC",
        ),
        (
            datetime(2026, 3, 1, 23, 30, tzinfo=timezone.utc),
            "+01:00",
            datetime(2026, 3, 2, 0, 30),
            "+01:00",
        ),
        (
            datetime(2026, 3, 1, 23, 30, tzinfo=timezone.utc),
            3600,
            datetime(2026, 3, 2, 0, 30),
            "+01:00",
        ),
        (
            datetime(2026, 3, 1, 3, 0, tzinfo=timezone.utc),
            "-05:00",
            datetime(2026, 2, 28, 22, 0),
            "-05:00",
        ),
        # An aware non-UTC datetime is an instant; the zone still defaults to UTC.
        (
            datetime(2026, 3, 2, 0, 30, tzinfo=timezone(timedelta(hours=1))),
            None,
            datetime(2026, 3, 1, 23, 30),
            "UTC",
        ),
    ],
)
def test_fixed_clock_through_recalculate_xlsx_file(
    tmp_path, timestamp, zone, local, label
):
    source, output = tmp_path / "in.xlsx", tmp_path / "out.xlsx"
    source.write_bytes(volatile_book())
    result = fz.recalculate_xlsx_file(
        str(source),
        str(output),
        deterministic_timestamp_utc=timestamp,
        deterministic_timezone=zone,
    )
    assert_clock(output.read_bytes(), local)
    clock = result["clock"]
    assert clock["fixed"] is True
    assert clock["timezone"] == label
    assert clock["now"] == timestamp
    assert clock["now"].replace(tzinfo=None) == local
    assert result["seed"] == DEFAULT_SEED


def test_same_clock_and_seed_are_byte_identical_and_seed_changes_rand(tmp_path):
    source = tmp_path / "in.xlsx"
    source.write_bytes(volatile_book())
    when = datetime(2026, 1, 31, 9, tzinfo=timezone.utc)

    def run(name, **options):
        out = tmp_path / name
        fz.recalculate_xlsx_file(str(source), str(out), **options)
        return out.read_bytes()

    a = run("a.xlsx", rng_seed=42, deterministic_timestamp_utc=when)
    b = run("b.xlsx", rng_seed=42, deterministic_timestamp_utc=when)
    c = run("c.xlsx", rng_seed=43, deterministic_timestamp_utc=when)
    assert a == b
    assert cached(a)[2:] != cached(c)[2:]
    # Default RAND is stable run to run and equals the echoed default seed.
    assert cached(run("d.xlsx"))[2:] == cached(run("e.xlsx", rng_seed=DEFAULT_SEED))[2:]
    in_memory = fz.recalculate_xlsx_bytes(
        source.read_bytes(), rng_seed=42, deterministic_timestamp_utc=when
    )
    assert in_memory["bytes"] == a
    assert in_memory["seed"] == 42


def test_system_clock_echo_replays_exactly(tmp_path):
    source = tmp_path / "in.xlsx"
    source.write_bytes(volatile_book())
    first = fz.recalculate_xlsx_bytes(source.read_bytes())
    clock = first["clock"]
    assert clock["fixed"] is False
    assert clock["timezone"] == "Local"
    now = clock["now"]
    assert now.tzinfo is not None and now.microsecond == 0
    assert_clock(first["bytes"], now.replace(tzinfo=None))
    replay = fz.recalculate_xlsx_bytes(
        source.read_bytes(),
        rng_seed=first["seed"],
        deterministic_timestamp_utc=now,
        deterministic_timezone=int(now.utcoffset().total_seconds()),
    )
    assert replay["bytes"] == first["bytes"]
    assert replay["clock"]["now"] == now


@pytest.mark.parametrize(
    ("options", "error"),
    [
        ({"deterministic_timezone": "utc"}, TypeError),
        (
            {
                "deterministic_timestamp_utc": datetime(
                    2026, 1, 1, tzinfo=timezone.utc
                ),
                "deterministic_timezone": "local",
            },
            ValueError,
        ),
        (
            {
                "deterministic_timestamp_utc": datetime(
                    2026, 1, 1, tzinfo=timezone.utc
                ),
                "deterministic_timezone": "Europe/Paris",
            },
            TypeError,
        ),
        ({"rng_seed": -1}, OverflowError),
    ],
)
def test_invalid_options_raise_before_recalculating(options, error):
    with pytest.raises(error):
        fz.recalculate_xlsx_bytes(volatile_book(), **options)


def test_console_script_inherits_now_tz_and_seed(tmp_path):
    source, output = tmp_path / "in.xlsx", tmp_path / "out.xlsx"
    source.write_bytes(volatile_book())
    args = ["--now", "2026-03-01T23:30:00Z", "--tz", "+01:00", "--seed", "42"]
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "formualizer",
            "recalc",
            str(source),
            "-o",
            str(output),
            "--json",
            *args,
        ],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert process.returncode == 0, process.stderr
    report = json.loads(process.stdout)
    assert report["clock"] == {
        "now": "2026-03-02T00:30:00+01:00",
        "timezone": "+01:00",
        "fixed": True,
    }
    assert report["seed"] == 42
    assert_clock(output.read_bytes(), datetime(2026, 3, 2, 0, 30))
    direct = fz.recalculate_xlsx_bytes(
        source.read_bytes(),
        rng_seed=42,
        deterministic_timestamp_utc=datetime(2026, 3, 1, 23, 30, tzinfo=timezone.utc),
        deterministic_timezone="+01:00",
    )
    assert direct["bytes"] == output.read_bytes()
    bad = subprocess.run(
        [
            sys.executable,
            "-m",
            "formualizer",
            "recalc",
            str(source),
            "--now",
            "2026-03-01T23:30:00",
            "--json",
        ],
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert bad.returncode == 64
    assert json.loads(bad.stdout)["clock"] is None
