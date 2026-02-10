from __future__ import annotations

import datetime
from typing import Any, Mapping

from . import SheetPortSession, Workbook


def _mypy_api_smoke(wb: Workbook, session: SheetPortSession) -> Mapping[str, Any]:
    # Workbook changelog metadata setters
    wb.set_actor_id(None)
    wb.set_correlation_id("corr")
    wb.set_reason("unit-test")

    # SheetPort determinism knobs
    ts = datetime.datetime(2024, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    out1 = session.evaluate_once(
        freeze_volatile=True,
        rng_seed=123,
        deterministic_timestamp_utc=ts,
        deterministic_timezone="utc",
    )
    out2 = session.evaluate_once(
        deterministic_timestamp_utc=ts,
        deterministic_timezone=0,
    )

    # Return a value so mypy checks mapping types.
    return {"out1": out1, "out2": out2}
