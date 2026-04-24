"""Day-scoped CP-SAT scheduler.

Public entrypoint: schedule_day(payload: dict) -> dict.
See docs/superpowers/specs/2026-04-24-scheduling-model-design.md for the I/O contract.
"""
from __future__ import annotations

from typing import Any

from ortools.sat.python import cp_model

# Commonsense temporal defaults (minutes since midnight)
LUNCH_WINDOW = (11 * 60 + 30, 13 * 60 + 30)   # 11:30–13:30
DINNER_WINDOW = (17 * 60, 20 * 60 + 30)        # 17:00–20:30
MIN_MEAL_GAP_MIN = 120
STATION_EXIT_BUFFER_MIN = 5
STATION_ENTRY_BUFFER_MIN = 20
DEFAULT_TIME_LIMIT_S = 5.0
DAY_START_MIN = 0
DAY_END_MIN = 24 * 60


def _parse_hhmm(s: str) -> int:
    """'07:14' -> 434."""
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def _format_hhmm(m: int) -> str:
    """434 -> '07:14'."""
    return f"{m // 60:02d}:{m % 60:02d}"


def schedule_day(payload: dict[str, Any]) -> dict[str, Any]:
    """Build + solve the CP-SAT intra-day model."""
    try:
        return _schedule_day_impl(payload)
    except Exception as exc:  # noqa: BLE001
        return {"status": "ERROR", "message": f"{type(exc).__name__}: {exc}", "unsat_core": []}


def _schedule_day_impl(payload: dict[str, Any]) -> dict[str, Any]:
    # Minimal scaffold: return FEASIBLE with the attractions echoed back.
    # Real model added in later tasks.
    schedule = []
    for a in payload.get("attractions", []):
        schedule.append({
            "type": "attraction",
            "name": a["name"],
            "start": a["open"],
            "end": a["open"],
        })
    return {
        "status": "FEASIBLE",
        "schedule": schedule,
        "total_transit_min": 0,
        "total_buffer_min": 0,
    }
