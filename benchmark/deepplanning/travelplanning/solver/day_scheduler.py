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
    builder = _ModelBuilder()
    activities = _collect_activities(payload)

    for a in activities:
        builder.add_activity(a)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = DEFAULT_TIME_LIMIT_S
    status = solver.Solve(builder.model)

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return builder.extract_feasible(solver)
    if status == cp_model.INFEASIBLE:
        return {"status": "INFEASIBLE", "unsat_core": [], "hint": ""}
    return {"status": "ERROR", "message": f"solver_status={status}", "unsat_core": []}


class _Activity:
    __slots__ = ("id", "name", "kind", "open_min", "close_min", "stay_min", "stay_max")

    def __init__(
        self,
        aid: str,
        name: str,
        kind: str,
        open_min: int,
        close_min: int,
        stay_min: int,
        stay_max: int,
    ):
        self.id = aid
        self.name = name
        self.kind = kind  # "attraction" | "meal" | "anchor"
        self.open_min = open_min
        self.close_min = close_min
        self.stay_min = stay_min
        self.stay_max = stay_max


def _collect_activities(payload: dict[str, Any]) -> list[_Activity]:
    activities: list[_Activity] = []
    for a in payload.get("attractions", []):
        activities.append(_Activity(
            aid=a["poi_id"],
            name=a["name"],
            kind="attraction",
            open_min=_parse_hhmm(a["open"]),
            close_min=_parse_hhmm(a["close"]),
            stay_min=int(a["stay_min"]),
            stay_max=int(a["stay_max"]),
        ))
    return activities


class _ModelBuilder:
    def __init__(self):
        self.model = cp_model.CpModel()
        self._activities: list[_Activity] = []
        self._vars: dict[str, tuple] = {}  # id -> (start, end, dur, interval)
        self._labels: list[tuple[cp_model.IntVar, str]] = []

    def add_activity(self, a: _Activity) -> None:
        start = self.model.NewIntVar(DAY_START_MIN, DAY_END_MIN, f"start_{a.id}")
        end = self.model.NewIntVar(DAY_START_MIN, DAY_END_MIN, f"end_{a.id}")
        dur = self.model.NewIntVar(a.stay_min, a.stay_max, f"dur_{a.id}")
        interval = self.model.NewIntervalVar(start, dur, end, f"iv_{a.id}")
        self._vars[a.id] = (start, end, dur, interval)
        self._activities.append(a)
        self._add_business_hours(a, start, end)

    def _add_business_hours(self, a: _Activity, start, end) -> None:
        lit = self.model.NewBoolVar(f"bh_{a.id}")
        self.model.Add(start >= a.open_min).OnlyEnforceIf(lit)
        self.model.Add(end <= a.close_min).OnlyEnforceIf(lit)
        self.model.AddAssumption(lit)
        self._labels.append((lit, f"business_hours({a.name}) in [{_format_hhmm(a.open_min)}, {_format_hhmm(a.close_min)}]"))

    def extract_feasible(self, solver: cp_model.CpSolver) -> dict[str, Any]:
        solved = []
        for a in self._activities:
            start, end, dur, _ = self._vars[a.id]
            solved.append((solver.Value(start), solver.Value(end), a))
        solved.sort()
        events = []
        for s_min, e_min, a in solved:
            events.append({
                "type": a.kind,
                "name": a.name,
                "start": _format_hhmm(s_min),
                "end": _format_hhmm(e_min),
            })
        return {
            "status": "FEASIBLE",
            "schedule": events,
            "total_transit_min": 0,
            "total_buffer_min": 0,
        }
