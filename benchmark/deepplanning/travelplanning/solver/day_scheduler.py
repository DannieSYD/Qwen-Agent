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

    builder.add_ordering_and_transit(payload.get("transits", {}))
    builder.add_no_overlap()
    builder.add_meal_constraints(payload)
    builder.add_arrival_anchor(payload)
    builder.add_departure_anchor(payload)

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = DEFAULT_TIME_LIMIT_S
    status = solver.Solve(builder.model)

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return builder.extract_feasible(solver)
    if status == cp_model.INFEASIBLE:
        return {
            "status": "INFEASIBLE",
            "unsat_core": builder.extract_unsat_core(solver),
            "hint": "",
        }
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
    lunch = payload.get("lunch_restaurant")
    if lunch:
        activities.append(_Activity(
            aid=lunch["poi_id"],
            name=lunch["name"],
            kind="meal",
            open_min=max(_parse_hhmm(lunch["open"]), LUNCH_WINDOW[0]),
            close_min=min(_parse_hhmm(lunch["close"]), LUNCH_WINDOW[1] + int(lunch["stay_max"])),
            stay_min=int(lunch["stay_min"]),
            stay_max=int(lunch["stay_max"]),
        ))
    dinner = payload.get("dinner_restaurant")
    if dinner:
        activities.append(_Activity(
            aid=dinner["poi_id"],
            name=dinner["name"],
            kind="meal",
            open_min=max(_parse_hhmm(dinner["open"]), DINNER_WINDOW[0]),
            close_min=min(_parse_hhmm(dinner["close"]), DINNER_WINDOW[1] + int(dinner["stay_max"])),
            stay_min=int(dinner["stay_min"]),
            stay_max=int(dinner["stay_max"]),
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

    def add_no_overlap(self) -> None:
        intervals = [self._vars[a.id][3] for a in self._activities]
        if intervals:
            self.model.AddNoOverlap(intervals)

    def add_meal_constraints(self, payload: dict[str, Any]) -> None:
        """Enforce meal-start windows (separate from business hours since
        restaurant open hours may extend beyond the commonsense meal window),
        and enforce a minimum gap between lunch and dinner.
        """
        lunch_id = (payload.get("lunch_restaurant") or {}).get("poi_id")
        dinner_id = (payload.get("dinner_restaurant") or {}).get("poi_id")

        if lunch_id and lunch_id in self._vars:
            start = self._vars[lunch_id][0]
            lit = self.model.NewBoolVar(f"lunch_window_{lunch_id}")
            self.model.Add(start >= LUNCH_WINDOW[0]).OnlyEnforceIf(lit)
            self.model.Add(start <= LUNCH_WINDOW[1]).OnlyEnforceIf(lit)
            self.model.AddAssumption(lit)
            self._labels.append((lit, f"lunch_window in [{_format_hhmm(LUNCH_WINDOW[0])}, {_format_hhmm(LUNCH_WINDOW[1])}]"))

        if dinner_id and dinner_id in self._vars:
            start = self._vars[dinner_id][0]
            lit = self.model.NewBoolVar(f"dinner_window_{dinner_id}")
            self.model.Add(start >= DINNER_WINDOW[0]).OnlyEnforceIf(lit)
            self.model.Add(start <= DINNER_WINDOW[1]).OnlyEnforceIf(lit)
            self.model.AddAssumption(lit)
            self._labels.append((lit, f"dinner_window in [{_format_hhmm(DINNER_WINDOW[0])}, {_format_hhmm(DINNER_WINDOW[1])}]"))

        if lunch_id and dinner_id and lunch_id in self._vars and dinner_id in self._vars:
            lunch_end = self._vars[lunch_id][1]
            dinner_start = self._vars[dinner_id][0]
            lit = self.model.NewBoolVar("meal_gap")
            self.model.Add(dinner_start - lunch_end >= MIN_MEAL_GAP_MIN).OnlyEnforceIf(lit)
            self.model.AddAssumption(lit)
            self._labels.append((lit, f"meal_gap >= {MIN_MEAL_GAP_MIN}min"))

    def extract_unsat_core(self, solver: cp_model.CpSolver) -> list[str]:
        """Map CP-SAT's minimal unsat assumption indices back to labeled strings."""
        core_indices = set(solver.SufficientAssumptionsForInfeasibility())
        labels: list[str] = []
        for lit, label in self._labels:
            if lit.Index() in core_indices:
                labels.append(label)
        return labels

    def add_arrival_anchor(self, payload: dict[str, Any]) -> None:
        """Day 1: every activity's start must be >= arrival + exit_buffer + transit(station, i).
        Imposing unconditionally gives the tightest bound on whichever activity ends up first;
        for non-first activities, the precedence chain provides a tighter bound anyway.
        """
        arrival = payload.get("arrival")
        if not arrival:
            return
        arrival_time = _parse_hhmm(arrival["time"])
        station = arrival["station_poi"]
        earliest_anywhere = arrival_time + STATION_EXIT_BUFFER_MIN
        transits = payload.get("transits", {})
        for a in self._activities:
            key = f"('{station}', '{a.id}')"
            tau = int(transits.get(key, {}).get("duration_min", 0))
            start = self._vars[a.id][0]
            lit = self.model.NewBoolVar(f"arrival_bound_{a.id}")
            self.model.Add(start >= earliest_anywhere + tau).OnlyEnforceIf(lit)
            self.model.AddAssumption(lit)
            self._labels.append((lit, f"arrival({arrival['time']}) + exit_buffer + transit -> {a.name}"))

    def add_departure_anchor(self, payload: dict[str, Any]) -> None:
        """Day D: every activity's end + transit(i, station) + entry_buffer <= departure."""
        departure = payload.get("departure")
        if not departure:
            return
        dep_time = _parse_hhmm(departure["time"])
        station = departure["station_poi"]
        latest_anywhere = dep_time - STATION_ENTRY_BUFFER_MIN
        transits = payload.get("transits", {})
        for a in self._activities:
            key = f"('{a.id}', '{station}')"
            tau = int(transits.get(key, {}).get("duration_min", 0))
            end = self._vars[a.id][1]
            lit = self.model.NewBoolVar(f"departure_bound_{a.id}")
            self.model.Add(end + tau <= latest_anywhere).OnlyEnforceIf(lit)
            self.model.AddAssumption(lit)
            self._labels.append((lit, f"{a.name} + transit + entry_buffer -> departure({departure['time']})"))

    def add_ordering_and_transit(self, transits: dict[str, dict]) -> None:
        """For each ordered pair (i, j) of activities, create next_ij boolean.
        At most one successor per activity. If next_ij = 1, then
        end_i + tau_ij + buffer_ij <= start_j. Exactly (n-1) successors in
        total, which forces all activities into a single chain.
        """
        ids = [a.id for a in self._activities]
        self._next: dict[tuple[str, str], cp_model.IntVar] = {}
        self._buffer: dict[tuple[str, str], cp_model.IntVar] = {}
        self._transit_min: dict[tuple[str, str], int] = {}

        for i in ids:
            for j in ids:
                if i == j:
                    continue
                key = f"('{i}', '{j}')"
                if key not in transits:
                    # No transit data = cannot be adjacent in that direction.
                    continue
                tau = int(transits[key]["duration_min"])
                self._transit_min[(i, j)] = tau
                n_ij = self.model.NewBoolVar(f"next_{i}_{j}")
                b_ij = self.model.NewIntVar(0, DAY_END_MIN, f"buf_{i}_{j}")
                self._next[(i, j)] = n_ij
                self._buffer[(i, j)] = b_ij
                start_j = self._vars[j][0]
                end_i = self._vars[i][1]
                self.model.Add(end_i + tau + b_ij <= start_j).OnlyEnforceIf(n_ij)

        # At most one successor and one predecessor per activity.
        for i in ids:
            succs = [self._next[(i, j)] for j in ids if (i, j) in self._next]
            if succs:
                self.model.Add(sum(succs) <= 1)
            preds = [self._next[(j, i)] for j in ids if (j, i) in self._next]
            if preds:
                self.model.Add(sum(preds) <= 1)

        # Force all activities into a single chain: exactly n-1 next arcs.
        n = len(ids)
        all_next = list(self._next.values())
        if n >= 2 and all_next:
            self.model.Add(sum(all_next) == n - 1)

    def extract_feasible(self, solver: cp_model.CpSolver) -> dict[str, Any]:
        solved = []
        for a in self._activities:
            start, end, dur, _ = self._vars[a.id]
            solved.append((solver.Value(start), solver.Value(end), a))
        solved.sort()

        events: list[dict[str, Any]] = []
        total_transit = 0
        total_buffer = 0
        for idx, (s_min, e_min, a) in enumerate(solved):
            events.append({
                "type": a.kind,
                "name": a.name,
                "start": _format_hhmm(s_min),
                "end": _format_hhmm(e_min),
            })
            if idx + 1 < len(solved):
                next_a = solved[idx + 1][2]
                pair = (a.id, next_a.id)
                if pair in getattr(self, "_next", {}) and solver.Value(self._next[pair]) == 1:
                    tau = self._transit_min[pair]
                    buf = solver.Value(self._buffer[pair])
                    transit_start = e_min
                    transit_end = transit_start + tau
                    events.append({
                        "type": "transit",
                        "from": a.name,
                        "to": next_a.name,
                        "start": _format_hhmm(transit_start),
                        "end": _format_hhmm(transit_end),
                    })
                    total_transit += tau
                    total_buffer += buf

        return {
            "status": "FEASIBLE",
            "schedule": events,
            "total_transit_min": total_transit,
            "total_buffer_min": total_buffer,
        }
