# Intra-Day CP-SAT Scheduler MVP — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the v4 entity-selection CP-SAT model and greedy scheduler with a day-scoped CP-SAT scheduler that owns intra-day ordering, timing, transit insertion, and buffer management. LLM owns all entity selection, day assignment, and meal-slot mapping.

**Architecture:** New `solver/day_scheduler.py` module with a single public function `schedule_day(payload: dict) → dict`. Agent calls it D times per trip (once per day), each call independent. CP-SAT model uses `NewIntervalVar` per activity, `NewBoolVar` for pairwise ordering (`next_ij`), `NewIntVar` for durations and buffers. Feasibility-only — no objective. Unsat core via `AddAssumption()` + `SufficientAssumptionsForInfeasibility()`. Agent retry is prompt-driven; no framework changes.

**Tech Stack:** Python 3.10+, OR-Tools CP-SAT (`ortools.sat.python.cp_model`), pytest for unit tests.

**Spec:** `docs/superpowers/specs/2026-04-24-scheduling-model-design.md` (commit `96f28f4`).

---

## File Structure

```
benchmark/deepplanning/travelplanning/
  solver/
    day_scheduler.py          [CREATE] CP-SAT intra-day model + schedule_day entrypoint (~400 LOC)
    test_day_scheduler.py     [CREATE] pytest unit tests (~200 LOC)
    cp_template.py            [DELETE] v4 entity selector (717 LOC)
    scheduler.py              [DELETE] v4 greedy (493 LOC)
    executor.py               [MODIFY] remove run_solver_template, run_solver_selection, format_solver_selection, check_plan_faithfulness
  agent/
    tools_fn_agent.py         [MODIFY] add SCHEDULE_DAY_SCHEMA + dispatch, remove RUN_SOLVER_SCHEMA / RESOLVE_CONSTRAINT_SCHEMA / ASSEMBLE_DAY_SCHEMA
    prompts_guided.py         [MODIFY] add harness_v5 prompt variant
    working_memory.py         [MODIFY] delete assemble_day method
    constraint_extractor.py   [DELETE] 306 LOC, no consumer after cp_template.py removal
  run.py                      [MODIFY] add harness_v5 choice; remove harness_v4 wiring
```

### Boundaries & responsibilities

- `day_scheduler.py` has ONE public function: `schedule_day(payload) -> dict`. Everything else is private (`_`-prefixed).
- The module depends only on `ortools.sat.python.cp_model` and stdlib — no agent, no working_memory, no sandbox.
- Commonsense temporal constants (meal windows, meal gap, station buffers) live at module top; not configurable per call.
- `test_day_scheduler.py` can be run in isolation without the agent stack.

---

## Task 1: Scaffold `day_scheduler.py` with smoke test

**Files:**
- Create: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Create: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

- [ ] **Step 1: Write the failing smoke test**

Create `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py` with:

```python
"""Unit tests for day_scheduler.schedule_day."""
from solver.day_scheduler import schedule_day


def _minimal_payload() -> dict:
    """Single attraction, no meals, no anchors — simplest feasible case."""
    return {
        "day_index": 1,
        "weekday": "Wed",
        "arrival": None,
        "departure": None,
        "start_location": "HOTEL",
        "end_location": "HOTEL",
        "attractions": [
            {
                "poi_id": "A1",
                "name": "Park",
                "open": "08:00",
                "close": "18:00",
                "stay_min": 30,
                "stay_max": 120,
            }
        ],
        "lunch_restaurant": None,
        "dinner_restaurant": None,
        "transits": {
            "('HOTEL', 'A1')": {"duration_min": 10},
            "('A1', 'HOTEL')": {"duration_min": 10},
        },
    }


def test_smoke_feasible_single_attraction():
    result = schedule_day(_minimal_payload())
    assert result["status"] == "FEASIBLE"
    assert isinstance(result["schedule"], list)
    assert any(evt["type"] == "attraction" and evt["name"] == "Park" for evt in result["schedule"])
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_smoke_feasible_single_attraction -v`

Expected: FAIL with `ModuleNotFoundError: No module named 'solver.day_scheduler'`

- [ ] **Step 3: Write minimal implementation**

Create `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`:

```python
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_smoke_feasible_single_attraction -v`

Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): scaffold day_scheduler module with smoke test

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Interval variables + business hours + feasible output

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

This task introduces the assumption-based constraint pattern used throughout — every "important" constraint (business hours, meal windows, precedence, etc.) is enforced via `OnlyEnforceIf(bool_lit)` + `AddAssumption(bool_lit)`, and a `label` is recorded. Later tasks (Task 7) read those labels when the model is infeasible.

- [ ] **Step 1: Write the failing test**

Add to `solver/test_day_scheduler.py`:

```python
def test_single_attraction_respects_business_hours():
    payload = _minimal_payload()
    payload["attractions"][0]["open"] = "09:00"
    payload["attractions"][0]["close"] = "17:00"
    payload["attractions"][0]["stay_min"] = 60
    payload["attractions"][0]["stay_max"] = 60

    result = schedule_day(payload)
    assert result["status"] == "FEASIBLE"
    attr_event = next(e for e in result["schedule"] if e["type"] == "attraction")
    start_min = _hhmm_to_min(attr_event["start"])
    end_min = _hhmm_to_min(attr_event["end"])
    assert 9 * 60 <= start_min
    assert end_min <= 17 * 60
    assert end_min - start_min == 60


def _hhmm_to_min(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_single_attraction_respects_business_hours -v`

Expected: FAIL — stub returns `start == open` with `end == open` (zero duration), so `end - start == 0 != 60`.

- [ ] **Step 3: Write minimal implementation**

Replace the body of `_schedule_day_impl` in `day_scheduler.py`:

```python
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
```

Add these helper classes and functions to `day_scheduler.py`:

```python
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
        events = []
        # Sort activities by solved start time
        solved = []
        for a in self._activities:
            start, end, dur, _ = self._vars[a.id]
            solved.append((solver.Value(start), solver.Value(end), a))
        solved.sort()
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
```

- [ ] **Step 4: Run tests (both should pass now)**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v`

Expected: PASS (both `test_smoke_feasible_single_attraction` and `test_single_attraction_respects_business_hours`).

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): add interval vars + business hours (assumption-labeled)

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Precedence + transit between activities

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

- [ ] **Step 1: Write the failing test**

Add to `solver/test_day_scheduler.py`:

```python
def test_two_attractions_ordered_with_transit():
    payload = _minimal_payload()
    payload["attractions"] = [
        {"poi_id": "A1", "name": "Morning POI", "open": "08:00", "close": "12:00",
         "stay_min": 60, "stay_max": 60},
        {"poi_id": "A2", "name": "Afternoon POI", "open": "13:00", "close": "18:00",
         "stay_min": 60, "stay_max": 60},
    ]
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 10},
        "('HOTEL', 'A2')": {"duration_min": 15},
        "('A1', 'A2')": {"duration_min": 20},
        "('A2', 'A1')": {"duration_min": 20},
        "('A1', 'HOTEL')": {"duration_min": 10},
        "('A2', 'HOTEL')": {"duration_min": 15},
    }
    result = schedule_day(payload)
    assert result["status"] == "FEASIBLE"
    attractions = [e for e in result["schedule"] if e["type"] == "attraction"]
    assert [e["name"] for e in attractions] == ["Morning POI", "Afternoon POI"]
    # Transit event should appear between them
    kinds = [e["type"] for e in result["schedule"]]
    assert "transit" in kinds
    # End of first + transit <= start of second
    m_end = _hhmm_to_min(attractions[0]["end"])
    a_start = _hhmm_to_min(attractions[1]["start"])
    assert a_start >= m_end + 20
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_two_attractions_ordered_with_transit -v`

Expected: FAIL — current `extract_feasible` doesn't emit `transit` events, and no precedence enforced.

- [ ] **Step 3: Write minimal implementation**

In `day_scheduler.py`, extend `_ModelBuilder` to add ordering/transit logic. Add this method and update `extract_feasible`:

```python
    def add_ordering_and_transit(self, transits: dict[str, dict]) -> None:
        """For each ordered pair (i, j) of activities, create next_ij boolean.
        Exactly one successor (or none = last). If next_ij = 1, then
        end_i + transit_ij + buffer_ij <= start_j.
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
                    # No transit data = cannot be adjacent. Forbid ordering.
                    # We model this by not creating a next_ij variable for this pair.
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

        # Each activity has at most one successor and at most one predecessor.
        for i in ids:
            succs = [self._next[(i, j)] for j in ids if (i, j) in self._next]
            if succs:
                self.model.Add(sum(succs) <= 1)
            preds = [self._next[(j, i)] for j in ids if (j, i) in self._next]
            if preds:
                self.model.Add(sum(preds) <= 1)

        # Require that all activities are connected in a chain:
        # every activity (n-1 of them) has exactly one successor except one.
        n = len(ids)
        all_next = list(self._next.values())
        if n >= 2 and all_next:
            self.model.Add(sum(all_next) == n - 1)
```

Update `_schedule_day_impl` to call this after adding all activities:

```python
def _schedule_day_impl(payload: dict[str, Any]) -> dict[str, Any]:
    builder = _ModelBuilder()
    activities = _collect_activities(payload)

    for a in activities:
        builder.add_activity(a)

    builder.add_ordering_and_transit(payload.get("transits", {}))

    solver = cp_model.CpSolver()
    solver.parameters.max_time_in_seconds = DEFAULT_TIME_LIMIT_S
    status = solver.Solve(builder.model)

    if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return builder.extract_feasible(solver)
    if status == cp_model.INFEASIBLE:
        return {"status": "INFEASIBLE", "unsat_core": [], "hint": ""}
    return {"status": "ERROR", "message": f"solver_status={status}", "unsat_core": []}
```

Update `extract_feasible` on `_ModelBuilder` to emit transit events between adjacent activities:

```python
    def extract_feasible(self, solver: cp_model.CpSolver) -> dict[str, Any]:
        # Sort activities by solved start time
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
            # If this has a solved successor in self._next, emit transit
            if idx + 1 < len(solved):
                next_a = solved[idx + 1][2]
                pair = (a.id, next_a.id)
                if pair in self._next and solver.Value(self._next[pair]) == 1:
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
```

- [ ] **Step 4: Run tests**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v`

Expected: all three tests PASS.

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): add pairwise ordering + transit chain

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: NoOverlap per day (belt-and-suspenders)

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

- [ ] **Step 1: Write the failing test**

Add to `solver/test_day_scheduler.py`:

```python
def test_forced_overlap_is_infeasible():
    """Two attractions whose business hours force an overlap."""
    payload = _minimal_payload()
    payload["attractions"] = [
        {"poi_id": "A1", "name": "POI1", "open": "10:00", "close": "11:30",
         "stay_min": 60, "stay_max": 60},
        {"poi_id": "A2", "name": "POI2", "open": "10:00", "close": "11:30",
         "stay_min": 60, "stay_max": 60},
    ]
    # Both must be 60min within a 90-min window; no transit slack can save it.
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 5},
        "('HOTEL', 'A2')": {"duration_min": 5},
        "('A1', 'A2')": {"duration_min": 5},
        "('A2', 'A1')": {"duration_min": 5},
        "('A1', 'HOTEL')": {"duration_min": 5},
        "('A2', 'HOTEL')": {"duration_min": 5},
    }
    result = schedule_day(payload)
    assert result["status"] == "INFEASIBLE"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_forced_overlap_is_infeasible -v`

Expected: FAIL or PASS depending on Task 3 state — if the precedence chain alone catches it, PASS early. Usually it will PASS because the `end_i + tau + buffer <= start_j` chain already prevents overlap. Verify by inspection: if PASS, NoOverlap is redundant but harmless; still add it per spec.

Even if it PASSes, proceed to Step 3 so NoOverlap is explicit.

- [ ] **Step 3: Write minimal implementation**

Add to `_ModelBuilder`:

```python
    def add_no_overlap(self) -> None:
        intervals = [self._vars[a.id][3] for a in self._activities]
        if intervals:
            self.model.AddNoOverlap(intervals)
```

And call it in `_schedule_day_impl` after `add_ordering_and_transit`:

```python
    builder.add_ordering_and_transit(payload.get("transits", {}))
    builder.add_no_overlap()
```

- [ ] **Step 4: Run tests**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): add NoOverlap per day

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Meal windows + meal-gap

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

- [ ] **Step 1: Write the failing test**

Add to `solver/test_day_scheduler.py`:

```python
def _meal_payload() -> dict:
    """Payload with one attraction + lunch + dinner to exercise meal constraints."""
    p = _minimal_payload()
    p["attractions"] = [
        {"poi_id": "A1", "name": "Museum", "open": "09:00", "close": "21:00",
         "stay_min": 60, "stay_max": 60},
    ]
    p["lunch_restaurant"] = {
        "poi_id": "R1", "name": "Lunch R",
        "open": "11:00", "close": "14:00",
        "stay_min": 60, "stay_max": 60,
    }
    p["dinner_restaurant"] = {
        "poi_id": "R2", "name": "Dinner R",
        "open": "17:00", "close": "21:00",
        "stay_min": 60, "stay_max": 60,
    }
    p["transits"] = {
        f"('{x}', '{y}')": {"duration_min": 10}
        for x in ["HOTEL", "A1", "R1", "R2"]
        for y in ["HOTEL", "A1", "R1", "R2"]
        if x != y
    }
    return p


def test_lunch_and_dinner_in_windows_with_gap():
    result = schedule_day(_meal_payload())
    assert result["status"] == "FEASIBLE"

    meals = [e for e in result["schedule"] if e["type"] == "meal"]
    assert len(meals) == 2
    lunch = next(e for e in meals if e["name"] == "Lunch R")
    dinner = next(e for e in meals if e["name"] == "Dinner R")

    lunch_start = _hhmm_to_min(lunch["start"])
    lunch_end = _hhmm_to_min(lunch["end"])
    dinner_start = _hhmm_to_min(dinner["start"])

    assert 11 * 60 + 30 <= lunch_start <= 13 * 60 + 30
    assert 17 * 60 <= dinner_start <= 20 * 60 + 30
    assert dinner_start - lunch_end >= 120
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_lunch_and_dinner_in_windows_with_gap -v`

Expected: FAIL — `_collect_activities` only looks at `attractions`; meals are ignored, so `len(meals) == 0`.

- [ ] **Step 3: Write minimal implementation**

Update `_collect_activities` in `day_scheduler.py` to include meals, and add meal-window + meal-gap constraints to `_ModelBuilder`:

```python
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
```

Add this method to `_ModelBuilder` and call it in `_schedule_day_impl`:

```python
    def add_meal_constraints(self, payload: dict[str, Any]) -> None:
        # Meal-start must fall in the meal window (separate from business hours
        # since restaurant open hours may be wider than the commonsense window).
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
```

Update `_schedule_day_impl`:

```python
    builder.add_ordering_and_transit(payload.get("transits", {}))
    builder.add_no_overlap()
    builder.add_meal_constraints(payload)
```

- [ ] **Step 4: Run tests**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v`

Expected: all four tests PASS.

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): add meal windows + meal-gap constraints

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Arrival + departure anchors

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

- [ ] **Step 1: Write the failing test**

Add to `solver/test_day_scheduler.py`:

```python
def test_arrival_anchor_bounds_first_activity():
    payload = _minimal_payload()
    payload["arrival"] = {"time": "09:00", "station_poi": "STN"}
    payload["start_location"] = "STN"
    payload["attractions"] = [
        {"poi_id": "A1", "name": "POI", "open": "08:00", "close": "18:00",
         "stay_min": 60, "stay_max": 60},
    ]
    payload["transits"] = {
        "('STN', 'A1')": {"duration_min": 20},
        "('A1', 'STN')": {"duration_min": 20},
    }
    result = schedule_day(payload)
    assert result["status"] == "FEASIBLE"
    attr = next(e for e in result["schedule"] if e["type"] == "attraction")
    # arrival 09:00 + STATION_EXIT_BUFFER_MIN (5) + transit 20 = 09:25 minimum
    assert _hhmm_to_min(attr["start"]) >= 9 * 60 + 25


def test_departure_anchor_forces_infeasibility_when_too_tight():
    payload = _minimal_payload()
    payload["departure"] = {"time": "12:00", "station_poi": "STN"}
    payload["end_location"] = "STN"
    payload["attractions"] = [
        {"poi_id": "A1", "name": "POI", "open": "10:00", "close": "18:00",
         "stay_min": 120, "stay_max": 120},
    ]
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 10},
        "('A1', 'STN')": {"duration_min": 30},
    }
    # POI requires 120min + 30min transit + 20min station buffer = 170min
    # Even starting at open (10:00), end of chain is 10:00 + 120 + 30 + 20 = 13:10 > 12:00
    result = schedule_day(payload)
    assert result["status"] == "INFEASIBLE"
```

- [ ] **Step 2: Run test to verify they fail**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_arrival_anchor_bounds_first_activity solver/test_day_scheduler.py::test_departure_anchor_forces_infeasibility_when_too_tight -v`

Expected: both FAIL — anchors are not yet enforced.

- [ ] **Step 3: Write minimal implementation**

Add to `_ModelBuilder`:

```python
    def add_arrival_anchor(self, payload: dict[str, Any]) -> None:
        arrival = payload.get("arrival")
        if not arrival:
            return
        arrival_time = _parse_hhmm(arrival["time"])
        station = arrival["station_poi"]
        # Every activity i's start must be >= arrival_time + exit_buffer + tau(station, i)
        # IF activity i is the "first" one on the day. We approximate by imposing
        # the bound unconditionally — transit chain from station already subsumes
        # this when that activity is first; when it's not first, the precedence
        # chain from an earlier activity will give a tighter bound anyway.
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
```

Update `_schedule_day_impl`:

```python
    builder.add_ordering_and_transit(payload.get("transits", {}))
    builder.add_no_overlap()
    builder.add_meal_constraints(payload)
    builder.add_arrival_anchor(payload)
    builder.add_departure_anchor(payload)
```

- [ ] **Step 4: Run tests**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v`

Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): add arrival + departure anchors

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 7: Unsat-core extraction

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/day_scheduler.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

Wires up the infeasible-output path. `SufficientAssumptionsForInfeasibility()` returns variable-index ints from the CP-SAT model; we map them back to the labels recorded in `_ModelBuilder._labels`.

- [ ] **Step 1: Write the failing test**

Add to `solver/test_day_scheduler.py`:

```python
def test_infeasible_returns_labeled_unsat_core():
    payload = _minimal_payload()
    payload["departure"] = {"time": "12:00", "station_poi": "STN"}
    payload["end_location"] = "STN"
    payload["attractions"] = [
        {"poi_id": "A1", "name": "POI", "open": "10:00", "close": "18:00",
         "stay_min": 120, "stay_max": 120},
    ]
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 10},
        "('A1', 'STN')": {"duration_min": 30},
    }
    result = schedule_day(payload)
    assert result["status"] == "INFEASIBLE"
    assert isinstance(result["unsat_core"], list)
    assert len(result["unsat_core"]) >= 1
    # The departure-anchor label should be in the core for this conflict.
    assert any("departure" in s for s in result["unsat_core"])
    assert result.get("hint", "") == ""
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_infeasible_returns_labeled_unsat_core -v`

Expected: FAIL — `unsat_core` is empty `[]` from the current INFEASIBLE branch.

- [ ] **Step 3: Write minimal implementation**

Add a method to `_ModelBuilder` and update the INFEASIBLE branch of `_schedule_day_impl`:

```python
    def extract_unsat_core(self, solver: cp_model.CpSolver) -> list[str]:
        core_indices = set(solver.SufficientAssumptionsForInfeasibility())
        labels: list[str] = []
        for lit, label in self._labels:
            if lit.Index() in core_indices:
                labels.append(label)
        return labels
```

Update `_schedule_day_impl`:

```python
    if status == cp_model.INFEASIBLE:
        return {
            "status": "INFEASIBLE",
            "unsat_core": builder.extract_unsat_core(solver),
            "hint": "",
        }
```

- [ ] **Step 4: Run tests**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v`

Expected: all PASS, including the new unsat-core test.

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/day_scheduler.py benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "feat(solver): extract labeled unsat_core on infeasibility

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 8: Error handling & duration-flexibility test

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py`

This task is mostly a test-only task — error handling is already wrapped in Task 1 (the outer try/except in `schedule_day`). We add one test that exercises the duration-variable feasibility lever (Task 2 already built the mechanism; we confirm it works) and one that exercises the error path.

- [ ] **Step 1: Write the failing tests**

Add to `solver/test_day_scheduler.py`:

```python
def test_duration_shrinks_to_stay_min_in_tight_day():
    """Day only has ~1h between open and anchor; POI has stay_min=30, stay_max=120.
    Solver must pick stay_min to fit.
    """
    payload = _minimal_payload()
    payload["departure"] = {"time": "11:00", "station_poi": "STN"}
    payload["end_location"] = "STN"
    payload["attractions"] = [
        {"poi_id": "A1", "name": "POI", "open": "10:00", "close": "18:00",
         "stay_min": 30, "stay_max": 120},
    ]
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 5},
        "('A1', 'STN')": {"duration_min": 5},
    }
    # From 10:00, need 30+5+20 = 55min to reach 10:55 < 11:00 → must pick stay=30
    result = schedule_day(payload)
    assert result["status"] == "FEASIBLE"
    attr = next(e for e in result["schedule"] if e["type"] == "attraction")
    duration = _hhmm_to_min(attr["end"]) - _hhmm_to_min(attr["start"])
    assert duration == 30


def test_malformed_payload_returns_error():
    result = schedule_day({"not": "a valid payload"})
    assert result["status"] == "ERROR"
    assert "message" in result
    assert result["unsat_core"] == []
```

- [ ] **Step 2: Run tests to verify them**

Run: `cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py::test_duration_shrinks_to_stay_min_in_tight_day solver/test_day_scheduler.py::test_malformed_payload_returns_error -v`

Expected: both PASS (Task 2 already implemented duration as a variable; Task 1 wrapped the outer `schedule_day` in try/except).

If either fails, the failure points at a bug in earlier tasks — do NOT add new code here; instead, diagnose and fix in the task where the behavior belongs.

- [ ] **Step 3: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/solver/test_day_scheduler.py
git commit -m "test(solver): verify duration flexibility + error-path handling

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 9: Wire `SCHEDULE_DAY_SCHEMA` into agent

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/agent/tools_fn_agent.py`

Adds the new tool schema and dispatch branch, parallel to the existing `run_solver` dispatch. Does NOT yet remove the old schemas — that happens in Task 12.

- [ ] **Step 1: Add `SCHEDULE_DAY_SCHEMA` near the existing schemas**

Add this block to `agent/tools_fn_agent.py` near `RUN_SOLVER_SCHEMA` (around line 110, after its definition):

```python
SCHEDULE_DAY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "schedule_day",
        "description": (
            "Given a set of entities already selected for a specific day "
            "(attractions, optionally a lunch and dinner restaurant, plus "
            "transit times between every pair of them), return a timed "
            "schedule with correct ordering, business-hour compliance, "
            "meal-window compliance, and transit/buffer insertion. "
            "Call once per day of the trip. "
            "If the response is INFEASIBLE, read unsat_core, adjust your "
            "selection (e.g., re-assign a day, swap a restaurant, pick a "
            "later inbound train), and call again. Limit 3 retries per day."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "payload": {
                    "type": "object",
                    "description": (
                        "See spec §5 for full schema. Required keys: "
                        "day_index (int), weekday (str), arrival (object|null), "
                        "departure (object|null), start_location (str), "
                        "end_location (str), attractions (list of POI objects), "
                        "lunch_restaurant (object|null), dinner_restaurant (object|null), "
                        "transits (object mapping \"('A', 'B')\" string keys to "
                        "{duration_min: int})."
                    ),
                },
            },
            "required": ["payload"],
        },
    },
}
```

- [ ] **Step 2: Add the dispatch branch**

In `agent/tools_fn_agent.py`, find the `run()` method's tool-call loop (around line 1040 where `call['name'] == 'run_solver'` is handled). Add a branch BEFORE the `run_solver` branch:

```python
                    # --- Special handling: schedule_day (v5 intra-day scheduler) ---
                    if call['name'] == 'schedule_day':
                        try:
                            from solver.day_scheduler import schedule_day
                        except ImportError:
                            from agent.solver.day_scheduler import schedule_day
                        payload = call['arguments'].get('payload', {})
                        if isinstance(payload, str):
                            import json as _json
                            try:
                                payload = _json.loads(payload)
                            except Exception:
                                pass
                        result = schedule_day(payload)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                        continue
```

- [ ] **Step 3: Add the same branch in the `_finalize_plan` path (second tool-call loop)**

Find the second tool-call loop around line 1290 (the one that already handles `assemble_day` and `run_solver`). Add the same `schedule_day` branch before those:

```python
                    # --- Special handling: schedule_day (v5 intra-day scheduler) ---
                    if call['name'] == 'schedule_day':
                        try:
                            from solver.day_scheduler import schedule_day
                        except ImportError:
                            from agent.solver.day_scheduler import schedule_day
                        payload = call['arguments'].get('payload', {})
                        if isinstance(payload, str):
                            import json as _json
                            try:
                                payload = _json.loads(payload)
                            except Exception:
                                pass
                        result = schedule_day(payload)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                        continue
```

- [ ] **Step 4: Quick sanity check — module imports cleanly**

Run: `cd benchmark/deepplanning/travelplanning && python -c "from agent.tools_fn_agent import SCHEDULE_DAY_SCHEMA; print(SCHEDULE_DAY_SCHEMA['function']['name'])"`

Expected output: `schedule_day`

- [ ] **Step 5: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/agent/tools_fn_agent.py
git commit -m "feat(agent): add schedule_day tool schema and dispatch

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 10: Add `harness_v5` prompt variant

**Files:**
- Modify: `benchmark/deepplanning/travelplanning/agent/prompts_guided.py`
- Modify: `benchmark/deepplanning/travelplanning/run.py`

- [ ] **Step 1: Add a new prompt variant at the bottom of `prompts_guided.py`**

Append to `agent/prompts_guided.py`:

```python
HARNESS_V5_PROMPT = """You are a travel planning agent. You pick entities; the solver times them.

## Your job

1. Read the user's request directly. There is no pre-extraction step — parse the NL yourself.
2. Use sandbox tools (query_hotels, query_trains, recommend_restaurants_near, query_attractions, get_distance_matrix, etc.) to pick:
   - Trains (outbound for Day 1, inbound for Day D)
   - A hotel (or two if spec differs per day)
   - Attractions, partitioned into a set for each day
   - A restaurant for lunch and/or dinner on each day (consider opening hours, tags, cuisine — whatever the user asked for)
3. For each day, call `schedule_day(payload)` with:
   - `day_index` (1-indexed)
   - `weekday` (for the day's date)
   - `arrival`: {time, station_poi} on Day 1 only; null otherwise
   - `departure`: {time, station_poi} on Day D only; null otherwise
   - `start_location`: "STATION" if Day 1 (arrival station), else the hotel's name/id
   - `end_location`: "STATION" if Day D (departure station), else the hotel's name/id
   - `attractions`: list of {poi_id, name, open, close, stay_min, stay_max}
   - `lunch_restaurant` / `dinner_restaurant`: null or a dict with the same shape
   - `transits`: dict mapping "('A', 'B')" → {duration_min} for EVERY pair that could be adjacent in the day's sequence. If you miss a pair, the solver cannot place them next to each other — query `get_distance_matrix` for all needed pairs before calling.
4. If `schedule_day` returns `"FEASIBLE"`, keep the schedule and move to the next day.
5. If it returns `"INFEASIBLE"`:
   - Read `unsat_core`. It lists the minimal set of conflicting constraints.
   - Decide what to change: re-assign an attraction to a different day, swap a restaurant for one with different hours, pick a later inbound train, drop an optional attraction, etc.
   - Call `schedule_day` again. Limit to 3 retries per day; after that, move on with a best-effort partial plan.
6. After all days are scheduled, assemble the final plan text yourself:
   - Stitch per-day `schedule` lists into the judge's expected markdown format.
   - **You are responsible for budget verification.** Sum prices × people/rooms; if over budget, go back and swap cheaper entities.
   - Include all required fields (transport, accommodation, meals, sightseeing).

## Rules

- NEVER write plans without calling `schedule_day`. Plans written manually are REJECTED.
- NEVER pre-commit a stay duration for an attraction — pass `stay_min` and `stay_max` from the sandbox; the solver picks the best duration within that range.
- ALWAYS pre-query all transit pairs for a day before calling `schedule_day`. The transit dict must cover every pair (start_location, POI_i), (POI_i, POI_j), (POI_k, end_location) that could be adjacent.
- If `schedule_day` returns `ERROR`, your payload is malformed — fix it and retry.
"""
```

- [ ] **Step 2: Register the new variant in `run.py`**

Find the argparse block that lists prompt choices (around line 153 in `run.py`). Update:

Before:
```python
choices=['default', 'explore', 'guided', 'guided_memory', 'harness_v1', 'harness_v2', 'harness_v3', 'harness_v4'],
```

After:
```python
choices=['default', 'explore', 'guided', 'guided_memory', 'harness_v1', 'harness_v2', 'harness_v3', 'harness_v4', 'harness_v5'],
```

And add the following wiring wherever the prompt variants are dispatched to `ToolsFnAgent.run()` (search for `'harness_v4'` in `run.py` to find the pattern). Use the same wiring pattern but without `extracted_constraints` (v5 skips extraction). Example:

```python
    elif args.prompt_variant == 'harness_v5':
        from agent.prompts_guided import HARNESS_V5_PROMPT
        system_prompt = HARNESS_V5_PROMPT
        # No constraint extraction for v5
        agent_kwargs = dict(
            enable_memory=True,
            compact_outputs=True,
            enable_solver=True,     # Needed to register schedule_day tool
            solver_version='v5',
        )
```

**Note:** the `solver_version='v5'` flag must be handled in `tools_fn_agent.py`. If `solver_version == 'v5'`, register `SCHEDULE_DAY_SCHEMA` instead of `RUN_SOLVER_SCHEMA`.

Update `tools_fn_agent.py` around line 898 (the `if enable_solver:` block):

Before:
```python
                if enable_solver:
                    schema = RUN_SOLVER_SCHEMA if solver_version == 'v4' else RUN_SOLVER_SCHEMA_V3
                    extra_tools.append(schema)
                    if solver_version == 'v4':
                        extra_tools.append(RESOLVE_CONSTRAINT_SCHEMA)
                    else:
                        extra_tools.append(ASSEMBLE_DAY_SCHEMA)
```

After:
```python
                if enable_solver:
                    if solver_version == 'v5':
                        extra_tools.append(SCHEDULE_DAY_SCHEMA)
                    elif solver_version == 'v4':
                        extra_tools.append(RUN_SOLVER_SCHEMA)
                        extra_tools.append(RESOLVE_CONSTRAINT_SCHEMA)
                    else:
                        extra_tools.append(RUN_SOLVER_SCHEMA_V3)
                        extra_tools.append(ASSEMBLE_DAY_SCHEMA)
```

- [ ] **Step 3: Integration smoke test on id_0**

From the `travelplanning/` directory, run a single-case smoke test:

```bash
cd benchmark/deepplanning/travelplanning && python run.py \
    --model gpt-5.2-2025-12-11-high \
    --language en \
    --workers 1 \
    --rerun-ids 0 \
    --prompt-variant harness_v5 \
    --output-dir /tmp/v5_smoke_id0
```

(`--rerun-ids` accepts a comma-separated list of sample IDs; passing `0` alone runs only id_0. If the target output dir is empty, the flag treats all IDs as "new runs"; if it contains prior trajectories, only the listed IDs are rerun.)

Expected: The run completes. Inspect `/tmp/v5_smoke_id0/trajectories/id_0.json` and confirm:
- The agent called `schedule_day` at least twice (once per day of id_0).
- `final_plan` is non-empty.

Exact pass/fail will depend on LLM behavior; a non-empty plan with `schedule_day` calls is the bar for this step.

- [ ] **Step 4: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add benchmark/deepplanning/travelplanning/agent/prompts_guided.py benchmark/deepplanning/travelplanning/agent/tools_fn_agent.py benchmark/deepplanning/travelplanning/run.py
git commit -m "feat(agent): add harness_v5 prompt variant wiring

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Task 11: Delete v4 dead code

**Files:**
- Delete: `benchmark/deepplanning/travelplanning/solver/cp_template.py`
- Delete: `benchmark/deepplanning/travelplanning/solver/scheduler.py`
- Delete: `benchmark/deepplanning/travelplanning/agent/constraint_extractor.py`
- Modify: `benchmark/deepplanning/travelplanning/solver/executor.py`
- Modify: `benchmark/deepplanning/travelplanning/agent/tools_fn_agent.py`
- Modify: `benchmark/deepplanning/travelplanning/agent/working_memory.py`
- Modify: `benchmark/deepplanning/travelplanning/run.py`

- [ ] **Step 1: Remove v4 wiring from `run.py`**

In `run.py`, update the prompt-variant choices:

Before:
```python
choices=['default', 'explore', 'guided', 'guided_memory', 'harness_v1', 'harness_v2', 'harness_v3', 'harness_v4', 'harness_v5'],
```

After:
```python
choices=['default', 'explore', 'guided', 'guided_memory', 'harness_v1', 'harness_v2', 'harness_v3', 'harness_v5'],
```

Remove the `elif args.prompt_variant == 'harness_v4':` block entirely (find by grep).

- [ ] **Step 2: Remove v4 schemas and dispatch from `agent/tools_fn_agent.py`**

Delete these definitions from `agent/tools_fn_agent.py`:
- `RUN_SOLVER_SCHEMA` (the v4 one around line 110) — keep `RUN_SOLVER_SCHEMA_V3`
- `RESOLVE_CONSTRAINT_SCHEMA` (around line 130)
- `ASSEMBLE_DAY_SCHEMA` (around line 163)

Delete the `run_solver` dispatch branch in `run()` (around line 1040-1110, the block starting with `if call['name'] == 'run_solver' and memory:`), AND any subsequent code that imports `format_solver_selection` or `run_solver_template`.

Delete the `assemble_day` dispatch branch in `run()` (around line 971-980).

Delete the identical dispatch branches in the `_finalize_plan` tool-call loop (around line 1288-1340 for assemble_day, 1303-1340 for run_solver).

Update the `if enable_solver:` block in `run()` (the one edited in Task 10):

Before (after Task 10):
```python
                if enable_solver:
                    if solver_version == 'v5':
                        extra_tools.append(SCHEDULE_DAY_SCHEMA)
                    elif solver_version == 'v4':
                        extra_tools.append(RUN_SOLVER_SCHEMA)
                        extra_tools.append(RESOLVE_CONSTRAINT_SCHEMA)
                    else:
                        extra_tools.append(RUN_SOLVER_SCHEMA_V3)
                        extra_tools.append(ASSEMBLE_DAY_SCHEMA)
```

After:
```python
                if enable_solver:
                    if solver_version == 'v5':
                        extra_tools.append(SCHEDULE_DAY_SCHEMA)
                    else:  # v3 retained for legacy path
                        extra_tools.append(RUN_SOLVER_SCHEMA_V3)
```

Remove the `extra_tools.append(ASSEMBLE_DAY_SCHEMA)` in the `else` branch (no-solver case around line 908).

Remove `extracted_constraints` parameter from `ToolsFnAgent.run()` signature and all references to it. Also remove the corresponding import and caller-side wiring in `run_agent_inference` (around line 1452 and 1492-1510).

- [ ] **Step 3: Remove `assemble_day` method from `working_memory.py`**

In `agent/working_memory.py`, find and delete the entire `assemble_day` method. Grep for `def assemble_day` to locate.

- [ ] **Step 4: Delete v4 files**

```bash
cd /data1/dannie/projects/Qwen-Agent
rm benchmark/deepplanning/travelplanning/solver/cp_template.py
rm benchmark/deepplanning/travelplanning/solver/scheduler.py
rm benchmark/deepplanning/travelplanning/agent/constraint_extractor.py
```

- [ ] **Step 5: Slim `solver/executor.py`**

Delete these functions from `solver/executor.py` (grep by name to find):
- `run_solver_template` (around line 309)
- `run_solver_selection` (around line 373)
- `format_solver_selection` (around line 420)
- `check_plan_faithfulness` (around line 571)
- `_validate_output_against_constraints` (around line 94)

Keep `run_solver_code` (line 200) — it's used by the v3 path which we are not touching in this plan.

- [ ] **Step 6: Verify no broken imports**

```bash
cd /data1/dannie/projects/Qwen-Agent/benchmark/deepplanning/travelplanning
python -c "from agent.tools_fn_agent import ToolsFnAgent, SCHEDULE_DAY_SCHEMA, RUN_SOLVER_SCHEMA_V3"
python -c "from solver.day_scheduler import schedule_day"
python -c "from solver.executor import run_solver_code"
```

Expected: no ImportError, no NameError.

If any import fails, you have a stale reference to the deleted code — grep for the stale name and clean up.

- [ ] **Step 7: Run all unit tests**

```bash
cd benchmark/deepplanning/travelplanning && python -m pytest solver/test_day_scheduler.py -v
```

Expected: all PASS.

- [ ] **Step 8: Commit**

```bash
cd /data1/dannie/projects/Qwen-Agent
git add -A benchmark/deepplanning/travelplanning/
git commit -m "refactor: delete v4 entity-selector, greedy scheduler, constraint extractor

v5 architecture (LLM selects, CP-SAT schedules per-day via day_scheduler)
makes the v4 pipeline dead code. Removes cp_template.py (717 LOC),
scheduler.py (493 LOC), constraint_extractor.py (306 LOC), and the
associated tool schemas and dispatch branches.

Co-Authored-By: Claude Opus 4.7 (1M context) <noreply@anthropic.com>"
```

---

## Spec Coverage Checklist

| Spec section | Covered by |
|---|---|
| §3 Architecture (new file `day_scheduler.py`, deletions, slimmed executor, tool swap, prompt update) | Tasks 1, 9, 10, 11 |
| §4 Activities, variables (start/end/duration/interval, next_ij, buffer_ij) | Tasks 2, 3 |
| §4 Business hours | Task 2 |
| §4 Meal windows, meal-gap | Task 5 |
| §4 Arrival/departure anchors | Task 6 |
| §4 Ordering & transit (precedence, buffer) | Task 3 |
| §4 NoOverlap | Task 4 |
| §4 Must-visit (implicit via instantiation) | Task 1 structural — always instantiates passed attractions |
| §4 Objective (none, feasibility-only) | Task 2 (no objective added) |
| §4 Time granularity (minutes) | Task 2 (`NewIntVar(0, 24*60, ...)`) |
| §4 Time budget 5s | Task 2 (`max_time_in_seconds = DEFAULT_TIME_LIMIT_S`) |
| §5 Input schema | Task 1 payload + Task 3 transits + Task 5 meals + Task 6 anchors |
| §5 FEASIBLE output | Tasks 2, 3 (events list, totals) |
| §5 INFEASIBLE output (unsat_core + hint="") | Task 7 |
| §5 ERROR output | Task 1 (outer try/except) + Task 8 (test) |
| §6 Retry policy (prompt-driven) | Task 10 prompt text (3-retry cap) |
| §6 Solver crash handling | Task 1 outer try/except |
| §6 Solver time budget (5s) | Task 2 |
| §6 Re-entrance | Task 1 (fresh CpModel per call — no globals) |
| §7 Unit tests (5 listed) | Tasks 1, 2, 3 (smoke + ordering), 5 (meal), 6 (anchor-infeasible), 7 (unsat-core), 8 (duration flex) |
| §7 Integration test on id_0 | Task 10 Step 3 |
| §7 Full benchmark | Not code — separate execution after this plan completes |
| §8 Limitations | Captured in spec; no code task needed |
| §9 Deliverables (files, tests, results dir, comparison) | Covered by Tasks 1–11; benchmark run is execution-time, not planning |

---

## Execution Notes

- **No worktree was created.** Plan executes on the current branch of `/data1/dannie/projects/Qwen-Agent`. If isolation is desired, create a worktree before starting (`git worktree add ../Qwen-Agent-v5 -b v5-mvp`).
- **The Qwen-Agent repo currently has 5 unstaged files unrelated to this plan** (`models_config.json`, `prompts_guided.py`, `tools_fn_agent.py`, `run.py`, `run.sh`). Before starting, decide: either commit them first (preserving that work), or stash them, or include them in one of the modify tasks if they happen to overlap.
- **Benchmark run is NOT part of this plan.** After Task 11 commits cleanly and unit tests pass, invoke the benchmark per §7 of the spec:
  ```bash
  BENCHMARK_MODEL=gpt-5.2-2025-12-11-high \
  BENCHMARK_LANGUAGE=en \
  BENCHMARK_OUTPUT_DIR=/home/dannie/projects/dp_result/results_v5_mvp \
  BENCHMARK_PROMPT_VARIANT=harness_v5 \
  bash benchmark/deepplanning/travelplanning/run.sh
  ```
  (Confirm the `run.sh` reads `BENCHMARK_PROMPT_VARIANT` — add it if not, parallel to `BENCHMARK_MODEL`.)
- **Go/no-go call** after the benchmark run lives in the spec §7; not in this plan.
