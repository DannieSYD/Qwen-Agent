# Intra-Day Scheduling Model — MVP Design

**Date**: 2026-04-24
**Status**: Design — pending implementation plan

## 1. Motivation

The QWC ablation (`2026-04-23-qwc-ablation-design.md`, run 2026-04-23) landed
in Band 1 per its decision matrix: Layer-1 bulletization does not lift
GPT-5.2-High's case_acc. After isolating the numeric-info confound, QWC-v2
reached case_acc 0.533 — within noise of the 0.575 paragraph baseline.

The ablation's residual failures concentrate on **intra-day scheduling**, not
entity selection or arithmetic:

| Failure mode (QWC-v2) | Count | Nature |
|---|---:|---|
| `reasonable_transfer_time` | 34 | Transit time between consecutive activities |
| Time Feasibility (dim, imperfect) | 34 | Broader time-chain correctness |
| `essential_meal_coverage` | 10 | Meal placement / inclusion |
| `essential_attraction_coverage` | 8 | Must-visit attractions missing |
| `cost_calc_correctness` | 4 | Arithmetic (near-ceiling, not the bottleneck) |

GPT-5.2-High's LLM-alone case_acc plateau ≈ 0.58 regardless of input format;
`reasonable_transfer_time` alone caps any LLM-only approach at ≤ 0.717.
Building a proper scheduling model — interval variables, precedence with
transit, NoOverlap, temporal chains — is the intervention the data points at.

The v4 solver (`cp_template.py`, 717 LOC) is a *binary entity-selection model*
that duplicates the sandbox's SQL filters; the actual scheduling lives in
`scheduler.py` (493 LOC of hand-rolled greedy). The refactor reverses the
allocation: **LLM owns selection (including day-assignment and meal-slot
mapping); solver owns intra-day timing, ordering, transit insertion, and
buffer management**.

## 2. Scope — What This MVP Is and Isn't

**This MVP is a day-scoped scheduling solver.** One `CpModel` per day, solved
independently. The LLM agent calls `schedule_day` D times per trip (D =
number of days) and stitches the results.

**Not in MVP:**
- No entity-presence booleans (LLM commits entities)
- No day-assignment variables (LLM commits day per entity)
- No cross-day coupling in the solver (LLM re-balances days itself)
- No objective function — first-feasible wins (solver is a feasibility checker)
- No budget constraint in solver (LLM verifies)
- No multi-city / flight handling (benchmark is single-city train-only)
- No closure-weekday check in solver (LLM is responsible; mis-assignment
  surfaces as generic "business hours infeasible")

**Deferred to follow-up work if MVP results warrant:**
- Objective function (min transit, max visit duration) if first-feasible
  schedules prove over-cramped
- `hint` field in the INFEASIBLE response (template-filled natural language)
  if LLM cannot recover from bare `unsat_core`
- Day-assignment as a solver decision variable (open decision #4 in
  `v5_design.md`) — becomes valuable at 5+ day trips
- Multi-day coupling if cross-day re-balancing loops become common

## 3. Architecture

```
                                     ┌────────────────────────────┐
 Agent loop (LLM, unchanged tools)   │   sandbox queries          │
 ────────────────────────────────    │   (existing)               │
 1. LLM reads NL query directly      │   query_hotels / trains /  │
    (no constraint-extraction step)  │   recommend_restaurants /  │
 2. LLM calls sandbox tools to pick  │   get_distance_matrix      │
    entities                         └────────────┬───────────────┘
 3. LLM decides day-assignment +                  │ raw data
    meal-slot mapping                             ▼
 4. LLM calls ────────────────────►  schedule_day(day, entities, transits)
    schedule_day per day                          │
 5. LLM reads result; if infeasible,              ▼
    re-plans and retries             ┌────────────────────────────┐
 6. LLM assembles final plan text    │   solver/day_scheduler.py  │
    + verifies budget itself         │   (NEW — CP-SAT intra-day) │
                                     └────────────────────────────┘
```

### File changes

| Component | Current | After |
|---|---|---|
| `solver/day_scheduler.py` | — | **New**, ~300-400 LOC. CP-SAT intra-day model + `schedule_day` entrypoint. |
| `solver/cp_template.py` | 717 LOC (entity selector) | **Deleted.** LLM owns selection. |
| `solver/scheduler.py` | 493 LOC (greedy) | **Deleted.** Replaced by `day_scheduler.py`. |
| `solver/executor.py` | 630 LOC | Slimmed. Drops `run_solver_template` dispatcher; keeps generic CP-SAT runner utilities if still useful. |
| `agent/working_memory.py` `assemble_day` | ~200 LOC | **Deleted.** |
| Constraint extraction pipeline | (feeds cp_template) | **Deleted.** No consumer under LLM-does-selection. |
| `agent/tools_fn_agent.py` | — | Swap tool `run_solver_template` → `schedule_day`. |
| `agent/prompts_guided.py` | — | Updated: "you pick entities + day + meal-slot; solver times each day you hand it. If INFEASIBLE, read `unsat_core` and retry." |

**Net:** ~1200+ LOC deleted (selector + greedy + assemble_day + extraction), ~400 LOC added (`day_scheduler.py`). Overall less code.

### Call pattern

Agent calls `schedule_day` **D times per trip**, each independent. No persistent solver state. Cross-day concerns (e.g., inbound-train deadline constraining Day D) surface as that day's unsat_core; LLM re-plans.

## 4. Solver Model

### Activities per day

- Arrival anchor (Day 1 only): fixed-start, zero-duration interval at train arrival time
- Each attraction assigned to this day
- Lunch restaurant (if LLM assigned)
- Dinner restaurant (if LLM assigned)
- Departure anchor (Day D only): fixed-end, zero-duration interval at train departure time

**Not modeled as intervals:** hotel check-in/out, breakfast.

### Variables

- `start_i`, `end_i`, `duration_i` — integer minutes-since-midnight, 1-minute granularity
- `duration_i ∈ [stay_min_i, stay_max_i]` — **variable**, bounded by POI's sandbox-provided stay range. Solver picks within this range; this is the main feasibility lever for tight days.
- `interval_i = NewIntervalVar(start_i, duration_i, end_i)`
- `next_ij ∈ {0, 1}` for each ordered pair `(i, j)` of same-day activities — "j immediately follows i"
- `buffer_ij ≥ 0` — slack-absorbing gap when `next_ij = 1`

### Constraints

**Temporal windows:**
- Business hours per activity: `open_i ≤ start_i ∧ end_i ≤ close_i`
- Meal windows (hardcoded commonsense): `lunch.start ∈ [11:30, 13:30]`, `dinner.start ∈ [17:00, 20:30]`
- Meal-to-meal gap (if both present): `dinner.start − lunch.end ≥ 120 min`
- Arrival anchor (Day 1): `first_activity.start ≥ arrival_time + station_exit_buffer + τ(station, first_poi)`
- Departure anchor (Day D): `last_activity.end + τ(last_poi, station) + station_entry_buffer ≤ departure_time`

**Ordering & transit:**
- Each activity has exactly one successor (except the last): `Σ_j next_ij ≤ 1` per i, `Σ_i next_ij ≤ 1` per j
- If `next_ij = 1`: `end_i + τ_ij + buffer_ij ≤ start_j`
- `AddNoOverlap([interval_i ...])` — belt-and-suspenders

**Must-visit:** implicit — the solver instantiates intervals for exactly the entities the LLM committed to.

### Objective

**None.** `solver.Solve(model)` returns the first feasible schedule. Rationale: the benchmark's `reasonable_transfer_time` is a threshold check, not a minimization target; any feasible schedule with the meal-gap and buffer constraints satisfied should pass. If MVP runs show first-feasible schedules are over-cramped, add an objective (`min total_transit + total_buffer` or `max total_visit_duration`) in a follow-up.

### Commonsense defaults (hardcoded in `day_scheduler.py`)

- Lunch window: `[11:30, 13:30]`
- Dinner window: `[17:00, 20:30]`
- Meal-to-meal gap: ≥ 120 min
- Station-exit buffer (arrival): 5 min (time from train to leaving station)
- Station-entry buffer (departure): 20 min (security + boarding)

These are not "extracted from user NL" — they're baked commonsense. User-specific specs (budget, "evening return", "hotel with pool") stay entirely with the LLM.

### Time granularity

1-minute buckets. With <100 intervals per day and no objective, CP-SAT handles minute resolution trivially. 5-minute buckets are a fallback only if search time becomes a problem.

### Solver time budget

`solver.parameters.max_time_in_seconds = 5.0` per call. Normal calls should complete in <1s; the cap is a safety net. Hitting the cap returns `ERROR`, not partial results.

## 5. Interface — `schedule_day`

### Input (one call per day)

```python
{
  "day_index": 1,
  "weekday": "Wed",
  "arrival":   {"time": "07:14", "station_poi": "Nanjing South"} | null,
  "departure": {"time": "17:48", "station_poi": "Nanjing South"} | null,
  "start_location": "Orange Hotel ..." | "arrival_station",
  "end_location":   "Orange Hotel ..." | "departure_station",
  "attractions": [
    { "poi_id": "...", "name": "City Wall Taicheng",
      "open": "08:00", "close": "17:30",
      "stay_min": 30, "stay_max": 120 },
    ...
  ],
  "lunch_restaurant":  { "poi_id": ..., "open": "11:00", "close": "14:00",
                         "stay_min": 45, "stay_max": 90 } | null,
  "dinner_restaurant": { ... } | null,
  "transits": {
    "(start_location, City Wall)": { "duration_min": 22 },
    "(City Wall, Lunch R)":        { "duration_min": 18 },
    ...     # LLM must pre-query every pair that might be adjacent
  }
}
```

Note: `transits` carries `duration_min` only. Distance and cost are not the solver's concern.

### Output — feasible case

```python
{
  "status": "FEASIBLE",
  "schedule": [
    { "type": "anchor",     "name": "Arrive Nanjing South", "time": "07:14" },
    { "type": "transit",    "from": "Nanjing South", "to": "City Wall",
      "start": "07:19", "end": "07:41" },
    { "type": "attraction", "name": "City Wall Taicheng",
      "start": "08:00", "end": "10:00" },
    { "type": "transit",    "from": "City Wall", "to": "Lunch R",
      "start": "10:00", "end": "10:18" },
    { "type": "meal",       "name": "Lunch R",
      "start": "11:30", "end": "12:30" },
    ...
  ],
  "total_transit_min": 68,
  "total_buffer_min":  42
}
```

LLM stitches per-day outputs into the final plan text for the judge and
independently verifies the budget sum.

### Output — infeasible case

```python
{
  "status": "INFEASIBLE",
  "unsat_core": [
    "attraction(City Wall Taicheng).close = 17:30",
    "attraction(City Wall Taicheng).stay_min = 30",
    "transit(City Wall → Nanjing South) = 22min",
    "departure.time = 17:48"
  ],
  "hint": ""
}
```

- `unsat_core`: obtained via CP-SAT `AddAssumption()` + `solver.SufficientAssumptionsForInfeasibility()` over labeled constraints. Minimal subset that causes infeasibility; no manual reasoning needed from us.
- `hint`: **empty string in MVP.** Field reserved for template-filled natural-language summaries in a follow-up; populated only if LLM-recovery evaluation shows bare `unsat_core` is insufficient.

### Output — error case

```python
{ "status": "ERROR", "message": "<str>", "unsat_core": [] }
```

Returned on exceptions (bad input shape, solver timeout, CP-SAT internal error). Agent treats as equivalent to INFEASIBLE with no usable core.

## 6. Error Handling & Retry Loop

- **Retry policy:** prompt-driven. Agent prompt: "If `schedule_day` returns INFEASIBLE, read `unsat_core`, decide what to change (re-assign a day, swap a restaurant, pick a later inbound train, etc.), and retry. Limit ≤ 3 retries per day."
- **Giveup:** existing `max_llm_calls` cap prevents runaway. Terminal-infeasible day: LLM submits best partial plan it has.
- **Solver crash:** try/except around `Solve()`; returns `ERROR`, agent treats as INFEASIBLE with no core.
- **Time budget:** 5.0s per call. Cap hit → ERROR, not partial result.
- **Re-entrance:** each call builds a fresh `CpModel`. No persistent state.

No new framework module for retry — purely prompt + existing budget.

## 7. Testing & Validation

### Unit tests on `solver/day_scheduler.py` (target 5-10 tests, <2s total)

1. **Trivial feasible:** 1 attraction, 1 meal, no anchor day → FEASIBLE with meal in its window.
2. **Anchor-bounded feasible (id_0 Day 1 shape):** arrival 07:14, 2 attractions with tight closure (City Wall 17:30), lunch + dinner → FEASIBLE with legal order.
3. **Anchor-bounded infeasible (id_0 Day 2 shape, forced conflict):** attraction closing 17:30 on departure day, inbound train 16:00 → INFEASIBLE with close + transit + departure in `unsat_core`.
4. **Meal-gap conflict:** restaurant hours that force lunch-end after 16:00 with dinner window → INFEASIBLE with meal-gap in core.
5. **Duration flexibility:** tight day where `stay_max` is infeasible but `stay_min` fits → FEASIBLE with duration at `stay_min`, proves variable-duration lever works.

### Integration test on id_0

Run full agent on id_0 with new tool. Verify: (a) `schedule_day` called twice, (b) plan passes `reasonable_transfer_time` + Time Feasibility, (c) case passes overall `case_acc`. Smoke test before full benchmark.

### Full benchmark comparison

Protocol matches QWC ablation: 120 EN cases, GPT-5.2-High, single seed, same judge.
Results dir: `/home/dannie/projects/dp_result/results_v5_mvp/gpt-5.2-2025-12-11-high_en/`.

Compare against:
- **Baseline** (paragraph NL, v4 pipeline, case_acc 0.575): headline comparison.
- **QWC-v2** (case_acc 0.533, 34 transfer-time failures, selection-unchanged): isolates the scheduling delta — v5 MVP and QWC-v2 share LLM-side selection; only scheduler differs.

### Success criteria (pre-committed)

| Metric | QWC-v2 | Target | Rationale |
|---|---:|---:|---|
| `reasonable_transfer_time` failures | 34 | **≤ 10** | Solver's direct job. If it can't kill this, the architecture is wrong. |
| Time Feasibility dim (perfect/120) | 86 | **≥ 110** | Solver's direct job; broader time-chain correctness. |
| case_acc | 0.533 | **≥ 0.65** | Must lift meaningfully above both QWC-v2 and baseline, else v5 isn't paying rent. |
| personalized_score | 0.808 | **no regression (±3 pp)** | Selection layer unchanged; a regression would mean we broke something we shouldn't. |
| cost_calc_correctness (perfect/120) | 116 | **no regression** | LLM still owns budget; should stay near-ceiling. |

### Diagnostic (inspect, not gated)

Distribution of `schedule_day` outcomes across 120 cases: fraction FEASIBLE first try / INFEASIBLE-recovered / INFEASIBLE-exhausted. Tells us whether the LLM-solver loop is actually working or whether first-shot dominates.

### Go/no-go after MVP run

- Hit case_acc ≥ 0.65 + transfer-time ≤ 10 → architecture validated. Plan Phase 2: objective function, day-assignment as solver var, multi-day coupling.
- Hit transfer-time but miss case_acc → solver works; LLM is bottleneck. Investigate where retry loops fail.
- Miss both → revisit design assumptions (likely: LLM handing solver bad inputs, or `unsat_core` not actionable enough).

## 8. Known Limitations

1. **Cross-day re-balancing is LLM-only.** If `schedule_day` returns INFEASIBLE and the fix requires moving an attraction to a different day, the LLM must detect that from the unsat_core and re-call `schedule_day` for both affected days. Solver offers no cross-day coordination.
2. **Closure-weekday surfaces as generic infeasibility.** A Wednesday-closed attraction assigned to a Wednesday produces "business hours infeasible" rather than "attraction closed Wednesday." LLM is responsible for the closure check. Defensive check in solver is a candidate for v2 if this becomes a common failure.
3. **No paired statistical test on the benchmark.** 120 cases, single seed. A case_acc delta within ~5 pp is considered noise. If the MVP lift is in that band, a follow-up 3-seed run is required before claiming a result.
4. **Single model (GPT-5.2-High).** MVP results may not generalize to weaker local models. The v5 architecture is being validated *for* the strong-model regime; generalization is a separate study.
5. **Benchmark scope: 2-day single-city trips.** Day-scoped architecture makes no claim about 7-day trips where day-assignment becomes combinatorially hard; that's the open decision #4 in `v5_design.md`.

## 9. Deliverables

1. New file `benchmark/deepplanning/travelplanning/solver/day_scheduler.py` — CP-SAT day-level model + `schedule_day` entrypoint.
2. Deletions: `cp_template.py`, `scheduler.py`, `WorkingMemory.assemble_day`, constraint-extraction pipeline (specific files TBD during implementation).
3. Updates: `tools_fn_agent.py` (tool swap), `prompts_guided.py` (prompt rewrite), `executor.py` (slim).
4. Unit test file (location under existing test conventions).
5. Integration-test script for id_0.
6. Benchmark results at `/home/dannie/projects/dp_result/results_v5_mvp/gpt-5.2-2025-12-11-high_en/`.
7. Comparison table (baseline vs. QWC-v2 vs. v5 MVP, all tracked metrics).
8. Post-run write-up appended to the Notion weekly page, with the go/no-go call from §7.
