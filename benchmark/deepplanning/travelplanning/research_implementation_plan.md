# Implementation Plan: Dual-Space Architecture for DeepPlanning

## Context
The CLAUDE.md describes a research vision — a Dual-Space Architecture (Outer Space: LLM-driven POMDP exploration + Inner Space: solver-driven NP-hard optimization) with VoI reasoning, constraint formalization, TSPTW routing, and fuzzy constraint handling. Currently, the codebase is a single ReAct loop with working memory and an `assemble_day` formatter — no solver, no constraint formalization, no feedback loop. This plan bridges the gap in 7 incremental, ablatable phases.

## Current State
- **ReAct agent loop** (`agent/tools_fn_agent.py:664-814`): LLM + 9 tools + working memory + assemble_day
- **Working memory** (`agent/working_memory.py`): accumulates flights/trains/hotels/attractions/restaurants/routes, renders snapshots
- **Evaluation** (`evaluation/`): 8-dimension commonsense score + hard constraint score, 120 test cases
- **Pipeline** (`run.py`): inference -> conversion -> evaluation, resumable

## Phased Implementation

### Phase 0: Baseline Measurement (1 day)
Run current `harness_v1` variant on all 120 English cases. Record per-case and aggregate metrics as `baseline_metrics.json`. No code changes.

---

### Phase 1: Constraint Formalization (1-2 weeks)
**Goal**: Extract formal constraint tuples from natural language queries at inference time.

**Create**:
- `agent/constraint_extractor.py`: `ConstraintTuple` dataclass + `extract_constraints(query, model)` using LLM call
  ```python
  @dataclass
  class ConstraintTuple:
      variable: str        # "hotel.star", "budget", "attraction.name"
      type: str            # categorical | numerical | spatial | temporal
      operator: str        # = | <= | >= | in | near | before | after
      value: Any
      certainty: str       # precise | fuzzy
      schedule_sensitivity: str  # existential | temporal
      layer: int           # 1=local, 2=global, 3=commonsense
  ```
- `evaluation/constraint_extraction_eval.py`: Compare extracted vs ground-truth `meta_info.hard_constraints`

**Modify**:
- `agent/working_memory.py`: Add `constraints`, `meta` fields; render in snapshot
- `agent/tools_fn_agent.py`: Call `extract_constraints()` before main loop for `harness_v2` variant

**Toggle**: `prompt_variant='harness_v2'` activates; `config.enable_constraint_extraction`

---

### Phase 2: Inner Space Solver MVP (2-3 weeks)
**Goal**: OR-Tools CP-SAT solver replaces manual plan assembly. Given accumulated data + constraints, find feasible plan.

**Create**:
- `solver/__init__.py`
- `solver/model_builder.py`: `TravelPlanModel` — decision vars for transport/hotel/attractions/restaurants per day, constraints for budget/entities/time/business hours
- `solver/solution_formatter.py`: Convert solver output to same text format as `assemble_day`
- `solver/solver_runner.py`: `solve_plan()` — build model, run with 30s limit, return plan or infeasibility info

**Modify**:
- `agent/tools_fn_agent.py`: Add `solve_plan` as callable tool; LLM decides when to invoke it
- `agent/working_memory.py`: Add `to_solver_input()` export method
- `tools/tool_schema_harness_v2_en.json`: Add `solve_plan` tool schema

**Toggle**: `config.enable_solver`, `config.solver_fallback_to_assemble_day`

**Dependency**: Phase 1 (constraints), `pip install ortools`

---

### Phase 3: TSPTW Route Optimization (2 weeks)
**Goal**: Optimize within-day visit ordering via Travelling Salesman with Time Windows.

**Create**:
- `solver/tsptw.py`: `solve_day_routing()` using OR-Tools routing solver (`pywrapcp`)

**Modify**:
- `solver/model_builder.py`: Replace simplified per-day ordering with TSPTW sub-solver
- `solver/solution_formatter.py`: Use optimized ordering

**Toggle**: `config.enable_tsptw`, `config.tsptw_max_nodes_per_day=8`

**Dependency**: Phase 2

---

### Phase 4: Outer-Inner Feedback Loop (2-3 weeks)
**Goal**: Benders-like loop — solver reports infeasibility/missing data -> LLM interprets -> targeted queries -> re-solve.

**Create**:
- `solver/feedback.py`: `SolverFeedback` (status, missing_data, binding_constraints, infeasibility_reason, relaxation_suggestions) + `format_feedback_for_llm()`

**Modify**:
- `solver/solver_runner.py`: Analyze IIS on infeasibility, enumerate missing routes/details
- `agent/tools_fn_agent.py`: `solve_plan` returns structured feedback; loop counter (max 3 retries)
- `agent/prompts_guided.py`: Add solver-feedback interpretation guidance

**Toggle**: `config.enable_solver_feedback`, `config.max_solver_retries=3`

**Dependency**: Phase 2-3

---

### Phase 5: VoI-Guided Query Strategy (2-3 weeks, parallel with 3-4)
**Goal**: Prioritize queries by Value of Information. Track what's known/unknown, suggest high-value next queries.

**Create**:
- `agent/voi_tracker.py`: `InformationState` (what's been queried), `estimate_voi()`, `suggest_next_queries()`, `is_saturated()`

**Modify**:
- `agent/working_memory.py`: Add `information_state` field, update on each tool call, render suggestions in snapshot

**Toggle**: `config.enable_voi`

**Dependency**: Phase 1 + Phase 2

---

### Phase 6: Fuzzy Constraint Handling (2 weeks, parallel with 3-4)
**Goal**: Membership functions + graduated relaxation for "near", "evening", etc.

**Create**:
- `solver/fuzzy_constraints.py`: `FuzzyConstraint` with `mu(x)`, `theta_min/theta_max`, predefined functions for spatial/temporal/monetary fuzzy terms

**Modify**:
- `solver/model_builder.py`: Use conservative threshold, relax on infeasibility
- `agent/constraint_extractor.py`: Identify fuzzy terms, assign membership functions

**Toggle**: `config.enable_fuzzy`, `config.fuzzy_initial_threshold`

**Dependency**: Phase 1 + Phase 2

---

## Central Configuration

**Create** `travelplanning/config.py`:
```python
@dataclass
class PipelineConfig:
    enable_constraint_extraction: bool = True   # Phase 1
    enable_solver: bool = True                  # Phase 2
    solver_time_limit_seconds: float = 30.0
    solver_fallback_to_assemble_day: bool = True
    enable_tsptw: bool = True                   # Phase 3
    tsptw_max_nodes_per_day: int = 8
    enable_solver_feedback: bool = True         # Phase 4
    max_solver_retries: int = 3
    enable_voi: bool = True                     # Phase 5
    enable_fuzzy: bool = True                   # Phase 6
    fuzzy_initial_threshold: str = "conservative"
```

## Dependency Graph
```
Phase 0 (Baseline)
    |
Phase 1 (Constraints) ──────────────────┐
    |                                     |
Phase 2 (Solver MVP) ───┬── Phase 5 (VoI)
    |                    |
Phase 3 (TSPTW)     Phase 6 (Fuzzy)
    |
Phase 4 (Feedback Loop)
```
Phases 5 & 6 can be developed in parallel with Phases 3 & 4.

## Expected Impact
| Phase | Metric Improvement | Primary Dimension |
|-------|-------------------|-------------------|
| 1 | +2-5% case_acc | Enables constraint awareness |
| 2 | +10-15% commonsense, +5-10% personalized | Cost Accuracy, Time Feasibility |
| 3 | +5-8% commonsense | Time Feasibility, Route Consistency |
| 4 | +5-10% personalized | Entity satisfaction via targeted queries |
| 5 | +3-5% composite | Data completeness, fewer budget exhaustions |
| 6 | +2-3% personalized | Fuzzy constraint satisfaction |

## Verification
Each phase is testable against the existing evaluation pipeline:
1. Run inference on 120 cases with new variant (`harness_v2` + relevant config flags)
2. Run conversion (unchanged)
3. Run evaluation (unchanged)
4. Compare metrics against Phase 0 baseline

## Critical Files
- `agent/tools_fn_agent.py` (main loop, lines 664-814)
- `agent/working_memory.py` (data accumulation, solver input)
- `evaluation/constraints_hard.py` (ground-truth constraints, 38 types)
- `evaluation/eval_converted.py` (scoring pipeline)
- `run.py` (pipeline orchestration)
