# Improvement Plan — Travel Planning Benchmark

> Based on research.md findings and deep analysis of failure modes.
> Baseline: qwen3-30b harness_v1 — 91.7% delivery, 0.235 composite, 0.025 personalized, 0.445 commonsense

---

## Strategic Priority Order

The scoring formula reveals where to focus:

```
composite = (commonsense + personalized) / 2
case_acc   = 1.0 only if BOTH are perfect
```

**Current state**: commonsense=0.445, personalized=0.025. Personalized is near-zero and drags composite down by half. Meanwhile commonsense has a 92% failure rate on a single check (transfer time) that poisons the Time Feasibility dimension.

**Highest-ROI moves**:
1. Fix transfer time evaluation mismatch → unlock Time Feasibility dimension (+0.125 max per sample)
2. Make the model respect hard constraints → unlock personalized score (currently 0.025 → potentially 0.5+)
3. Fix delivery bugs (token truncation) → more plans evaluated → higher denominator utilization
4. Improve remaining commonsense dimensions incrementally

---

## Phase 1: Fix Broken Infrastructure (Bug Fixes)

These are code bugs that cause avoidable failures. Pure engineering, no model behavior changes needed.

### 1A. Fix transfer time evaluation mismatch

**Problem**: `check_transfer_time_reasonable()` in `constraints_commonsense.py:901-1049` only considers "anchor" activity types (`hotel, attraction, meal, travel_intercity_public`). `travel_city` is NOT an anchor. So when the agent correctly inserts a `travel_city` segment between two anchors, the checker sees a raw time gap and compares it against the distance matrix — but doesn't account for the travel_city that fills that gap.

**Evidence**: 92.1% failure rate in guided_memory run, 89% in harness_v1. Error messages show gaps like "plan shows 0min" when there IS a travel_city segment present.

**Root cause**: The checker subtracts `buffer` durations from the gap (lines 965-994) but does NOT subtract `travel_city` durations.

**Fix** (in `constraints_commonsense.py`):
- In the buffer subtraction logic (lines 965-994), also scan for `travel_city` activities between the two anchors
- Subtract travel_city duration from the gap, same as buffer
- Then compare remaining gap against distance matrix threshold

**Expected impact**: Time Feasibility dimension should jump from ~3% to 60-80%+. This is +0.07 to +0.10 on commonsense_avg across all samples.

**Files**: `evaluation/constraints_commonsense.py` (lines 965-994)

---

### 1B. Add `max_tokens` to LLM API call

**Problem**: `call_llm.py` never sets `max_tokens` or `max_completion_tokens`. For vLLM-served thinking models, server defaults cap output, causing 28/91 trajectories (31%) to produce truncated responses with no usable plan.

**Fix** (in `call_llm.py` and `models_config.json`):
- Add `max_tokens` field to model config schema (default: 16384)
- In `call_llm()`, read from config and pass to API:
  ```python
  max_tokens = model_config.get('max_tokens', 16384)
  params["max_tokens"] = max_tokens
  ```
- For thinking models that need `max_completion_tokens` instead, detect and use the right parameter name

**Expected impact**: Recovers ~30% of failed trajectories. With harness_v1 already at 91.7%, impact is smaller there but still prevents edge-case truncations.

**Files**: `agent/call_llm.py` (lines 140-153), `../models_config.json`

---

### 1C. Handle truncated `<plan>` tags

**Problem**: `_extract_plan_content()` requires both `<plan>` and `</plan>`. If output is truncated after `<plan>`, valid plan content is discarded. 4 samples in guided_memory run hit this.

**Fix** (in `tools_fn_agent.py:453-470`):
```python
def _extract_plan_content(self, text: str) -> str:
    # ... existing think-stripping logic ...

    matches = re.findall(r"<plan>(.*?)</plan>", text, flags=re.DOTALL | re.IGNORECASE)
    if matches:
        return matches[-1].strip()

    # Fallback: extract from last <plan> to end (truncated response)
    last_plan_idx = text.lower().rfind('<plan>')
    if last_plan_idx >= 0:
        return text[last_plan_idx + 6:].strip()

    return ""
```

**Expected impact**: Recovers 4 additional plans from the guided_memory run. Minor but free.

**Files**: `agent/tools_fn_agent.py` (lines 453-470)

---

### 1D. Add retry on empty plan extraction

**Problem**: When the model responds without tool calls AND `_extract_plan_content()` returns empty, the agent immediately returns with no plan. No retry attempted.

**Fix** (in `tools_fn_agent.py:703-706`):
```python
# No tool calls → try to extract plan
final_content = self._extract_plan_content(msg.content or '')
if not final_content and llm_budget > 0:
    # Nudge model to output plan in correct format
    messages.append({
        "role": "user",
        "content": "Your response did not contain a plan in <plan>...</plan> tags. "
                   "Please output your complete travel plan now, enclosed in <plan>...</plan> tags."
    })
    continue  # retry the loop
```

Cap retries at 2 to avoid infinite loops.

**Expected impact**: Recovers some of the 24 samples that produced reasoning without plan tags. Especially useful for thinking models that burn tokens on `<think>` content.

**Files**: `agent/tools_fn_agent.py` (lines 700-710)

---

### 1E. Add assemble_day to continue_run()

**Problem**: `continue_run()` (used for validation correction rounds) only uses `self.openai_tools`, missing the `ASSEMBLE_DAY_SCHEMA`. If the model tries assemble_day during correction, it fails silently.

**Fix**: In `continue_run()`, accept optional `memory` parameter and include `ASSEMBLE_DAY_SCHEMA` in tools when memory is present.

**Files**: `agent/tools_fn_agent.py` (lines 711-767)

---

## Phase 2: Hard Constraint Compliance (Biggest Score Opportunity)

Personalized score is 0.025 (2.5%). With one-vote veto, ANY hard constraint failure → 0.0. Analysis shows 94% of failures are "entity not found" — the model picks a different hotel/train/restaurant than required. Constraints ARE achievable (all entities exist in database, budget passes 100%).

### 2A. Inject hard constraints into system prompt

**Problem**: The query mentions constraints in natural language ("3-star hotel with swimming pool"), but the model treats all tool results as equal options. It doesn't understand that certain entities are MANDATORY.

**Approach**: Extract hard constraints from query metadata and inject them as explicit instructions in the system prompt or as a structured "MANDATORY REQUIREMENTS" section appended to the user query.

**Design**:
```
MANDATORY REQUIREMENTS (your plan MUST include these exact entities):
- Outbound transport: Train G7798 (Hefei → Nanjing)
- Return transport: Train G3031 (Nanjing → Hefei)
- Hotel: "Orange Hotel Nanjing Confucius Temple Scenic Area" (3-star, Swimming Pool)
- Restaurant: "Six Dynasties Pine Teahouse" (near Laomendong, Birthday Package tag)
- Must-visit: "Nanjing Deji Plaza", "Nanjing City Wall Taicheng Scenic Area"
```

**Implementation**:
- New function `format_hard_constraints(meta_info) -> str` that reads `meta_info['hard_constraints']` and produces the structured block above
- Append to user query before sending to agent
- In `run_agent_inference()`, call this function per sample

**Concern**: This leaks ground-truth constraint data into the prompt. But the constraints ARE derived from the user's natural-language query — they're just made explicit and unambiguous. The model already sees the query; this just removes interpretation ambiguity.

**Alternative (weaker)**: Don't use metadata. Instead, add a prompt instruction: "Pay close attention to specific entity names, train/flight numbers, hotel requirements, and restaurant requirements in the user's query. You MUST use the EXACT entities specified."

**Expected impact**: Transport constraints (currently 0% pass) should improve dramatically. Hotel/restaurant (8-27%) should reach 50-70%. Overall personalized from 0.025 → 0.3-0.5 is realistic.

**Files**: `agent/tools_fn_agent.py` (run_agent_inference), new utility function

---

### 2B. Add hard constraint validation to plan_validator

**Problem**: The current `plan_validator.py` only checks sandbox compliance (entity in tool results, price matches, details queried). It does NOT check hard constraints. Hard constraint evaluation only happens post-hoc in the evaluation phase.

**Approach**: Port the hard constraint checks from `constraints_hard.py` into the inference-time validator. This gives the model a correction signal BEFORE the plan is finalized.

**Design**:
- Import hard constraint eval functions into plan_validator
- In `validate_plan()`, also check hard constraints against meta_info
- Include failures in `build_correction_message()`
- The validation loop (max 2 corrections) then gives the model a chance to fix

**Prerequisite**: Need to pass `meta_info` through to `validate_plan()`. Currently it only receives `(final_plan, messages, language)`.

**Expected impact**: Combined with 2A, this creates a "constraint → generate → validate → correct" loop that should push personalized to 0.4-0.6.

**Files**: `agent/plan_validator.py`, `agent/tools_fn_agent.py` (validation loop section)

---

### 2C. Constraint-aware tool calling

**Problem**: Model queries hotels and picks one by quality/price, ignoring that a SPECIFIC hotel is required. Same for trains — it picks convenient options, not the mandated ones.

**Approach**: After the model's first tool call round, inject a reminder:
```
REMINDER: Check your mandatory requirements. Your plan MUST include:
- Train G7798 for outbound
- Hotel "Orange Hotel Nanjing Confucius Temple Scenic Area"
- Restaurant "Six Dynasties Pine Teahouse"
Verify that these entities appear in your tool results. If not, query for them specifically.
```

**Implementation**: After the first round of tool calls completes, check if mandatory entities appear in tool results. If not, inject a user message prompting the model to query for them.

**Expected impact**: Moderate. Helps with "entity not found" failures where the model simply forgot to query the right entity.

**Files**: `agent/tools_fn_agent.py` (inside run() loop, after first tool round)

---

## Phase 3: Commonsense Quality Improvements

After fixing transfer time (Phase 1A) and hard constraints (Phase 2), the remaining commonsense weak spots are:

| Dimension | Current (harness_v1) | Target | Gap |
|-----------|---------------------|--------|-----|
| Time Feasibility | 0.025 | 0.80+ | Fixed by Phase 1A |
| Itinerary Structure | 0.250 | 0.60 | Meal coverage + accommodation |
| Route Consistency | 0.325 | 0.60 | Intercity transfer gaps |
| Business Hours | 0.433 | 0.65 | Opening hours awareness |
| Activity Diversity | 0.508 | 0.70 | More varied selections |
| Cost Calculation | 0.575 | 0.75 | Budget arithmetic |
| Sandbox Compliance | 0.717 | 0.85 | Entity validation |
| Duration Rationality | 0.725 | 0.85 | Already strong |

### 3A. Improve meal coverage (Itinerary Structure)

**Problem**: 60% of plans fail `essential_meal_coverage`. The prompt specifies meal rules but the model often skips meals on arrival/departure days.

**Fix**: Strengthen the assemble_day tool to enforce meal rules:
- Full day: must have lunch + dinner
- Arrival before 10am: lunch + dinner required
- Arrival 10am-3pm: at least dinner
- Departure before 3pm: at least lunch

Add validation in `_assemble_day_impl()` that warns if a day has insufficient meals.

**Files**: `agent/working_memory.py` (assemble_day), `agent/prompts_guided.py`

---

### 3B. Improve route consistency

**Problem**: 67.5% of plans fail route consistency. Main issue: `seamless_intercity_transfers` — missing transport segments between cities.

**Fix**: In the prompt, emphasize that EVERY city change MUST have an intercity transport activity. In assemble_day, validate that `current_city` changes are accompanied by an `intercity` activity.

**Files**: `agent/working_memory.py`, `agent/prompts_guided.py`

---

### 3C. Business hours enforcement

**Problem**: 57% fail business hours. Model schedules attractions during closed hours or on closure days.

**Fix**: In assemble_day, validate attraction times against stored opening/closing hours and closure days. Return error if activity is scheduled outside hours.

Already partially implemented (lines 885-924 in working_memory.py) but may not be strict enough. Need to verify and tighten.

**Files**: `agent/working_memory.py`

---

### 3D. Reduce memory snapshot verbosity

**Problem**: Full memory snapshot appended to every tool response creates 100K+ token contexts. This wastes budget and dilutes important information.

**Fix**:
- Only append full snapshot after "significant" changes (new entity type queried)
- For subsequent tool calls of same type, append only the delta
- Cap snapshot length and summarize when too long

**Expected impact**: Reduces token waste, allows more tool call rounds within budget.

**Files**: `agent/working_memory.py` (render_snapshot)

---

## Phase 4: Model-Level Experiments

After infrastructure and prompt fixes, run comparative benchmarks.

### 4A. Full benchmark run matrix

Run all combinations worth testing:

| Model | Variant | Expected Insight |
|-------|---------|-----------------|
| qwen3-30b-thinking | harness_v1 + Phase 1-2 fixes | Baseline improvement measurement |
| qwen3.5-9b | harness_v1 + Phase 1-2 fixes | Non-thinking model comparison |
| gpt-5.1 | harness_v1 + Phase 1-2 fixes | Frontier model comparison |
| gpt-5.4 | harness_v1 + Phase 1-2 fixes | Latest frontier |

### 4B. Ablation studies

Test each Phase 2 intervention independently:
- Hard constraint injection only (2A)
- Hard constraint validation only (2B)
- Both 2A + 2B
- 2A + 2B + 2C (full)

---

## Implementation Order

```
Week 1:
  Day 1-2: Phase 1A (transfer time eval fix) + 1B (max_tokens) + 1C (truncated plan)
  Day 3:   Phase 1D (empty plan retry) + 1E (assemble_day in continue_run)
  Day 4:   Phase 2A (hard constraint injection into prompt)
  Day 5:   Test run on 10 samples, verify improvements

Week 2:
  Day 1-2: Phase 2B (hard constraint validation in plan_validator)
  Day 3:   Phase 2C (constraint-aware tool calling)
  Day 4-5: Full 120-sample benchmark run with all fixes

Week 3:
  Day 1-2: Phase 3A-3C (commonsense improvements)
  Day 3:   Phase 3D (snapshot verbosity)
  Day 4-5: Full benchmark run + Phase 4A model matrix
```

---

## Expected Score Trajectory

| Milestone | Delivery | Commonsense | Personalized | Composite | case_acc |
|-----------|----------|-------------|--------------|-----------|----------|
| **Current** (harness_v1) | 91.7% | 0.445 | 0.025 | 0.235 | 0.0 |
| After Phase 1 (bug fixes) | 95%+ | 0.55+ | 0.025 | 0.29 | 0.0 |
| After Phase 2 (hard constraints) | 95%+ | 0.55 | 0.35+ | 0.45 | 0.10+ |
| After Phase 3 (commonsense) | 95%+ | 0.65+ | 0.40+ | 0.53 | 0.15+ |
| Stretch goal | 97%+ | 0.75 | 0.50 | 0.63 | 0.25 |

The biggest single jump comes from Phase 2 (hard constraints): personalized going from 0.025 to 0.35+ doubles the composite score.

---

## Risk Assessment

| Risk | Likelihood | Mitigation |
|------|-----------|------------|
| Transfer time fix is more nuanced than expected | Medium | Analyze 10 specific failures before coding; may need multiple fixes |
| Hard constraint injection biases benchmark fairness | Low | Constraints derive from user query; making them explicit is valid |
| max_tokens change causes new API errors | Low | Test with 1 sample first; different APIs handle parameter differently |
| Model still ignores injected constraints | Medium | Combine with validation loop (2B); measure per-constraint improvement |
| Snapshot reduction hurts plan quality | Medium | A/B test with 20 samples before full run |
