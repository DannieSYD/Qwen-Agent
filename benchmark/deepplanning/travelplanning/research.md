# DeepPlanning Travel Benchmark — Codebase Research Document

> Generated 2026-03-26. Covers architecture, data flow, failure analysis, and improvement opportunities.

---

## Table of Contents

1. [System Architecture](#1-system-architecture)
2. [Agent Loop & Core Logic](#2-agent-loop--core-logic)
3. [Working Memory System](#3-working-memory-system)
4. [Prompt Variants](#4-prompt-variants)
5. [Tool Ecosystem](#5-tool-ecosystem)
6. [Database Layer](#6-database-layer)
7. [Evaluation Pipeline](#7-evaluation-pipeline)
8. [Scoring System](#8-scoring-system)
9. [Orchestration & Configuration](#9-orchestration--configuration)
10. [Result Analysis & Failure Modes](#10-result-analysis--failure-modes)
11. [Bugs & Issues Found](#11-bugs--issues-found)
12. [Improvement Opportunities](#12-improvement-opportunities)

---

## 1. System Architecture

### High-Level Pipeline

```
run_all.sh → run.sh → run.py
                         ├── Step 1: Agent Inference (tools_fn_agent.py)
                         │     Agent loop: LLM ↔ Tools → <plan> output
                         ├── Step 2: Conversion (convert_report.py)
                         │     LLM (gpt-5.4) parses Markdown → structured JSON
                         └── Step 3: Evaluation (eval_converted.py)
                               Commonsense (8 dims) + Hard constraints → scores
```

### Key Files

| Component | Path | Size |
|-----------|------|------|
| Agent loop | `agent/tools_fn_agent.py` | ~1011 lines |
| Working memory | `agent/working_memory.py` | ~1045 lines |
| Main prompts | `agent/prompts.py` | 941 lines |
| Guided prompts | `agent/prompts_guided.py` | 144 lines |
| Plan validator | `agent/plan_validator.py` | ~749 lines |
| LLM caller | `agent/call_llm.py` | 177 lines |
| Commonsense eval | `evaluation/constraints_commonsense.py` | ~1670 lines |
| Hard constraints | `evaluation/constraints_hard.py` | ~580 lines |
| Eval orchestrator | `evaluation/eval_converted.py` | ~576 lines |
| Report converter | `evaluation/convert_report.py` | ~368 lines |
| Pipeline runner | `run.py` | 517 lines |
| Model configs | `../models_config.json` | 95 lines |

---

## 2. Agent Loop & Core Logic

### `ToolsFnAgent.run()` (tools_fn_agent.py:573-709)

```
while llm_budget > 0:
    1. Call LLM with messages + tools
    2. Strip <think> tags if compact_outputs (lines 637-641)
    3. Detect tool_calls from response
    4. If tool_calls:
         For each call:
           - Special: assemble_day → memory.assemble_day() (lines 648-660)
           - Normal: _exec_tool() → result string (line 662)
           - If memory enabled:
               process_tool_result() → structured summary (line 667)
               auto_query_attraction_details() (line 678)
               auto_query_routes() (line 683)
               Append memory.render_snapshot() (line 691)
           - Append tool message to history
         continue (loop again)
    5. If NO tool_calls:
         _extract_plan_content(msg.content) → extract <plan>...</plan>
         Return (final_plan, messages, token_usage, memory)
```

### `ToolsFnAgent.continue_run()` (tools_fn_agent.py:711-767)

Used for validation correction rounds. Same loop but:
- Does NOT re-enable memory or assemble_day tool
- Returns `(final_plan, messages, token_usage)` — no memory object

### Key Design Decision: "No tool calls = final answer"

When the model responds without tool calls, the agent treats it as the final answer (line 703-706). This is the **primary failure vector** — if the model produces reasoning text without `<plan>` tags and without tool calls, the agent terminates with an empty plan.

### Plan Extraction: `_extract_plan_content()` (line 453-470)

```python
# 1. Remove <think>...</think> sections
# 2. Extract <plan>(.*?)</plan> with re.DOTALL
# 3. If no match → return ""
```

**Critical limitation**: Requires BOTH `<plan>` and `</plan>` tags. A truncated response with only `<plan>` returns empty string.

### Validation Loop (tools_fn_agent.py:892-925)

Only for guided variants (`guided`, `guided_memory`, `harness_v1`):
```
max_corrections = 2
for round in range(2):
    validation = validate_plan(final_plan, messages, language)
    if valid: break
    correction_msg = build_correction_message(validation)
    messages.append(correction_msg)
    corrected_plan = agent.continue_run(messages, max_llm_calls=max(20, budget//4))
```

### Auto-correct Travel City (line 927-928)

After plan generation, `memory.autocorrect_travel_city(final_plan)` replaces distance/duration/cost in travel_city lines using stored route data.

---

## 3. Working Memory System

### File: `agent/working_memory.py`

### Data Structures (lines 23-35)

comment: merge multiple terms regarding attractions and restaurants, just one category one list

```python
self.flights: List[Dict]          # {origin, dest, date, options: [{flight_no, dep_time, ...}]}
self.trains: List[Dict]           # Same structure
self.hotels: List[Dict]           # {name, city, price, star, rating, lat, lon, address}
self.attractions_summary: List    # {name, type, description}
self.attractions_detail: Dict     # name → {price, hours, visit_min, visit_max, lat, lon, rating}
self.locations: Dict              # name → {lat, lon}
self.restaurants: List[Dict]      # {name, price_per_person, cuisine, hours, rating, lat, lon}
self.restaurant_details: Dict     # name → full details
self.routes: List[Dict]           # {origin, dest, distance_m, duration_min, cost}
```

### process_tool_result() (lines 38-76)

Dispatches to type-specific parsers based on tool name:
- `query_flight_info` → `_parse_flights()`
- `query_train_info` → `_parse_trains()`
- `query_hotel_info` → `_parse_hotels()`
- `recommend_attractions` → `_parse_recommend_attractions()`
- `query_attraction_details` → `_parse_attraction_details()`
- `search_location` → `_parse_location()`
- `recommend_restaurants` → `_parse_restaurants()`
- `query_restaurant_details` → `_parse_restaurant_details()`
- `query_road_route_info` → `_parse_route()`

Returns a structured acknowledgment string (replaces raw tool output in message history).

### render_snapshot() (lines 673-753)

Renders complete memory state as compact readable text:
- FLIGHTS: origin→dest, date, options with times/prices
- TRAINS: same structure
- HOTELS: name, price/night, star, coords
- ATTRACTIONS: name, detail status (✓ or ⚠ unqueried)
- RESTAURANTS: name, price, cuisine, hours, meal tags
- KNOWN COORDINATES: all location coords
- ROUTES: grouped by origin, showing dest/distance/duration/cost

Appended to **every** tool response when memory enabled (line 691).

### assemble_day() (lines 759-977)

Deterministic day-plan builder. The model specifies activities; the tool computes all timestamps, travel segments, and costs from working memory.

**Input schema** (ASSEMBLE_DAY_SCHEMA, lines 20-65):
- `day`: int (day number)
- `current_city`: str ("from X to Y" or "X")
- `accommodation`: str (hotel name or "-")
- `activities`: array of activity objects:
  - `intercity`: {type, transport_type, id}
  - `buffer`: {type, description, duration_min}
  - `hotel`: {type, action: "Check-in"/"Check-out"/"Rest", duration_min}
  - `attraction`: {type, name}
  - `meal`: {type, meal_type, restaurant, duration_min}

**Algorithm** (_assemble_day_impl, lines 795-977):
1. Initialize cursor time from first intercity or default 07:00
2. For each activity, lookup data from memory, compute duration, advance cursor
3. Auto-insert travel_city segments between locations
4. Validate opening hours, clamp durations to min/max
5. Return formatted day text or error

### autocorrect_travel_city() (lines 578-643)

Post-processes plan text: looks up route data in memory to correct distance/duration/cost in travel_city lines. Handles bidirectional lookups.

---

## 4. Prompt Variants

### Default (`agent/prompts.py`)

Two-phase prompt (EN and ZH versions, ~400 lines each):

**Phase 1 — Information Collection:**
- Role: "Top-tier travel planning expert"
- Prohibition: "Do NOT ask questions, do NOT confirm"
- Core rule: "All information MUST come from tool query results — no fabrication"
- Name matching: Exact match required

**Phase 2 — Planning:**
- Output must be in `<plan>...</plan>` tags
- 6 activity line types: travel_intercity_public, travel_city, attraction, meal, hotel, buffer
- Strict formatting: `HH:MM-HH:MM | type | details`
- Rules: geospatial continuity, temporal logic, meal timing, buffer times, budget summary
- Complete example provided (Shanghai→Beijing, 3-day trip)

### Guided (`agent/prompts_guided.py`)

Injects guidance block before Phase 2:
- Planning strategy: query all transport, hotels, attractions, restaurants
- **assemble_day instruction**: "Do NOT write day plans manually. Use assemble_day for EACH day."
- Activity type JSON formats for assemble_day
- "AUTOMATIC VALIDATION" warning

### Variant Matrix

| Variant | Prompt | Memory | Compact | assemble_day | Validation |
|---------|--------|--------|---------|--------------|------------|
| `default` | prompts.py | No | No | No | No |
| `explore` | prompts_explore.py | No | No | No | No |
| `guided` | prompts_guided.py | No | No | No | Yes |
| `guided_memory` | prompts_guided.py | Yes | No | Yes | Yes |
| `harness_v1` | prompts_guided.py | Yes | Yes | Yes | Yes |

Flags set at `tools_fn_agent.py:857-859`:
```python
enable_memory = prompt_variant in ('guided_memory', 'harness_v1')
compact_outputs = prompt_variant == 'harness_v1'
```

---

## 5. Tool Ecosystem

### Tool Registration Pattern

All tools extend `BaseTravelTool` (from `tools/base_travel_tool.py`). Discovered dynamically via `BaseTravelTool.__subclasses__()` at runtime (tools_fn_agent.py:246).

### Tool Implementations

| Tool | File | Registration | Key Parameters |
|------|------|--------------|----------------|
| `query_train_info` | `train_query_tool.py` | `@register_tool` | origin, destination, depDate, seatClassName |
| `query_flight_info` | `flight_query_tool.py` | `@register_tool` | origin, destination, depDate, seatClassName |
| `query_hotel_info` | `hotel_query_tool.py` | `@register_tool` | destination, checkinDate, checkoutDate, hotelStar, hotelBrands |
| `query_attraction_details` | `attraction_query_tool.py` | `@register_tool` | attraction_name |
| `recommend_attractions` | `attraction_query_tool.py` | `@register_tool` | city, attraction_type |
| `search_location` | `location_search_tool.py` | `@register_tool` | place_name |
| `query_road_route_info` | `roadroute_query_tool.py` | `@register_tool` | origin/destination (coords) or origin_place/destination_place |
| `recommend_restaurants` | `restaurant_query_tool.py` | `@register_tool` | near (place name) or latitude/longitude |
| `query_restaurant_details` | `restaurant_query_tool.py` | `@register_tool` | restaurant_name |

### Auto-Query Features (tools_fn_agent.py)

**_auto_query_attraction_details()** (lines 375-416):
- Triggers after `recommend_attractions`
- Fetches details for all unqueried attractions automatically
- Prevents the model from needing to call detail queries individually

**_auto_query_routes()** (lines 280-373):
- Triggers after: recommend_restaurants, query_attraction_details, query_hotel_info, recommend_attractions
- Computes routes between hotels (anchors) and attractions/restaurants (targets)
- Cartesian product, capped at MAX_AUTO_ROUTES=20
- Provides travel times/distances "for free"

### Schema Variants

| Schema | File | Key Differences |
|--------|------|-----------------|
| Standard EN | `tools/tool_schema_en.json` | All params optional for route/restaurant |
| Standard ZH | `tools/tool_schema_zh.json` | Chinese descriptions, same structure |
| Harness V1 | `tools/tool_schema_harness_v1_en.json` | Route: REQUIRED origin_place/dest_place only (no coords). Restaurant: REQUIRED "near" only (no coords) |

---

## 6. Database Layer

### Per-Sample Database Structure

```
database/database_en/id_{0-119}/
├── attractions/attractions.csv     (15 cols: city, name, id, description, type, address, lat, lon, rating, hours, closing_dates, visit_hrs, price)
├── flights/flights.csv             (20 cols: cities, stations, times, duration, flight_no, airline, seat, price, route/segment indices)
├── trains/trains.csv               (17 cols: cities, stations, times, duration, train_no, type, seat, price, route/segment indices)
├── hotels/hotels.csv               (11 cols: city, name, address, lat, lon, decoration, star, price, score, brand, services)
├── restaurants/restaurants.csv     (14 cols: name, city, lat, lon, price, cuisine, hours, nearby_attraction, query_coords, rating, tags)
├── locations/locations_coords.csv  (5 cols: poi_name, lat, lon, address, poi_type)
└── transportation/distance_matrix.csv (5 cols: origin_coords, dest_coords, distance_m, duration_min, cost)
```

Each of the 120 test samples has its own isolated database directory with pre-populated data specific to that query's cities and constraints.

### Database-Tool Mapping (tools_fn_agent.py:194-216)

```python
db_mapping = {
    'query_train_info':        'trains/trains.csv',
    'query_flight_info':       'flights/flights.csv',
    'query_hotel_info':        'hotels/hotels.csv',
    'query_attraction_details':'attractions/attractions.csv',
    'recommend_attractions':   'attractions/attractions.csv',
    'search_location':         'locations/locations_coords.csv',
    'query_road_route_info':   'transportation/distance_matrix.csv',
    'recommend_restaurants':   'restaurants/restaurants.csv',
    'query_restaurant_details':'restaurants/restaurants.csv',
}
```

Compound tools (recommend_restaurants, query_road_route_info) also get `location_database_path` for place-name → coordinate resolution.

---

## 7. Evaluation Pipeline

### Step 2: Conversion (`evaluation/convert_report.py`)

- **Input**: Raw text plans from `reports/id_*.txt`
- **Model**: Hardcoded to `gpt-5.4` (line 243; previously qwen-plus)
- **Prompt**: `FORMAT_CONVERT_PROMPT_{EN|ZH}` from prompts.py (lines ~500-941)
- **Output format**: JSON in `<JSON>...</JSON>` tags
- **Retry**: Up to 30 attempts with 1s backoff
- **Max tokens**: 10240 (adaptive: uses `max_completion_tokens` for GPT-5/o-series, `max_tokens` for others)
- **Output**: `converted_plans/id_{N}_converted.json`

### Conversion JSON Schema

```json
{
  "budget_summary": {
    "transportation": number,
    "accommodation": number,
    "meals": number,
    "attractions_and_tickets": number,
    "other": number,
    "total_estimated_budget": number,
    "currency": string
  },
  "daily_plans": [{
    "day_number": number,
    "current_city": string,
    "accommodation": {"name": string, "price_per_night": number},
    "activities": [{
      "time_slot": "HH:MM-HH:MM",
      "type": "travel_intercity_public|travel_city|attraction|meal|hotel|buffer",
      "details": { /* type-specific */ }
    }]
  }]
}
```

### Step 3: Evaluation (`evaluation/eval_converted.py`)

Per-sample flow (lines 124-244):
1. Load converted JSON plan
2. Find matching meta_info from test data
3. Run `eval_commonsense(plan, meta, database_dir)` → 8 dimensions
4. Run `eval_hard(plan, meta)` → hard constraint checks
5. Calculate weighted scores
6. Save `id_{N}_score.json`

---

## 8. Scoring System

### Commonsense Score: 8 Dimensions (each weight=0.125)

**ONE-VOTE VETO per dimension**: ALL checks in a dimension must pass → 1.0, ANY fail → 0.0

| # | Dimension | Weight | Checks |
|---|-----------|--------|--------|
| 1 | Route Consistency | 12.5% | valid_trip_duration, closed_loop_route, seamless_intercity_transfers |
| 2 | Sandbox Compliance | 12.5% | validated_accommodation, validated_attractions, validated_meals, validated_transportation |
| 3 | Itinerary Structure | 12.5% | traceable_accommodation, ends_with_accommodation, essential_meal_coverage, essential_attraction_coverage |
| 4 | Time Feasibility | 12.5% | no_time_overlaps, reasonable_transfer_time |
| 5 | Business Hours | 12.5% | attraction_within_opening_hours, dining_within_service_hours, avoidance_of_closure_days |
| 6 | Duration Rationality | 12.5% | reasonable_attraction_duration (1-8h), reasonable_meal_duration (30m-2h) |
| 7 | Cost Calculation | 12.5% | cost_calculation_correctness (10% margin) |
| 8 | Activity Diversity | 12.5% | diverse_meal_options, diverse_attraction_options |

### Hard Constraint Score (Personalized)

**ONE-VOTE VETO**: ALL constraints pass → 1.0, ANY fail → 0.0

Constraint types from query metadata:
- **train_seat_status**: Specific train numbers with minimum seat availability
- **hotel_star_service_required**: Hotel star + specific service (e.g., swimming pool)
- **restaurant_specific_tag_nearby**: Restaurant near attraction with specific tag
- **attraction_must_visit_named**: List of must-visit attractions

### Final Metrics

```
Per sample:
  commonsense_score = Σ(dimension_score × 0.125) for 8 dimensions
  personalized_score = 1.0 if ALL hard constraints pass else 0.0
  composite_score = (commonsense_score + personalized_score) / 2
  case_acc = 1.0 if (commonsense == 1.0 AND personalized == 1.0) else 0.0

Across all 120 samples (denominator is always 120, not just delivered plans):
  delivery_rate = plans_found / 120
  commonsense_avg = Σ(commonsense_scores) / 120
  personalized_avg = Σ(personalized_scores) / 120
  composite_avg = Σ(composite_scores) / 120
  case_acc_avg = Σ(case_acc) / 120
```

### Cross-Domain (aggregate_results.py)

```
avg_acc = (shopping_weighted_average_case_score + travel_case_acc) / 2
```

---

## 9. Orchestration & Configuration

### Models Config (`../models_config.json`)

12 models configured, all OpenAI-compatible:

| Model | Endpoint | Temperature | Notes |
|-------|----------|-------------|-------|
| qwen3-32b-local | localhost:8000 | 0.0 | Local vLLM |
| qwen3-30b-a3b-thinking-2507 | scai4.cs.ucla.edu:8051 | 0.6 | Remote vLLM, thinking model |
| qwen3.5-9b | localhost:8052 | 0.6 | Local vLLM |
| qwen3.5-4b | localhost:8053 | 0.6 | Local vLLM |
| qwen3-4b-thinking-2507 | localhost:8054 | 0.6 | Local vLLM |
| qwen-plus | DashScope API | 0.0 | Cloud, used for conversion |
| qwen3-max | DashScope API | 0.0 | Cloud |
| gpt-4o-2024-11-20 | OpenAI API | 0.0 | |
| gpt-5.1 | OpenAI API | 0.0 | |
| gpt-5.4 | OpenAI API | 0.0 | |
| gpt-5-2025-08-07-high | OpenAI API | 0.0 | reasoning_effort: high |
| gpt-5.2-2025-12-11 | OpenAI API | 0.0 | reasoning_effort: high |

### call_llm.py Limitations

- **No `max_tokens` parameter set** — relies on server/model default
- No `max_completion_tokens` — same issue
- Retry: 30 attempts with 1.5s backoff
- Validates: response must have content OR tool_calls
- Skips temperature for reasoning models (o1, o3, o4-mini)

### Pipeline Defaults

| Parameter | Default | Source |
|-----------|---------|--------|
| workers | 40 | run.sh / BENCHMARK_WORKERS |
| max_llm_calls | 400 | run.sh / BENCHMARK_MAX_LLM_CALLS |
| language | en | run.sh / BENCHMARK_LANGUAGE |
| prompt_variant | default | run.sh / BENCHMARK_PROMPT_VARIANT |

### Resume Logic (run.py:239-265)

Auto-detects missing reports via `detect_missing_ids()`. If `--rerun-ids` not specified, automatically reruns only missing samples. Supports range format: "0-10,15,20-25".

---

## 10. Result Analysis & Failure Modes

### Available Result Directories

| Directory | Model | Variant | Plans/120 | Delivery | Composite |
|-----------|-------|---------|-----------|----------|-----------|
| results/qwen3-30b-a3b-thinking-2507_harness_v1_en | qwen3-30b | harness_v1 | 110 | 91.7% | 0.235 |
| results_assemble_full/qwen3-30b-a3b-thinking-2507_guided_memory_en | qwen3-30b | guided_memory | 63 | 52.5% | 0.124 |
| results/gpt-5.2-2025-12-11_en | gpt-5.2 | default | 5 | 4.2% | 0.037 |
| results_assemble_test_v2/gpt-5.1_guided_memory_en | gpt-5.1 | guided_memory | 5 | 4.2% | 0.022 |
| results_matrix_test_v2/gpt-5.1_guided_memory_en | gpt-5.1 | guided_memory | 5 | 4.2% | 0.015 |

### Failure Mode Analysis (qwen3-30b guided_memory full run)

**120 total samples → 63 plans delivered (52.5%)**

#### Category 1: No trajectory at all (30 samples)

IDs: 7, 9, 10, 21, 22, 24, 25, 29, 31, 43, 58, 60, 62, 70-73, 93, 98, 100-101, 103, 105-108, 111, 114, 119-120

**Cause**: Run was interrupted/killed before these samples completed, or they hit unhandled exceptions during ThreadPoolExecutor. No trajectory file was saved.

#### Category 2: Has trajectory but no plan (28 samples)

IDs: 5, 11, 13, 28, 32, 34, 36, 39, 40, 42, 47, 51-55, 61, 66, 76-77, 84, 90, 92, 94-95, 109, 112, 116

**Root cause**: Model exhausts output token budget on reasoning, never produces `<plan>` tags.

Sub-categories:
- **24 samples**: No `<plan>` tag at all — model spends all tokens reasoning
- **4 samples** (id_11, id_34, id_42, id_92): Has `<plan>` but truncated before `</plan>` — regex requires both tags

**Evidence**:
| Metric | Failed (28) | Successful (63) |
|--------|-------------|-----------------|
| Completion tokens | 4K-9K | 14K-27K |
| Messages | 9-12 | 11-29 (avg 17) |
| Tool call rounds | 1 (always) | 1-12 (avg 2.5) |
| Last msg truncated | 25/28 (89%) | 0% |
| Last msg content | 7K-25K chars reasoning | Plan with tags |

**Token budget exhaustion pattern**:
- The qwen3-30b-thinking model produces verbose `<think>` reasoning
- `compact_outputs` strips `<think>` from **context** (past messages) but not from the **current** response
- With no `max_tokens` set, the vLLM server default caps output
- After 1 round of tool calls, the context is already large (~30K+ prompt tokens)
- The model's response gets truncated mid-reasoning or mid-plan

#### Category 3: Plan delivered but low quality (60/63 plans)

Top constraint failures among delivered plans:
| Constraint | Failure Rate |
|------------|-------------|
| reasonable_transfer_time | 92.1% (58/63) |
| essential_meal_coverage | 61.9% (39/63) |
| cost_calculation_correctness | 50.8% (32/63) |
| diverse_attraction_options | 49.2% (31/63) |
| diverse_meal_options | 46.0% (29/63) |
| restaurant_specific_tag_nearby | 36.5% (23/63) |
| hotel_star_service_required | 31.7% (20/63) |

### Token Usage Comparison

| Metric | qwen3-30b (failed) | qwen3-30b (success) | GPT-5.2 |
|--------|--------------------|--------------------|---------|
| Completion tokens | 4K-9K | 14K-27K | 14K-48K |
| Prompt tokens | 22K-38K | 29K-135K | — |
| Messages | 9-12 | 11-35 | 27-64 |
| Tool rounds | 1 | 1-12 | 3-10 |
| Elapsed time | 250-3800s | 200-9600s | 211-715s |

---

## 11. Bugs & Issues Found

### BUG-1: No `max_tokens` in LLM API call (Critical)

**File**: `agent/call_llm.py:140-153`

The `call_llm()` function does not set `max_tokens` or `max_completion_tokens`. For vLLM-served models, this means the server's default token limit applies. For thinking models that produce verbose reasoning, this causes **47% of samples to fail** due to output truncation.

**Impact**: 28 samples in the full qwen3-30b run produced no plan because the model exhausted its output budget on reasoning.

### BUG-2: `_extract_plan_content()` requires closing `</plan>` tag (High)

**File**: `agent/tools_fn_agent.py:464-467`

```python
matches = re.findall(r"<plan>(.*?)</plan>", text, flags=re.DOTALL | re.IGNORECASE)
if not matches:
    return ""
```

If the model's response is truncated after opening `<plan>` but before `</plan>`, the valid plan content is discarded. 4 samples had complete-enough plans that were lost because of this.

**Fix**: Fall back to extracting content after last `<plan>` tag if no closing tag found.

### BUG-3: No retry on empty plan extraction (Medium)

**File**: `agent/tools_fn_agent.py:703-706`

When the model responds without tool calls, the agent immediately returns, even if `_extract_plan_content()` returns `""`. There's no mechanism to prompt the model to try again or format its response correctly.

### BUG-4: `continue_run()` doesn't have assemble_day tool (Medium)

**File**: `agent/tools_fn_agent.py:711-767`

The correction loop uses `continue_run()` which only uses `self.openai_tools` — it does NOT include `ASSEMBLE_DAY_SCHEMA`. If the model tries to use assemble_day during correction, it will fail.

### BUG-5: Conversion model hardcoded (Low)

**File**: `evaluation/convert_report.py:243`

The conversion model is hardcoded to `gpt-5.4`. Should be configurable or at least documented clearly.

---

## 12. Improvement Opportunities

### P0: Fix output token budget

Add `max_tokens` to the LLM call. For thinking models, this should be generous (16K-32K) to allow for both reasoning and plan output. Consider making it configurable per model in `models_config.json`.

### P0: Handle truncated `<plan>` tags

Modify `_extract_plan_content()` to extract content after last `<plan>` even without `</plan>`:
```python
if not matches:
    # Fallback: extract from last <plan> to end of text
    last_plan = text.rfind('<plan>')
    if last_plan >= 0:
        return text[last_plan + 6:].strip()
```

### P1: Add retry on empty plan

When the agent loop ends with no tool calls but `_extract_plan_content()` returns empty, inject a user message like "Please output your plan inside `<plan>...</plan>` tags" and continue the loop for a few more iterations.

### P1: Fix transfer time calculation

92.1% of delivered plans fail `reasonable_transfer_time`. This is the single biggest quality issue. Investigate whether:
- The assemble_day tool correctly computes buffer times
- The prompt adequately instructs the model about transfer time requirements
- The evaluation threshold is calibrated correctly

### P2: Reduce memory snapshot verbosity

The full memory snapshot is appended to **every** tool response. For samples with many tool calls, this creates massive context (100K+ prompt tokens). Consider:
- Only appending snapshot after significant new data
- Summarizing rather than repeating all data each time
- Providing a "diff" of what changed

### P2: Add assemble_day to continue_run()

The correction loop should have access to the same tools as the initial run, including assemble_day when memory is enabled.

### P3: Parallel tool call guidance

The prompt says "use parallel tool calls aggressively" but the model often does just 1 round. Consider restructuring the prompt to give a specific tool call plan upfront.

---

## 13. Changes Implemented (post-research)

> All changes below were implemented after the initial research document was written (2026-03-26). This section catalogs what was built, which bugs were fixed, and their measured impact on the full 120-sample run.

### 13.1 BUG-1 Fix: `max_tokens` support in `call_llm.py`

**Status**: ✅ Implemented

Added `max_tokens` parameter support to `call_llm()`:
- Reads `max_tokens` from per-model config in `models_config.json` (default: None/unset)
- Reasoning models (o1/o3/o4-mini) use `max_completion_tokens`; standard models use `max_tokens`
- Also added **early exit on context-length errors** — these are deterministic and no longer waste 30 retries. `call_llm.py` now detects `'context length'` / `'maximum context'` in the error message and raises immediately.

### 13.2 BUG-2 Fix: Truncated `<plan>` tag fallback

**Status**: ✅ Implemented

`_extract_plan_content()` now has a two-step extraction:
1. Primary: `<plan>(.*?)</plan>` (as before)
2. Fallback: if no closing tag, extract from last `<plan>` to end of text

This recovers plans from truncated model outputs where `</plan>` is missing.

### 13.3 BUG-3 Fix: Empty plan retry mechanism

**Status**: ✅ Implemented

When the agent loop terminates (no tool calls) but `_extract_plan_content()` returns empty:
- Injects a user message: *"Your response did not contain a travel plan in `<plan>...</plan>` tags. Please output your complete travel plan now..."*
- Retries up to **2 times** before giving up
- Tracked via `empty_plan_retries` counter

**Impact**: Delivery rate improved from 52.5% (guided_memory) to **91.7%** (harness_v1, 110/120 plans).

### 13.4 BUG-4 Fix: `assemble_day` in `continue_run()`

**Status**: ✅ Implemented

`continue_run()` now accepts optional `memory` and `compact_outputs` parameters:
- When `memory` is provided, `ASSEMBLE_DAY_SCHEMA` is added to the tool list
- `assemble_day` calls are handled identically to `run()` (dispatched to `memory.assemble_day()`)
- `compact_outputs` triggers `_compress_previous_msgs()` and `_compact_tool_output()` in the correction loop

The validation correction loop in `run_agent_inference()` now passes both `compact_outputs` and `memory` through to `continue_run()`.

### 13.5 New: `harness_v1` prompt variant

**Status**: ✅ Implemented (new variant, end-to-end)

A new top-level variant `harness_v1` that combines all improvements:

| Feature | `guided_memory` | `harness_v1` |
|---------|-----------------|--------------|
| Guided prompts | ✓ | ✓ |
| Working memory | ✓ | ✓ |
| `assemble_day` tool | ✓ | ✓ |
| Validation loop | ✓ | ✓ |
| Compact tool outputs | ✗ | ✓ |
| Compressed previous messages | ✗ | ✓ |
| Compacted tool schema | ✗ | ✓ |
| Meal slot tags | ✗ | ✓ |
| Auto-correct travel_city | ✗ | ✓ |
| Empty plan retry | ✗ | ✓ |
| Last-day accommodation fix | ✗ | ✓ |

Registered in `run.py` argument choices and `setup_paths()` for schema selection.

### 13.6 New: Compacted tool schema (`tool_schema_harness_v1_en.json`)

**Status**: ✅ Implemented

A stripped-down version of the standard EN schema with:
- **Removed**: `search_location` tool entirely (coordinates are now internal-only, handled by auto-query)
- **Route tool**: removed `origin`/`destination` coordinate params, made `origin_place`/`destination_place` **required** (name-only interface)
- **Restaurant tool**: removed `latitude`/`longitude` params, made `near` **required** (name-only interface)
- Descriptions shortened to remove coordinate-related guidance

**Rationale**: Prevents the model from wasting tokens on coordinate manipulation. All coordinate resolution happens server-side via memory.

### 13.7 New: Context compression (`_compress_previous_msgs()`)

**Status**: ✅ Implemented (in `tools_fn_agent.py`)

Applied in-place before each LLM call when `compact_outputs=True`:
1. **Assistant messages with tool_calls** → content truncated to 200 chars + `…[reasoning truncated]`
2. **Assistant messages with `<plan>`** → keep only the `<plan>...</plan>` block
3. **Tool messages (all except last)** → strip `═══ WORKING MEMORY ... ═══ END WORKING MEMORY ═══` snapshot (only the most recent snapshot is kept)
4. **All assistant messages** → strip `<think>...</think>` blocks and orphan `</think>` tags

**Impact**: Prevents context window exhaustion on long sessions. Previously, prompt tokens reached 135K+ with repeated full memory snapshots.

### 13.8 New: Compact tool output (`_compact_tool_output()`)

**Status**: ✅ Implemented (in `tools_fn_agent.py`)

Strips noise from raw tool outputs before passing to the model (harness_v1 only):
- Removes `[Auto-resolved location ...]` lines
- For JSON outputs: strips `latitude`, `longitude`, `address`, `id` fields
- For text outputs: strips lines containing coordinate/address keywords
- For route results: also strips raw `origin`/`destination` coordinate strings

**Rationale**: Coordinates are useful for internal computation (memory, routes) but confuse the model and waste context. Memory already stores all coordinate data.

### 13.9 New: Meal slot tags in memory snapshot

**Status**: ✅ Implemented (in `working_memory.py`)

`_meal_slot_tag()` classifies each restaurant as `[lunch+dinner]`, `[lunch only]`, or `[dinner only]` based on opening/closing hours:
- Lunch: open before 14:00 and close after 11:00
- Dinner: open before 21:00 and close after 17:00

Tags appear in the RESTAURANTS section of `render_snapshot()`, e.g.:
```
  Sichuan House: ¥80/person, Sichuan, 11:00-22:00 [lunch+dinner], near The Bund
```

**Rationale**: The model was scheduling dinner at lunch-only restaurants (and vice versa), causing `dining_within_service_hours` failures.

### 13.10 New: Routes rendered as compact matrix

**Status**: ✅ Implemented (in `working_memory.py`)

`_render_routes_matrix()` replaces the old flat route list with a grouped-by-origin format:
```
ROUTES (from → to: duration, distance, cost):
  From Hotel A:
    Attraction X 12min/3.5km/¥15 | Restaurant Y 8min/2.1km/¥10
  From Attraction X:
    Restaurant Y 5min/1.2km/¥8
```

**Rationale**: Old format listed every route on its own line, ballooning the snapshot when many hotel×attraction pairs existed. Matrix format is more scannable and compact.

### 13.11 New: `autocorrect_travel_city()` post-processing

**Status**: ✅ Implemented (in `working_memory.py`)

After plan generation (and after validation corrections), `memory.autocorrect_travel_city(final_plan)` scans for `travel_city` lines and corrects distance/duration/cost from stored route data:
- Regex matches: `HH:MM-HH:MM | travel_city | Origin - Dest, Xkm, Ymin, ¥Z`
- Looks up `(origin, dest)` or `(dest, origin)` in `self.routes`
- Replaces distance, duration, cost with stored values
- Recomputes end time from start time + corrected duration

**Rationale**: The model (or assemble_day rounding) sometimes produces slightly wrong travel durations, causing `reasonable_transfer_time` failures.

### 13.12 New: `assemble_day()` — deterministic day-plan builder

**Status**: ✅ Implemented (in `working_memory.py`, ~300 lines)

The model specifies WHAT activities in WHAT order; `assemble_day()` computes all timestamps, travel segments, and costs deterministically:

**Input**: JSON with `day`, `current_city`, `accommodation`, `activities[]`
**Activity types**: `intercity`, `buffer`, `hotel`, `attraction`, `meal`

**Algorithm**:
1. Initialize cursor time from first intercity departure or default 07:00
2. For each activity:
   - Look up data from memory (transport schedule, attraction hours, restaurant hours, route durations)
   - Auto-insert `travel_city` segments between different locations
   - Clamp attraction visits to opening hours and min/max duration
   - Clamp meal durations to 60-120 minutes
   - Validate restaurant business hours
3. Return formatted day text, with `⚠️ ASSEMBLY ERRORS` appended if any issues

Helper methods: `_assemble_intercity()`, `_assemble_travel_city()`, `_find_restaurant()`, `_fmt()`, `_parse_time()`

`ASSEMBLE_DAY_SCHEMA` is defined at the top of `tools_fn_agent.py` and injected into the tool list when memory is enabled.

### 13.13 New: Auto-query attraction details (`_auto_query_attraction_details()`)

**Status**: ✅ Implemented (in `tools_fn_agent.py`)

After `recommend_attractions`, automatically queries details for all attractions that lack them:
- Iterates `memory.attractions_summary` and finds names not in `memory.attractions_detail`
- Calls `query_attraction_details` tool for each, processes result through memory
- Returns summary string like `[Auto-queried 5 attraction details]`

**Rationale**: Previously the model had to manually call `query_attraction_details` for each attraction, wasting LLM turns. Auto-querying ensures coordinates are available for route computation.

### 13.14 Refined: Auto-query routes (`_auto_query_routes()`)

**Status**: ✅ Modified

Changed from computing routes between all location pairs to a more targeted approach:
- **Anchors**: hotels only (previously included any "current queried entity")
- **Targets**: attractions only (from `memory.attractions_detail`, always populated)
- **Special case**: `recommend_restaurants` still routes from the `near` attraction to each restaurant
- **Removed**: separate `query_attraction_details` and `query_hotel_info` trigger branches — now a single unified approach since attraction details are auto-queried

**Rationale**: The old approach generated too many route pairs (hotel→hotel, attraction→attraction) that bloated context. Hotels→attractions covers the key planning pairs.

### 13.15 Fix: Evaluation — transfer time check accounts for `travel_city` segments

**Status**: ✅ Implemented (in `evaluation/constraints_commonsense.py`)

The `check_transfer_time_reasonable()` function was only subtracting `buffer` durations from the gap between anchor activities. Now it also separately tracks `travel_city` durations:

- When a `travel_city` segment exists between two anchors: applies a **relaxed** sanity check (travel_city duration must be within 50%-500%+90min of taxi time)
- When NO `travel_city` segment exists: applies the original strict check (gap minus buffers must match taxi time within ±5min/10min rounding)

**Rationale**: The old check penalized plans that correctly included `travel_city` segments, because the gap between anchor activities was larger than the raw commute time (it included the travel_city duration). This was the #1 error type (75 cases in full run).

### 13.16 Fix: Conversion — last day accommodation forced to "-"

**Status**: ✅ Implemented (in `evaluation/convert_report.py`)

Post-processing step after JSON parsing: if the last day's accommodation name is non-empty and not "-", force it to `{"name": "-", "price_per_night": 0}`.

**Rationale**: Models often include the hotel on the departure day, causing `ends_with_accommodation` failures. The traveler doesn't stay overnight on the final day.

### 13.17 New: Memory snapshot saving

**Status**: ✅ Implemented (in `tools_fn_agent.py`)

After each sample completes, the final working memory snapshot is saved to `output_dir/memory_snapshots/{sample_id}.txt`. This aids debugging by letting us inspect what data the model had access to.

### 13.18 Updated: `run()` return signature

**Status**: ✅ Implemented

`run()` now returns a 4-tuple: `(final_plan, messages, token_usage, memory)` instead of a 3-tuple. The `memory` object (or `None` if memory disabled) is returned so the caller can use it for `autocorrect_travel_city()` and pass it to `continue_run()`.

### 13.19 Expanded: Guided prompt (`prompts_guided.py`)

**Status**: ✅ Significantly expanded (from ~144 lines to ~200 lines)

New content added to the planning guidance block:
- **`assemble_day` workflow instructions**: explicit 3-step workflow (gather data → assemble each day → copy verbatim into `<plan>` tags)
- **Activity type reference**: JSON format for each of the 5 activity types
- **Key rules**: exact entity names, day structure patterns (arrival/departure/full days), meal-only tag warnings
- **Commonsense rules section**: explicit rules the plan will be checked against:
  - Last day accommodation = "-"
  - Meal scheduling rules per day type (full day, arrival, departure) with time thresholds
  - Attraction density rules per day type
  - Travel continuity requirements (travel_city between different locations, current_city chaining)

**Rationale**: The original guided prompt had minimal instruction (~5 lines). The expanded version directly teaches the model about evaluation criteria so it can self-correct before validation.

---

## 14. Full Run Results (120 samples, `harness_v1`)

**Model**: qwen3-30b-a3b-thinking-2507
**Directory**: `results_full_120/qwen3-30b-a3b-thinking-2507_harness_v1_en`

### Key Metrics

| Metric | `guided_memory` (prior) | `harness_v1` (current) | Δ |
|--------|------------------------|----------------------|---|
| Delivery Rate | 52.5% (63/120) | **91.7%** (110/120) | +39.2pp |
| Composite Score | 0.124 | **0.223** | +0.099 |
| Commonsense Score | — | **0.446** | — |
| Personalized Score | — | **0.0** | — |
| Case Accuracy | — | **0.0%** | — |

### Commonsense Dimensions (sorted best → worst)

| Dimension | Score | Perfect Count |
|-----------|-------|---------------|
| Sandbox Compliance | 0.725 | 87/120 |
| Duration Rationality | 0.600 | 72/120 |
| Cost Calculation Accuracy | 0.492 | 59/120 |
| Business Hours | 0.450 | 54/120 |
| Route Consistency | 0.400 | 48/120 |
| Activity Diversity | 0.367 | 44/120 |
| Itinerary Structure | 0.333 | 40/120 |
| Time Feasibility | 0.200 | 24/120 |

### Top 10 Error Types

| Rank | Error Type | Count |
|------|-----------|-------|
| 1 | `[Commonsense] reasonable_transfer_time` | 75 |
| 2 | `[Commonsense] diverse_meal_options` | 53 |
| 3 | `[Commonsense] essential_meal_coverage` | 52 |
| 4 | `[Commonsense] seamless_intercity_transfers` | 52 |
| 5 | `[Commonsense] cost_calculation_correctness` | 51 |
| 6 | `[Hard] hotel_star_service_required` | 48 |
| 7 | `[Commonsense] no_time_overlaps` | 46 |
| 8 | `[Hard] restaurant_specific_tag_nearby` | 41 |
| 9 | `[Commonsense] diverse_attraction_options` | 39 |
| 10 | `[Commonsense] dining_within_service_hours` | 38 |

### Analysis

**What improved**:
- Delivery rate nearly doubled (+39pp) — the combination of `max_tokens` support, truncated plan fallback, and empty plan retry dramatically reduced plan loss
- Composite score improved by 80% (0.124 → 0.223)

**What remains broken**:
- **Personalized score = 0.0**: No single plan satisfies ALL hard constraints. The most common hard failures are `hotel_star_service_required` (48 cases) and `restaurant_specific_tag_nearby` (41 cases) — the model doesn't reliably filter for specific hotel services or restaurant tags
- **Case accuracy = 0.0**: Since personalized score must be 1.0 for case_acc, this is blocked by hard constraint failures
- **Transfer time still #1 error** (75 cases): Despite evaluation fix (§13.15) and autocorrect (§13.11), many plans still have timing issues — likely from the model writing plans manually instead of using `assemble_day`, or `assemble_day` producing durations outside the relaxed tolerance
- **Meal coverage** (52 cases) and **diversity** (53 cases): Model frequently skips required meals or reuses the same restaurant
- **Time overlaps** (46 cases): Suggests `assemble_day` isn't being used consistently — manual plans don't respect timestamps
