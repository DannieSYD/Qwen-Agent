"""
Prompts for Travel Planning Agent - Guided Variant (Option C - Light Prompt)

Inherits the full original prompt and injects a small guidance block at the end
of Phase 1 to encourage thorough data gathering + warn about post-hoc validation.

Changes from original:
- ~10 lines added to Phase 1 (encouragement, not mandates)
- 1 paragraph about automatic validation (creates incentive to query everything)
- All planning rules, format, and examples remain identical
"""

try:
    from .prompts import (
        SYSTEM_PROMPT_EN as _ORIGINAL_EN,
        SYSTEM_PROMPT_ZH as _ORIGINAL_ZH,
        FORMAT_CONVERT_PROMPT_EN,
        FORMAT_CONVERT_PROMPT_ZH,
        get_format_convert_prompt,
    )
except ImportError:
    from prompts import (
        SYSTEM_PROMPT_EN as _ORIGINAL_EN,
        SYSTEM_PROMPT_ZH as _ORIGINAL_ZH,
        FORMAT_CONVERT_PROMPT_EN,
        FORMAT_CONVERT_PROMPT_ZH,
        get_format_convert_prompt,
    )

# ============================================================================
# Guidance blocks to inject
# ============================================================================

_GUIDANCE_EN = """

================================================================
PLAN ASSEMBLY (CRITICAL — READ CAREFULLY)
================================================================
Do NOT write day plans manually. Instead, use the `assemble_day` tool for EACH day.
You specify the sequence of activities; the tool computes all timestamps, travel times,
distances, and costs deterministically. This guarantees correct travel durations and
prevents time conflicts.

**Workflow:**
1. Gather all data (Phase 1 — query tools as above).
2. For each day, call `assemble_day` with the ordered activity list.
   - The tool auto-inserts travel_city segments between locations.
   - It validates business hours, attraction hours, and visit durations.
   - If there are errors, fix them (e.g., swap restaurant, reorder) and call again.
3. After all days are assembled successfully, output the final plan in <plan> tags:
   - Copy the assembled day outputs verbatim (do NOT modify timestamps or durations).
   - Append a **Budget Summary** section at the end.

**Activity types for assemble_day:**
- intercity: {type:"intercity", transport_type:"train"/"flight", id:"G7798"}
- buffer: {type:"buffer", description:"Deplaning, baggage claim", duration_min:40}
- hotel: {type:"hotel", action:"Check-in"/"Check-out"/"Rest", duration_min:50}
- attraction: {type:"attraction", name:"The Palace Museum"}
- meal: {type:"meal", meal_type:"Lunch"/"Dinner", restaurant:"Restaurant Name", duration_min:60}

**Key rules:**
- Use exact entity names from Working Memory (hotels, restaurants, attractions).
- Start each day with the intercity arrival or hotel departure, end with hotel Rest (except final day).
- On arrival days: buffer after intercity, then hotel Check-in, then activities.
- On departure days: activities, then travel to station, buffer, then intercity.
- Full sightseeing days: at least 2 attractions + lunch + dinner.
- Restaurants tagged [dinner only] cannot be used for Lunch. Check the tag in Working Memory.
- If assemble_day returns errors, address them before proceeding.

================================================================
COMMONSENSE RULES (YOUR PLAN WILL BE CHECKED AGAINST THESE)
================================================================
**Last day accommodation**: On the final day (returning to origin), set `Accommodation: -` (the traveler does not stay overnight). Do NOT write the hotel name on the departure day.

**Meal scheduling rules** (per day):
  - Full sightseeing day (no intercity travel): MUST have lunch AND dinner, gap >= 2 hours between end of lunch and start of dinner.
  - Arrival day: arrive before 10:00 → 2 meals; arrive 10:00-15:00 → at least 1 meal; arrive after 15:00 → 0 or 1 meal.
  - Departure day: depart before 9:00 → 0 meals; depart 9:00-15:00 → at most 1 meal (lunch); depart after 15:00 → at least 1 meal.
  - ALL meals must fall within the restaurant's opening_time-closing_time. Check hours in Working Memory before scheduling.

**Attraction density rules** (per day):
  - Full sightseeing day: at least 2 attractions OR total attraction-related time (including travel to/from) >= 4 hours.
  - Arrival day arriving before 12:00: at least 1 attraction.
  - Departure day departing after 16:00: at least 1 attraction.

**Travel continuity**:
  - A `travel_city` segment MUST appear between every pair of consecutive activities at different locations (even short distances).
  - `current_city` headers must chain correctly: Day N's destination = Day N+1's origin.
  - Do NOT fabricate travel times. Use only values from Working Memory ROUTES section or assemble_day output.

AUTOMATIC VALIDATION: After you generate your plan, it will be automatically checked against your tool query results. Any entity not exactly matching will be flagged as invalid.

"""

_GUIDANCE_ZH = """

================================================================
规划策略（重要）
================================================================
在生成计划之前，请确保你已收集了充分的数据。推荐策略：

1. 查询每一段城际交通（航班和火车），包括返程。
2. 查询每个目的地城市的酒店。
3. 使用 recommend_attractions 发现选项，然后对你可能使用的每个景点调用 query_attraction_details（获取开放时间、游览时长、门票价格）。
4. 使用 recommend_restaurants(near="地点名称") 查找计划地点附近的餐厅。
5. 各地点之间的出行时间会自动计算，显示在工作记忆的 ROUTES 部分。编写 travel_city 时使用精确值。

提示：积极使用并行工具调用——你可以在一个回合中同时查询航班、火车、酒店和景点。

自动验证：在你生成计划后，系统会自动将计划中的实体与你的工具查询结果进行比对。计划中任何与工具结果不完全匹配的实体都将被标记为无效。请确保查询了你计划使用的所有实体。

"""

# ============================================================================
# Inject guidance into original prompts
# ============================================================================

def _inject_guidance(original: str, guidance: str, marker_en: str, marker_zh: str) -> str:
    """Insert guidance block right before the Planning Phase section."""
    # Try English marker first
    for marker in [marker_en, marker_zh]:
        idx = original.find(marker)
        if idx != -1:
            return original[:idx] + guidance + "\n" + original[idx:]
    # Fallback: append at end of Phase 1 area (before the first ===PHASE 2 or ===阶段2)
    return original + guidance


# Insert guidance right before Phase 2 / Planning Phase
SYSTEM_PROMPT_EN = _inject_guidance(
    _ORIGINAL_EN,
    _GUIDANCE_EN,
    marker_en="================================================================\nPHASE 2",
    marker_zh="================================================================\n阶段2",
)

SYSTEM_PROMPT_ZH = _inject_guidance(
    _ORIGINAL_ZH,
    _GUIDANCE_ZH,
    marker_en="================================================================\nPHASE 2",
    marker_zh="================================================================\n阶段2",
)


_SOLVER_GUIDANCE_EN = """

================================================================
MANDATORY: USE run_solver TO BUILD YOUR PLAN
================================================================
⚠️ YOU MUST call `run_solver` to generate your plan. DO NOT write plans manually.
DO NOT use `assemble_day`. Plans not from `run_solver` will be REJECTED.

**Workflow (iterative outer-inner loop):**
1. Gather data (trains BOTH directions, hotels, attractions with details, restaurants).
2. Call `run_solver` with Python code using OR-Tools CP-SAT to select optimal entities and schedule.
3. If SOLVER_ERROR or SOLVER_FEEDBACK:
   - Read the error/feedback carefully
   - If it's a CODE error: fix your code and call `run_solver` again
   - If it's a DATA gap (e.g., no hotels with required services): call query tools to
     gather more data, THEN call `run_solver` again — the `data` dict auto-refreshes
4. Repeat step 2-3 until the solver produces a valid plan.
5. The solver output becomes the final plan automatically (no need to copy into <plan> tags).

**`run_solver` environment:** Pre-loaded: `data` dict (Working Memory), `cp_model` (ortools), `datetime`, `timedelta`, `json`, `math`.

**`data` dict keys:** `trip_meta` (origin, destinations, days, depart_date, return_date, people, rooms), `constraints` (list of {variable, operator, value, layer}), `outbound_transport` / `inbound_transport` (list of {id, mode, dep_time, arr_time, dep_station, arr_station, price, duration, seat_class}), `hotels` (list of {name, city, price, star, rating, services}), `attractions` (dict name→{price, open, close, visit_min_hrs, visit_max_hrs, type}), `restaurants` (list of {name, price_per_person, cuisine, open, close, near, tags}), `routes` (dict "A -> B"→{duration_min, distance_km, cost}).

**⚠️ CRITICAL: Do NOT redefine `data` in your code.** The variable `data` is ALREADY pre-loaded
with all working memory data. Just use `data["hotels"]`, `data["constraints"]`, etc. directly.
If you write `data = {...}`, you will SHADOW the pre-loaded data and lose all queried info.

**Output format:** Your code must print() each day as:
```
Day N:
Current City: from X to Y
Accommodation: Hotel Name, ¥price/room/night   (use - on last day)
HH:MM-HH:MM | type | description
```
Activity types: `travel_intercity_public`, `buffer`, `travel_city` (with km, min, ¥cost), `hotel` (Check-in/Check-out/Rest), `attraction` (with ¥/person), `meal` (Lunch/Dinner, with ¥/person).
End with Budget Summary (Transportation, Accommodation, Meals, Attractions, Total).

**CP-SAT pattern — selecting from a list:**
Do NOT index a Python list with an IntVar (e.g. `hotels[hotel_var]` is WRONG).
Use boolean selection variables instead:
```python
# Example: select one hotel from N candidates
hotel_selected = [model.NewBoolVar(f'hotel_{i}') for i in range(len(data["hotels"]))]
model.AddExactlyOne(hotel_selected)
# Compute cost using selected hotel
hotel_cost = model.NewIntVar(0, 999999, 'hotel_cost')
model.Add(hotel_cost == sum(hotel_selected[i] * int(data["hotels"][i]["price"] * data["trip_meta"]["rooms"]) for i in range(len(data["hotels"]))))
# After solving, find which was selected:
# selected_hotel_idx = next(i for i in range(len(data["hotels"])) if solver.Value(hotel_selected[i]))
```
Always read data from the pre-loaded `data` dict — do NOT hardcode values.

**Solver rules:**
1. CP-SAT selects transport, hotel, attractions, restaurants via boolean variables. Then schedule greedily with travel times.
2. Budget: (outbound+inbound price)×people + hotel×rooms×(days-1) + Σ(attraction×people) + Σ(meal×people) + Σ(city_transport×people).
3. Route lookup: check "A -> B" in `data["routes"]`, else try "B -> A", else estimate 15min/¥15.
4. Opening hours: visits within open-close. Lunch 11:00-14:00, dinner 17:00-21:00.
5. Last day: Accommodation: - (no hotel).
6. If INFEASIBLE: print "SOLVER_INFEASIBLE:" + conflicting constraints, then best-effort plan.

**If `run_solver` returns an error, read the traceback, fix your code, and call again.**

"""

_SOLVER_GUIDANCE_ZH = _SOLVER_GUIDANCE_EN  # Use English guidance for now


# ============================================================================
# v4: Simplified solver guidance (fixed template, no LLM code)
# ============================================================================

_SOLVER_V4_GUIDANCE_EN = """

================================================================
MANDATORY: USE run_solver TO BUILD YOUR PLAN
================================================================
After gathering enough data, call `run_solver` (NO arguments needed).
The solver automatically:
1. Reads all Working Memory data (transport, hotels, attractions, restaurants, routes)
2. Reads all extracted constraints from Phase 1
3. Uses CP-SAT optimization to select entities satisfying all constraints
4. Schedules them into a feasible day-by-day plan

**Workflow:**
1. Gather ALL data first:
   - Outbound transport: query flights/trains for departure date
   - Inbound transport: query flights/trains for return date
   - Hotels: query hotels in destination city
   - Attractions: use recommend_attractions, then query_attraction_details for each
   - Restaurants: use recommend_restaurants near planned locations
   - Routes are computed automatically between known locations
2. Call `run_solver` — no code, no arguments. Just call it.
3. If SOLVER_FEEDBACK says data is missing:
   - Query the missing data using the appropriate tool calls
   - Call `run_solver` again (it re-reads Working Memory automatically)
4. If SOLVER_INFEASIBLE:
   - Read the feedback to understand which constraints conflict
   - Query more options (e.g., more hotels, different transport) to expand choices
   - Call `run_solver` again
5. The solver output becomes the final plan automatically.

**Tips for data gathering:**
- Query transport in BOTH directions (outbound AND inbound)
- After recommend_attractions, call query_attraction_details for each attraction
- After querying hotels, use recommend_restaurants near attractions or near the hotel
- The more data you gather, the better the solver can optimize

⚠️ DO NOT write plans manually. Plans not from `run_solver` will be REJECTED.
⚠️ DO NOT call `assemble_day` — the solver handles scheduling internally.

"""

_SOLVER_V4_GUIDANCE_ZH = _SOLVER_V4_GUIDANCE_EN  # Use English guidance for now


# Build solver-enabled prompt: replace the assemble_day guidance with solver guidance
def _build_solver_prompt(base: str, solver_guidance: str) -> str:
    """Replace the PLAN ASSEMBLY (assemble_day) block with solver guidance."""
    # Find and remove the assemble_day block that conflicts with solver instructions
    marker_start = "================================================================\nPLAN ASSEMBLY (CRITICAL"
    marker_end = "AUTOMATIC VALIDATION:"
    idx_start = base.find(marker_start)
    idx_end = base.find(marker_end)
    if idx_start != -1 and idx_end != -1:
        # Find end of the AUTOMATIC VALIDATION line
        line_end = base.find("\n", idx_end)
        if line_end == -1:
            line_end = len(base)
        base = base[:idx_start] + base[line_end:]
    return base + solver_guidance


SYSTEM_PROMPT_SOLVER_EN = _build_solver_prompt(SYSTEM_PROMPT_EN, _SOLVER_GUIDANCE_EN)
SYSTEM_PROMPT_SOLVER_ZH = _build_solver_prompt(SYSTEM_PROMPT_ZH, _SOLVER_GUIDANCE_ZH)

# v4: fixed template solver
SYSTEM_PROMPT_SOLVER_V4_EN = _build_solver_prompt(SYSTEM_PROMPT_EN, _SOLVER_V4_GUIDANCE_EN)
SYSTEM_PROMPT_SOLVER_V4_ZH = _build_solver_prompt(SYSTEM_PROMPT_ZH, _SOLVER_V4_GUIDANCE_ZH)


def get_system_prompt(language: str = 'zh', variant: str = 'guided') -> str:
    """Get guided system prompt based on language and variant.

    Args:
        language: 'zh' or 'en'
        variant: 'guided' | 'solver' (harness_v3) | 'solver_v4' (harness_v4)
    """
    if variant == 'solver_v4':
        return SYSTEM_PROMPT_SOLVER_V4_EN if language == 'en' else SYSTEM_PROMPT_SOLVER_V4_ZH
    if variant == 'solver':
        return SYSTEM_PROMPT_SOLVER_EN if language == 'en' else SYSTEM_PROMPT_SOLVER_ZH
    if language == 'zh':
        return SYSTEM_PROMPT_ZH
    elif language == 'en':
        return SYSTEM_PROMPT_EN
    else:
        raise ValueError(f"Unsupported language: {language}")
