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
PLANNING STRATEGY (IMPORTANT)
================================================================
Before generating your plan, ensure you have gathered sufficient data. A good strategy:

1. Query intercity transport (flights AND trains) for EVERY segment, including the return trip.
2. Query hotels for every destination city.
3. Use recommend_attractions to discover options, then query_attraction_details for each one you might use (to get opening hours, visit duration, ticket prices).
4. Use recommend_restaurants(near="Place Name") to find restaurants near your planned locations.
5. Travel times between locations are AUTO-COMPUTED and shown in the ROUTES section of Working Memory.

Tip: Use parallel tool calls aggressively — you can query flights, trains, hotels, and attractions all in one turn.

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


def get_system_prompt(language: str = 'zh') -> str:
    """Get guided system prompt based on language"""
    if language == 'zh':
        return SYSTEM_PROMPT_ZH
    elif language == 'en':
        return SYSTEM_PROMPT_EN
    else:
        raise ValueError(f"Unsupported language: {language}")
