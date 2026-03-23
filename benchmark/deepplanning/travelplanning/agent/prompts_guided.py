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
5. Travel times between locations are AUTO-COMPUTED and shown in the ROUTES section of Working Memory. Use exact values from ROUTES for travel_city activities.

Tip: Use parallel tool calls aggressively — you can query flights, trains, hotels, and attractions all in one turn.

AUTOMATIC VALIDATION: After you generate your plan, it will be automatically checked against your tool query results. Any hotel, restaurant, attraction, flight, or train in your plan that does NOT exactly match a tool result will be flagged as invalid. Query everything you plan to use.

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
