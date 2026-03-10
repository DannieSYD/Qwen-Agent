"""
Stage 4: Query Generation

Generates natural language queries from task metadata and hard constraints.
Uses LLM (via call_llm) for conversational queries and templates for structured queries.
"""
import random
import sys
import os
from typing import Any, Dict, List, Optional, Tuple

from .config import TaskConfig
from .utils import weekday_name, weekday_from_date


# ============================================================================
# Few-shot examples (adapted from existing 120 tasks)
# ============================================================================

_FEW_SHOT_EXAMPLES = [
    {
        "travel_details": {
            "origin": "Hefei", "dest": "Nanjing", "days": 2,
            "depart_date": "2025-11-12", "return_date": "2025-11-13",
            "people_number": 3, "room_number": 2,
        },
        "constraint_bullets": [
            "3 travelers, traveling by train, please ensure sufficient remaining tickets",
            "Please select a 3-star hotel that offers Swimming Pool",
            "Include one meal at a restaurant near 'Laomendong' that provides Birthday Package service",
            "The itinerary must include visits to 'Nanjing Deji Plaza' and 'Nanjing City Wall Taicheng Scenic Area'",
        ],
        "query": (
            "I'm planning a two-day trip from Hefei to Nanjing on November 12, 2025, "
            "returning in the evening of the 13th. The total budget for this trip should "
            "be within 3000 yuan. There are three of us traveling, and we'll take the train "
            "since it should be quite convenient\u2014please help me choose a suitable train schedule.\n\n"
            "For accommodation, I have specific preferences: I'd like a three-star hotel with "
            "a swimming pool, and please book two rooms.\n\n"
            "There are several places I must visit during this trip, including 'Nanjing Deji Plaza' "
            "and 'Nanjing City Wall Taicheng Scenic Area'\u2014please make sure to include both in the "
            "itinerary. Also, I'd like to have a meal near 'Laomendong', preferably at a restaurant "
            "that offers birthday set menus, as one of my friends has a birthday and we'd like to "
            "celebrate together.\n\n"
            "That's basically everything\u2014I've provided all the details clearly. Please go ahead "
            "and plan the itinerary for me!"
        ),
    },
    {
        "travel_details": {
            "origin": "Harbin", "dest": "Dalian", "days": 2,
            "depart_date": "2025-11-12", "return_date": "2025-11-13",
            "people_number": 4, "room_number": 2,
        },
        "constraint_bullets": [
            "4 travelers, traveling by train, please ensure sufficient remaining tickets",
            "Accommodation should be hotels with renovations completed in 2025 or later",
            "Include one meal at a restaurant near 'Xinghai Bay Boardwalk' that offers Private Room service",
            "Must visit all 'Leisure Experience' type attractions",
        ],
        "query": (
            "I'm planning a two-day trip from Harbin to Dalian on November 12, 2025, returning "
            "on November 13. Could you help me arrange the itinerary? There are four of us "
            "traveling, and we'd like to take the train round-trip\u2014please just pick a suitable "
            "train for us.\n\n"
            "For accommodation, I have a small request: I'd like to find a hotel that has been "
            "newly renovated after 2025. Staying somewhere newer just puts us in a better mood. "
            "Since there are four of us, we'll need to book two rooms\u2014please help arrange that "
            "as well.\n\n"
            "By the way, for dining, I'd like to have one meal arranged near 'Xinghai Bay Boardwalk'. "
            "Ideally, it would be at a restaurant offering private room service\u2014I think it would "
            "feel more comfortable and private.\n\n"
            "Also, this trip is all about relaxation. I've heard Dalian has some fantastic 'Leisure "
            "Experience' attractions, and I'd like to visit all the recommended spots of this type. "
            "Please include all of them in the plan\u2014don't miss any."
        ),
    },
    {
        "travel_details": {
            "origin": "Changchun", "dest": "Dalian", "days": 2,
            "depart_date": "2025-11-12", "return_date": "2025-11-13",
            "people_number": 1, "room_number": 1,
        },
        "constraint_bullets": [
            "Please select the latest direct train for the return journey",
            "For accommodation, please choose a 3-star hotel that provides Washer and Dryer",
            "The itinerary must include the highest-rated attraction in the 'Natural Scenery' category",
            "Arrange one meal at a restaurant near 'Donggang Business District' that offers Waiting Area service",
        ],
        "query": (
            "I plan to travel from Changchun to Dalian on November 12, 2025, stay for one day, "
            "and return to Changchun on the evening of November 13. Could you please help me plan "
            "my itinerary, including transportation, accommodation, meals, and attractions?\n\n"
            "Regarding transportation, I'd like to arrive back in Changchun as late as possible\u2014"
            "could you help me choose the latest direct train that arrives on the same day? I prefer "
            "a more relaxed pace, so please don't make the schedule too tight.\n\n"
            "For accommodation, I'm looking for a three-star hotel with both a washing machine and "
            "dryer, which would be more convenient since I'm only staying one night and want things "
            "to be stress-free. By the way, I'm quite interested in Dalian's natural scenery\u2014I've "
            "heard the coastal views are especially beautiful. Could you include the highest-rated "
            "nature attraction in the itinerary so I can experience the highlight?\n\n"
            "For dining, I'd like to find a restaurant near 'Donggang Business District'\u2014I've "
            "heard it's a great area. The restaurant must offer a waiting area service so I won't "
            "have to worry about long queues."
        ),
    },
    {
        "travel_details": {
            "origin": "Xi'an", "dest": "Taiyuan", "days": 2,
            "depart_date": "2025-11-12", "return_date": "2025-11-13",
            "people_number": 2, "room_number": 1,
        },
        "constraint_bullets": [
            "For the outbound journey, select the shortest-duration direct train",
            "For accommodation, please select a 2-star hotel that provides Washer and Dryer",
            "Arrange one meal at a restaurant near 'Fenhe Scenic Area' that offers Online Queue service",
            "Arrange one meal at the highest-rated restaurant near 'Chunyang Palace'",
        ],
        "query": (
            "I plan to travel from Xi'an to Taiyuan for a one-day trip on November 12, 2025, "
            "and return to Xi'an on November 13, 2025. Could you please help me plan the entire "
            "itinerary, including transportation, accommodation, dining, and sightseeing arrangements?\n\n"
            "Regarding transportation, I'd like to take a train for the outbound journey, preferably "
            "a direct one with the shortest possible travel time, so I can have more time to explore "
            "upon arrival. The accommodation doesn't need to be fancy\u2014I just need a two-star hotel. "
            "However, I have quite a few clothes to wash; could you please choose a hotel that has "
            "both a washing machine and a dryer? There are two of us traveling, so we only need to "
            "book one room.\n\n"
            "By the way, there are two dining spots I'm particularly interested in\u2014could you help "
            "me arrange them? One is a restaurant near 'Fenhe Scenic Area', and I'd like to be able "
            "to take a virtual queue number online in advance so we don't have to wait too long. The "
            "other is around 'Chunyang Palace'; I've heard there are many great local eats nearby. "
            "Please help me pick the highest-rated restaurant there and include it in the plan."
        ),
    },
]


def _build_few_shot_prompt(
    travel_details: Dict,
    constraint_bullets: List[str],
) -> str:
    """Build the LLM prompt with few-shot examples."""

    system_msg = (
        "You are a travel planning query writer. Given travel details and constraints, "
        "write a conversational, natural-sounding travel request as if written by a real person. "
        "The query should:\n"
        "1. Sound natural and personal (use 'I', 'we', 'please')\n"
        "2. Include ALL constraint requirements but weave them into natural language\n"
        "3. NEVER reveal exact entity names like hotel names, train/flight numbers, or restaurant names\n"
        "4. Include the departure weekday naturally (e.g., 'departing on a Wednesday')\n"
        "5. Be 2-4 paragraphs long\n"
        "6. End with a line encouraging the planner to proceed\n\n"
        "IMPORTANT: Incorporate constraint requirements into the conversational text. "
        "Do NOT just list them as bullet points."
    )

    prompt = system_msg + "\n\nHere are some examples:\n\n"

    for i, ex in enumerate(_FEW_SHOT_EXAMPLES):
        td = ex["travel_details"]
        prompt += f"--- Example {i+1} ---\n"
        prompt += f"Travel Details: {td['origin']} to {td['dest']}, "
        prompt += f"{td['days']} days, {td['people_number']} people, "
        prompt += f"{td['room_number']} rooms, "
        prompt += f"depart {td['depart_date']}, return {td['return_date']}\n"
        prompt += "Constraints:\n"
        for bullet in ex["constraint_bullets"]:
            prompt += f"  - {bullet}\n"
        prompt += f"\nQuery:\n{ex['query']}\n\n"

    # Now the actual task
    td = travel_details
    depart_weekday = weekday_name(weekday_from_date(td["depart_date"]))
    prompt += "--- Your Task ---\n"
    prompt += f"Travel Details: {td['origin']} to {td['dest']}, "
    prompt += f"{td['days']} days, {td['people_number']} people, "
    prompt += f"{td['room_number']} rooms, "
    prompt += f"depart {td['depart_date']} ({depart_weekday}), return {td['return_date']}\n"
    prompt += "Constraints:\n"
    for bullet in constraint_bullets:
        prompt += f"  - {bullet}\n"
    prompt += "\nQuery:\n"

    return prompt


# ============================================================================
# Extract constraint bullets from hard_constraints dict
# ============================================================================

def _constraints_to_bullets(hard_constraints: Dict[str, Dict]) -> List[str]:
    """Convert hard_constraints dict to human-readable bullet points."""
    bullets = []
    for cname, cdata in hard_constraints.items():
        ctx = cdata.get("constraint_context", "")
        if ctx:
            bullets.append(ctx)
        elif cname == "budget_constraint":
            max_budget = cdata.get("max_budget", 0)
            bullets.append(f"The total budget should not exceed {max_budget} yuan")
    return bullets


# ============================================================================
# Build structured query_with_constraints (template-based, no LLM)
# ============================================================================

def _build_structured_query(
    config: TaskConfig,
    hard_constraints: Dict[str, Dict],
) -> str:
    """Build the structured query_with_constraints string."""
    lines = [
        f"I plan to travel from {config.origin} to {config.dest}, "
        f"with a departure date of {config.depart_date} and return date of {config.return_date}. "
        f"Please help me plan the itinerary, including transportation, accommodation, "
        f"dining, and sightseeing arrangements.",
    ]

    bullets = _constraints_to_bullets(hard_constraints)
    for bullet in bullets:
        lines.append(f"- {bullet}")

    return "\n".join(lines)


# ============================================================================
# Main entry point
# ============================================================================

def generate_query(
    config: TaskConfig,
    hard_constraints: Dict[str, Dict],
    solution: Dict,
    model_name: str = "qwen-plus",
    rng: Optional[random.Random] = None,
) -> Dict[str, str]:
    """
    Generate natural language query and structured query for a task.

    Args:
        config: Task configuration.
        hard_constraints: The hard constraints dict from Stage 3.
        solution: The reference solution (not used in query, just for safety check).
        model_name: LLM model to use for query generation.
        rng: Random generator.

    Returns:
        Dict with "query" and "query_with_constraints" keys.
    """
    if rng is None:
        rng = random.Random()

    # Build structured query (template-based)
    query_with_constraints = _build_structured_query(config, hard_constraints)

    # Build conversational query via LLM
    travel_details = {
        "origin": config.origin,
        "dest": config.dest,
        "days": config.days,
        "people_number": config.people_number,
        "room_number": config.room_number,
        "depart_date": config.depart_date,
        "return_date": config.return_date,
    }
    constraint_bullets = _constraints_to_bullets(hard_constraints)

    query = _generate_conversational_query(
        travel_details, constraint_bullets, model_name
    )

    # Safety check: query should not leak entity names from solution
    query = _sanitize_query(query, hard_constraints)

    return {
        "query": query,
        "query_with_constraints": query_with_constraints,
    }


def _generate_conversational_query(
    travel_details: Dict,
    constraint_bullets: List[str],
    model_name: str,
) -> str:
    """Call LLM to generate a conversational travel query."""
    prompt_text = _build_few_shot_prompt(travel_details, constraint_bullets)

    try:
        # Import call_llm from the agent module
        agent_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "agent",
        )
        if agent_dir not in sys.path:
            sys.path.insert(0, agent_dir)
        from call_llm import call_llm

        messages = [
            {"role": "user", "content": prompt_text},
        ]
        response = call_llm(config_name=model_name, messages=messages)
        content = response.choices[0].message.content

        if content and content.strip():
            return content.strip()
    except Exception as e:
        print(f"[Stage 4] LLM query generation failed: {e}")

    # Fallback: return the structured query
    return _build_fallback_query(travel_details, constraint_bullets)


def _build_fallback_query(
    travel_details: Dict,
    constraint_bullets: List[str],
) -> str:
    """Build a simple fallback query when LLM is unavailable."""
    td = travel_details
    depart_weekday = weekday_name(weekday_from_date(td["depart_date"]))

    lines = [
        f"I'm planning a {td['days']}-day trip from {td['origin']} to {td['dest']}, "
        f"departing on {td['depart_date']} ({depart_weekday}) and returning on "
        f"{td['return_date']}.",
    ]

    if td["people_number"] > 1:
        lines.append(
            f"There are {td['people_number']} of us traveling, "
            f"and we'll need {td['room_number']} room{'s' if td['room_number'] > 1 else ''}."
        )

    lines.append(
        "Could you please help me plan the complete itinerary, including "
        "transportation, accommodation, dining, and sightseeing?"
    )

    if constraint_bullets:
        lines.append("\nHere are my specific requirements:")
        for bullet in constraint_bullets:
            lines.append(f"- {bullet}")

    lines.append(
        "\nPlease plan everything for me. I've provided all the necessary details."
    )

    return "\n".join(lines)


def _sanitize_query(query: str, hard_constraints: Dict[str, Dict]) -> str:
    """Remove any leaked entity names from the query."""
    # Collect entity names that should NOT appear in the query
    forbidden = set()

    for cname, cdata in hard_constraints.items():
        for key in ["hotel_name", "restaurant_name"]:
            val = cdata.get(key, "")
            if val:
                forbidden.add(val)
        for key in ["outbound_train_no", "inbound_train_no",
                     "outbound_flight_no", "inbound_flight_no"]:
            val = cdata.get(key, "")
            if val:
                forbidden.add(val)

    # Replace any leaked names with generic descriptions
    sanitized = query
    for name in forbidden:
        if name in sanitized:
            # Replace with a generic placeholder based on what it is
            if any(c.isdigit() for c in name) and len(name) < 10:
                # Likely a train/flight number
                sanitized = sanitized.replace(name, "a suitable option")
            else:
                sanitized = sanitized.replace(name, "the selected option")

    return sanitized
