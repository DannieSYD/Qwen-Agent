"""
Constraint Extractor — Phase 1 of the Dual-Space Architecture.

Parses a natural-language travel query into formal ConstraintTuple objects
using an LLM call. These tuples are stored in WorkingMemory and later fed
to the solver (Phase 2+).

Also extracts trip metadata (origin, destinations, dates, people, rooms).
"""

import json
import re
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

from agent.call_llm import call_llm


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class ConstraintTuple:
    """Formal representation of a single planning constraint."""
    variable: str               # e.g. "hotel.star", "budget", "attraction.name"
    type: str                   # categorical | numerical | spatial | temporal
    operator: str               # = | <= | >= | in | near | before | after
    value: Any                  # the constraint value (str, int, float, list)
    certainty: str              # precise | fuzzy
    schedule_sensitivity: str   # existential | temporal
    layer: int                  # 1=local/specialized, 2=global/coupling, 3=commonsense/implicit
    source: str = "explicit"    # explicit (from query) | inferred (commonsense)
    raw_text: str = ""          # the original NL fragment this was extracted from
    superlative: str = ""       # "" | "max" | "min" — indicates argmax/argmin over this field


@dataclass
class TripMeta:
    """High-level trip metadata extracted from the query."""
    origin: str = ""
    destinations: List[str] = None
    days: int = 0
    depart_date: str = ""
    return_date: str = ""
    people_number: int = 1
    room_number: int = 1

    def __post_init__(self):
        if self.destinations is None:
            self.destinations = []

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# Extraction prompt
# ---------------------------------------------------------------------------

EXTRACTION_PROMPT = """\
You are a constraint extraction system for a travel planning benchmark. \
Given a user's travel query, extract ALL constraints into a structured JSON format.

## Output Format
Return a JSON object with two keys:
```json
{
  "meta": {
    "origin": "city name",
    "destinations": ["city1", "city2"],
    "days": 2,
    "depart_date": "YYYY-MM-DD",
    "return_date": "YYYY-MM-DD",
    "people_number": 3,
    "room_number": 2
  },
  "constraints": [
    {
      "variable": "hotel.star",
      "type": "numerical",
      "operator": "=",
      "value": 3,
      "certainty": "precise",
      "schedule_sensitivity": "existential",
      "layer": 1,
      "source": "explicit",
      "raw_text": "I'd like a three-star hotel",
      "superlative": ""
    }
  ]
}
```

## Constraint Fields

- **variable**: What the constraint applies to. Use dot notation:
  - Transport: `transport.outbound.mode`, `transport.outbound.time`, `transport.outbound.duration`, `transport.inbound.mode`, `transport.inbound.time`, `transport.inbound.duration`, `transport.seat_class`
  - Hotel: `hotel.star`, `hotel.service`, `hotel.brand`, `hotel.price`, `hotel.name`, `hotel.decoration_time`
  - Restaurant: `restaurant.name`, `restaurant.tag`, `restaurant.cuisine`, `restaurant.location`, `restaurant.price`, `restaurant.rating`
  - Attraction: `attraction.name`, `attraction.type`, `attraction.must_visit`
  - Budget: `budget.total`
  - Schedule: `schedule.departure`, `schedule.return`

- **type**: `categorical` (discrete options), `numerical` (quantities/prices), `spatial` (locations/distances), `temporal` (times/dates)

- **operator**: `=` (exact), `<=` (at most), `>=` (at least), `in` (member of set), `near` (spatial proximity), `before`/`after` (temporal)

- **value**: The constraint value. Use lists for `in` operator. Use strings for names/dates. Use numbers for quantities.

- **certainty**: `precise` (unambiguous, directly encodable) or `fuzzy` (requires interpretation, e.g. "near", "evening", "budget-friendly")

- **schedule_sensitivity**: `existential` (can be checked once, doesn't change with schedule) or `temporal` (depends on timing, must be rechecked as plan evolves)

- **layer**:
  - 1 = Local/specialized (per-entity, independently verifiable: hotel star, must-visit attraction, specific train)
  - 2 = Global/coupling (cross-entity joint reasoning: total budget, time chain feasibility)
  - 3 = Commonsense/implicit (not stated but expected: meal times, check-in buffers, opening hours)

- **source**: `explicit` (directly stated in query) or `inferred` (commonsense, not written but expected)

- **superlative**: `""` (none), `"max"` (pick entity with highest value), or `"min"` (pick entity with lowest value).
  Use this when the user wants the BEST entity by some criterion:
  - "highest-rated restaurant" → variable=`restaurant.rating`, superlative=`"max"`
  - "cheapest hotel" → variable=`hotel.price`, superlative=`"min"`
  - "newest renovated hotel" → variable=`hotel.decoration_time`, superlative=`"max"`
  - "latest arrival train" → variable=`transport.inbound.time`, superlative=`"max"`
  - "earliest departure" → variable=`transport.outbound.time`, superlative=`"min"`
  - "shortest duration train" → variable=`transport.outbound.duration`, superlative=`"min"`
  When superlative is set, the `value` field should contain the scope/filter (e.g., location name, brand name) or be empty if no filter.
  The `operator` should be `=` for the scope filter.

## Guidelines
- Extract EVERY constraint, including implicit ones (e.g., if traveling by train, the train must have enough seats for the group)
- For "near" constraints, mark certainty as "fuzzy"
- Time-of-day preferences like "evening return" are fuzzy+temporal
- Budget constraints are layer 2 (global coupling)
- Must-visit attractions are layer 1, precise, existential
- Include commonsense constraints (layer 3): meals at appropriate times, reasonable visit durations, business hour compliance
- Return ONLY the JSON object, no other text.
- Do NOT include any reasoning, explanation, or commentary. Output the raw JSON only.
- Your entire response must be a single valid JSON object starting with { and ending with }.
"""


# ---------------------------------------------------------------------------
# Core extraction function
# ---------------------------------------------------------------------------

def extract_constraints(
    query: str,
    model: str,
) -> tuple[TripMeta, list[ConstraintTuple]]:
    """
    Extract formal constraints from a natural-language travel query.

    Args:
        query: The user's travel planning query
        model: Model config name from models_config.json

    Returns:
        (trip_meta, constraints): Extracted trip metadata and constraint tuples
    """
    messages = [
        {"role": "system", "content": EXTRACTION_PROMPT},
        {"role": "user", "content": query},
    ]

    resp = call_llm(config_name=model, messages=messages)
    raw = resp.choices[0].message.content or ""

    # Parse JSON from LLM response (handle markdown fences)
    json_str = _extract_json(raw)
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError:
        # Fallback: return empty extraction rather than crash
        return TripMeta(), []

    # Build TripMeta
    meta_dict = data.get("meta", {})
    trip_meta = TripMeta(
        origin=meta_dict.get("origin", ""),
        destinations=meta_dict.get("destinations", []),
        days=meta_dict.get("days", 0),
        depart_date=meta_dict.get("depart_date", ""),
        return_date=meta_dict.get("return_date", ""),
        people_number=meta_dict.get("people_number", 1),
        room_number=meta_dict.get("room_number", 1),
    )

    # Build ConstraintTuples
    constraints = []
    for c in data.get("constraints", []):
        try:
            ct = ConstraintTuple(
                variable=c.get("variable", ""),
                type=c.get("type", "categorical"),
                operator=c.get("operator", "="),
                value=c.get("value"),
                certainty=c.get("certainty", "precise"),
                schedule_sensitivity=c.get("schedule_sensitivity", "existential"),
                layer=c.get("layer", 1),
                source=c.get("source", "explicit"),
                raw_text=c.get("raw_text", ""),
                superlative=c.get("superlative", ""),
            )
            constraints.append(ct)
        except (TypeError, KeyError):
            continue

    return trip_meta, constraints


# ---------------------------------------------------------------------------
# Rendering for working memory
# ---------------------------------------------------------------------------

def render_constraints_for_prompt(
    trip_meta: TripMeta,
    constraints: list[ConstraintTuple],
) -> str:
    """
    Render extracted constraints as a readable block for the system prompt
    or working memory snapshot.
    """
    lines = ["EXTRACTED CONSTRAINTS:"]

    # Trip metadata
    lines.append(f"  Trip: {trip_meta.origin} → {', '.join(trip_meta.destinations)}")
    lines.append(f"  Dates: {trip_meta.depart_date} to {trip_meta.return_date} ({trip_meta.days} days)")
    lines.append(f"  Travelers: {trip_meta.people_number}, Rooms: {trip_meta.room_number}")
    lines.append("")

    # Group constraints by layer
    by_layer = {1: [], 2: [], 3: []}
    for c in constraints:
        by_layer.setdefault(c.layer, []).append(c)

    layer_names = {
        1: "Layer 1 — Local/Specialized (per-entity)",
        2: "Layer 2 — Global/Coupling (cross-entity)",
        3: "Layer 3 — Commonsense/Implicit",
    }

    for layer_num in (1, 2, 3):
        layer_constraints = by_layer.get(layer_num, [])
        if not layer_constraints:
            continue
        lines.append(f"  {layer_names[layer_num]}:")
        for c in layer_constraints:
            certainty_tag = " [FUZZY]" if c.certainty == "fuzzy" else ""
            temporal_tag = " [TEMPORAL]" if c.schedule_sensitivity == "temporal" else ""
            lines.append(
                f"    - {c.variable} {c.operator} {c.value}{certainty_tag}{temporal_tag}"
            )
            if c.raw_text:
                lines.append(f"      └ \"{c.raw_text}\"")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_json(text: str) -> str:
    """Extract JSON from LLM response, handling think tags, code fences, and verbose reasoning."""
    # Strip <think>...</think> blocks (thinking models)
    text = re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL)
    # Try to find JSON in code fences first (most reliable)
    match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
    if match:
        return match.group(1).strip()
    # Find the largest balanced JSON object containing "meta" and "constraints"
    # by scanning for opening braces and finding their matching close
    best = ""
    for i, ch in enumerate(text):
        if ch == '{':
            depth = 0
            for j in range(i, len(text)):
                if text[j] == '{':
                    depth += 1
                elif text[j] == '}':
                    depth -= 1
                    if depth == 0:
                        candidate = text[i:j+1]
                        if '"meta"' in candidate and '"constraints"' in candidate:
                            if len(candidate) > len(best):
                                best = candidate
                        elif not best and len(candidate) > len(best):
                            best = candidate
                        break
    if best:
        return best
    # Last resort: first {...}
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0)
    return text.strip()


def constraints_to_dicts(constraints: list[ConstraintTuple]) -> list[dict]:
    """Serialize constraints to list of dicts (for JSON saving)."""
    return [asdict(c) for c in constraints]
