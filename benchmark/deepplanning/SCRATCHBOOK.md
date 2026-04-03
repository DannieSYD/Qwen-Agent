# Scratchbook

Design notes and iteration log for DeepPlanning benchmark improvements.

---

## Harness v1 (`--prompt-variant harness_v1`)

**Goal**: Reduce agent errors caused by the tool interface itself — coordinate confusion, unnecessary intermediate steps, context bloat from raw JSON outputs.

### Components

| Component | What it does | Rationale |
|---|---|---|
| **Compacted tool schema** | `recommend_restaurants` only accepts `near` (place name). `query_road_route_info` only accepts `origin_place`/`destination_place`. Coordinate params (`latitude`, `longitude`, `origin`, `destination`) removed from model's view. `search_location` tool removed entirely. | Models frequently hallucinate or mis-copy 6-decimal coordinate strings. Name-based resolution is already implemented server-side — removing coordinate params eliminates a major error source without losing any capability. |
| **Structured working memory** | After each tool call, raw JSON response is parsed into a structured memory (hotels, attractions, restaurants, routes). The raw output is replaced with a compact acknowledgment + accumulated memory snapshot. | Raw tool outputs are verbose and repetitive. Structured memory reduces context window usage and gives the model a cleaner view of what it knows so far. |
| **Auto-route querying** | After restaurant/attraction tool calls, automatically queries travel times between known locations and injects them as free context. | Eliminates a common multi-step pattern (query location → get coords → query route) that models frequently get wrong. |
| **Plan validation + correction** | After the model outputs a plan, a validator checks for hallucinated entities (names not from tool results). If found, sends a correction message (up to 2 rounds). | Catches factual errors before they reach evaluation. |

### Files changed/added
- `travelplanning/tools/tool_schema_harness_v1_en.json` — compacted schema (no coord params, no search_location)
- `travelplanning/run.py` — added `harness_v1` choice, routes to harness schema
- `travelplanning/agent/tools_fn_agent.py` — `harness_v1` triggers guided prompt + working memory + validation

### Baseline comparison (same as `guided_memory` except for tool schema)
- `default` → raw prompts, raw tool outputs, full tool schema
- `guided` → guided prompts + validation, raw tool outputs, full tool schema
- `guided_memory` → guided prompts + validation + working memory, full tool schema
- `harness_v1` → guided prompts + validation + working memory, **compacted tool schema**

### Usage
```bash
python run.py --model <model> --language en --prompt-variant harness_v1 --rerun-ids "0,1,2,3,4"
```

### TODO / Future iterations
- [ ] Harness v1 zh schema (`tool_schema_harness_v1_zh.json`) — currently only English
- [ ] Measure: does removing search_location hurt any cases where the model legitimately needed raw coordinates?
- [ ] Consider further compaction: hide coordinate fields from tool *outputs* too (not just inputs)
- [ ] Consider compacting hotel/attraction output to only fields the model needs for planning
