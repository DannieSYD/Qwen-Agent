# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
AI agent benchmark evaluating planning capabilities across two domains:
- **Shopping Planning** (`shoppingplanning/`): E-commerce shopping tasks with 3 difficulty levels
- **Travel Planning** (`travelplanning/`): Travel itinerary planning in Chinese and English

Currently only focus on travel planning, English cases. Ignore Chinese cases in travel planning and shopping for now.

Results across different servers are saved in **/home/dannie/projects/dp_result**

## Research Context

### Core Research Question
> How to train an LLM agent that, under partial observability, produces plans satisfying heterogeneous multi-layered constraints through multi-turn tool-calling interactions?

### Problem Complexity
Long-horizon planning is **NP-hard + POMDP**:
- **NP-hard**: Sub-decisions (train, hotel, restaurant, route) couple through shared resources (budget, time). Route optimization is a **TSPTW** (Travelling Salesman Problem with Time Windows). The full problem nests TSPTW inside multi-dimensional resource-constrained optimization.
- **POMDP**: The agent cannot observe the full database. It queries the sandbox API to discover candidates, transportation edges, and prices incrementally.

### Dual-Space Architecture (Core Contribution)
Two sources of difficulty map to two computational spaces with different optimal tools:

**Outer Space (POMDP → Exploration, LLM-driven)**:
- Goal: Decide what to query, in what order, when to stop
- Method: Value of Information (VoI) reasoning
- LLM strengths: semantic understanding, constraint prioritization, adaptive strategy
- RL trains: query ordering, information saturation judgment, proactive coupling-info gathering, solver-failure interpretation

**Inner Space (NP-hard → Exploitation, Solver-driven)**:
- Goal: Given acquired info, find a feasible plan satisfying all constraints
- Method: Compile info into formal optimization instance → symbolic solver (OR-Tools, Z3, Gurobi)
- Solver strengths: exact arithmetic, exhaustive combinatorial search, provable constraint satisfaction
- LLM fills gaps: fuzzy threshold estimation ("near" → [1km, 3km]), commonsense parameters (stay durations, meal timing), qualitative feasibility validation

**The Outer-Inner Loop** (resembles Benders decomposition):
1. LLM queries sandbox → extracts/classifies constraints → estimates fuzzy params → compiles formal instance
2. Solver attempts to solve; if infeasible or under-specified → failure signal feeds back to LLM
3. LLM redirects queries or adjusts parameters → resubmits to solver
4. Repeat until feasible plan or interaction budget exhausted

### Constraint Taxonomy

**Three Layers:**
- Layer 1 — Specialized (Local): per-decision, independently verifiable, encodable as SQL WHERE (hotel star, pool, must-visit POIs)
- Layer 2 — Global (Coupling): cross-decision joint reasoning (budget: `3·train_go + 3·train_back + 2·hotel + 3·restaurant + Σ3·transport ≤ 3000`, time chain feasibility)
- Layer 3 — Commonsense (Implicit): not in any DB (meal hours, check-in buffer, opening hours, pacing)

**2×2 Sub-classification** (determines handling strategy):

| | Existential | Temporal |
|---|---|---|
| **Precise** | hotel.star==3, must-include Deji Plaza | departure date, budget≤3000 |
| **Fuzzy** | "birthday set menu" (field name uncertain) | "evening" return, "near Laomendong" |

- Existential → front-load as query filters, O(1) verification, resolve once
- Temporal → dynamic verification, O(|plan|) cost, recheck as plan evolves
- Fuzzy → conservative threshold (minimax), graduated relaxation

### Unified Constraint Specification Tuple
```
c_i = (variable, type, operator, value, certainty, schedule_sensitivity)
```
- type: categorical | numerical | spatial | temporal
- operator: = | ≤ | ≥ | ∈ | near | before | after
- certainty: precise | fuzzy
- schedule_sensitivity: existential | temporal

For fuzzy constraints, use membership function μ_i(x): X → [0,1] with LLM-estimated [θ_min, θ_max].

### Progressive Heuristic Narrowing
The feasible set contracts: F_0 ⊇ F_1 ⊇ ... ⊇ F_T ≠ ∅

- Phase 1 — Outer Space: Local Filtering (LLM-driven, existential constraints first, high-VoI edges)
- Phase 2 — Inner Space: Global Optimization (Solver-driven, exact budget/time, TSPTW routing)
- Phase 3 — LLM Gap-Filling (fuzzy thresholds, commonsense params, qualitative validation)

### What RL Trains (and What It Does Not)
RL does NOT train arithmetic, temporal propagation, or combinatorial search (solver's job). RL trains:
- **Outer space**: VoI-optimal query ordering, information saturation detection, proactive coupling queries, solver-failure interpretation
- **Inner space gap-filling**: fuzzy constraint calibration via trial-and-error, commonsense parameter estimation

### Why LLMs Fail at Direct Planning
**Outer space** (learnable via RL): templated querying, redundant queries, missing critical queries, no backtracking
**Inner space** (delegate to solver): imprecise arithmetic, fragile temporal chains, incomplete constraint checking, locally greedy selection

### Intra-city Transportation Graph
Sandbox provides weighted directed graph G=(V,E) with edge attrs (distance, duration, cost). Introduces:
- Strong coupling: hotel location determines transit cost/time to every POI
- TSPTW substructure: visit ordering with time windows is NP-hard
- Route-dependent cost: C_transport = Σ n_people · c_edge(i,j)
- O(n²) query space: agent can only query a fraction within interaction budget

### Key Terminology
- **Progressive heuristic narrowing under partial observability**: overall strategy
- **Value of Information (VoI)**: query prioritization criterion in outer space
- **Constraint specification uncertainty**: fuzzy constraints where g(x) ≤ θ, θ unknown
- **TSPTW**: Travelling Salesman Problem with Time Windows
- **Benders decomposition**: analogy for outer-inner loop
- **Neurosymbolic architecture**: LLM (neural, perception) + Solver (symbolic, reasoning)

### Research Gaps
1. No decomposition of constraint types (local/global/commonsense, precise/fuzzy × existential/temporal)
2. NP-hard coupling (TSPTW) ignored by current planning agents
3. No progressive information acquisition / VoI reasoning
4. Constraint specification uncertainty (fuzzy NL → unknown θ)
5. No separation of neural (LLM) and symbolic (solver) responsibilities

### Testbeds
- DeepPlanning (Travel Planning, Shop Planning) — primary
- APEX Agent, GAIA / GAIA 2, OSWorld — secondary

### Notion Page
Detailed writeup: Weekly Updates → Mar. 29th - Apr. 3rd (under Microsoft Project / RL Project)

## Key Commands

```bash
# Environment setup
conda create -n deepplanning python=3.10 -y && conda activate deepplanning
pip install -r requirements.txt

# Pre-command
source ~/.bashrc
conda activate deepplanning
cd /data1/projects/Qwen-Agent/benchmark/deepplanning

# Database extraction (required before first run)
cd shoppingplanning/database_zip && tar -xzf database_level1.tar.gz -C .. && tar -xzf database_level2.tar.gz -C .. && tar -xzf database_level3.tar.gz -C .. && cd ../..
cd travelplanning/database && unzip database_zh.zip && unzip database_en.zip && cd ../..

# Run full benchmark (configure models/domains at top of run_all.sh first)
bash run_all.sh

# Run individual domains
cd shoppingplanning && bash run.sh
cd travelplanning && bash run.sh

# Aggregate results across domains
python aggregate_results.py --model_name <model>
```

## Architecture

### Agent Loop (Both Domains)
Both domains implement the same core pattern: call LLM with tools → detect tool calls → execute tools → append results to message history → repeat until no tool calls remain or `max_llm_calls` is reached. All LLM calls use OpenAI-compatible API via `agent/call_llm.py`.

### Shopping Domain Pipeline
Single-phase: inference runs the agent which searches products, filters, and builds a cart. Evaluation happens offline by comparing `cart.json` against `validation_cases.json`. The agent runs two sequential loops: a general exploration loop, then a cart-finalization loop with an explicit "add to cart" prompt.

- Tool registration: `@register_tool('name')` decorator → `base_shopping_tool.TOOL_REGISTRY`
- Per-sample isolation: each sample gets its own `case_{id}/` directory with `messages.json` and `cart.json`

### Travel Domain Pipeline
Three sequential phases, each resumable via `--start-from`:
1. **Inference**: Agent queries flights, hotels, restaurants, attractions to build a travel plan. Has optional working memory (structured summaries replacing raw tool output) and auto-route querying (auto-injects travel times between locations).
2. **Conversion**: Uses `qwen-plus` model (hardcoded in `evaluation/convert_report.py`) to parse raw agent reports into structured JSON.
3. **Evaluation**: Validates converted plans against temporal, spatial, budget, and personalization constraints.

- Tool registration: inheritance from `BaseTravelTool`, discovered via `__subclasses__()`
- Multi-language: zh and en run sequentially in separate output dirs

### Concurrency
Both domains use `ThreadPoolExecutor` with configurable `--workers`. Each sample is processed independently in its own thread with isolated directories.

### Orchestration Flow
`run_all.sh` → sets env vars (`BENCHMARK_MODEL`, `BENCHMARK_WORKERS`, etc.) → `cd domain/ && bash run.sh` for each domain → `python aggregate_results.py` per model. Domain `run.sh` scripts read these env vars but can also be configured standalone.

## Configuration
- **Models**: Defined in `models_config.json` (model_name, model_type, base_url, api_key_env, temperature, optional extra_body)
- **API Keys**: Stored in `.env` (copy from `.env.example`; never commit)
- **Run settings**: Configured at top of `run_all.sh` (domains, models, workers, levels, language)
- **`qwen-plus` is required**: used by travel domain's conversion phase regardless of which model is being benchmarked

## Key Metrics
- **Shopping**: `match_rate`, `weighted_average_case_score`
- **Travel**: `composite_score`, `case_acc`, `commonsense_score`, `personalized_score`
  - `Delivery` = Sandbox Compliance (valid DB entries)
  - `Commonsense` = implicit temporal/spatial constraints satisfied
  - `Personalized` = user-specific preferences met
  - `Case Accuracy` = fraction where ALL constraints simultaneously satisfied
- **Cross-domain**: `avg_acc` = average of shopping `weighted_average_case_score` and travel `case_acc`

## Result Locations
- Shopping: `shoppingplanning/result_report/{model}_statistics.json`
- Travel: `travelplanning/results/{model}_{language}/evaluation/evaluation_summary.json`
- Aggregated: `aggregated_results/{model}_aggregated.json`

## Todos