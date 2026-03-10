# DeepPlanning Benchmark

## Project Overview
AI agent benchmark evaluating planning capabilities across two domains:
- **Shopping Planning** (`shoppingplanning/`): E-commerce shopping tasks with 3 difficulty levels
- **Travel Planning** (`travelplanning/`): Travel itinerary planning in Chinese and English

## Project Structure
```
deepplanning/
├── run_all.sh              # Unified benchmark runner (main entry point)
├── aggregate_results.py    # Cross-domain result aggregation
├── models_config.json      # Model configurations (API endpoints, keys)
├── .env                    # API keys (DASHSCOPE_API_KEY, OPENAI_API_KEY, VLLM_API_KEY)
├── requirements.txt        # Python dependencies (conda env: deepplanning, python 3.10)
├── aggregated_results/     # Cross-domain aggregated results
├── shoppingplanning/
│   ├── run.py              # Shopping benchmark entry point
│   ├── run.sh              # Shopping domain runner script
│   ├── agent/              # LLM agent (call_llm.py, prompts.py, shopping_agent.py)
│   ├── tools/              # Shopping tools (search, filter, cart operations)
│   ├── evaluation/         # Evaluation pipeline and scoring
│   ├── database_zip/       # Compressed databases (level 1-3)
│   └── result_report/      # Shopping results
└── travelplanning/
    ├── run.py              # Travel benchmark entry point
    ├── run.sh              # Travel domain runner script
    ├── agent/              # LLM agent (call_llm.py, prompts.py, tools_fn_agent.py)
    ├── tools/              # Travel tools (flight, hotel, restaurant, attraction queries)
    ├── evaluation/         # Evaluation (constraints, convert_report.py)
    ├── database/           # Travel databases (zh/en)
    └── results/            # Travel results
```

## Key Commands
```bash
# Run full benchmark
bash run_all.sh

# Run individual domains
cd shoppingplanning && bash run.sh
cd travelplanning && bash run.sh

# Aggregate results
python aggregate_results.py --model_name <model>
```

## Configuration
- **Models**: Defined in `models_config.json` with model_name, model_type (openai), base_url, api_key_env, temperature
- **API Keys**: Stored in `.env` (never commit this file)
- **Run settings**: Configured at top of `run_all.sh` (domains, models, workers, levels, language)

## Key Metrics
- **Shopping**: `match_rate`, `weighted_average_case_score`
- **Travel**: `composite_score`, `case_acc`, `commonsense_score`, `personalized_score`
- **Cross-domain**: `avg_acc` (average of shopping weighted_average_case_score and travel case_acc)

## Important Notes
- `qwen-plus` model config is required (used by travel domain's convert_report.py)
- All models use OpenAI-compatible API format
- Shopping databases need extraction from tar.gz before running
- Travel databases need extraction from zip before running
- `.env` is gitignored - use `.env.example` as template
