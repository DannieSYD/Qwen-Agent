BENCHMARK_PROMPT_VARIANT=guided BENCHMARK_MODEL=qwen3-30b-a3b-thinking-2507 
BENCHMARK_WORKERS=5 BENCHMARK_LANGUAGE=en 

python run.py \
  --model qwen3-30b-a3b-thinking-2507 \
  --language en \
  --prompt-variant guided \
  --workers 5 \
  --rerun-ids "0,1,2,3,4"

