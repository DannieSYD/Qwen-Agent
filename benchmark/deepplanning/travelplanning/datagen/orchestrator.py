"""
Pipeline Orchestrator

Runs the full data construction pipeline: Stage 0-5 with parallel execution,
checkpointing, and retry logic.
"""
import json
import os
import random
import shutil
import time
import traceback
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple

from .config import (
    DATABASE_EN_DIR, QUERY_EN_PATH, OUTPUT_DIR,
    DatabaseIndex, PipelineConfig, TaskConfig,
)
from .utils import save_json, load_json


# ============================================================================
# Per-task processing (runs in worker process)
# ============================================================================

def _process_single_task(
    task_config: TaskConfig,
    index_data: Dict,
    pipeline_config: PipelineConfig,
) -> Dict:
    """
    Process a single task through all stages.

    Runs in a worker process. Returns a result dict with status and details.
    """
    task_id = task_config.task_id
    output_dir = os.path.join(pipeline_config.output_dir, f"id_{task_id}")

    result = {
        "task_id": task_id,
        "status": "failed",
        "errors": [],
        "stages_completed": [],
    }

    rng = random.Random(task_id)  # Deterministic per task

    try:
        # Reconstruct DatabaseIndex from serialized data
        from .config import DbEntry, DatabaseIndex
        index = _deserialize_index(index_data)

        # Stage 1: Fork database
        from .stage1_db_fork import fork_database, select_source_db
        source_entry = select_source_db(index, task_config, rng)
        task_config.source_db_entry = source_entry

        db_dir = os.path.join(output_dir, "database")
        fork_database(source_entry, task_config, db_dir, rng)
        result["stages_completed"].append("stage1_db_fork")

        # Stage 2: Build solution
        from .stage2_solution_construction import build_solution
        solution = build_solution(db_dir, task_config, rng)
        if solution is None:
            result["errors"].append("Stage 2: Failed to build valid solution")
            return result
        result["stages_completed"].append("stage2_solution")

        # Stage 3: Inject constraints
        from .stage3_constraint_injection import inject_constraints
        hard_constraints = inject_constraints(
            solution, db_dir, task_config, rng,
            budget_prob=pipeline_config.budget_constraint_prob,
        )
        if not hard_constraints:
            result["errors"].append("Stage 3: No constraints selected")
            return result
        result["stages_completed"].append("stage3_constraints")

        # Build meta dict (matches existing task format)
        meta = {
            "org": task_config.origin,
            "dest": [task_config.dest],
            "days": task_config.days,
            "people_number": task_config.people_number,
            "room_number": task_config.room_number,
            "visiting_city_number": 1,
            "depart_date": task_config.depart_date,
            "return_date": task_config.return_date,
            "hard_constraints": hard_constraints,
        }

        # Stage 4: Generate query
        from .stage4_query_generation import generate_query
        query_data = generate_query(
            task_config, hard_constraints, solution,
            model_name=pipeline_config.query_model,
            rng=rng,
        )
        result["stages_completed"].append("stage4_query")

        # Stage 5: Validate
        from .stage5_validation import validate_task
        passed, errors = validate_task(solution, meta, db_dir, query_data)
        if not passed:
            result["errors"].extend(errors)
            # Try to fix and re-validate once
            if "stage2_solution" in result["stages_completed"]:
                solution2 = build_solution(db_dir, task_config, rng)
                if solution2:
                    passed2, errors2 = validate_task(solution2, meta, db_dir, query_data)
                    if passed2:
                        solution = solution2
                        passed = True
                        result["errors"] = []

        if not passed:
            result["status"] = "validation_failed"
            return result

        result["stages_completed"].append("stage5_validation")

        # Save all outputs
        _save_task_outputs(output_dir, task_config, meta, solution, query_data, hard_constraints)
        result["status"] = "success"

    except Exception as e:
        result["errors"].append(f"Unhandled error: {e}\n{traceback.format_exc()}")

    return result


def _save_task_outputs(
    output_dir: str,
    config: TaskConfig,
    meta: Dict,
    solution: Dict,
    query_data: Dict,
    hard_constraints: Dict,
):
    """Save all task artifacts."""
    os.makedirs(output_dir, exist_ok=True)

    # Save solution
    save_json(solution, os.path.join(output_dir, "solution.json"))

    # Save meta
    save_json(meta, os.path.join(output_dir, "meta.json"))

    # Save query
    save_json(query_data, os.path.join(output_dir, "query.json"))

    # Save full task entry (matches travelplanning_query_en.json format)
    task_entry = {
        "id": config.task_id,
        "query": query_data.get("query", ""),
        "query_with_constraints": query_data.get("query_with_constraints", ""),
        "meta_info": meta,
    }
    save_json(task_entry, os.path.join(output_dir, "task.json"))

    # Save validation info
    save_json({
        "task_id": config.task_id,
        "difficulty": config.difficulty,
        "source_db_id": config.source_db_entry.task_id if config.source_db_entry else None,
        "origin": config.origin,
        "dest": config.dest,
        "days": config.days,
        "people_number": config.people_number,
        "room_number": config.room_number,
    }, os.path.join(output_dir, "validation.json"))


# ============================================================================
# Index serialization (for multiprocessing)
# ============================================================================

def _serialize_index(index: DatabaseIndex) -> Dict:
    """Serialize DatabaseIndex for passing to worker processes."""
    entries = []
    for e in index.entries:
        entries.append({
            "task_id": e.task_id,
            "origin": e.origin,
            "dest": e.dest,
            "days": e.days,
            "people_number": e.people_number,
            "room_number": e.room_number,
            "db_path": e.db_path,
            "num_hotels": e.num_hotels,
            "num_attractions": e.num_attractions,
            "num_restaurants": e.num_restaurants,
            "num_trains": e.num_trains,
            "num_flights": e.num_flights,
            "attraction_types": list(e.attraction_types),
            "hotel_brands": list(e.hotel_brands),
            "hotel_stars": list(e.hotel_stars),
            "has_flights": e.has_flights,
            "has_trains": e.has_trains,
        })
    return {"entries": entries}


def _deserialize_index(data: Dict) -> DatabaseIndex:
    """Deserialize DatabaseIndex from dict."""
    from collections import defaultdict
    from .config import DbEntry

    entries = []
    by_route = defaultdict(list)
    origin_cities = set()
    dest_cities = set()

    for ed in data["entries"]:
        entry = DbEntry(
            task_id=ed["task_id"],
            origin=ed["origin"],
            dest=ed["dest"],
            days=ed["days"],
            people_number=ed["people_number"],
            room_number=ed["room_number"],
            db_path=ed["db_path"],
            num_hotels=ed["num_hotels"],
            num_attractions=ed["num_attractions"],
            num_restaurants=ed["num_restaurants"],
            num_trains=ed["num_trains"],
            num_flights=ed["num_flights"],
            attraction_types=set(ed["attraction_types"]),
            hotel_brands=set(ed["hotel_brands"]),
            hotel_stars=set(ed["hotel_stars"]),
            has_flights=ed["has_flights"],
            has_trains=ed["has_trains"],
        )
        entries.append(entry)
        route = (entry.origin, entry.dest)
        by_route[route].append(entry)
        origin_cities.add(entry.origin)
        dest_cities.add(entry.dest)

    return DatabaseIndex(
        entries=entries,
        by_route=dict(by_route),
        all_routes=sorted(by_route.keys()),
        origin_cities=origin_cities,
        dest_cities=dest_cities,
    )


# ============================================================================
# Manifest / Checkpointing
# ============================================================================

def _load_manifest(output_dir: str) -> Dict:
    """Load or create the generation manifest."""
    manifest_path = os.path.join(output_dir, "manifest.json")
    if os.path.exists(manifest_path):
        return load_json(manifest_path)
    return {"completed": [], "failed": [], "in_progress": []}


def _save_manifest(output_dir: str, manifest: Dict):
    """Save the generation manifest."""
    save_json(manifest, os.path.join(output_dir, "manifest.json"))


# ============================================================================
# Main orchestrator
# ============================================================================

def run_pipeline(
    pipeline_config: Optional[PipelineConfig] = None,
    seed: int = 42,
):
    """
    Run the full data construction pipeline.

    Args:
        pipeline_config: Pipeline configuration. Uses defaults if None.
        seed: Random seed for reproducibility.
    """
    if pipeline_config is None:
        pipeline_config = PipelineConfig()

    rng = random.Random(seed)
    os.makedirs(pipeline_config.output_dir, exist_ok=True)

    print("=" * 60)
    print("Data Construction Pipeline")
    print("=" * 60)
    print(f"  Tasks to generate: {pipeline_config.num_tasks}")
    print(f"  Workers:           {pipeline_config.workers}")
    print(f"  Output:            {pipeline_config.output_dir}")
    print(f"  Query model:       {pipeline_config.query_model}")
    print(f"  Seed:              {seed}")
    print("=" * 60)

    # Stage 0: Build database index
    print("\n[Stage 0] Building database index...")
    from .stage0_db_index import build_db_index
    index = build_db_index()

    # Generate task configs
    print(f"\n[Config] Generating {pipeline_config.num_tasks} task configurations...")
    from .difficulty import generate_task_configs, print_difficulty_distribution
    configs = generate_task_configs(index, pipeline_config, rng)
    print_difficulty_distribution(configs)

    # Load manifest for checkpoint recovery
    manifest = _load_manifest(pipeline_config.output_dir)
    completed_ids = set(manifest["completed"])

    # Filter out already-completed tasks
    pending_configs = [c for c in configs if c.task_id not in completed_ids]
    print(f"[Checkpoint] {len(completed_ids)} already completed, "
          f"{len(pending_configs)} remaining")

    if not pending_configs:
        print("All tasks already completed!")
        _print_final_summary(manifest, pipeline_config)
        return

    # Serialize index for worker processes
    index_data = _serialize_index(index)

    # Run tasks
    start_time = time.time()
    success_count = len(completed_ids)
    fail_count = len(manifest["failed"])

    workers = min(pipeline_config.workers, len(pending_configs))

    if workers <= 1:
        # Sequential execution (useful for debugging)
        for config in pending_configs:
            result = _process_single_task(config, index_data, pipeline_config)
            _handle_result(result, manifest, pipeline_config)
            if result["status"] == "success":
                success_count += 1
            else:
                fail_count += 1
            _print_progress(success_count, fail_count, len(configs), start_time)
    else:
        # Parallel execution
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _process_single_task, config, index_data, pipeline_config
                ): config
                for config in pending_configs
            }

            for future in as_completed(futures):
                config = futures[future]
                try:
                    result = future.result(timeout=300)
                except Exception as e:
                    result = {
                        "task_id": config.task_id,
                        "status": "error",
                        "errors": [f"Worker error: {e}"],
                        "stages_completed": [],
                    }

                _handle_result(result, manifest, pipeline_config)
                if result["status"] == "success":
                    success_count += 1
                else:
                    fail_count += 1
                _print_progress(success_count, fail_count, len(configs), start_time)

    # Save final manifest
    _save_manifest(pipeline_config.output_dir, manifest)

    # Merge all task entries into a single query JSON
    _merge_task_entries(pipeline_config.output_dir)

    _print_final_summary(manifest, pipeline_config)


def _handle_result(result: Dict, manifest: Dict, pipeline_config: PipelineConfig):
    """Handle a completed task result."""
    task_id = result["task_id"]
    if result["status"] == "success":
        manifest["completed"].append(task_id)
    else:
        manifest["failed"].append(task_id)
        if result["errors"]:
            print(f"\n  [FAIL] Task {task_id}: {result['errors'][0][:100]}")

    # Periodic manifest save (every 50 tasks)
    total_processed = len(manifest["completed"]) + len(manifest["failed"])
    if total_processed % 50 == 0:
        _save_manifest(pipeline_config.output_dir, manifest)


def _print_progress(success: int, fail: int, total: int, start_time: float):
    """Print progress update."""
    done = success + fail
    elapsed = time.time() - start_time
    rate = done / elapsed if elapsed > 0 else 0
    eta = (total - done) / rate if rate > 0 else 0
    print(f"\r  Progress: {done}/{total} "
          f"(success={success}, fail={fail}) "
          f"[{rate:.1f} tasks/s, ETA {eta:.0f}s]", end="", flush=True)


def _print_final_summary(manifest: Dict, pipeline_config: PipelineConfig):
    """Print final pipeline summary."""
    completed = len(manifest["completed"])
    failed = len(manifest["failed"])
    total = completed + failed

    print(f"\n\n{'='*60}")
    print(f"Pipeline Complete")
    print(f"{'='*60}")
    print(f"  Successful: {completed}")
    print(f"  Failed:     {failed}")
    print(f"  Total:      {total}")
    print(f"  Success rate: {completed/total*100:.1f}%" if total > 0 else "")
    print(f"  Output: {pipeline_config.output_dir}")
    merged_path = os.path.join(pipeline_config.output_dir, "travelplanning_query_generated.json")
    if os.path.exists(merged_path):
        print(f"  Merged query file: {merged_path}")
    print(f"{'='*60}\n")


def _merge_task_entries(output_dir: str):
    """Merge all individual task.json files into one query JSON."""
    all_tasks = []
    task_dirs = sorted(
        [d for d in os.listdir(output_dir) if d.startswith("id_")],
        key=lambda x: int(x.split("_")[1]),
    )

    for task_dir in task_dirs:
        task_path = os.path.join(output_dir, task_dir, "task.json")
        if os.path.exists(task_path):
            task_data = load_json(task_path)
            all_tasks.append(task_data)

    if all_tasks:
        merged_path = os.path.join(output_dir, "travelplanning_query_generated.json")
        save_json(all_tasks, merged_path)
        print(f"[Merge] Merged {len(all_tasks)} tasks into {merged_path}")


# ============================================================================
# CLI entry point
# ============================================================================

def main():
    """CLI entry point for the pipeline."""
    import argparse

    parser = argparse.ArgumentParser(description="Travel Planning Data Construction Pipeline")
    parser.add_argument("--num-tasks", type=int, default=1000, help="Number of tasks to generate")
    parser.add_argument("--workers", type=int, default=40, help="Number of parallel workers")
    parser.add_argument("--start-id", type=int, default=1000, help="Starting task ID")
    parser.add_argument("--output-dir", type=str, default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--query-model", type=str, default="qwen-plus", help="LLM model for query generation")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--budget-prob", type=float, default=0.15, help="Probability of budget constraint")
    parser.add_argument("--difficulty-weights", type=str, default="0.4,0.35,0.25",
                        help="Difficulty distribution (easy,medium,hard)")
    args = parser.parse_args()

    dw = tuple(float(x) for x in args.difficulty_weights.split(","))

    config = PipelineConfig(
        num_tasks=args.num_tasks,
        workers=args.workers,
        start_task_id=args.start_id,
        output_dir=args.output_dir,
        query_model=args.query_model,
        budget_constraint_prob=args.budget_prob,
        difficulty_weights=dw,
    )

    run_pipeline(config, seed=args.seed)


if __name__ == "__main__":
    main()
