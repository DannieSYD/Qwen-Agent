"""
Difficulty control for curriculum learning.

Controls task parameters based on difficulty level (1=easy, 2=medium, 3=hard).
"""
import random
from typing import Dict, List, Optional, Tuple

from .config import DatabaseIndex, PipelineConfig, TaskConfig
from .utils import date_add_days, weekday_from_date


# ============================================================================
# Difficulty-dependent parameter ranges
# ============================================================================

DIFFICULTY_PARAMS = {
    1: {  # Easy
        "days_range": (2, 3),
        "people_range": (1, 2),
        "num_constraints": 4,
        "budget_prob": 0.10,
    },
    2: {  # Medium
        "days_range": (3, 5),
        "people_range": (2, 3),
        "num_constraints": 4,
        "budget_prob": 0.15,
    },
    3: {  # Hard
        "days_range": (5, 7),
        "people_range": (3, 4),
        "num_constraints": 5,
        "budget_prob": 0.20,
    },
}


def _room_number_for_people(people: int, rng: random.Random) -> int:
    """Determine room number based on people count."""
    if people <= 2:
        return 1
    elif people == 3:
        return rng.choice([1, 2])
    else:
        return 2


# ============================================================================
# Generate a batch of diverse task configs
# ============================================================================

def generate_task_configs(
    index: DatabaseIndex,
    pipeline_config: PipelineConfig,
    rng: Optional[random.Random] = None,
) -> List[TaskConfig]:
    """
    Generate diverse task configurations.

    Ensures uniform coverage of routes, days, people counts, and difficulties.
    """
    if rng is None:
        rng = random.Random()

    configs = []
    num_tasks = pipeline_config.num_tasks
    start_id = pipeline_config.start_task_id

    # Available routes
    routes = list(index.all_routes)
    if not routes:
        raise ValueError("No routes available in database index")

    # Difficulty distribution
    w_easy, w_med, w_hard = pipeline_config.difficulty_weights
    total_w = w_easy + w_med + w_hard
    difficulties = rng.choices(
        population=[1, 2, 3],
        weights=[w_easy / total_w, w_med / total_w, w_hard / total_w],
        k=num_tasks,
    )

    # Base date pool — spread across different months
    base_dates = [
        "2025-11-12", "2025-12-05", "2026-01-15",
        "2026-02-20", "2026-03-10", "2026-04-08",
        "2026-05-17", "2026-06-22", "2026-07-14",
        "2026-08-03", "2026-09-11", "2026-10-18",
    ]

    # Route usage tracking for diversity
    route_usage = {r: 0 for r in routes}

    for i in range(num_tasks):
        task_id = start_id + i
        difficulty = difficulties[i]
        params = DIFFICULTY_PARAMS[difficulty]

        # Select route — prefer underrepresented routes
        route = _select_route_weighted(routes, route_usage, rng)
        route_usage[route] += 1
        origin, dest = route

        # Days — clamp to max feasible for the route's databases
        route_entries = index.by_route.get(route, [])
        max_feasible = max((e.max_feasible_days for e in route_entries), default=7)
        days_lo, days_hi = params["days_range"]
        days_hi = min(days_hi, max_feasible)
        if days_lo > days_hi:
            days_lo = days_hi
        days = rng.randint(days_lo, days_hi)

        people = rng.randint(*params["people_range"])
        rooms = _room_number_for_people(people, rng)

        # Dates
        base_date = rng.choice(base_dates)
        # Add some random offset to avoid all tasks having the same date
        offset = rng.randint(0, 20)
        depart_date = date_add_days(base_date, offset)
        return_date = date_add_days(depart_date, days - 1)
        depart_weekday = weekday_from_date(depart_date)

        configs.append(TaskConfig(
            task_id=task_id,
            origin=origin,
            dest=dest,
            days=days,
            people_number=people,
            room_number=rooms,
            depart_date=depart_date,
            return_date=return_date,
            depart_weekday=depart_weekday,
            difficulty=difficulty,
        ))

    return configs


def _select_route_weighted(
    routes: List[Tuple[str, str]],
    route_usage: Dict[Tuple[str, str], int],
    rng: random.Random,
) -> Tuple[str, str]:
    """Select a route, favoring underrepresented ones."""
    max_usage = max(route_usage.values()) if route_usage else 1
    # Weight = (max_usage + 1 - current_usage), so least-used routes get highest weight
    weights = [max_usage + 1 - route_usage.get(r, 0) for r in routes]
    return rng.choices(routes, weights=weights, k=1)[0]


# ============================================================================
# Difficulty summary
# ============================================================================

def print_difficulty_distribution(configs: List[TaskConfig]):
    """Print the difficulty distribution of generated configs."""
    from collections import Counter
    diff_counts = Counter(c.difficulty for c in configs)
    day_counts = Counter(c.days for c in configs)
    people_counts = Counter(c.people_number for c in configs)
    route_counts = Counter((c.origin, c.dest) for c in configs)

    print(f"\n{'='*50}")
    print(f"Task Config Summary ({len(configs)} tasks)")
    print(f"{'='*50}")
    print(f"Difficulty: {dict(sorted(diff_counts.items()))}")
    print(f"Days:       {dict(sorted(day_counts.items()))}")
    print(f"People:     {dict(sorted(people_counts.items()))}")
    print(f"Routes:     {len(route_counts)} unique (min {min(route_counts.values())}, "
          f"max {max(route_counts.values())} per route)")
    print(f"{'='*50}\n")
