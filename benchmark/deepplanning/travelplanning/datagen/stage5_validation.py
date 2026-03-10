"""
Stage 5: Validation

End-to-end validation that each generated task is valid and solvable.
Verifies commonsense + hard constraints, uniqueness, and query safety.
"""
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import DB_FILES, TaskConfig
from .utils import read_csv_as_dicts, safe_float, safe_int


def validate_task(
    solution: Dict,
    meta: Dict,
    db_dir: str,
    query_data: Optional[Dict] = None,
) -> Tuple[bool, List[str]]:
    """
    Run all validation checks on a generated task.

    Args:
        solution: The reference solution JSON.
        meta: Task metadata (org, dest, days, people_number, room_number, hard_constraints, ...).
        db_dir: Path to the task's database directory.
        query_data: Optional dict with "query" and "query_with_constraints".

    Returns:
        (passed, list_of_error_messages)
    """
    errors = []

    # Check 1: Hard constraints
    hard_ok, hard_errors = _check_hard_constraints(solution, meta, db_dir)
    errors.extend(hard_errors)

    # Check 2: Commonsense constraints
    cs_ok, cs_errors = _check_commonsense(solution, meta, db_dir)
    errors.extend(cs_errors)

    # Check 3: Database integrity
    db_ok, db_errors = _check_database_integrity(solution, db_dir)
    errors.extend(db_errors)

    # Check 4: Query safety (no answer leakage)
    if query_data:
        q_ok, q_errors = _check_query_safety(query_data, meta)
        errors.extend(q_errors)

    passed = len(errors) == 0
    return passed, errors


def _check_hard_constraints(
    solution: Dict, meta: Dict, db_dir: str,
) -> Tuple[bool, List[str]]:
    """Check hard constraint evaluation."""
    errors = []
    try:
        from travelplanning.evaluation.constraints_hard import eval_hard
        results = eval_hard(solution, meta)
        for cname, (passed, msg) in results.items():
            if passed is not None and not passed:
                errors.append(f"[Hard] {cname}: {msg}")
    except Exception as e:
        errors.append(f"[Hard] Evaluation error: {e}")

    return len(errors) == 0, errors


def _check_commonsense(
    solution: Dict, meta: Dict, db_dir: str,
) -> Tuple[bool, List[str]]:
    """Check commonsense constraint evaluation."""
    errors = []
    try:
        from travelplanning.evaluation.constraints_commonsense import eval_commonsense
        results = eval_commonsense(solution, meta, database_dir=Path(db_dir))
        for cname, (passed, msg) in results.items():
            if passed is not None and not passed:
                errors.append(f"[Commonsense] {cname}: {msg}")
    except Exception as e:
        errors.append(f"[Commonsense] Evaluation error: {e}")

    return len(errors) == 0, errors


def _check_database_integrity(
    solution: Dict, db_dir: str,
) -> Tuple[bool, List[str]]:
    """Check that all solution entities exist in the database."""
    errors = []
    import os

    # Check hotels
    hotels_path = os.path.join(db_dir, DB_FILES["hotels"])
    if os.path.exists(hotels_path):
        hotel_names = {r["name"] for r in read_csv_as_dicts(hotels_path)}
        for day in solution.get("daily_plans", []):
            accom = day.get("accommodation")
            if accom and isinstance(accom, dict):
                name = accom.get("name", "")
                if name and name != "-" and name not in hotel_names:
                    errors.append(f"[DB] Hotel not in database: {name}")

    # Check attractions
    attractions_path = os.path.join(db_dir, DB_FILES["attractions"])
    if os.path.exists(attractions_path):
        attr_names = {r["attraction_name"] for r in read_csv_as_dicts(attractions_path)}
        for day in solution.get("daily_plans", []):
            for act in day.get("activities", []):
                if act.get("type") == "attraction":
                    name = act.get("details", {}).get("name", "")
                    if name and name not in attr_names:
                        errors.append(f"[DB] Attraction not in database: {name}")

    # Check restaurants
    restaurants_path = os.path.join(db_dir, DB_FILES["restaurants"])
    if os.path.exists(restaurants_path):
        rest_names = {r["restaurant_name"] for r in read_csv_as_dicts(restaurants_path)}
        for day in solution.get("daily_plans", []):
            for act in day.get("activities", []):
                if act.get("type") == "meal":
                    name = act.get("details", {}).get("name", "")
                    if name and name not in rest_names:
                        errors.append(f"[DB] Restaurant not in database: {name}")

    return len(errors) == 0, errors


def _check_query_safety(
    query_data: Dict, meta: Dict,
) -> Tuple[bool, List[str]]:
    """Check that the query doesn't leak answer entity names."""
    errors = []
    query_text = query_data.get("query", "")

    hard_constraints = meta.get("hard_constraints", {})
    for cname, cdata in hard_constraints.items():
        # Skip must-eat-named — restaurant name IS supposed to be in query
        if cname == "restaurant_must_eat_named":
            continue

        # Check hotel name
        hotel_name = cdata.get("hotel_name", "")
        if hotel_name and hotel_name in query_text:
            errors.append(f"[Query] Leaks hotel name: {hotel_name}")

        # Check restaurant name
        rest_name = cdata.get("restaurant_name", "")
        if rest_name and rest_name in query_text:
            errors.append(f"[Query] Leaks restaurant name: {rest_name}")

        # Check transport numbers
        for key in ["outbound_train_no", "inbound_train_no",
                     "outbound_flight_no", "inbound_flight_no"]:
            transport_no = cdata.get(key, "")
            if transport_no and transport_no in query_text:
                errors.append(f"[Query] Leaks transport number: {transport_no}")

    return len(errors) == 0, errors
