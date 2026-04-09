"""
Solver executor — three modes:

1. run_solver_template (v4): Fixed CP-SAT template + greedy scheduler.
   No LLM-generated code. Reads constraints + data, solves, returns plan.

2. run_solver_selection (v4): Fixed CP-SAT template, returns structured
   entity selection for LLM-driven scheduling. The LLM arranges entities
   into a day-by-day plan heuristically, faithful to the solver's picks.

3. run_solver_code (v3, legacy): Sandboxed execution of LLM-generated code.
   Kept for backward compatibility.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any, Dict


# Template that wraps the LLM-generated code in a safe execution environment.
# The data dict is passed via a temp file to avoid shell escaping issues.
_WRAPPER_TEMPLATE = """\
import sys
import json
import math
from datetime import datetime, timedelta

# Load solver data
with open(sys.argv[1], 'r', encoding='utf-8') as _f:
    data = json.load(_f)

# Pre-import OR-Tools
try:
    from ortools.sat.python import cp_model
except ImportError:
    print("ERROR: ortools not installed. Run: pip install ortools")
    sys.exit(1)

# ---- LLM-generated code below ----
{code}
"""


def _check_code_quality(code: str) -> str:
    """
    Static analysis of LLM-generated solver code.
    Returns feedback string if issues found, empty string if OK.
    """
    issues = []

    # Must actually call solver.Solve()
    if 'Solve(' not in code and 'solve(' not in code:
        issues.append(
            "Your code never calls solver.Solve(model). You MUST build a CP-SAT model "
            "with constraints, solve it, then use solver.Value() to read the results. "
            "Do NOT hardcode selections."
        )

    # Must read from data dict, not hardcode everything
    if 'data[' not in code and 'data.get(' not in code:
        issues.append(
            "Your code does not read from the `data` dict. You MUST use "
            "data['hotels'], data['outbound_transport'], data['restaurants'], "
            "data['constraints'], etc. Do NOT hardcode values."
        )

    # Must NOT redefine the pre-loaded data dict
    import re
    if re.search(r'\bdata\s*=\s*\{', code):
        issues.append(
            "Your code redefines `data = {...}` which SHADOWS the pre-loaded data dict. "
            "REMOVE your `data = {...}` block. The variable `data` is already loaded with "
            "all working memory data. Just use data['hotels'], data['constraints'], etc."
        )

    # Must use solver.Value() to extract results
    if 'Solve(' in code and 'Value(' not in code:
        issues.append(
            "Your code calls Solve() but never calls solver.Value() to read results. "
            "After solving, use solver.Value(var) to determine which options were selected."
        )

    if issues:
        return (
            "SOLVER_ERROR: Code quality check failed.\n\n" + "\n\n".join(issues)
            + "\n\nThe `data` dict is auto-populated from working memory — do NOT redefine it."
        )
    return ""


def _validate_output_against_constraints(
    output: str, data: Dict[str, Any]
) -> str:
    """
    Check if the solver output plan satisfies the constraints from data.
    Returns feedback string if violations found, empty string if OK.
    """
    violations = []
    constraints = data.get('constraints', [])
    output_lower = output.lower()

    for c in constraints:
        var = c.get('variable', '')
        value = c.get('value', '')
        operator = c.get('operator', '')

        # Check hotel service constraints (e.g., swimming pool)
        if var == 'hotel.service' and value:
            service_lower = str(value).lower()
            hotels = data.get('hotels', [])
            selected_hotel = None
            for h in hotels:
                if h.get('name', '').lower() in output_lower:
                    selected_hotel = h
                    break
            if selected_hotel:
                services = [s.lower() for s in selected_hotel.get('services', [])]
                if service_lower not in services:
                    # Find hotels that DO have the service
                    valid_hotels = [
                        h['name'] for h in hotels
                        if service_lower in [s.lower() for s in h.get('services', [])]
                    ]
                    hint = f" Hotels with '{value}': {valid_hotels}" if valid_hotels else f" No hotels in data have '{value}' — query more hotels."
                    violations.append(
                        f"Constraint '{var} = {value}': Selected hotel "
                        f"'{selected_hotel['name']}' does not have '{value}'.{hint}"
                    )

        # Check restaurant tag constraints (e.g., birthday set menu)
        if var == 'restaurant.tag' and value:
            tag_lower = str(value).lower()
            restaurants = data.get('restaurants', [])
            # Find which restaurants have this tag
            matching_restaurants = [
                r['name'] for r in restaurants
                if any(tag_lower in t.lower() for t in r.get('tags', []))
            ]
            if matching_restaurants:
                # Check if any matching restaurant appears in the plan
                found = any(
                    r.lower() in output_lower for r in matching_restaurants
                )
                if not found:
                    violations.append(
                        f"Constraint '{var} = {value}': None of the matching "
                        f"restaurants ({matching_restaurants}) appear in your plan. "
                        f"You must select a restaurant whose tags include '{value}'."
                    )

        # Check must-visit attractions
        if var == 'attraction.must_visit' or (var == 'attraction.name' and operator == 'in'):
            names = value if isinstance(value, list) else [value]
            for name in names:
                if name.lower() not in output_lower:
                    violations.append(
                        f"Constraint 'must visit {name}': This attraction does "
                        f"not appear in your plan output."
                    )

        # Check budget constraint
        if var == 'budget.total' and operator == '<=' and value:
            try:
                max_budget = float(value)
                # Try to extract total from output
                import re
                total_matches = re.findall(
                    r'total[^:]*:\s*¥?\s*(\d+)', output_lower
                )
                if not total_matches:
                    total_matches = re.findall(
                        r'total[^:]*:\s*(\d+)', output_lower
                    )
                if total_matches:
                    total = float(total_matches[-1])
                    if total > max_budget:
                        violations.append(
                            f"Constraint 'budget <= {max_budget}': Your plan "
                            f"total is {total}, which exceeds the budget."
                        )
            except (ValueError, TypeError):
                pass

    if violations:
        return (
            "SOLVER_FEEDBACK: Your plan was generated but violates constraints:\n\n"
            + "\n".join(f"- {v}" for v in violations)
            + "\n\nYou have two options:\n"
            "1. If the needed data is in `data`, fix your solver constraints (model.Add) and re-run.\n"
            "2. If data is MISSING (e.g., no hotels with required services), call query tools "
            "to gather more data FIRST, then call `run_solver` again — the `data` dict will "
            "be refreshed with the updated working memory."
        )
    return ""


def run_solver_code(
    code: str,
    data: Dict[str, Any],
    timeout: float = 60.0,
) -> str:
    """
    Execute LLM-generated Python code with pre-loaded working memory data.

    The code has access to:
    - `data`: dict with all working memory data (see data_export.py)
    - `cp_model`: from ortools.sat.python
    - `datetime`, `timedelta`, `json`, `math`

    The code should print() the final plan text to stdout.

    Includes two feedback mechanisms:
    1. Code quality gate: checks if code actually uses CP-SAT (not just hardcoded)
    2. Output constraint validation: checks if output satisfies data constraints

    Args:
        code: Python code string generated by the LLM
        data: Working memory data dict (from export_memory_as_dict)
        timeout: Max execution time in seconds

    Returns:
        stdout output on success, or error/feedback message on failure
    """
    if not code or not code.strip():
        return "SOLVER_ERROR: No code provided."

    # --- Phase 1: Code quality gate ---
    quality_feedback = _check_code_quality(code)
    if quality_feedback:
        return quality_feedback

    # Indent LLM code to fit inside the wrapper (it's at top level after the imports)
    # Actually, the code is inserted at the module level, no indentation needed.
    wrapped_code = _WRAPPER_TEMPLATE.format(code=code)

    # Write data to a temp file (avoids stdin/pipe size limits)
    try:
        data_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.json', delete=False, encoding='utf-8'
        )
        json.dump(data, data_file, ensure_ascii=False)
        data_file.close()

        code_file = tempfile.NamedTemporaryFile(
            mode='w', suffix='.py', delete=False, encoding='utf-8'
        )
        code_file.write(wrapped_code)
        code_file.close()

        result = subprocess.run(
            [sys.executable, code_file.name, data_file.name],
            capture_output=True,
            text=True,
            timeout=timeout,
            cwd=str(Path(__file__).resolve().parent.parent),
        )

        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        if result.returncode != 0:
            # Return error with traceback for the LLM to debug
            error_msg = stderr if stderr else f"Process exited with code {result.returncode}"
            # Strip the wrapper line numbers to show LLM-relative line numbers
            return (
                f"SOLVER_ERROR: Code execution failed.\n\n{error_msg}\n\n"
                "Fix your code and call `run_solver` again. "
                "Remember: `data` is pre-loaded — do NOT redefine it. "
                "If data is missing, call query tools first to gather more info."
            )

        if not stdout:
            if stderr:
                return f"SOLVER_ERROR: Code produced no output.\nStderr: {stderr}"
            return "SOLVER_ERROR: Code produced no output. Your code must print() the plan text."

        # --- Phase 2: Output constraint validation ---
        constraint_feedback = _validate_output_against_constraints(stdout, data)
        if constraint_feedback:
            return constraint_feedback

        return stdout

    except subprocess.TimeoutExpired:
        return f"SOLVER_ERROR: Code execution timed out after {timeout}s. Simplify your model or add a solver time limit."

    except Exception as e:
        return f"SOLVER_ERROR: Failed to execute code: {e}"

    finally:
        # Clean up temp files
        try:
            Path(data_file.name).unlink(missing_ok=True)
        except Exception:
            pass
        try:
            Path(code_file.name).unlink(missing_ok=True)
        except Exception:
            pass


# ======================================================================
# v4: Fixed CP-SAT template (no LLM code)
# ======================================================================

def run_solver_template(
    data: Dict[str, Any],
    memory,
    timeout: float = 60.0,
) -> str:
    """
    Run the parameterized CP-SAT template on working memory data.

    No LLM-generated code. The template reads constraints from data["constraints"]
    and compiles them generically into a CP-SAT model.

    Args:
        data: Working memory data dict (from export_memory_as_dict)
        memory: WorkingMemory instance (needed for assemble_day in scheduling)
        timeout: Max execution time (not enforced here since no subprocess)

    Returns:
        Plan text on success, or prefixed feedback message on failure:
        - "SOLVER_FEEDBACK: ..." — missing data, LLM should query more
        - "SOLVER_INFEASIBLE: ..." — constraints conflict
        - "SOLVER_ERROR: ..." — internal error
    """
    try:
        from solver.cp_template import CPSATEntitySelector
        from solver.scheduler import GreedyScheduler
    except ImportError:
        from .cp_template import CPSATEntitySelector
        from .scheduler import GreedyScheduler

    # Phase 1: CP-SAT entity selection
    selector = CPSATEntitySelector(data)
    result = selector.solve()

    if result.status == "MISSING_DATA":
        return result.feedback

    if result.status == "INFEASIBLE":
        return result.feedback

    if result.status == "ERROR":
        return f"SOLVER_ERROR: {result.feedback}"

    # Check for unresolved fuzzy constraints — return feedback before scheduling
    # so LLM can resolve them and re-run with correct constraints
    if result.feedback and "SOLVER_FUZZY_UNRESOLVED" in result.feedback:
        return result.feedback

    # Phase 2: Greedy scheduling
    scheduler = GreedyScheduler(data, result, memory)
    plan_text, sched_feedback = scheduler.schedule()

    if not plan_text:
        return sched_feedback or "SOLVER_ERROR: Scheduler produced no output."

    if sched_feedback:
        return plan_text + "\n\n" + sched_feedback

    return plan_text


# ======================================================================
# v4 LLM-scheduled: CP-SAT selects entities, LLM arranges them
# ======================================================================

def run_solver_selection(
    data: Dict[str, Any],
) -> str:
    """
    Run CP-SAT entity selection only — no scheduling.

    Returns a structured entity selection prompt for the LLM to arrange
    into a day-by-day plan heuristically. The LLM must use exactly these
    entities (faithfulness constraint).

    Args:
        data: Working memory data dict (from export_memory_as_dict)

    Returns:
        Structured selection text on success, or prefixed feedback on failure.
    """
    try:
        from solver.cp_template import CPSATEntitySelector
    except ImportError:
        from .cp_template import CPSATEntitySelector

    selector = CPSATEntitySelector(data)
    result = selector.solve()

    if result.status == "MISSING_DATA":
        return result.feedback

    if result.status == "INFEASIBLE":
        return result.feedback

    if result.status == "ERROR":
        return f"SOLVER_ERROR: {result.feedback}"

    if result.feedback and "SOLVER_FUZZY_UNRESOLVED" in result.feedback:
        return result.feedback

    # Format as structured selection for LLM
    return format_solver_selection(result, data)


def format_solver_selection(result, data: Dict[str, Any]) -> str:
    """
    Format SolverResult into a structured entity selection prompt.

    The LLM receives this as the run_solver tool output and must arrange
    these entities into a coherent day-by-day travel plan.
    """
    meta = data.get("trip_meta", {})
    attractions_data = data.get("attractions", {})
    routes = data.get("routes", {})

    lines = []
    lines.append("SOLVER_SELECTION: The optimizer has selected the following entities.")
    lines.append("You MUST use ALL of these in your plan — do not add, remove, or swap any.")
    lines.append("")

    # Trip metadata
    lines.append(f"Trip: {meta.get('origin', '?')} → {', '.join(meta.get('destinations', []))} "
                 f"| {meta.get('days', '?')} days | {meta.get('people', 1)} people "
                 f"| {meta.get('rooms', 1)} room(s)")
    lines.append(f"Dates: {meta.get('depart_date', '?')} to {meta.get('return_date', '?')}")
    lines.append("")

    # Outbound transport
    ob = result.outbound
    if ob:
        lines.append("── Outbound Transport ──")
        lines.append(f"  {ob.get('mode', 'train').capitalize()} {ob.get('id', '?')}: "
                     f"{ob.get('dep_station', '?')} → {ob.get('arr_station', '?')}")
        lines.append(f"  Departure: {ob.get('dep_time', '?')} | Arrival: {ob.get('arr_time', '?')}")
        lines.append(f"  Price: ¥{ob.get('price', '?')}/person | Seat: {ob.get('seat_class', '?')}")
        lines.append("")

    # Inbound transport
    ib = result.inbound
    if ib:
        lines.append("── Inbound Transport ──")
        lines.append(f"  {ib.get('mode', 'train').capitalize()} {ib.get('id', '?')}: "
                     f"{ib.get('dep_station', '?')} → {ib.get('arr_station', '?')}")
        lines.append(f"  Departure: {ib.get('dep_time', '?')} | Arrival: {ib.get('arr_time', '?')}")
        lines.append(f"  Price: ¥{ib.get('price', '?')}/person | Seat: {ib.get('seat_class', '?')}")
        lines.append("")

    # Hotel
    h = result.hotel
    if h:
        lines.append("── Hotel ──")
        lines.append(f"  {h.get('name', '?')} | {h.get('star', '?')}★ | ¥{h.get('price', '?')}/room/night")
        services = h.get('services', [])
        if services:
            lines.append(f"  Services: {', '.join(services)}")
        lines.append("")

    # Attractions
    if result.attractions:
        lines.append(f"── Attractions ({len(result.attractions)} selected) ──")
        for attr_name in result.attractions:
            detail = attractions_data.get(attr_name, {})
            price = detail.get('price', 0) or 0
            open_t = detail.get('open', '?')
            close_t = detail.get('close', '?')
            visit_min = detail.get('visit_min_hrs', 1.0)
            visit_max = detail.get('visit_max_hrs', 2.0)
            lines.append(f"  • {attr_name}: ¥{price}/person, open {open_t}-{close_t}, "
                         f"visit {visit_min}-{visit_max}h")
        lines.append("")

    # Restaurants
    if result.restaurants:
        lines.append(f"── Restaurants ({len(result.restaurants)} selected) ──")
        for r in result.restaurants:
            tags = r.get('tags', [])
            tag_str = f" [{', '.join(tags)}]" if tags else ""
            near = r.get('near', '')
            near_str = f" (near {near})" if near else ""
            lines.append(f"  • {r.get('name', '?')}: ¥{r.get('price_per_person', '?')}/person, "
                         f"open {r.get('open', '?')}-{r.get('close', '?')}{tag_str}{near_str}")
        lines.append("")

    # Relevant routes
    all_locations = []
    if h:
        all_locations.append(h.get('name', ''))
    all_locations.extend(result.attractions)
    all_locations.extend(r.get('name', '') for r in result.restaurants)
    # Add stations
    if ob:
        all_locations.append(ob.get('arr_station', ''))
    if ib:
        all_locations.append(ib.get('dep_station', ''))

    relevant_routes = []
    for key, route_data in routes.items():
        parts = key.split(" -> ")
        if len(parts) == 2:
            origin, dest = parts
            if origin in all_locations and dest in all_locations:
                dur = route_data.get('duration_min', '?')
                dist = route_data.get('distance_km', '?')
                cost = route_data.get('cost', '?')
                relevant_routes.append(f"  {origin} → {dest}: {dur}min, {dist}km, ¥{cost}/person")

    if relevant_routes:
        lines.append("── Routes Between Selected Locations ──")
        lines.extend(relevant_routes)
        lines.append("")

    # Cost breakdown
    bd = result.cost_breakdown
    lines.append("── Cost Breakdown (solver estimate) ──")
    lines.append(f"  Transport: ¥{bd.get('transport', 0)}")
    lines.append(f"  Hotel: ¥{bd.get('hotel', 0)}")
    lines.append(f"  Meals: ¥{bd.get('meals', 0)}")
    lines.append(f"  Attractions: ¥{bd.get('attractions', 0)}")
    lines.append(f"  City transport (est.): ¥{bd.get('city_transport', 0)}")
    lines.append(f"  Total: ¥{result.total_cost}")
    lines.append("")

    # Instructions
    lines.append("── YOUR TASK ──")
    lines.append("Arrange these entities into a complete day-by-day travel plan.")
    lines.append("Rules:")
    lines.append("  1. Use EVERY entity above — do not skip, add, or substitute any.")
    lines.append("  2. Respect opening hours, visit durations, and transport times.")
    lines.append("  3. Schedule meals at sensible times (lunch ~11:00-14:00, dinner ~17:00-21:00).")
    lines.append("  4. Allow buffer time after arriving by train/flight (luggage, transit).")
    lines.append("  5. Include travel_city segments between locations with duration, distance, and cost.")
    lines.append("  6. End with a Budget Summary totaling all costs.")
    lines.append("  7. Output the plan in <plan>...</plan> tags.")

    return "\n".join(lines)


def check_plan_faithfulness(plan_text: str, result, data: Dict[str, Any]) -> str:
    """
    Check that all solver-selected entities appear in the LLM's plan.

    Args:
        plan_text: The plan text generated by the LLM
        result: SolverResult from run_solver_selection
        data: Working memory data dict

    Returns:
        Empty string if faithful, or a correction message listing missing entities.
    """
    plan_lower = plan_text.lower()
    missing = []

    # Check outbound transport
    if result.outbound:
        ob_id = result.outbound.get('id', '')
        if ob_id and ob_id.lower() not in plan_lower:
            missing.append(f"Outbound transport: {ob_id}")

    # Check inbound transport
    if result.inbound:
        ib_id = result.inbound.get('id', '')
        if ib_id and ib_id.lower() not in plan_lower:
            missing.append(f"Inbound transport: {ib_id}")

    # Check hotel
    if result.hotel:
        h_name = result.hotel.get('name', '')
        if h_name and h_name.lower() not in plan_lower:
            missing.append(f"Hotel: {h_name}")

    # Check attractions
    for attr_name in result.attractions:
        if attr_name.lower() not in plan_lower:
            missing.append(f"Attraction: {attr_name}")

    # Check restaurants
    for r in result.restaurants:
        r_name = r.get('name', '')
        if r_name and r_name.lower() not in plan_lower:
            missing.append(f"Restaurant: {r_name}")

    if not missing:
        return ""

    lines = [
        "FAITHFULNESS CHECK FAILED: Your plan is missing solver-selected entities.",
        "You MUST include ALL of the following in your plan:",
        "",
    ]
    for m in missing:
        lines.append(f"  ✗ {m}")
    lines.append("")
    lines.append("Rewrite your plan to include every entity above. "
                 "Do not remove any — just add the missing ones into appropriate day(s).")
    lines.append("Output the corrected plan in <plan>...</plan> tags.")

    return "\n".join(lines)
