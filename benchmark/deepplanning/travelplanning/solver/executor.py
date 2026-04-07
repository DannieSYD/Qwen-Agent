"""
Solver executor — two modes:

1. run_solver_template (v4): Fixed CP-SAT template + greedy scheduler.
   No LLM-generated code. Reads constraints + data, solves, returns plan.

2. run_solver_code (v3, legacy): Sandboxed execution of LLM-generated code.
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
