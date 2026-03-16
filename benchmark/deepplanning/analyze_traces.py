"""
Trace analysis module for DeepPlanning benchmark.
Derives metrics from existing trajectory and evaluation data:
- Turn counting
- Failure mode classification
- Lethal reasoning step identification
- Tool call summary and aggregation
- Token usage extraction
"""

import json
from typing import Any, Dict, List, Optional


def count_turns(messages: List[Dict[str, Any]]) -> int:
    """Count the number of LLM calls (assistant messages) in a conversation trace."""
    return sum(1 for m in messages if isinstance(m, dict) and m.get('role') == 'assistant')


def get_token_usage(trajectory: Dict[str, Any]) -> Optional[Dict[str, int]]:
    """Extract token_usage from trajectory data. Returns None for old data without it."""
    usage = trajectory.get('token_usage')
    if usage and isinstance(usage, dict) and usage.get('total_tokens', 0) > 0:
        return usage
    return None


def extract_tool_call_summary(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract per-case tool call information from message trace.

    Returns:
        {
            "tools": {tool_name: {"count": N, "success": N, "error": N}},
            "total_calls": N,
            "call_order": [tool_name, ...],
        }
    """
    tools: Dict[str, Dict[str, int]] = {}
    call_order: List[str] = []
    # Build a map of tool_call_id -> tool_name for matching responses
    pending_calls: Dict[str, str] = {}

    for msg in messages:
        if not isinstance(msg, dict):
            continue
        role = msg.get('role', '')

        if role == 'assistant':
            for tc in msg.get('tool_calls', []):
                func = tc.get('function', {})
                name = func.get('name', 'unknown')
                tc_id = tc.get('id', '')
                call_order.append(name)
                if name not in tools:
                    tools[name] = {"count": 0, "success": 0, "error": 0}
                tools[name]["count"] += 1
                if tc_id:
                    pending_calls[tc_id] = name

        elif role == 'tool':
            tc_id = msg.get('tool_call_id', '')
            name = msg.get('name', '') or pending_calls.get(tc_id, 'unknown')
            content = msg.get('content', '')
            if name not in tools:
                tools[name] = {"count": 0, "success": 0, "error": 0}
            # Check if tool response indicates error
            is_error = False
            if isinstance(content, str):
                try:
                    parsed = json.loads(content)
                    if isinstance(parsed, dict) and 'error' in parsed:
                        is_error = True
                except (json.JSONDecodeError, TypeError):
                    pass
                if '"error"' in content[:100]:
                    is_error = True
            if is_error:
                tools[name]["error"] += 1
            else:
                tools[name]["success"] += 1

    return {
        "tools": tools,
        "total_calls": sum(t["count"] for t in tools.values()),
        "call_order": call_order,
    }


def aggregate_tool_stats(all_case_summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate tool call stats across all cases for a model.

    Args:
        all_case_summaries: List of extract_tool_call_summary() results

    Returns:
        {
            "tools": {tool_name: {"total_calls": N, "total_success": N, "total_error": N, "cases_used": N}},
            "total_calls_all_cases": N,
            "avg_calls_per_case": float,
            "num_cases": N,
        }
    """
    agg_tools: Dict[str, Dict[str, int]] = {}
    total_calls = 0
    num_cases = len(all_case_summaries)

    for summary in all_case_summaries:
        for name, stats in summary.get("tools", {}).items():
            if name not in agg_tools:
                agg_tools[name] = {"total_calls": 0, "total_success": 0, "total_error": 0, "cases_used": 0}
            agg_tools[name]["total_calls"] += stats.get("count", 0)
            agg_tools[name]["total_success"] += stats.get("success", 0)
            agg_tools[name]["total_error"] += stats.get("error", 0)
            agg_tools[name]["cases_used"] += 1
        total_calls += summary.get("total_calls", 0)

    return {
        "tools": agg_tools,
        "total_calls_all_cases": total_calls,
        "avg_calls_per_case": total_calls / num_cases if num_cases > 0 else 0,
        "num_cases": num_cases,
    }


def classify_failure_mode(eval_result: Dict[str, Any], messages: List[Dict[str, Any]], domain: str) -> Optional[str]:
    """
    Classify why a case failed based on evaluation results and message trace.

    Args:
        eval_result: Per-case evaluation result (id_X_score.json or case_X_report.json)
        messages: Conversation message history
        domain: "travel" or "shopping"

    Returns:
        Failure mode string, or None if the case passed
    """
    if domain == "travel":
        return _classify_travel_failure(eval_result, messages)
    elif domain == "shopping":
        return _classify_shopping_failure(eval_result, messages)
    return None


def _classify_travel_failure(eval_result: Dict[str, Any], messages: List[Dict[str, Any]]) -> Optional[str]:
    scores = eval_result.get('scores', {})

    # Perfect pass
    if scores.get('case_acc', 0) == 1.0:
        return None

    # Check if agent produced no plan
    has_final_content = False
    for msg in reversed(messages):
        if isinstance(msg, dict) and msg.get('role') == 'assistant':
            content = msg.get('content', '')
            if content and len(content.strip()) > 20:
                has_final_content = True
            break
    if not has_final_content:
        return "no_plan_generated"

    # Check for tool errors
    tool_errors = _count_tool_errors(messages)
    if tool_errors > 3:
        return f"tool_errors ({tool_errors})"

    # Check max turns exceeded
    turns = count_turns(messages)
    if turns >= 95:  # close to typical max of 100
        return "max_turns_exceeded"

    # Identify which constraints failed
    failed_dims = []
    cs_details = eval_result.get('commonsense_dimension_details', {})
    for dim, detail in cs_details.items():
        if isinstance(detail, dict) and detail.get('score', 1.0) < 1.0:
            failed_checks = [c['name'] for c in detail.get('checks', []) if not c.get('passed', True)]
            if failed_checks:
                failed_dims.append(f"{dim}: {', '.join(failed_checks)}")

    personalized = eval_result.get('personalized_dimension_score', {})
    failed_personal = []
    for name, detail in personalized.get('constraints', {}).items():
        if isinstance(detail, dict) and not detail.get('passed', True):
            failed_personal.append(name)

    parts = []
    if failed_dims:
        parts.append(f"commonsense_violation ({'; '.join(failed_dims[:3])})")
    if failed_personal:
        parts.append(f"personalized_violation ({', '.join(failed_personal[:3])})")

    if parts:
        return " + ".join(parts)

    # Fallback
    composite = scores.get('composite_score', 0)
    return f"low_composite_score ({composite:.2f})"


def _classify_shopping_failure(eval_result: Dict[str, Any], messages: List[Dict[str, Any]]) -> Optional[str]:
    summary = eval_result.get('summary', {})
    score = summary.get('score', 0)

    # Perfect pass
    if score >= 1.0:
        return None

    matched = summary.get('matched_count', 0)
    expected = summary.get('expected_count', 0)
    extra = summary.get('extra_products_count', 0)

    if matched == 0 and expected > 0:
        return "no_products_found"

    # Check max turns
    turns = count_turns(messages)
    if turns >= 95:
        return "max_turns_exceeded"

    # Check tool errors
    tool_errors = _count_tool_errors(messages)
    if tool_errors > 3:
        return f"tool_errors ({tool_errors})"

    parts = []
    if matched < expected:
        parts.append(f"missing_products ({expected - matched}/{expected})")
    if extra > 0:
        parts.append(f"extra_products ({extra})")

    if parts:
        return " + ".join(parts)

    return f"low_score ({score:.2f})"


def _count_tool_errors(messages: List[Dict[str, Any]]) -> int:
    count = 0
    for msg in messages:
        if isinstance(msg, dict) and msg.get('role') == 'tool':
            content = msg.get('content', '')
            if isinstance(content, str) and '"error"' in content[:100]:
                count += 1
    return count


def find_lethal_step(eval_result: Dict[str, Any], messages: List[Dict[str, Any]], domain: str) -> Optional[Dict[str, Any]]:
    """
    Identify which reasoning step most likely caused the failure.

    Returns:
        {"step_index": int, "role": str, "summary": str, "reason": str} or None
    """
    if domain == "travel":
        return _find_travel_lethal_step(eval_result, messages)
    elif domain == "shopping":
        return _find_shopping_lethal_step(eval_result, messages)
    return None


def _find_travel_lethal_step(eval_result: Dict[str, Any], messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    scores = eval_result.get('scores', {})
    if scores.get('case_acc', 0) == 1.0:
        return None

    # Collect failed constraint info
    failed_entities = set()
    personalized = eval_result.get('personalized_dimension_score', {})
    for name, detail in personalized.get('constraints', {}).items():
        if isinstance(detail, dict) and not detail.get('passed', True):
            msg = detail.get('message', '') or ''
            failed_entities.add(name)
            # Extract entity names from error messages
            for word in msg.split():
                if len(word) > 3:
                    failed_entities.add(word.lower())

    cs_details = eval_result.get('commonsense_dimension_details', {})
    failed_checks = []
    for dim, detail in cs_details.items():
        if isinstance(detail, dict):
            for check in detail.get('checks', []):
                if not check.get('passed', True):
                    failed_checks.append(check.get('name', ''))
                    msg = check.get('message', '') or ''
                    for word in msg.split():
                        if len(word) > 3:
                            failed_entities.add(word.lower())

    if not failed_entities and not failed_checks:
        return None

    # Walk backward through messages to find the decision point
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if not isinstance(msg, dict):
            continue

        if msg.get('role') == 'assistant':
            content = (msg.get('content', '') or '').lower()
            tool_calls = msg.get('tool_calls', [])

            # Check if this assistant message references any failed entity
            matches = [e for e in failed_entities if e in content]
            if matches:
                summary = f"Assistant message mentioning: {', '.join(matches[:3])}"
                reason = f"Failed constraints involve these entities"
                return {"step_index": i, "role": "assistant", "summary": summary, "reason": reason}

            # Check tool call arguments
            for tc in tool_calls:
                args_str = tc.get('function', {}).get('arguments', '').lower()
                matches = [e for e in failed_entities if e in args_str]
                if matches:
                    func_name = tc.get('function', {}).get('name', 'unknown')
                    summary = f"Tool call {func_name} with args mentioning: {', '.join(matches[:3])}"
                    reason = f"This tool call relates to failed constraints"
                    return {"step_index": i, "role": "assistant", "summary": summary, "reason": reason}

    # Fallback: the last assistant message is the lethal step (the final plan)
    for i in range(len(messages) - 1, -1, -1):
        if isinstance(messages[i], dict) and messages[i].get('role') == 'assistant':
            return {"step_index": i, "role": "assistant", "summary": "Final plan output", "reason": "Plan did not satisfy all constraints"}

    return None


def _find_shopping_lethal_step(eval_result: Dict[str, Any], messages: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    summary = eval_result.get('summary', {})
    if summary.get('score', 0) >= 1.0:
        return None

    # Collect unmatched product info
    unmatched = eval_result.get('unmatched_ground_truth_products', [])
    extra = eval_result.get('extra_products', [])

    search_terms = set()
    for p in unmatched:
        name = (p.get('name', '') or '').lower()
        for word in name.split():
            if len(word) > 3:
                search_terms.add(word)
    for p in extra:
        pid = p.get('product_id', '').lower()
        if pid:
            search_terms.add(pid)

    if not search_terms:
        # Fallback to last assistant message
        for i in range(len(messages) - 1, -1, -1):
            if isinstance(messages[i], dict) and messages[i].get('role') == 'assistant':
                return {"step_index": i, "role": "assistant", "summary": "Final response", "reason": "Cart did not match expected products"}
        return None

    # Walk backward
    for i in range(len(messages) - 1, -1, -1):
        msg = messages[i]
        if not isinstance(msg, dict):
            continue
        if msg.get('role') == 'assistant':
            content = (msg.get('content', '') or '').lower()
            tool_calls = msg.get('tool_calls', [])

            for tc in tool_calls:
                func = tc.get('function', {})
                name = func.get('name', '')
                args = func.get('arguments', '').lower()
                if name in ('add_to_cart', 'add_item_to_cart'):
                    matches = [t for t in search_terms if t in args]
                    if matches:
                        return {"step_index": i, "role": "assistant", "summary": f"Cart operation: {name}", "reason": f"Added wrong/missing product"}

            matches = [t for t in search_terms if t in content]
            if matches:
                return {"step_index": i, "role": "assistant", "summary": f"Decision mentioning: {', '.join(list(matches)[:3])}", "reason": "Product selection error"}

    # Fallback
    for i in range(len(messages) - 1, -1, -1):
        if isinstance(messages[i], dict) and messages[i].get('role') == 'assistant':
            return {"step_index": i, "role": "assistant", "summary": "Final response", "reason": "Cart did not match expected products"}
    return None


def analyze_case(trajectory: Dict[str, Any], eval_result: Dict[str, Any], domain: str) -> Dict[str, Any]:
    """
    Run all analysis on a single case. Convenience function for visualization.

    Returns:
        {
            "turns": int,
            "token_usage": dict or None,
            "failure_mode": str or None,
            "lethal_step": dict or None,
            "tool_summary": dict,
            "elapsed_time": float or None,
        }
    """
    messages = trajectory.get('messages', [])
    # Handle shopping format where messages may be nested
    if isinstance(messages, dict) and 'messages' in messages:
        messages = messages['messages']

    return {
        "turns": count_turns(messages),
        "token_usage": get_token_usage(trajectory),
        "failure_mode": classify_failure_mode(eval_result, messages, domain),
        "lethal_step": find_lethal_step(eval_result, messages, domain),
        "tool_summary": extract_tool_call_summary(messages),
        "elapsed_time": trajectory.get('elapsed_time'),
    }
