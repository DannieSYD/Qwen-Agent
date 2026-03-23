#!/usr/bin/env python3
"""
Comprehensive error analysis for DeepPlanning travel planning benchmark.
Compares failure patterns across models to inform intervention strategies.
"""

import json
import os
import glob
from collections import Counter, defaultdict
from pathlib import Path

BASE_DIR = Path("/data1/dannie/projects/Qwen-Agent/benchmark/deepplanning/travelplanning/results")

MODELS = [
    "gpt-5.4",
    "gpt-5.2-2025-12-11",
    "gpt-5.1",
    "qwen3-30b-a3b-thinking-2507",
    "qwen3-4b-thinking-2507",
    "qwen3-32b-local",
    "qwen3.5-4b",
    "qwen3.5-9b",
]

COMMONSENSE_DIMS = [
    "Route Consistency",
    "Sandbox Compliance",
    "Itinerary Structure",
    "Time Feasibility",
    "Business Hours",
    "Duration Rationality",
    "Cost Calculation Accuracy",
    "Activity Diversity",
]

# Short aliases for display
MODEL_SHORT = {
    "gpt-5.4": "GPT-5.4",
    "gpt-5.2-2025-12-11": "GPT-5.2",
    "gpt-5.1": "GPT-5.1",
    "qwen3-30b-a3b-thinking-2507": "Qwen3-30B",
    "qwen3-4b-thinking-2507": "Qwen3-4B",
    "qwen3-32b-local": "Qwen3-32B",
    "qwen3.5-4b": "Qwen3.5-4B",
    "qwen3.5-9b": "Qwen3.5-9B",
}

def load_summary(model):
    path = BASE_DIR / f"{model}_en" / "evaluation" / "evaluation_summary.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None

def load_score(model, case_id):
    path = BASE_DIR / f"{model}_en" / "evaluation" / f"id_{case_id}_score.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None

def load_trajectory(model, case_id):
    path = BASE_DIR / f"{model}_en" / "trajectories" / f"id_{case_id}.json"
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None

def count_available_scores(model):
    pattern = str(BASE_DIR / f"{model}_en" / "evaluation" / "id_*_score.json")
    return len(glob.glob(pattern))

def print_divider(title, char="="):
    width = 100
    print(f"\n{char * width}")
    print(f"  {title}")
    print(f"{char * width}")

def fmt(val, width=10):
    if val is None:
        return "-".center(width)
    if isinstance(val, float):
        return f"{val:.3f}".rjust(width)
    return str(val).rjust(width)

# ============================================================
# 1. Load all available evaluation data
# ============================================================
print_divider("1. DATA AVAILABILITY")

summaries = {}
score_counts = {}
for m in MODELS:
    s = load_summary(m)
    if s:
        summaries[m] = s
        sc = count_available_scores(m)
        score_counts[m] = sc
        print(f"  {MODEL_SHORT[m]:>12s}: summary loaded | {sc} individual score files | "
              f"{s.get('evaluation_success_count', '?')}/{s.get('total_test_samples', '?')} evaluated")
    else:
        print(f"  {MODEL_SHORT[m]:>12s}: NO evaluation_summary.json found")

# ============================================================
# 2. Overall metrics comparison table
# ============================================================
print_divider("2. OVERALL METRICS COMPARISON")

header = f"{'Model':>12s} {'delivery':>10s} {'cs_score':>10s} {'pers_score':>10s} {'composite':>10s} {'case_acc':>10s} {'N_eval':>8s}"
print(header)
print("-" * len(header))

for m in MODELS:
    if m not in summaries:
        continue
    met = summaries[m]["metrics"]
    n_eval = summaries[m].get("evaluation_success_count", "?")
    print(f"{MODEL_SHORT[m]:>12s} {fmt(met.get('delivery_rate'))} {fmt(met.get('commonsense_score'))} "
          f"{fmt(met.get('personalized_score'))} {fmt(met.get('composite_score'))} "
          f"{fmt(met.get('case_acc'))} {str(n_eval):>8s}")

# ============================================================
# 3. Commonsense dimension breakdown
# ============================================================
print_divider("3. COMMONSENSE DIMENSION BREAKDOWN (score per model)")

# Header
dim_short = {
    "Route Consistency": "RouteCon",
    "Sandbox Compliance": "Sandbox",
    "Itinerary Structure": "ItinStru",
    "Time Feasibility": "TimeFeas",
    "Business Hours": "BizHours",
    "Duration Rationality": "DurRatio",
    "Cost Calculation Accuracy": "CostCalc",
    "Activity Diversity": "ActDiver",
}

header = f"{'Model':>12s}" + "".join(f"{dim_short[d]:>10s}" for d in COMMONSENSE_DIMS)
print(header)
print("-" * len(header))

for m in MODELS:
    if m not in summaries:
        continue
    dims = summaries[m]["metrics"]["commonsense_dimensions"]
    row = f"{MODEL_SHORT[m]:>12s}"
    for d in COMMONSENSE_DIMS:
        if d in dims:
            row += f"{dims[d]['score']:>10.3f}"
        else:
            row += f"{'-':>10s}"
    print(row)

# Gap between GPT-5.4 and Qwen3-30B
if "gpt-5.4" in summaries and "qwen3-30b-a3b-thinking-2507" in summaries:
    print(f"\n  Gap (GPT-5.4 minus Qwen3-30B):")
    gpt_dims = summaries["gpt-5.4"]["metrics"]["commonsense_dimensions"]
    qwen_dims = summaries["qwen3-30b-a3b-thinking-2507"]["metrics"]["commonsense_dimensions"]
    gaps = []
    for d in COMMONSENSE_DIMS:
        g = gpt_dims[d]["score"] - qwen_dims[d]["score"]
        gaps.append((d, g))
    gaps.sort(key=lambda x: -abs(x[1]))
    for d, g in gaps:
        bar = "+" * int(abs(g) * 40) if g > 0 else "-" * int(abs(g) * 40)
        print(f"    {dim_short[d]:>10s}: {g:+.3f}  {bar}")

# ============================================================
# 4. Error type frequency analysis
# ============================================================
print_divider("4. ERROR TYPE FREQUENCY ANALYSIS")

for m in MODELS:
    if m not in summaries:
        continue
    errors = summaries[m].get("error_statistics", [])
    if not errors:
        continue

    cs_errors = [(e["error_type"], e["count"]) for e in errors if e["error_type"].startswith("[Commonsense]")]
    ps_errors = [(e["error_type"], e["count"]) for e in errors if e["error_type"].startswith("[Personalized]")]
    other_errors = [(e["error_type"], e["count"]) for e in errors
                    if not e["error_type"].startswith("[Commonsense]") and not e["error_type"].startswith("[Personalized]")]

    total_cs = sum(c for _, c in cs_errors)
    total_ps = sum(c for _, c in ps_errors)
    total_other = sum(c for _, c in other_errors)

    print(f"\n  {MODEL_SHORT[m]} (total errors: CS={total_cs}, Personalized={total_ps}, Other={total_other}):")
    print(f"    {'Error Type':<55s} {'Count':>6s}")
    print(f"    {'-'*55} {'-'*6}")
    for etype, cnt in sorted(cs_errors + ps_errors + other_errors, key=lambda x: -x[1])[:20]:
        print(f"    {etype:<55s} {cnt:>6d}")

# Side-by-side for GPT-5.4 vs Qwen3-30B
if "gpt-5.4" in summaries and "qwen3-30b-a3b-thinking-2507" in summaries:
    print_divider("4b. ERROR TYPE COMPARISON: GPT-5.4 vs Qwen3-30B")

    gpt_errs = {e["error_type"]: e["count"] for e in summaries["gpt-5.4"].get("error_statistics", [])}
    qwen_errs = {e["error_type"]: e["count"] for e in summaries["qwen3-30b-a3b-thinking-2507"].get("error_statistics", [])}
    all_types = sorted(set(gpt_errs.keys()) | set(qwen_errs.keys()))

    print(f"  {'Error Type':<55s} {'GPT-5.4':>8s} {'Qwen3-30B':>10s} {'Delta':>8s}")
    print(f"  {'-'*55} {'-'*8} {'-'*10} {'-'*8}")
    rows = []
    for et in all_types:
        g = gpt_errs.get(et, 0)
        q = qwen_errs.get(et, 0)
        rows.append((et, g, q, q - g))
    rows.sort(key=lambda x: -x[3])  # Sort by delta (most extra errors in Qwen first)
    for et, g, q, delta in rows:
        print(f"  {et:<55s} {g:>8d} {q:>10d} {delta:>+8d}")

# ============================================================
# 5. Per-case failure analysis for Qwen3-30B
# ============================================================
print_divider("5. PER-CASE FAILURE ANALYSIS FOR QWEN3-30B")

qwen_model = "qwen3-30b-a3b-thinking-2507"
categories = Counter()
cs_fail_checks = Counter()
ps_fail_checks = Counter()
case_details = []

for case_id in range(120):
    score = load_score(qwen_model, case_id)
    if score is None:
        continue

    cs_pass = score["scores"]["commonsense_weighted_score"] >= 1.0
    ps_pass = score["scores"]["personalized_score"] >= 1.0

    if cs_pass and ps_pass:
        cat = "both_pass"
    elif not cs_pass and ps_pass:
        cat = "commonsense_only_fail"
    elif cs_pass and not ps_pass:
        cat = "personalized_only_fail"
    else:
        cat = "both_fail"

    categories[cat] += 1

    # Collect failed checks
    failed_cs = []
    for dim, details in score["commonsense_dimension_details"].items():
        for check in details["checks"]:
            if not check["passed"]:
                cs_fail_checks[check["name"]] += 1
                failed_cs.append(check["name"])

    failed_ps = []
    if "personalized_dimension_score" in score:
        for cname, cdata in score["personalized_dimension_score"].get("constraints", {}).items():
            if not cdata["passed"]:
                ps_fail_checks[cname] += 1
                failed_ps.append(cname)

    case_details.append({
        "id": case_id,
        "cat": cat,
        "cs_score": score["scores"]["commonsense_weighted_score"],
        "ps_score": score["scores"]["personalized_score"],
        "failed_cs": failed_cs,
        "failed_ps": failed_ps,
    })

print(f"  Category distribution (out of {sum(categories.values())} cases):")
for cat in ["both_pass", "commonsense_only_fail", "personalized_only_fail", "both_fail"]:
    pct = categories[cat] / sum(categories.values()) * 100
    bar = "#" * int(pct / 2)
    print(f"    {cat:<28s}: {categories[cat]:>4d} ({pct:5.1f}%)  {bar}")

print(f"\n  Most frequent commonsense check failures:")
for check, cnt in cs_fail_checks.most_common(15):
    print(f"    {check:<45s}: {cnt:>4d}/120 ({cnt/120*100:.1f}%)")

print(f"\n  Most frequent personalized constraint failures:")
for check, cnt in ps_fail_checks.most_common(15):
    print(f"    {check:<45s}: {cnt:>4d}/120 ({cnt/120*100:.1f}%)")

# ============================================================
# 6. Gap analysis: GPT-5.4 passes but Qwen3-30B fails
# ============================================================
print_divider("6. GAP ANALYSIS: Cases where GPT-5.4 passes but Qwen3-30B fails")

gpt_model = "gpt-5.4"
gap_cases = []
gap_cs_errors = Counter()
gap_ps_errors = Counter()

for case_id in range(120):
    gpt_score = load_score(gpt_model, case_id)
    qwen_score = load_score(qwen_model, case_id)
    if gpt_score is None or qwen_score is None:
        continue

    gpt_acc = gpt_score["scores"]["case_acc"]
    qwen_acc = qwen_score["scores"]["case_acc"]

    if gpt_acc >= 1.0 and qwen_acc < 1.0:
        # Qwen fails where GPT passes - find what went wrong
        failed_cs = []
        for dim, details in qwen_score["commonsense_dimension_details"].items():
            for check in details["checks"]:
                if not check["passed"]:
                    failed_cs.append(check["name"])
                    gap_cs_errors[check["name"]] += 1

        failed_ps = []
        if "personalized_dimension_score" in qwen_score:
            for cname, cdata in qwen_score["personalized_dimension_score"].get("constraints", {}).items():
                if not cdata["passed"]:
                    failed_ps.append(cname)
                    gap_ps_errors[cname] += 1

        gap_cases.append({
            "id": case_id,
            "qwen_cs": qwen_score["scores"]["commonsense_weighted_score"],
            "qwen_ps": qwen_score["scores"]["personalized_score"],
            "failed_cs": failed_cs,
            "failed_ps": failed_ps,
        })

# Also count cases where both pass, GPT passes but Qwen fails, etc.
both_pass = 0
gpt_only = 0
qwen_only = 0
both_fail = 0
for case_id in range(120):
    gpt_score = load_score(gpt_model, case_id)
    qwen_score = load_score(qwen_model, case_id)
    if gpt_score is None or qwen_score is None:
        continue
    g = gpt_score["scores"]["case_acc"] >= 1.0
    q = qwen_score["scores"]["case_acc"] >= 1.0
    if g and q: both_pass += 1
    elif g and not q: gpt_only += 1
    elif not g and q: qwen_only += 1
    else: both_fail += 1

print(f"  Case-level agreement (case_acc=1.0 threshold):")
print(f"    Both pass:        {both_pass:>4d}")
print(f"    GPT passes only:  {gpt_only:>4d}  <-- fixable gap")
print(f"    Qwen passes only: {qwen_only:>4d}")
print(f"    Both fail:        {both_fail:>4d}")

print(f"\n  In the {len(gap_cases)} 'fixable gap' cases, Qwen3-30B's errors:")
print(f"\n    Commonsense check failures:")
for check, cnt in gap_cs_errors.most_common(10):
    print(f"      {check:<45s}: {cnt:>3d}")
print(f"\n    Personalized constraint failures:")
for check, cnt in gap_ps_errors.most_common(10):
    print(f"      {check:<45s}: {cnt:>3d}")

# What fraction of the gap is CS vs PS?
gap_cs_only = sum(1 for c in gap_cases if c["failed_cs"] and not c["failed_ps"])
gap_ps_only = sum(1 for c in gap_cases if not c["failed_cs"] and c["failed_ps"])
gap_both = sum(1 for c in gap_cases if c["failed_cs"] and c["failed_ps"])
print(f"\n    Gap case breakdown:")
print(f"      CS errors only:   {gap_cs_only:>3d}")
print(f"      PS errors only:   {gap_ps_only:>3d}")
print(f"      Both CS & PS:     {gap_both:>3d}")

# ============================================================
# 7. Personalized constraint analysis
# ============================================================
print_divider("7. PERSONALIZED CONSTRAINT ANALYSIS (Qwen3-30B)")

# Categorize constraint types
constraint_categories = defaultdict(lambda: {"total": 0, "fail": 0, "messages": []})

for case_id in range(120):
    score = load_score(qwen_model, case_id)
    if score is None:
        continue
    if "personalized_dimension_score" not in score:
        continue
    constraints = score["personalized_dimension_score"].get("constraints", {})
    for cname, cdata in constraints.items():
        constraint_categories[cname]["total"] += 1
        if not cdata["passed"]:
            constraint_categories[cname]["fail"] += 1
            if cdata.get("message"):
                constraint_categories[cname]["messages"].append(cdata["message"])

print(f"  {'Constraint Type':<50s} {'Total':>6s} {'Fail':>6s} {'Rate':>8s}")
print(f"  {'-'*50} {'-'*6} {'-'*6} {'-'*8}")

sorted_constraints = sorted(constraint_categories.items(), key=lambda x: -x[1]["fail"])
for cname, data in sorted_constraints:
    rate = data["fail"] / data["total"] * 100 if data["total"] > 0 else 0
    print(f"  {cname:<50s} {data['total']:>6d} {data['fail']:>6d} {rate:>7.1f}%")

# Show sample messages for top failing constraints
print(f"\n  Sample failure messages for top failing constraints:")
for cname, data in sorted_constraints[:5]:
    if data["messages"]:
        print(f"\n    {cname}:")
        # Show up to 3 unique messages
        unique_msgs = list(set(data["messages"]))[:3]
        for msg in unique_msgs:
            truncated = msg[:150] + "..." if len(msg) > 150 else msg
            print(f"      - {truncated}")

# Also do same for GPT-5.4
if "gpt-5.4" in summaries:
    print_divider("7b. PERSONALIZED CONSTRAINT ANALYSIS (GPT-5.4)")
    gpt_constraints = defaultdict(lambda: {"total": 0, "fail": 0})
    for case_id in range(120):
        score = load_score(gpt_model, case_id)
        if score is None:
            continue
        if "personalized_dimension_score" not in score:
            continue
        constraints = score["personalized_dimension_score"].get("constraints", {})
        for cname, cdata in constraints.items():
            gpt_constraints[cname]["total"] += 1
            if not cdata["passed"]:
                gpt_constraints[cname]["fail"] += 1

    print(f"  {'Constraint Type':<50s} {'GPT-5.4':>10s} {'Qwen3-30B':>12s} {'Delta':>8s}")
    print(f"  {'-'*50} {'-'*10} {'-'*12} {'-'*8}")
    all_cnames = sorted(set(list(gpt_constraints.keys()) + list(constraint_categories.keys())))
    for cname in all_cnames:
        g_fail = gpt_constraints.get(cname, {"fail": 0})["fail"]
        q_fail = constraint_categories.get(cname, {"fail": 0})["fail"]
        delta = q_fail - g_fail
        if gpt_constraints.get(cname, {"total": 0})["total"] > 0 or constraint_categories.get(cname, {"total": 0})["total"] > 0:
            print(f"  {cname:<50s} {g_fail:>10d} {q_fail:>12d} {delta:>+8d}")

# ============================================================
# 8. Trajectory analysis for Qwen3-30B
# ============================================================
print_divider("8. TRAJECTORY ANALYSIS (Qwen3-30B)")

tool_call_counts = []
turn_counts = []
tool_sequences = Counter()
tool_usage = Counter()
max_turn_cases = []

for case_id in range(120):
    traj = load_trajectory(qwen_model, case_id)
    if traj is None:
        continue

    messages = traj.get("messages", [])

    # Count assistant turns and tool calls
    n_assistant = 0
    n_tool_calls = 0
    case_tools = []

    for msg in messages:
        if msg.get("role") == "assistant":
            n_assistant += 1
            if msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    n_tool_calls += 1
                    fn_name = tc.get("function", {}).get("name", "unknown")
                    tool_usage[fn_name] += 1
                    case_tools.append(fn_name)

    tool_call_counts.append(n_tool_calls)
    turn_counts.append(n_assistant)

    # Track tool sequences (bigrams)
    for i in range(len(case_tools) - 1):
        tool_sequences[f"{case_tools[i]} -> {case_tools[i+1]}"] += 1

    # Check for potential max-turn cases (heuristic: high turn count)
    if n_assistant >= 20:
        max_turn_cases.append((case_id, n_assistant, n_tool_calls))

if tool_call_counts:
    avg_tools = sum(tool_call_counts) / len(tool_call_counts)
    avg_turns = sum(turn_counts) / len(turn_counts)
    print(f"  Analyzed {len(tool_call_counts)} trajectories")
    print(f"  Average tool calls per case:     {avg_tools:.1f}")
    print(f"  Average assistant turns per case: {avg_turns:.1f}")
    print(f"  Min/Max tool calls:              {min(tool_call_counts)} / {max(tool_call_counts)}")
    print(f"  Min/Max assistant turns:         {min(turn_counts)} / {max(turn_counts)}")

    print(f"\n  Tool usage frequency:")
    for tool, cnt in tool_usage.most_common(15):
        print(f"    {tool:<45s}: {cnt:>5d}")

    print(f"\n  Most common tool call transitions:")
    for seq, cnt in tool_sequences.most_common(10):
        print(f"    {seq:<65s}: {cnt:>4d}")

    if max_turn_cases:
        print(f"\n  Cases with >= 20 assistant turns (potential max-turn hits):")
        for cid, turns, tools in sorted(max_turn_cases, key=lambda x: -x[1])[:10]:
            print(f"    Case {cid:>3d}: {turns} turns, {tools} tool calls")
else:
    print("  No trajectory files found.")

# Also do GPT-5.4 trajectory analysis for comparison
print_divider("8b. TRAJECTORY ANALYSIS (GPT-5.4) for comparison")

gpt_tool_counts = []
gpt_turn_counts = []
gpt_tool_usage = Counter()

for case_id in range(120):
    traj = load_trajectory(gpt_model, case_id)
    if traj is None:
        continue
    messages = traj.get("messages", [])
    n_assistant = 0
    n_tool_calls = 0
    for msg in messages:
        if msg.get("role") == "assistant":
            n_assistant += 1
            if msg.get("tool_calls"):
                for tc in msg["tool_calls"]:
                    n_tool_calls += 1
                    fn_name = tc.get("function", {}).get("name", "unknown")
                    gpt_tool_usage[fn_name] += 1
    gpt_tool_counts.append(n_tool_calls)
    gpt_turn_counts.append(n_assistant)

if gpt_tool_counts:
    print(f"  Analyzed {len(gpt_tool_counts)} trajectories")
    print(f"  Average tool calls per case:     {sum(gpt_tool_counts)/len(gpt_tool_counts):.1f}")
    print(f"  Average assistant turns per case: {sum(gpt_turn_counts)/len(gpt_turn_counts):.1f}")
    print(f"  Min/Max tool calls:              {min(gpt_tool_counts)} / {max(gpt_tool_counts)}")
    print(f"  Min/Max assistant turns:         {min(gpt_turn_counts)} / {max(gpt_turn_counts)}")

    print(f"\n  Tool usage comparison (GPT-5.4 vs Qwen3-30B):")
    all_tools = sorted(set(list(gpt_tool_usage.keys()) + list(tool_usage.keys())))
    print(f"    {'Tool':<45s} {'GPT-5.4':>8s} {'Qwen3-30B':>10s}")
    print(f"    {'-'*45} {'-'*8} {'-'*10}")
    for t in all_tools:
        print(f"    {t:<45s} {gpt_tool_usage.get(t, 0):>8d} {tool_usage.get(t, 0):>10d}")

# ============================================================
# 9. Summary / Recommendations
# ============================================================
print_divider("9. SUMMARY & INTERVENTION RECOMMENDATIONS")

print("""
  KEY FINDINGS:
""")

# Compute key stats for summary
if "gpt-5.4" in summaries and "qwen3-30b-a3b-thinking-2507" in summaries:
    gpt_cs = summaries["gpt-5.4"]["metrics"]["commonsense_score"]
    qwen_cs = summaries["qwen3-30b-a3b-thinking-2507"]["metrics"]["commonsense_score"]
    gpt_ps = summaries["gpt-5.4"]["metrics"]["personalized_score"]
    qwen_ps = summaries["qwen3-30b-a3b-thinking-2507"]["metrics"]["personalized_score"]
    gpt_acc = summaries["gpt-5.4"]["metrics"]["case_acc"]
    qwen_acc = summaries["qwen3-30b-a3b-thinking-2507"]["metrics"]["case_acc"]

    print(f"  1. Overall gap: GPT-5.4 case_acc={gpt_acc:.3f} vs Qwen3-30B case_acc={qwen_acc:.3f}")
    print(f"     CS gap: {gpt_cs:.3f} vs {qwen_cs:.3f} (delta={gpt_cs-qwen_cs:+.3f})")
    print(f"     PS gap: {gpt_ps:.3f} vs {qwen_ps:.3f} (delta={gpt_ps-qwen_ps:+.3f})")

    print(f"\n  2. Fixable gap: {gpt_only} cases where GPT passes but Qwen fails")
    if gap_cs_only + gap_ps_only + gap_both > 0:
        print(f"     - {gap_cs_only} due to CS errors only -> OR-solver / structural verifier could help")
        print(f"     - {gap_ps_only} due to PS errors only -> constraint checker / verifier could help")
        print(f"     - {gap_both} due to both CS & PS errors -> need both interventions")

    print(f"\n  3. Top CS failures for Qwen3-30B (potential verifier targets):")
    for check, cnt in cs_fail_checks.most_common(5):
        print(f"     - {check}: {cnt} cases ({cnt/120*100:.0f}%)")

    print(f"\n  4. Top PS failures for Qwen3-30B (potential constraint solver targets):")
    for check, cnt in ps_fail_checks.most_common(5):
        print(f"     - {check}: {cnt} cases ({cnt/120*100:.0f}%)")

print(f"\n  INTERVENTION PRIORITY:")
print(f"  - HIGH: Structural verifier for itinerary completeness (meal coverage, hotel return)")
print(f"  - HIGH: Budget/cost constraint solver")
print(f"  - MEDIUM: Transfer time reasonableness checker")
print(f"  - MEDIUM: Business hours validator")
print(f"  - LOW: OR-solver for activity scheduling optimization")

print("\n" + "=" * 100)
print("  Analysis complete.")
print("=" * 100)
