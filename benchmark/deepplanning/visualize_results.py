#!/usr/bin/env python3
"""
DeepPlanning Benchmark Results Visualizer
Generates a self-contained HTML file with detailed evaluation results.

Usage:
    python visualize_results.py                           # auto-detect all models
    python visualize_results.py --models model1 model2    # specific models
    python visualize_results.py --output report.html      # custom output path
"""

import argparse
import html
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from analyze_traces import (
    analyze_case,
    aggregate_tool_stats,
    extract_tool_call_summary,
)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class ModelData:
    name: str
    aggregated: Optional[dict] = None
    shopping_stats: Optional[dict] = None            # {model}_statistics.json
    shopping_case_reports: Dict[int, Tuple[Optional[dict], List[dict]]] = field(default_factory=dict)
    travel_summaries: Dict[str, dict] = field(default_factory=dict)  # lang -> evaluation_summary
    travel_case_scores: Dict[str, List[dict]] = field(default_factory=dict)  # lang -> [id_X_score]
    # Trajectory data for enhanced visualization
    travel_trajectories: Dict[str, Dict[str, dict]] = field(default_factory=dict)  # lang -> {sample_id -> trajectory}
    shopping_messages: Dict[int, Dict[str, dict]] = field(default_factory=dict)    # level -> {case_id -> messages_data}
    # Analysis results (populated during load)
    travel_analysis: Dict[str, Dict[str, dict]] = field(default_factory=dict)  # lang -> {sample_id -> analysis}
    shopping_analysis: Dict[int, Dict[str, dict]] = field(default_factory=dict)  # level -> {case_id -> analysis}
    travel_tool_stats: Dict[str, dict] = field(default_factory=dict)  # lang -> aggregate tool stats
    shopping_tool_stats: Dict[int, dict] = field(default_factory=dict)  # level -> aggregate tool stats


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def auto_detect_models(aggregated_dir: Path) -> List[str]:
    if not aggregated_dir.exists():
        return []
    models = []
    for f in sorted(aggregated_dir.glob("*_aggregated.json")):
        name = f.stem.replace("_aggregated", "")
        models.append(name)
    return models


def load_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def find_shopping_level_folder(result_report_dir: Path, model: str, level: int) -> Optional[Path]:
    pattern = re.compile(r"^database_(.+?)_level([123])_(\d+)$")
    candidates = []
    if not result_report_dir.exists():
        return None
    for folder in result_report_dir.iterdir():
        if not folder.is_dir():
            continue
        m = pattern.match(folder.name)
        if m and m.group(1) == model and int(m.group(2)) == level:
            candidates.append((int(m.group(3)), folder))
    if not candidates:
        return None
    candidates.sort(key=lambda x: x[0], reverse=True)
    return candidates[0][1]


def load_shopping_case_reports(folder: Path) -> Tuple[Optional[dict], List[dict]]:
    summary = load_json(folder / "summary_report.json")
    cases = []
    for f in sorted(folder.glob("case_*_report.json"), key=lambda p: p.stem):
        d = load_json(f)
        if d:
            cases.append(d)
    return summary, cases


def load_travel_case_scores(eval_dir: Path) -> List[dict]:
    scores = []
    for f in sorted(eval_dir.glob("id_*_score.json")):
        d = load_json(f)
        if d:
            scores.append(d)
    scores.sort(key=lambda x: int(x.get("sample_id", 0)))
    return scores


def load_model_data(model: str, shopping_dir: Path, travel_dir: Path,
                    aggregated_dir: Path) -> ModelData:
    md = ModelData(name=model)

    # Aggregated
    md.aggregated = load_json(aggregated_dir / f"{model}_aggregated.json")

    # Shopping
    stats_file = shopping_dir / "result_report" / f"{model}_statistics.json"
    md.shopping_stats = load_json(stats_file)
    for level in (1, 2, 3):
        folder = find_shopping_level_folder(shopping_dir / "result_report", model, level)
        if folder:
            md.shopping_case_reports[level] = load_shopping_case_reports(folder)

    # Travel
    results_dir = travel_dir / "results"
    if results_dir.exists():
        for lang in ("en", "zh"):
            model_lang_dir = results_dir / f"{model}_{lang}"
            eval_dir = model_lang_dir / "evaluation"
            if eval_dir.exists():
                summary = load_json(eval_dir / "evaluation_summary.json")
                if summary:
                    md.travel_summaries[lang] = summary
                case_scores = load_travel_case_scores(eval_dir)
                if case_scores:
                    md.travel_case_scores[lang] = case_scores

            # Load trajectories
            traj_dir = model_lang_dir / "trajectories"
            if traj_dir.exists():
                trajs = {}
                for f in traj_dir.glob("id_*.json"):
                    data = load_json(f)
                    if data:
                        sid = str(data.get("id", f.stem.replace("id_", "")))
                        # Normalize: remove "id_" prefix if present
                        sid_key = sid.replace("id_", "") if sid.startswith("id_") else sid
                        trajs[sid_key] = data
                if trajs:
                    md.travel_trajectories[lang] = trajs

    # Load shopping messages from database_infered
    db_infered_dir = shopping_dir / "database_infered"
    if db_infered_dir.exists():
        for level in (1, 2, 3):
            folder = find_shopping_level_folder(db_infered_dir, model, level)
            if folder:
                msgs = {}
                for case_dir in sorted(folder.iterdir()):
                    if not case_dir.is_dir() or not case_dir.name.startswith("case_"):
                        continue
                    msg_file = case_dir / "messages.json"
                    if msg_file.exists():
                        data = load_json(msg_file)
                        if data:
                            case_id = case_dir.name.replace("case_", "")
                            msgs[case_id] = data
                if msgs:
                    md.shopping_messages[level] = msgs

    # Run analysis on trajectories
    _run_analysis(md)

    return md


def _run_analysis(md: ModelData) -> None:
    """Run trace analysis on all loaded trajectories and eval results."""
    # Travel analysis
    for lang in md.travel_trajectories:
        trajs = md.travel_trajectories[lang]
        case_scores = md.travel_case_scores.get(lang, [])
        eval_by_id = {str(cs.get("sample_id", "")): cs for cs in case_scores}
        analysis = {}
        tool_summaries = []
        for sid, traj in trajs.items():
            eval_r = eval_by_id.get(sid, {})
            a = analyze_case(traj, eval_r, "travel")
            analysis[sid] = a
            tool_summaries.append(a["tool_summary"])
        md.travel_analysis[lang] = analysis
        if tool_summaries:
            md.travel_tool_stats[lang] = aggregate_tool_stats(tool_summaries)

    # Shopping analysis
    for level in md.shopping_messages:
        msgs_by_case = md.shopping_messages[level]
        _, case_reports = md.shopping_case_reports.get(level, (None, []))
        eval_by_id = {}
        for cr in (case_reports or []):
            cname = cr.get("case_name", "")
            cid = cname.replace("case_", "")
            eval_by_id[cid] = cr
        analysis = {}
        tool_summaries = []
        for case_id, msg_data in msgs_by_case.items():
            # Shopping messages format: {step, description, messages: [...]}
            messages = msg_data.get("messages", []) if isinstance(msg_data, dict) else []
            traj = {"messages": messages, "token_usage": msg_data.get("token_usage")}
            eval_r = eval_by_id.get(case_id, {})
            a = analyze_case(traj, eval_r, "shopping")
            analysis[case_id] = a
            tool_summaries.append(a["tool_summary"])
        md.shopping_analysis[level] = analysis
        if tool_summaries:
            md.shopping_tool_stats[level] = aggregate_tool_stats(tool_summaries)


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def score_class(value: Optional[float]) -> str:
    if value is None:
        return ""
    if value >= 0.7:
        return "score-high"
    if value >= 0.3:
        return "score-mid"
    return "score-low"


def fmt_pct(value: Optional[float], decimals: int = 1) -> str:
    if value is None:
        return "--"
    return f"{value * 100:.{decimals}f}%"


def fmt_score(value: Optional[float], decimals: int = 4) -> str:
    if value is None:
        return "--"
    return f"{value:.{decimals}f}"


def esc(text: Any) -> str:
    return html.escape(str(text))


def bar_color(score: float) -> str:
    if score >= 0.7:
        return "#1a7f37"
    if score >= 0.3:
        return "#b35900"
    return "#cf222e"


def safe_get(d: Optional[dict], *keys, default=None):
    cur = d
    for k in keys:
        if cur is None or not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur


def model_id(name: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]", "-", name)


def _valid_badge(valid: bool) -> str:
    if valid:
        return '<span class="badge badge-valid">Yes</span>'
    return '<span class="badge badge-invalid">No</span>'


# ---------------------------------------------------------------------------
# CSS
# ---------------------------------------------------------------------------

def generate_css() -> str:
    return """
:root {
    --bg-primary: #faf9f7;
    --bg-secondary: #f0efe8;
    --bg-card: #ffffff;
    --bg-table-row: #ffffff;
    --bg-table-row-alt: #f9f8f5;
    --text-primary: #1a1a1a;
    --text-secondary: #6b6560;
    --text-muted: #9b958f;
    --accent-terra: #c96442;
    --accent-green: #1a7f37;
    --accent-yellow: #b35900;
    --accent-red: #cf222e;
    --accent-blue: #0969da;
    --border-color: #e0ddd5;
    --border-light: #ebe9e2;
}
*, *::before, *::after { box-sizing: border-box; }
body {
    margin: 0; padding: 0;
    background: var(--bg-primary);
    color: var(--text-primary);
    font-family: 'Söhne', -apple-system, BlinkMacSystemFont, 'Helvetica Neue', Arial, sans-serif;
    font-size: 14px; line-height: 1.6;
    -webkit-font-smoothing: antialiased;
}
.dp-page { max-width: 1200px; margin: 0 auto; padding: 40px 32px; }
h1 { font-size: 32px; font-weight: 700; margin: 0 0 4px; color: #1a1a1a; letter-spacing: -0.5px; }
h2 { font-size: 20px; font-weight: 600; margin: 40px 0 16px; border-bottom: 1px solid var(--border-color);
     padding-bottom: 10px; color: #1a1a1a; }
h3 { font-size: 17px; font-weight: 600; margin: 24px 0 12px; color: #1a1a1a; }
h4 { font-size: 14px; font-weight: 600; margin: 16px 0 8px; color: var(--text-secondary); }
.subtitle { color: var(--text-secondary); font-size: 14px; margin-bottom: 32px; }
/* Summary cards */
.summary-cards { display: flex; flex-wrap: wrap; gap: 16px; margin: 24px 0; }
.summary-card {
    background: var(--bg-card); border: 1px solid var(--border-color);
    border-radius: 12px; padding: 24px; min-width: 260px; flex: 1;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.summary-card .model-name { font-size: 14px; font-weight: 600; margin-bottom: 12px; color: var(--accent-terra); }
.summary-card .metric-row { display: flex; justify-content: space-between; align-items: baseline; margin: 6px 0; }
.summary-card .metric-label { font-size: 12px; color: var(--text-muted); }
.summary-card .metric-value { font-size: 16px; font-weight: 700; }
.summary-card .headline-value { font-size: 32px; font-weight: 700; margin: 8px 0; letter-spacing: -0.5px; }
.badge { display: inline-block; padding: 2px 10px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.badge-valid { background: #dafbe1; color: var(--accent-green); }
.badge-invalid { background: #ffebe9; color: var(--accent-red); }
.badge-na { background: var(--bg-secondary); color: var(--text-muted); }
/* Tables */
table { width: 100%; border-collapse: collapse; margin: 8px 0; }
th, td { padding: 10px 14px; text-align: left; border-bottom: 1px solid var(--border-light); }
th { background: var(--bg-secondary); font-weight: 600; font-size: 11px;
     text-transform: uppercase; letter-spacing: 0.6px; color: var(--text-secondary);
     position: sticky; top: 0; }
tbody tr { background: var(--bg-table-row); }
tbody tr:nth-child(even) { background: var(--bg-table-row-alt); }
tbody tr:hover { background: #f0efe8; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
/* Sortable */
th.sortable { cursor: pointer; user-select: none; }
th.sortable:hover { color: var(--text-primary); }
th .sort-arrow { opacity: 0.3; margin-left: 4px; font-size: 10px; }
th.sort-asc .sort-arrow { opacity: 1; }
th.sort-desc .sort-arrow { opacity: 1; }
/* Score colors */
.score-low { color: var(--accent-red); }
.score-mid { color: var(--accent-yellow); }
.score-high { color: var(--accent-green); }
.score-best { text-decoration: underline; font-weight: 700; }
/* Bar chart */
.bar-row { display: flex; align-items: center; gap: 10px; margin: 6px 0; }
.bar-label { width: 200px; font-size: 13px; flex-shrink: 0; color: var(--text-primary); }
.bar-track { flex: 1; height: 24px; background: var(--bg-secondary); border-radius: 6px; overflow: hidden; }
.bar-fill { height: 100%; border-radius: 6px; min-width: 2px; transition: width 0.4s ease; }
.bar-value { width: 80px; text-align: right; font-size: 13px; font-weight: 600; flex-shrink: 0; }
.bar-extra { width: 70px; text-align: right; font-size: 12px; color: var(--text-muted); flex-shrink: 0; }
/* Collapsible details */
details { border: 1px solid var(--border-color); border-radius: 8px; margin: 8px 0;
          background: var(--bg-card); }
details > summary {
    cursor: pointer; padding: 12px 16px; background: var(--bg-card);
    border-radius: 8px; font-weight: 600; font-size: 14px;
    list-style: none; display: flex; align-items: center; gap: 8px;
    color: var(--text-primary);
}
details > summary::-webkit-details-marker { display: none; }
details > summary::before { content: '\\25b6'; font-size: 10px; transition: transform 0.2s; color: var(--text-muted); }
details[open] > summary::before { transform: rotate(90deg); }
details[open] > summary { border-bottom: 1px solid var(--border-light); border-radius: 8px 8px 0 0; }
.details-body { padding: 16px; }
/* Model section */
.model-section { margin: 24px 0; border: 1px solid var(--border-color); border-radius: 12px;
                 overflow: hidden; background: var(--bg-card);
                 box-shadow: 0 1px 3px rgba(0,0,0,0.04); }
.model-section-header {
    background: var(--bg-card); padding: 20px 24px;
    border-bottom: 1px solid var(--border-light);
    display: flex; align-items: center; justify-content: space-between;
}
.model-section-header h3 { margin: 0; font-size: 18px; color: var(--text-primary); }
.model-section-body { padding: 24px; }
/* Check pass/fail */
.check-pass { color: var(--accent-green); }
.check-fail { color: var(--accent-red); }
.check-icon { font-weight: 700; margin-right: 6px; }
.check-msg { font-size: 12px; color: var(--text-muted); margin-left: 24px; word-break: break-word;
             background: var(--bg-secondary); padding: 4px 8px; border-radius: 4px; margin-top: 2px; }
/* Error stats */
.error-type { font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px; }
.error-msg { font-size: 12px; color: var(--text-muted); max-width: 500px; overflow: hidden;
             text-overflow: ellipsis; white-space: nowrap; }
/* Filter / search */
.controls { display: flex; gap: 12px; align-items: center; margin: 16px 0; flex-wrap: wrap; }
.controls select, .controls input {
    background: var(--bg-card); color: var(--text-primary);
    border: 1px solid var(--border-color); border-radius: 8px;
    padding: 8px 14px; font-size: 13px;
}
.controls input { width: 260px; }
.controls select:focus, .controls input:focus { outline: none; border-color: var(--accent-terra);
    box-shadow: 0 0 0 3px rgba(201,100,66,0.12); }
/* Product lists */
.product-matched { color: var(--accent-green); }
.product-unmatched { color: var(--accent-red); }
.product-extra { color: var(--accent-yellow); }
.product-list { margin: 4px 0 4px 16px; font-size: 13px; }
.product-list li { margin: 2px 0; }
.query-text { background: var(--bg-secondary); border-radius: 6px; padding: 12px 16px;
              font-size: 13px; margin: 8px 0; border-left: 3px solid var(--accent-terra);
              color: var(--text-secondary); white-space: pre-wrap; word-break: break-word; }
/* Constraint table */
.constraint-row { display: flex; align-items: flex-start; gap: 8px; margin: 3px 0; padding: 4px 0; }
.constraint-name { font-size: 13px; min-width: 200px; }
/* Responsive */
@media (max-width: 900px) {
    .dp-page { padding: 20px 16px; }
    .summary-cards { flex-direction: column; }
    .bar-label { width: 140px; }
    .controls { flex-direction: column; align-items: flex-start; }
}
/* Rollout trace */
.trace-container { max-height: 600px; overflow-y: auto; font-size: 13px; border: 1px solid var(--border-light);
                   border-radius: 8px; margin: 8px 0; }
.trace-msg { padding: 8px 12px; margin: 0; border-bottom: 1px solid var(--border-light); border-left: 3px solid; }
.trace-msg:last-child { border-bottom: none; }
.trace-msg.msg-system { border-left-color: #9b958f; background: #f5f4f0; }
.trace-msg.msg-user { border-left-color: var(--accent-blue); background: #f0f7ff; }
.trace-msg.msg-assistant { border-left-color: var(--accent-terra); background: #fdf6f3; }
.trace-msg.msg-tool { border-left-color: var(--accent-green); background: #f0faf3; }
.trace-msg.lethal { border-left-color: var(--accent-red); background: #fff0f0;
                     box-shadow: inset 0 0 0 1px rgba(207,34,46,0.2); }
.trace-role { font-weight: 600; font-size: 11px; text-transform: uppercase; color: var(--text-muted);
              margin-bottom: 2px; display: flex; align-items: center; gap: 8px; }
.trace-role .lethal-tag { font-size: 10px; background: var(--accent-red); color: #fff;
                          padding: 1px 6px; border-radius: 4px; text-transform: none; }
.trace-content { white-space: pre-wrap; word-break: break-word; font-family: 'SF Mono', 'Fira Code', monospace;
                 font-size: 12px; max-height: 200px; overflow-y: auto; }
.trace-content.collapsed { max-height: 80px; overflow: hidden; position: relative; }
.trace-content.collapsed::after { content: '...click to expand'; position: absolute; bottom: 0; left: 0; right: 0;
    height: 24px; background: linear-gradient(transparent, rgba(255,255,255,0.9)); text-align: center;
    font-size: 11px; color: var(--accent-blue); cursor: pointer; line-height: 24px; }
.trace-tool-call { background: var(--bg-secondary); border-radius: 4px; padding: 6px 10px; margin: 4px 0;
                   font-size: 12px; font-family: 'SF Mono', 'Fira Code', monospace; }
.trace-tool-call .fn-name { color: var(--accent-terra); font-weight: 600; }
.trace-reasoning { color: var(--text-muted); font-style: italic; border-left: 2px solid var(--text-muted);
                   padding-left: 8px; margin: 4px 0; font-size: 12px; max-height: 100px; overflow-y: auto; }
/* Meta badges */
.meta-badges { display: flex; gap: 8px; flex-wrap: wrap; margin: 8px 0; }
.meta-badge { display: inline-flex; align-items: center; gap: 4px; padding: 4px 10px; border-radius: 6px;
              font-size: 12px; background: var(--bg-secondary); border: 1px solid var(--border-light); }
.meta-badge .meta-label { color: var(--text-muted); }
.meta-badge .meta-value { font-weight: 700; }
.failure-badge { background: #ffebe9; border-color: rgba(207,34,46,0.2); }
.failure-badge .meta-value { color: var(--accent-red); }
/* Tool summary table */
.tool-summary-table { font-size: 12px; }
.tool-summary-table th { font-size: 10px; padding: 6px 10px; }
.tool-summary-table td { padding: 6px 10px; }
/* Aggregate tool stats */
.tool-stats { margin: 12px 0; }
.tool-stats .bar-label { width: 180px; font-size: 12px; }
.tool-stats .bar-extra { font-size: 11px; }
/* Print */
@media print {
    body { background: #fff; }
    .dp-page { max-width: 100%; }
    details { border-color: #ddd; }
    .no-print { display: none; }
    .trace-container { max-height: none; }
}
"""


# ---------------------------------------------------------------------------
# JavaScript
# ---------------------------------------------------------------------------

def generate_js() -> str:
    return """
// Sortable tables
document.querySelectorAll('th.sortable').forEach(th => {
    th.addEventListener('click', () => {
        const table = th.closest('table');
        const tbody = table.querySelector('tbody');
        if (!tbody) return;
        const col = parseInt(th.dataset.col);
        const isAsc = th.classList.contains('sort-asc');
        table.querySelectorAll('th.sortable').forEach(h => h.classList.remove('sort-asc', 'sort-desc'));
        th.classList.add(isAsc ? 'sort-desc' : 'sort-asc');
        const rows = Array.from(tbody.querySelectorAll(':scope > tr'));
        rows.sort((a, b) => {
            const ac = a.children[col], bc = b.children[col];
            if (!ac || !bc) return 0;
            let av = ac.dataset.sort !== undefined ? ac.dataset.sort : ac.textContent.trim();
            let bv = bc.dataset.sort !== undefined ? bc.dataset.sort : bc.textContent.trim();
            const an = parseFloat(av), bn = parseFloat(bv);
            if (!isNaN(an) && !isNaN(bn)) { return isAsc ? bn - an : an - bn; }
            return isAsc ? bv.localeCompare(av) : av.localeCompare(bv);
        });
        rows.forEach(r => tbody.appendChild(r));
    });
});

// Lazy-load templates inside <details>
document.querySelectorAll('details[data-lazy]').forEach(det => {
    det.addEventListener('toggle', function handler() {
        if (det.open) {
            const tpl = det.querySelector('template');
            if (tpl) {
                const body = det.querySelector('.details-body');
                if (body) { body.innerHTML = tpl.innerHTML; tpl.remove(); }
            }
        }
    });
});

// Model filter
const mf = document.getElementById('model-filter');
if (mf) {
    mf.addEventListener('change', e => {
        const val = e.target.value;
        document.querySelectorAll('.model-section').forEach(el => {
            el.style.display = (val === '__all__' || el.dataset.model === val) ? '' : 'none';
        });
    });
}

// Case search
document.querySelectorAll('.case-search').forEach(input => {
    input.addEventListener('input', () => {
        const q = input.value.toLowerCase();
        const container = input.closest('.domain-detail');
        if (!container) return;
        container.querySelectorAll('.case-item').forEach(row => {
            const text = row.textContent.toLowerCase();
            row.style.display = text.includes(q) ? '' : 'none';
        });
    });
});

// Expand/collapse trace content
document.addEventListener('click', e => {
    const el = e.target.closest('.trace-content.collapsed');
    if (el) { el.classList.remove('collapsed'); }
});

// Jump to lethal step
document.addEventListener('click', e => {
    const btn = e.target.closest('.jump-to-lethal');
    if (btn) {
        const container = btn.closest('.details-body');
        if (container) {
            const lethal = container.querySelector('.trace-msg.lethal');
            if (lethal) lethal.scrollIntoView({behavior: 'smooth', block: 'center'});
        }
    }
});
"""


# ---------------------------------------------------------------------------
# Rendering: Header & overview
# ---------------------------------------------------------------------------

def render_header(models: List[ModelData]) -> str:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    names = ", ".join(m.name for m in models)
    return (
        f"<h1>DeepPlanning Benchmark Results</h1>\n"
        f'<div class="subtitle">Generated {esc(ts)} &mdash; {len(models)} model(s): {esc(names)}</div>\n'
    )


def _card_metric(label: str, value: Optional[float], is_pct: bool = True) -> str:
    display = fmt_pct(value) if is_pct else fmt_score(value)
    cls = score_class(value)
    return (
        f'<div class="metric-row">'
        f'<span class="metric-label">{esc(label)}</span>'
        f'<span class="metric-value {cls}">{display}</span>'
        f'</div>'
    )


def render_overview_cards(models: List[ModelData]) -> str:
    parts = ['<div class="summary-cards">']
    for md in models:
        agg = md.aggregated or {}
        overall = agg.get("overall", {})
        travel = agg.get("domains", {}).get("travel", {})
        travel_comp = travel.get("composite_score")
        travel_ca = travel.get("case_acc")
        travel_cs = travel.get("commonsense_score")
        travel_ps = travel.get("personalized_score")

        parts.append(f'<div class="summary-card">')
        parts.append(f'<div class="model-name">{esc(md.name)}</div>')

        if travel_comp is not None:
            cls = score_class(travel_comp)
            parts.append(f'<div class="headline-value {cls}">{fmt_pct(travel_comp)}</div>')
            parts.append(f'<div class="metric-label">Travel Composite Score</div>')
        else:
            parts.append(f'<div class="headline-value" style="color:var(--text-muted)">--</div>')
            parts.append(f'<div class="metric-label">No travel data</div>')

        parts.append(_card_metric("Case Accuracy", travel_ca))
        parts.append(_card_metric("Commonsense", travel_cs))
        parts.append(_card_metric("Personalized", travel_ps))
        parts.append('</div>')

    parts.append('</div>')
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Rendering: Comparison table
# ---------------------------------------------------------------------------

def render_comparison_table(models: List[ModelData]) -> str:
    if len(models) < 2:
        return ""

    cols = [
        ("Model", False),
        ("Composite", True),
        ("Case Acc", True),
        ("Commonsense", True),
        ("Personalized", True),
        ("Delivery Rate", True),
    ]

    # Extract values per model
    rows_data = []
    for md in models:
        travel = (md.aggregated or {}).get("domains", {}).get("travel", {})
        # Also try to get delivery rate from travel summaries
        delivery_rates = []
        for lang_summary in md.travel_summaries.values():
            dr = lang_summary.get("metrics", {}).get("delivery_rate")
            if dr is not None:
                delivery_rates.append(dr)
        avg_dr = sum(delivery_rates) / len(delivery_rates) if delivery_rates else None

        row = {
            "name": md.name,
            "travel_comp": travel.get("composite_score"),
            "travel_ca": travel.get("case_acc"),
            "travel_cs": travel.get("commonsense_score"),
            "travel_ps": travel.get("personalized_score"),
            "travel_dr": avg_dr,
        }
        rows_data.append(row)

    # Find best per column
    metric_keys = ["travel_comp", "travel_ca", "travel_cs", "travel_ps", "travel_dr"]
    best = {}
    for k in metric_keys:
        vals = [(i, r[k]) for i, r in enumerate(rows_data) if r[k] is not None]
        if vals:
            best[k] = max(vals, key=lambda x: x[1])[0]

    parts = ['<h2>Model Comparison</h2>', '<table>', '<thead><tr>']
    for i, (label, sortable) in enumerate(cols):
        cls = 'class="sortable"' if sortable else ""
        dc = f'data-col="{i}"' if sortable else ""
        arrow = '<span class="sort-arrow">\u25b4\u25be</span>' if sortable else ""
        parts.append(f'<th {cls} {dc}>{esc(label)}{arrow}</th>')
    parts.append('</tr></thead><tbody>')

    for idx, rd in enumerate(rows_data):
        parts.append("<tr>")
        parts.append(f'<td data-sort="{esc(rd["name"])}">{esc(rd["name"])}</td>')
        for k in metric_keys:
            v = rd[k]
            sv = f"{v:.6f}" if v is not None else "-1"
            cls = score_class(v)
            if best.get(k) == idx:
                cls += " score-best"
            parts.append(f'<td class="num {cls}" data-sort="{sv}">{fmt_pct(v)}</td>')
        parts.append("</tr>")

    parts.append("</tbody></table>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Rendering: Trace & analysis components
# ---------------------------------------------------------------------------

def _render_meta_badges(analysis: Optional[dict]) -> str:
    """Render compact metric badges for turns, tokens, elapsed time, failure mode."""
    if not analysis:
        return ""
    parts = ['<div class="meta-badges">']

    turns = analysis.get("turns")
    if turns is not None:
        parts.append(f'<div class="meta-badge"><span class="meta-label">Turns</span>'
                     f'<span class="meta-value">{turns}</span></div>')

    token_usage = analysis.get("token_usage")
    if token_usage:
        total = token_usage.get("total_tokens", 0)
        prompt = token_usage.get("prompt_tokens", 0)
        completion = token_usage.get("completion_tokens", 0)
        parts.append(f'<div class="meta-badge" title="Prompt: {prompt:,} / Completion: {completion:,}">'
                     f'<span class="meta-label">Tokens</span>'
                     f'<span class="meta-value">{total:,}</span></div>')
    else:
        parts.append(f'<div class="meta-badge"><span class="meta-label">Tokens</span>'
                     f'<span class="meta-value" style="color:var(--text-muted)">N/A</span></div>')

    elapsed = analysis.get("elapsed_time")
    if elapsed is not None:
        parts.append(f'<div class="meta-badge"><span class="meta-label">Time</span>'
                     f'<span class="meta-value">{elapsed:.1f}s</span></div>')

    tool_summary = analysis.get("tool_summary", {})
    total_calls = tool_summary.get("total_calls", 0)
    if total_calls > 0:
        parts.append(f'<div class="meta-badge"><span class="meta-label">Tool Calls</span>'
                     f'<span class="meta-value">{total_calls}</span></div>')

    failure_mode = analysis.get("failure_mode")
    if failure_mode:
        fm_display = failure_mode if len(failure_mode) <= 60 else failure_mode[:57] + "..."
        parts.append(f'<div class="meta-badge failure-badge" title="{esc(failure_mode)}">'
                     f'<span class="meta-label">Failure</span>'
                     f'<span class="meta-value">{esc(fm_display)}</span></div>')

    parts.append('</div>')
    return "\n".join(parts)


def _render_tool_call_summary(analysis: Optional[dict]) -> str:
    """Render per-case tool call summary table."""
    if not analysis:
        return ""
    tool_summary = analysis.get("tool_summary", {})
    tools = tool_summary.get("tools", {})
    if not tools:
        return ""

    parts = ['<details style="border:none; margin:4px 0;">',
             '<summary style="background:transparent; padding:4px 8px; font-size:13px;">Tool Call Summary</summary>',
             '<div style="padding:4px 8px;">',
             '<table class="tool-summary-table"><thead><tr>']
    for h in ["Tool", "Calls", "Success", "Errors"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for name, stats in sorted(tools.items(), key=lambda x: x[1]["count"], reverse=True):
        err_cls = "score-low" if stats["error"] > 0 else ""
        parts.append(
            f'<tr>'
            f'<td class="error-type">{esc(name)}</td>'
            f'<td class="num">{stats["count"]}</td>'
            f'<td class="num">{stats["success"]}</td>'
            f'<td class="num {err_cls}">{stats["error"]}</td>'
            f'</tr>'
        )

    parts.append("</tbody></table>")

    # Call order
    call_order = tool_summary.get("call_order", [])
    if call_order:
        short_names = [n.replace("query_", "q_").replace("recommend_", "rec_").replace("search_", "s_")
                       for n in call_order]
        order_str = " → ".join(short_names)
        if len(order_str) > 200:
            order_str = order_str[:197] + "..."
        parts.append(f'<div style="font-size:11px; color:var(--text-muted); margin-top:4px;">'
                     f'<strong>Order:</strong> {esc(order_str)}</div>')

    parts.append('</div></details>')
    return "\n".join(parts)


def _render_aggregate_tool_stats(tool_stats: Optional[dict]) -> str:
    """Render model-level aggregate tool usage stats."""
    if not tool_stats:
        return ""
    tools = tool_stats.get("tools", {})
    if not tools:
        return ""

    num_cases = tool_stats.get("num_cases", 0)
    avg_calls = tool_stats.get("avg_calls_per_case", 0)
    total = tool_stats.get("total_calls_all_cases", 0)

    parts = ['<details>', f'<summary>Tool Usage Statistics ({total} total calls across {num_cases} cases, '
             f'avg {avg_calls:.1f}/case)</summary>',
             '<div class="details-body tool-stats">']

    # Sort by total calls
    sorted_tools = sorted(tools.items(), key=lambda x: x[1]["total_calls"], reverse=True)
    max_calls = sorted_tools[0][1]["total_calls"] if sorted_tools else 1

    for name, stats in sorted_tools:
        tc = stats["total_calls"]
        ts = stats["total_success"]
        te = stats["total_error"]
        cu = stats["cases_used"]
        pct = tc / max_calls * 100
        err_rate = te / tc * 100 if tc > 0 else 0
        color = bar_color(1.0 - err_rate / 100)  # green if low error rate

        parts.append(
            f'<div class="bar-row">'
            f'<div class="bar-label">{esc(name)}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%;background:{color};"></div></div>'
            f'<div class="bar-value">{tc}</div>'
            f'<div class="bar-extra" title="{ts} ok, {te} err, {cu} cases">'
            f'{"" if te == 0 else f"{te} err"}'
            f'</div>'
            f'</div>'
        )

    parts.append('</div></details>')
    return "\n".join(parts)


def _render_rollout_trace(messages: List[dict], lethal_step_index: Optional[int] = None) -> str:
    """Render the full agent conversation as a styled trace.
    System prompts are skipped. Tool responses and long content are truncated to keep HTML small."""
    if not messages:
        return ""

    # Filter out system messages for display (they're just long prompts)
    display_msgs = [(i, m) for i, m in enumerate(messages)
                    if isinstance(m, dict) and m.get("role") != "system"]

    parts = ['<details style="border:none; margin:4px 0;">',
             '<summary style="background:transparent; padding:4px 8px; font-size:13px;">',
             f'Rollout Trace ({len(display_msgs)} messages, system prompt hidden)']
    if lethal_step_index is not None:
        parts.append(' <button class="jump-to-lethal" style="font-size:11px; cursor:pointer; '
                     'background:var(--accent-red); color:#fff; border:none; border-radius:4px; '
                     'padding:2px 8px; margin-left:8px;">Jump to lethal step</button>')
    parts.append('</summary>')
    parts.append('<div class="trace-container">')

    MAX_CONTENT = 800  # max chars to show per message
    MAX_TOOL_RESPONSE = 400
    MAX_REASONING = 500

    for orig_idx, msg in display_msgs:
        role = msg.get("role", "unknown")
        is_lethal = (orig_idx == lethal_step_index)
        cls = f"trace-msg msg-{role}"
        if is_lethal:
            cls += " lethal"

        parts.append(f'<div class="{cls}">')

        # Role label
        role_label = role.upper()
        if role == "tool":
            tool_name = msg.get("name", "")
            if tool_name:
                role_label = f"TOOL: {tool_name}"
        lethal_tag = '<span class="lethal-tag">LETHAL STEP</span>' if is_lethal else ""
        parts.append(f'<div class="trace-role">{esc(role_label)} '
                     f'<span style="font-size:10px;color:var(--text-muted);">#{orig_idx}</span>'
                     f'{lethal_tag}</div>')

        # Reasoning content (for thinking models)
        reasoning = msg.get("reasoning_content", "")
        if reasoning:
            r_display = reasoning[:MAX_REASONING] + ("..." if len(reasoning) > MAX_REASONING else "")
            parts.append(f'<div class="trace-reasoning">{esc(r_display)}</div>')

        # Tool calls
        for tc in msg.get("tool_calls", []):
            func = tc.get("function", {})
            fn_name = func.get("name", "unknown")
            fn_args = func.get("arguments", "")
            if len(fn_args) > 200:
                fn_args = fn_args[:197] + "..."
            parts.append(f'<div class="trace-tool-call"><span class="fn-name">{esc(fn_name)}</span>'
                         f'({esc(fn_args)})</div>')

        # Content
        content = msg.get("content", "")
        if content:
            max_len = MAX_TOOL_RESPONSE if role == "tool" else MAX_CONTENT
            truncated = len(content) > max_len
            display = content[:max_len] + ("..." if truncated else "")
            parts.append(f'<div class="trace-content">{esc(display)}</div>')

        parts.append('</div>')

    parts.append('</div></details>')
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Rendering: Shopping domain
# ---------------------------------------------------------------------------

def render_shopping_section(md: ModelData) -> str:
    if not md.shopping_stats:
        return '<p style="color:var(--text-muted)">No shopping data available.</p>'

    stats = md.shopping_stats
    levels = stats.get("levels", {})
    total = stats.get("total", {})

    parts = ['<h3>Shopping Planning</h3>']

    # Level breakdown table
    parts.append('<table><thead><tr>')
    for h in ["Level", "Cases", "Success", "Avg Case Score", "Match Rate", "Incomplete Rate", "Valid"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for lk in sorted(levels.keys()):
        lv = levels[lk]
        level_num = lk.replace("level_", "")
        sc_cls = score_class(lv.get("average_case_score"))
        mr_cls = score_class(lv.get("overall_match_rate"))
        valid = lv.get("valid", False)
        parts.append(
            f'<tr>'
            f'<td>Level {esc(level_num)}</td>'
            f'<td class="num">{lv.get("total_cases", 0)}</td>'
            f'<td class="num">{lv.get("successful_cases", 0)}</td>'
            f'<td class="num {sc_cls}">{fmt_pct(lv.get("average_case_score"))}</td>'
            f'<td class="num {mr_cls}">{fmt_pct(lv.get("overall_match_rate"))}</td>'
            f'<td class="num">{fmt_pct(lv.get("incomplete_rate"))}</td>'
            f'<td>{_valid_badge(valid)}</td>'
            f'</tr>'
        )

    # Total row
    sc_cls = score_class(total.get("weighted_average_case_score"))
    mr_cls = score_class(total.get("match_rate"))
    valid = total.get("valid", False)
    parts.append(
        f'<tr style="font-weight:700; border-top: 2px solid var(--border-color);">'
        f'<td>Total</td>'
        f'<td class="num">{total.get("total_cases", 0)}</td>'
        f'<td class="num">{total.get("successful_cases", 0)}</td>'
        f'<td class="num {sc_cls}">{fmt_pct(total.get("weighted_average_case_score"))}</td>'
        f'<td class="num {mr_cls}">{fmt_pct(total.get("match_rate"))}</td>'
        f'<td class="num">{fmt_pct(total.get("incomplete_rate"))}</td>'
        f'<td>{_valid_badge(valid)}</td>'
        f'</tr>'
    )
    parts.append("</tbody></table>")

    # Per-level case drill-down
    for level in sorted(md.shopping_case_reports.keys()):
        summary, cases = md.shopping_case_reports[level]
        if not cases:
            continue

        # Add aggregate tool stats for this level
        tool_stats = md.shopping_tool_stats.get(level)
        if tool_stats:
            parts.append(_render_aggregate_tool_stats(tool_stats))

        analysis_map = md.shopping_analysis.get(level, {})
        messages_map = md.shopping_messages.get(level, {})
        mid = model_id(md.name)
        parts.append(f'<details data-lazy>')
        parts.append(f'<summary>Level {level} &mdash; {len(cases)} cases (click to expand)</summary>')
        parts.append(f'<div class="details-body"><template>{_render_shopping_cases(cases, analysis_map, messages_map)}</template></div>')
        parts.append(f'</details>')

    return "\n".join(parts)


def _render_shopping_cases(cases: List[dict], analysis_map: Optional[Dict[str, dict]] = None,
                           messages_map: Optional[Dict[str, dict]] = None) -> str:
    analysis_map = analysis_map or {}
    messages_map = messages_map or {}
    parts = ['<input type="text" class="case-search" placeholder="Search cases..." style="margin-bottom:10px;">']
    parts.append('<table><thead><tr>')
    for h in ["Case", "Score", "Matched", "Expected", "Extra", "Turns", "Failure Mode", "Status"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for c in cases:
        s = c.get("summary", {})
        score = s.get("score", 0)
        matched = s.get("matched_count", 0)
        expected = s.get("expected_count", 0)
        extra = s.get("extra_products_count", 0)
        case_name = c.get("case_name", "?")
        case_id = case_name.replace("case_", "")
        is_perfect = matched == expected and extra == 0
        cls = "score-high" if is_perfect else ("score-mid" if score > 0 else "score-low")
        status = "Perfect" if is_perfect else ("Partial" if score > 0 else "Failed")

        a = analysis_map.get(case_id, {})
        turns = a.get("turns", "")
        failure = a.get("failure_mode", "") or ""
        fm_short = failure if len(failure) <= 30 else failure[:27] + "..."

        parts.append(
            f'<tr class="case-item">'
            f'<td>{esc(case_name)}</td>'
            f'<td class="num {cls}">{fmt_pct(score)}</td>'
            f'<td class="num">{matched}</td>'
            f'<td class="num">{expected}</td>'
            f'<td class="num">{extra}</td>'
            f'<td class="num">{turns}</td>'
            f'<td class="error-type" title="{esc(failure)}">{esc(fm_short)}</td>'
            f'<td><span class="badge {"badge-valid" if is_perfect else "badge-invalid"}">{status}</span></td>'
            f'</tr>'
        )

    parts.append("</tbody></table>")

    # Detailed per-case expandable
    for c in cases:
        case_name = c.get("case_name", "?")
        case_id = case_name.replace("case_", "")
        a = analysis_map.get(case_id)
        msg_data = messages_map.get(case_id, {})
        messages = msg_data.get("messages", []) if isinstance(msg_data, dict) else []
        parts.append(f'<details class="case-item">')
        parts.append(f'<summary>{esc(case_name)} detail</summary>')
        parts.append(f'<div class="details-body">{_render_shopping_case_detail(c, a, messages)}</div>')
        parts.append(f'</details>')

    return "\n".join(parts)


def _render_shopping_case_detail(c: dict, analysis: Optional[dict] = None,
                                  messages: Optional[List[dict]] = None) -> str:
    parts = []

    # Meta badges (turns, tokens, failure mode, etc.)
    parts.append(_render_meta_badges(analysis))

    query = c.get("query", "")
    if query:
        parts.append(f'<h4>Query</h4><div class="query-text">{esc(query)}</div>')

    s = c.get("summary", {})
    parts.append(f'<h4>Score: {fmt_pct(s.get("score", 0))} '
                 f'({s.get("matched_count", 0)}/{s.get("expected_count", 0)} matched, '
                 f'{s.get("extra_products_count", 0)} extra)</h4>')

    # Unmatched ground truth
    unmatched = c.get("unmatched_ground_truth_products", [])
    if unmatched:
        parts.append('<h4 class="product-unmatched">Unmatched Ground Truth Products</h4>')
        parts.append('<ul class="product-list">')
        for p in unmatched:
            parts.append(f'<li class="product-unmatched">{esc(p.get("name", "?"))} (ID: {esc(p.get("product_id", "?"))})</li>')
        parts.append('</ul>')

    # Extra products
    extra = c.get("extra_products", [])
    if extra:
        parts.append('<h4 class="product-extra">Extra Products (not in ground truth)</h4>')
        parts.append('<ul class="product-list">')
        for p in extra:
            name = p.get("name", "?")
            parts.append(f'<li class="product-extra">{esc(name)} (ID: {esc(p.get("product_id", "?"))})</li>')
        parts.append('</ul>')

    # Matched products
    matched_ids = c.get("matched_products", [])
    if matched_ids:
        parts.append(f'<h4 class="product-matched">Matched Products ({len(matched_ids)})</h4>')
        parts.append('<ul class="product-list">')
        gt = {p.get("product_id"): p.get("name", "?") for p in c.get("ground_truth_products", [])}
        for pid in matched_ids:
            name = gt.get(pid, pid)
            parts.append(f'<li class="product-matched">{esc(name)} (ID: {esc(str(pid))})</li>')
        parts.append('</ul>')

    # Tool call summary
    parts.append(_render_tool_call_summary(analysis))

    # Rollout trace
    if messages:
        lethal_idx = None
        if analysis and analysis.get("lethal_step"):
            lethal_idx = analysis["lethal_step"].get("step_index")
        parts.append(_render_rollout_trace(messages, lethal_idx))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Rendering: Travel domain
# ---------------------------------------------------------------------------

def render_travel_section(md: ModelData) -> str:
    if not md.travel_summaries:
        return '<p style="color:var(--text-muted)">No travel data available.</p>'

    parts = ['<h3>Travel Planning</h3>']

    for lang in sorted(md.travel_summaries.keys()):
        summary = md.travel_summaries[lang]
        metrics = summary.get("metrics", {})
        total_samples = summary.get("total_test_samples", 0)

        parts.append(f'<h4>Language: {esc(lang.upper())} ({total_samples} samples)</h4>')

        # Key metrics row
        key_metrics = [
            ("Delivery Rate", metrics.get("delivery_rate")),
            ("Commonsense Score", metrics.get("commonsense_score")),
            ("Personalized Score", metrics.get("personalized_score")),
            ("Composite Score", metrics.get("composite_score")),
            ("Case Accuracy", metrics.get("case_acc")),
        ]
        parts.append('<table><thead><tr>')
        for label, _ in key_metrics:
            parts.append(f"<th>{esc(label)}</th>")
        parts.append("</tr></thead><tbody><tr>")
        for _, v in key_metrics:
            cls = score_class(v)
            parts.append(f'<td class="num {cls}" style="font-weight:600;">{fmt_pct(v)}</td>')
        parts.append("</tr></tbody></table>")

        # Commonsense dimension bars
        dims = metrics.get("commonsense_dimensions", {})
        if dims:
            parts.append('<h4>Commonsense Dimensions</h4>')
            parts.append(_render_commonsense_bars(dims, total_samples))

        # Error statistics
        errors = summary.get("error_statistics", [])
        if errors:
            parts.append(_render_error_statistics(errors))

        # Aggregate tool stats
        tool_stats = md.travel_tool_stats.get(lang)
        if tool_stats:
            parts.append(_render_aggregate_tool_stats(tool_stats))

        # Per-case drill-down
        case_scores = md.travel_case_scores.get(lang, [])
        if case_scores:
            analysis_map = md.travel_analysis.get(lang, {})
            trajectories_map = md.travel_trajectories.get(lang, {})
            mid = model_id(md.name)
            parts.append(f'<details data-lazy>')
            parts.append(f'<summary>{len(case_scores)} cases &mdash; per-case detail (click to expand)</summary>')
            parts.append(f'<div class="details-body"><template>'
                         f'{_render_travel_cases(case_scores, analysis_map, trajectories_map)}'
                         f'</template></div>')
            parts.append(f'</details>')

    return "\n".join(parts)


def _render_commonsense_bars(dims: dict, total_samples: int) -> str:
    parts = []
    for dim_name, dim_data in dims.items():
        score = dim_data.get("score", 0)
        perfect = dim_data.get("perfect_count", 0)
        color = bar_color(score)
        pct = score * 100
        parts.append(
            f'<div class="bar-row">'
            f'<div class="bar-label">{esc(dim_name)}</div>'
            f'<div class="bar-track"><div class="bar-fill" style="width:{pct:.1f}%;background:{color};"></div></div>'
            f'<div class="bar-value {score_class(score)}">{fmt_pct(score)}</div>'
            f'<div class="bar-extra">{perfect}/{total_samples}</div>'
            f'</div>'
        )
    return "\n".join(parts)


def _render_error_statistics(errors: list) -> str:
    parts = ['<details>', '<summary>Error Statistics (top failures)</summary>', '<div class="details-body">']
    parts.append('<table><thead><tr>')
    for h in ["Rank", "Error Type", "Count", "Example Message"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for err in errors[:15]:
        rank = err.get("rank", "?")
        etype = err.get("error_type", "?")
        count = err.get("count", 0)
        msgs = err.get("sample_messages", [])
        example = msgs[0] if msgs else "--"
        if len(example) > 120:
            example = example[:120] + "..."
        parts.append(
            f'<tr>'
            f'<td class="num">{rank}</td>'
            f'<td class="error-type">{esc(etype)}</td>'
            f'<td class="num">{count}</td>'
            f'<td class="error-msg" title="{esc(example)}">{esc(example)}</td>'
            f'</tr>'
        )

    parts.append("</tbody></table>")
    parts.append('</div></details>')
    return "\n".join(parts)


def _render_travel_cases(case_scores: List[dict], analysis_map: Optional[Dict[str, dict]] = None,
                          trajectories_map: Optional[Dict[str, dict]] = None) -> str:
    analysis_map = analysis_map or {}
    trajectories_map = trajectories_map or {}
    parts = ['<input type="text" class="case-search" placeholder="Search cases..." style="margin-bottom:10px;">']
    parts.append('<table><thead><tr>')
    for h in ["ID", "Composite", "Case Acc", "Commonsense", "Personalized", "Turns", "Tokens", "Failure Mode"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for cs in case_scores:
        sid = str(cs.get("sample_id", "?"))
        scores = cs.get("scores", {})
        comp = scores.get("composite_score")
        ca = scores.get("case_acc")
        cws = scores.get("commonsense_weighted_score")
        ps = scores.get("personalized_score")

        a = analysis_map.get(sid, {})
        turns = a.get("turns", "")
        token_usage = a.get("token_usage")
        tokens_display = f'{token_usage["total_tokens"]:,}' if token_usage else "N/A"
        failure = a.get("failure_mode", "") or ""
        fm_short = failure if len(failure) <= 30 else failure[:27] + "..."

        parts.append(
            f'<tr class="case-item">'
            f'<td>id_{esc(sid)}</td>'
            f'<td class="num {score_class(comp)}">{fmt_pct(comp)}</td>'
            f'<td class="num {score_class(ca)}">{fmt_pct(ca)}</td>'
            f'<td class="num {score_class(cws)}">{fmt_pct(cws)}</td>'
            f'<td class="num {score_class(ps)}">{fmt_pct(ps)}</td>'
            f'<td class="num">{turns}</td>'
            f'<td class="num">{tokens_display}</td>'
            f'<td class="error-type" title="{esc(failure)}">{esc(fm_short)}</td>'
            f'</tr>'
        )

    parts.append("</tbody></table>")

    # Per-case expandable detail
    for cs in case_scores:
        sid = str(cs.get("sample_id", "?"))
        a = analysis_map.get(sid)
        traj = trajectories_map.get(sid, {})
        messages = traj.get("messages", [])
        parts.append(f'<details class="case-item">')
        parts.append(f'<summary>id_{esc(sid)} &mdash; detail</summary>')
        parts.append(f'<div class="details-body">{_render_travel_case_detail(cs, a, messages)}</div>')
        parts.append(f'</details>')

    return "\n".join(parts)


def _render_travel_case_detail(cs: dict, analysis: Optional[dict] = None,
                                messages: Optional[List[dict]] = None) -> str:
    parts = []

    # Meta badges (turns, tokens, failure mode, etc.)
    parts.append(_render_meta_badges(analysis))

    scores = cs.get("scores", {})
    dim_scores = cs.get("commonsense_dimension_scores", {})
    dim_details = cs.get("commonsense_dimension_details", {})
    pers = cs.get("personalized_dimension_score", {})

    # Score summary
    parts.append('<table><thead><tr><th>Composite</th><th>Case Acc</th>'
                 '<th>Commonsense Weighted</th><th>Personalized</th></tr></thead><tbody><tr>')
    for k in ("composite_score", "case_acc", "commonsense_weighted_score", "personalized_score"):
        v = scores.get(k)
        parts.append(f'<td class="num {score_class(v)}" style="font-weight:600;">{fmt_pct(v)}</td>')
    parts.append('</tr></tbody></table>')

    # Commonsense dimensions
    parts.append('<h4>Commonsense Dimensions</h4>')
    for dim_name, dim_data in dim_details.items():
        dim_score = dim_scores.get(dim_name, 0)
        passed = dim_data.get("passed", 0)
        total = dim_data.get("total", 0)
        cls = "check-pass" if dim_score == 1.0 else "check-fail"
        icon = "\u2713" if dim_score == 1.0 else "\u2717"
        parts.append(
            f'<details style="border:none; margin:2px 0;">'
            f'<summary class="{cls}" style="background:transparent; padding:4px 8px; font-size:13px;">'
            f'<span class="check-icon">{icon}</span> {esc(dim_name)} ({passed}/{total})'
            f'</summary>'
            f'<div style="padding:4px 8px 4px 32px;">'
        )
        for check in dim_data.get("checks", []):
            chk_name = check.get("name", "?")
            chk_pass = check.get("passed", False)
            chk_msg = check.get("message")
            c_cls = "check-pass" if chk_pass else "check-fail"
            c_icon = "\u2713" if chk_pass else "\u2717"
            parts.append(f'<div class="constraint-row"><span class="check-icon {c_cls}">{c_icon}</span>'
                         f'<span class="constraint-name">{esc(chk_name)}</span></div>')
            if chk_msg:
                msg = str(chk_msg)
                if len(msg) > 300:
                    msg = msg[:300] + "..."
                parts.append(f'<div class="check-msg">{esc(msg)}</div>')
        parts.append('</div></details>')

    # Personalized constraints
    constraints = pers.get("constraints", {})
    if constraints:
        pers_score = pers.get("score", 0)
        cls = "check-pass" if pers_score == 1.0 else "check-fail"
        parts.append(f'<h4 class="{cls}">Personalized Constraints (score: {fmt_pct(pers_score)})</h4>')
        for cname, cdata in constraints.items():
            cp = cdata.get("passed", False)
            cmsg = cdata.get("message")
            c_cls = "check-pass" if cp else "check-fail"
            c_icon = "\u2713" if cp else "\u2717"
            parts.append(f'<div class="constraint-row"><span class="check-icon {c_cls}">{c_icon}</span>'
                         f'<span class="constraint-name">{esc(cname)}</span></div>')
            if cmsg:
                msg = str(cmsg)
                if len(msg) > 300:
                    msg = msg[:300] + "..."
                parts.append(f'<div class="check-msg">{esc(msg)}</div>')

    # Tool call summary
    parts.append(_render_tool_call_summary(analysis))

    # Rollout trace
    if messages:
        lethal_idx = None
        if analysis and analysis.get("lethal_step"):
            lethal_idx = analysis["lethal_step"].get("step_index")
        parts.append(_render_rollout_trace(messages, lethal_idx))

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Rendering: Per-model detail section
# ---------------------------------------------------------------------------

def render_model_detail(md: ModelData) -> str:
    mid = model_id(md.name)
    parts = [
        f'<div class="model-section" data-model="{esc(md.name)}">',
        f'<div class="model-section-header">',
        f'<h3>{esc(md.name)}</h3>',
    ]

    # Quick badge: travel composite score
    travel = (md.aggregated or {}).get("domains", {}).get("travel", {})
    comp = travel.get("composite_score")
    if comp is not None:
        cls = score_class(comp)
        parts.append(f'<span class="{cls}" style="font-size:18px;font-weight:700;">composite: {fmt_pct(comp)}</span>')

    parts.append('</div>')  # header
    parts.append('<div class="model-section-body domain-detail">')

    # Travel
    parts.append(render_travel_section(md))

    parts.append('</div>')  # body
    parts.append('</div>')  # section
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Rendering: Full page
# ---------------------------------------------------------------------------

def render_page(models: List[ModelData]) -> str:
    parts = [
        "<!DOCTYPE html>",
        "<html lang='en'>",
        "<head>",
        "<meta charset='utf-8'>",
        "<meta name='viewport' content='width=device-width, initial-scale=1'>",
        "<title>DeepPlanning Benchmark Results</title>",
        f"<style>{generate_css()}</style>",
        "</head>",
        "<body>",
        '<main class="dp-page">',
        render_header(models),
        render_overview_cards(models),
    ]

    if len(models) > 1:
        parts.append(render_comparison_table(models))

    # Model filter
    if len(models) > 1:
        parts.append('<div class="controls no-print">')
        parts.append('<label>Filter by model:</label>')
        parts.append('<select id="model-filter"><option value="__all__">All models</option>')
        for md in models:
            parts.append(f'<option value="{esc(md.name)}">{esc(md.name)}</option>')
        parts.append('</select></div>')

    parts.append('<h2>Detailed Results</h2>')

    for md in models:
        parts.append(render_model_detail(md))

    parts.append(f"<script>{generate_js()}</script>")
    parts.append("</main></body></html>")
    return "\n".join(parts)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate HTML visualization of DeepPlanning benchmark results"
    )
    parser.add_argument(
        "--models", nargs="*", default=None,
        help="Model names to include (auto-detect from aggregated_results/ if omitted)"
    )
    parser.add_argument(
        "--output", type=str, default="deepplanning_results.html",
        help="Output HTML file path (default: deepplanning_results.html)"
    )
    parser.add_argument(
        "--shopping-dir", type=str, default=None,
        help="Shopping results directory (default: shoppingplanning/)"
    )
    parser.add_argument(
        "--travel-dir", type=str, default=None,
        help="Travel results directory (default: travelplanning/)"
    )
    parser.add_argument(
        "--aggregated-dir", type=str, default=None,
        help="Aggregated results directory (default: aggregated_results/)"
    )
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent
    shopping_dir = Path(args.shopping_dir) if args.shopping_dir else project_root / "shoppingplanning"
    travel_dir = Path(args.travel_dir) if args.travel_dir else project_root / "travelplanning"
    aggregated_dir = Path(args.aggregated_dir) if args.aggregated_dir else project_root / "aggregated_results"

    models = args.models or auto_detect_models(aggregated_dir)
    if not models:
        print("No models found. Specify --models or ensure aggregated_results/ has data.")
        sys.exit(1)

    print(f"Loading data for {len(models)} model(s): {', '.join(models)}")
    model_data_list = [load_model_data(m, shopping_dir, travel_dir, aggregated_dir) for m in models]

    html_content = render_page(model_data_list)

    output_path = Path(args.output)
    output_path.write_text(html_content, encoding="utf-8")
    size_kb = len(html_content) // 1024
    print(f"Generated: {output_path} ({size_kb} KB)")


if __name__ == "__main__":
    main()
