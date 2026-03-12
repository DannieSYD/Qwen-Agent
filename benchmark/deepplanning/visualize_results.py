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
            eval_dir = results_dir / f"{model}_{lang}" / "evaluation"
            if eval_dir.exists():
                summary = load_json(eval_dir / "evaluation_summary.json")
                if summary:
                    md.travel_summaries[lang] = summary
                case_scores = load_travel_case_scores(eval_dir)
                if case_scores:
                    md.travel_case_scores[lang] = case_scores

    return md


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
        return "#66bb6a"
    if score >= 0.3:
        return "#ffa726"
    return "#ef5350"


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
    --bg-primary: #0d1117;
    --bg-secondary: #161b22;
    --bg-card: #1c2333;
    --bg-table-row: #161b22;
    --bg-table-row-alt: #1c2333;
    --text-primary: #e6edf3;
    --text-secondary: #8b949e;
    --text-muted: #6e7681;
    --accent-blue: #58a6ff;
    --accent-green: #3fb950;
    --accent-yellow: #d29922;
    --accent-red: #f85149;
    --accent-purple: #bc8cff;
    --border-color: #30363d;
    --border-light: #21262d;
}
*, *::before, *::after { box-sizing: border-box; }
body {
    margin: 0; padding: 0;
    background: var(--bg-primary);
    color: var(--text-primary);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
    font-size: 14px; line-height: 1.5;
}
.dp-page { max-width: 1400px; margin: 0 auto; padding: 24px; }
h1 { font-size: 28px; font-weight: 700; margin: 0 0 4px; }
h2 { font-size: 22px; font-weight: 600; margin: 32px 0 16px; border-bottom: 1px solid var(--border-color); padding-bottom: 8px; }
h3 { font-size: 17px; font-weight: 600; margin: 24px 0 12px; }
h4 { font-size: 15px; font-weight: 600; margin: 16px 0 8px; color: var(--text-secondary); }
.subtitle { color: var(--text-secondary); font-size: 14px; margin-bottom: 24px; }
/* Summary cards */
.summary-cards { display: flex; flex-wrap: wrap; gap: 16px; margin: 20px 0; }
.summary-card {
    background: var(--bg-card); border: 1px solid var(--border-color);
    border-radius: 8px; padding: 20px; min-width: 260px; flex: 1;
    position: relative; overflow: hidden;
}
.summary-card .model-name { font-size: 15px; font-weight: 600; margin-bottom: 12px; color: var(--accent-blue); }
.summary-card .metric-row { display: flex; justify-content: space-between; align-items: baseline; margin: 6px 0; }
.summary-card .metric-label { font-size: 12px; color: var(--text-secondary); }
.summary-card .metric-value { font-size: 16px; font-weight: 700; }
.summary-card .headline-value { font-size: 28px; font-weight: 700; margin: 8px 0; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 12px; font-size: 11px; font-weight: 600; }
.badge-valid { background: rgba(63,185,80,0.15); color: var(--accent-green); }
.badge-invalid { background: rgba(248,81,73,0.15); color: var(--accent-red); }
.badge-na { background: rgba(110,118,129,0.15); color: var(--text-muted); }
/* Tables */
table { width: 100%; border-collapse: collapse; margin: 8px 0; }
th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border-light); }
th { background: var(--bg-secondary); font-weight: 600; font-size: 12px;
     text-transform: uppercase; letter-spacing: 0.5px; color: var(--text-secondary);
     position: sticky; top: 0; }
tbody tr:nth-child(odd) { background: var(--bg-table-row); }
tbody tr:nth-child(even) { background: var(--bg-table-row-alt); }
tbody tr:hover { background: #1f2a3d; }
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
.bar-row { display: flex; align-items: center; gap: 10px; margin: 5px 0; }
.bar-label { width: 200px; font-size: 13px; flex-shrink: 0; }
.bar-track { flex: 1; height: 22px; background: var(--bg-primary); border-radius: 4px; overflow: hidden; position: relative; }
.bar-fill { height: 100%; border-radius: 4px; min-width: 2px; transition: width 0.4s ease; }
.bar-value { width: 80px; text-align: right; font-size: 13px; font-weight: 600; flex-shrink: 0; }
.bar-extra { width: 70px; text-align: right; font-size: 12px; color: var(--text-muted); flex-shrink: 0; }
/* Collapsible details */
details { border: 1px solid var(--border-color); border-radius: 6px; margin: 8px 0; }
details > summary {
    cursor: pointer; padding: 10px 16px; background: var(--bg-secondary);
    border-radius: 6px; font-weight: 600; font-size: 14px;
    list-style: none; display: flex; align-items: center; gap: 8px;
}
details > summary::-webkit-details-marker { display: none; }
details > summary::before { content: '\\25b6'; font-size: 10px; transition: transform 0.2s; }
details[open] > summary::before { transform: rotate(90deg); }
details[open] > summary { border-bottom: 1px solid var(--border-color); border-radius: 6px 6px 0 0; }
.details-body { padding: 16px; }
/* Model section */
.model-section { margin: 24px 0; border: 1px solid var(--border-color); border-radius: 8px; overflow: hidden; }
.model-section-header {
    background: var(--bg-card); padding: 16px 20px;
    border-bottom: 1px solid var(--border-color);
    display: flex; align-items: center; justify-content: space-between;
}
.model-section-header h3 { margin: 0; font-size: 18px; }
.model-section-body { padding: 20px; }
/* Check pass/fail */
.check-pass { color: var(--accent-green); }
.check-fail { color: var(--accent-red); }
.check-icon { font-weight: 700; margin-right: 6px; }
.check-msg { font-size: 12px; color: var(--text-muted); margin-left: 24px; word-break: break-word; }
/* Error stats */
.error-type { font-family: monospace; font-size: 12px; }
.error-msg { font-size: 12px; color: var(--text-muted); max-width: 500px; overflow: hidden;
             text-overflow: ellipsis; white-space: nowrap; }
/* Filter / search */
.controls { display: flex; gap: 12px; align-items: center; margin: 12px 0; flex-wrap: wrap; }
.controls select, .controls input {
    background: var(--bg-secondary); color: var(--text-primary);
    border: 1px solid var(--border-color); border-radius: 6px;
    padding: 6px 12px; font-size: 13px;
}
.controls input { width: 240px; }
/* Product lists */
.product-matched { color: var(--accent-green); }
.product-unmatched { color: var(--accent-red); }
.product-extra { color: var(--accent-yellow); }
.product-list { margin: 4px 0 4px 16px; font-size: 13px; }
.product-list li { margin: 2px 0; }
.query-text { background: var(--bg-primary); border-radius: 4px; padding: 10px 14px;
              font-size: 13px; margin: 8px 0; border-left: 3px solid var(--accent-blue);
              color: var(--text-secondary); white-space: pre-wrap; word-break: break-word; }
/* Constraint table */
.constraint-row { display: flex; align-items: flex-start; gap: 8px; margin: 3px 0; padding: 4px 0; }
.constraint-name { font-size: 13px; min-width: 200px; }
/* Responsive */
@media (max-width: 900px) {
    .summary-cards { flex-direction: column; }
    .bar-label { width: 140px; }
    .controls { flex-direction: column; align-items: flex-start; }
}
/* Print */
@media print {
    body { background: #fff; color: #000; }
    .dp-page { max-width: 100%; }
    details { border-color: #ccc; }
    details > summary { background: #f5f5f5; }
    th { background: #f0f0f0; color: #333; }
    tbody tr:nth-child(odd), tbody tr:nth-child(even) { background: #fff; }
    .no-print { display: none; }
    .score-low { color: #d32f2f; }
    .score-mid { color: #f57c00; }
    .score-high { color: #388e3c; }
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
        avg_acc = overall.get("avg_acc")
        shop_ws = overall.get("shopping_weighted_average_case_score")
        travel_ca = overall.get("travel_case_acc")
        valid = overall.get("valid", True)
        domains = overall.get("domains_completed", [])

        parts.append(f'<div class="summary-card">')
        parts.append(f'<div class="model-name">{esc(md.name)}</div>')

        if avg_acc is not None:
            cls = score_class(avg_acc)
            parts.append(f'<div class="headline-value {cls}">{fmt_pct(avg_acc)}</div>')
            parts.append(f'<div class="metric-label">Cross-Domain avg_acc</div>')
        else:
            parts.append(f'<div class="headline-value" style="color:var(--text-muted)">--</div>')
            parts.append(f'<div class="metric-label">avg_acc (need both domains)</div>')

        parts.append(_card_metric("Shopping weighted_case_score", shop_ws))
        parts.append(_card_metric("Travel case_acc", travel_ca))
        parts.append(f'<div style="margin-top:8px;">')
        badge_cls = "badge-valid" if valid else "badge-invalid"
        parts.append(f'<span class="badge {badge_cls}">{"Valid" if valid else "Invalid"}</span> ')
        for d in domains:
            parts.append(f'<span class="badge badge-na">{esc(d)}</span> ')
        parts.append('</div>')
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
        ("avg_acc", True),
        ("Shop match_rate", True),
        ("Shop weighted_score", True),
        ("Travel composite", True),
        ("Travel case_acc", True),
        ("Travel commonsense", True),
        ("Travel personalized", True),
    ]

    # Extract values per model
    rows_data = []
    for md in models:
        overall = (md.aggregated or {}).get("overall", {})
        shop = (md.aggregated or {}).get("domains", {}).get("shopping", {})
        travel = (md.aggregated or {}).get("domains", {}).get("travel", {})
        row = {
            "name": md.name,
            "avg_acc": overall.get("avg_acc"),
            "shop_mr": shop.get("match_rate"),
            "shop_ws": shop.get("weighted_average_case_score"),
            "travel_comp": travel.get("composite_score"),
            "travel_ca": travel.get("case_acc"),
            "travel_cs": travel.get("commonsense_score"),
            "travel_ps": travel.get("personalized_score"),
        }
        rows_data.append(row)

    # Find best per column
    metric_keys = ["avg_acc", "shop_mr", "shop_ws", "travel_comp", "travel_ca", "travel_cs", "travel_ps"]
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
        for ki, k in enumerate(metric_keys):
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
        mid = model_id(md.name)
        parts.append(f'<details data-lazy>')
        parts.append(f'<summary>Level {level} &mdash; {len(cases)} cases (click to expand)</summary>')
        parts.append(f'<div class="details-body"><template>{_render_shopping_cases(cases)}</template></div>')
        parts.append(f'</details>')

    return "\n".join(parts)


def _render_shopping_cases(cases: List[dict]) -> str:
    parts = ['<input type="text" class="case-search" placeholder="Search cases..." style="margin-bottom:10px;">']
    parts.append('<table><thead><tr>')
    for h in ["Case", "Score", "Matched", "Expected", "Extra", "Status"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for c in cases:
        s = c.get("summary", {})
        score = s.get("score", 0)
        matched = s.get("matched_count", 0)
        expected = s.get("expected_count", 0)
        extra = s.get("extra_products_count", 0)
        case_name = c.get("case_name", "?")
        is_perfect = matched == expected and extra == 0
        cls = "score-high" if is_perfect else ("score-mid" if score > 0 else "score-low")
        status = "Perfect" if is_perfect else ("Partial" if score > 0 else "Failed")
        parts.append(
            f'<tr class="case-item">'
            f'<td>{esc(case_name)}</td>'
            f'<td class="num {cls}">{fmt_pct(score)}</td>'
            f'<td class="num">{matched}</td>'
            f'<td class="num">{expected}</td>'
            f'<td class="num">{extra}</td>'
            f'<td><span class="badge {"badge-valid" if is_perfect else "badge-invalid"}">{status}</span></td>'
            f'</tr>'
        )

    parts.append("</tbody></table>")

    # Detailed per-case expandable
    for c in cases:
        case_name = c.get("case_name", "?")
        parts.append(f'<details class="case-item">')
        parts.append(f'<summary>{esc(case_name)} detail</summary>')
        parts.append(f'<div class="details-body">{_render_shopping_case_detail(c)}</div>')
        parts.append(f'</details>')

    return "\n".join(parts)


def _render_shopping_case_detail(c: dict) -> str:
    parts = []
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
        # Try to find names from ground truth
        gt = {p.get("product_id"): p.get("name", "?") for p in c.get("ground_truth_products", [])}
        for pid in matched_ids:
            name = gt.get(pid, pid)
            parts.append(f'<li class="product-matched">{esc(name)} (ID: {esc(str(pid))})</li>')
        parts.append('</ul>')

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

        # Per-case drill-down
        case_scores = md.travel_case_scores.get(lang, [])
        if case_scores:
            mid = model_id(md.name)
            parts.append(f'<details data-lazy>')
            parts.append(f'<summary>{len(case_scores)} cases &mdash; per-case detail (click to expand)</summary>')
            parts.append(f'<div class="details-body"><template>{_render_travel_cases(case_scores)}</template></div>')
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


def _render_travel_cases(case_scores: List[dict]) -> str:
    parts = ['<input type="text" class="case-search" placeholder="Search cases..." style="margin-bottom:10px;">']
    parts.append('<table><thead><tr>')
    for h in ["ID", "Composite", "Case Acc", "Commonsense", "Personalized"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for cs in case_scores:
        sid = cs.get("sample_id", "?")
        scores = cs.get("scores", {})
        comp = scores.get("composite_score")
        ca = scores.get("case_acc")
        cws = scores.get("commonsense_weighted_score")
        ps = scores.get("personalized_score")
        parts.append(
            f'<tr class="case-item">'
            f'<td>id_{esc(str(sid))}</td>'
            f'<td class="num {score_class(comp)}">{fmt_pct(comp)}</td>'
            f'<td class="num {score_class(ca)}">{fmt_pct(ca)}</td>'
            f'<td class="num {score_class(cws)}">{fmt_pct(cws)}</td>'
            f'<td class="num {score_class(ps)}">{fmt_pct(ps)}</td>'
            f'</tr>'
        )

    parts.append("</tbody></table>")

    # Per-case expandable detail
    for cs in case_scores:
        sid = cs.get("sample_id", "?")
        parts.append(f'<details class="case-item">')
        parts.append(f'<summary>id_{esc(str(sid))} &mdash; detail</summary>')
        parts.append(f'<div class="details-body">{_render_travel_case_detail(cs)}</div>')
        parts.append(f'</details>')

    return "\n".join(parts)


def _render_travel_case_detail(cs: dict) -> str:
    parts = []
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

    # Quick badges
    agg = md.aggregated or {}
    overall = agg.get("overall", {})
    avg_acc = overall.get("avg_acc")
    if avg_acc is not None:
        cls = score_class(avg_acc)
        parts.append(f'<span class="{cls}" style="font-size:18px;font-weight:700;">avg_acc: {fmt_pct(avg_acc)}</span>')

    parts.append('</div>')  # header
    parts.append('<div class="model-section-body domain-detail">')

    # Shopping
    parts.append(render_shopping_section(md))
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
