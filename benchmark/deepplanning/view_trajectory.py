#!/usr/bin/env python3
"""
DeepPlanning Trajectory Viewer
Generates a self-contained HTML file showing every detail of a single agent trajectory.

Usage:
    python view_trajectory.py --index                          # Generate index page listing all trajectories
    python view_trajectory.py --case travel:en:42              # View travel trajectory (lang:id)
    python view_trajectory.py --case shopping:2:007            # View shopping trajectory (level:case_id)
    python view_trajectory.py --model gpt-5.2-2025-12-11      # Filter by model (used with --index or --case)
    python view_trajectory.py --output my_viewer.html          # Custom output path
"""

import argparse
import html as html_lib
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


BASE_DIR = Path(__file__).resolve().parent
SHOPPING_DIR = BASE_DIR / "shoppingplanning"
TRAVEL_DIR = BASE_DIR / "travelplanning"
OUTPUT_DIR = BASE_DIR / "trajectory_viewer"


def esc(s: str) -> str:
    return html_lib.escape(str(s)) if s else ""


def load_json(path: Path) -> Optional[dict]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def trajectory_filename(case_key: str, model: str) -> str:
    """Compute the HTML filename for a trajectory."""
    safe_name = case_key.replace(":", "_")
    return f"trajectory_{safe_name}_{model}.html"


def pretty_json(s: str) -> str:
    """Try to pretty-print a JSON string; return original on failure."""
    try:
        return json.dumps(json.loads(s), indent=2, ensure_ascii=False)
    except Exception:
        return s


# ---------------------------------------------------------------------------
# Index: scan all available trajectories
# ---------------------------------------------------------------------------

def scan_trajectories(model_filter: Optional[str] = None) -> List[Dict[str, str]]:
    """Scan for all trajectory files and return index entries."""
    entries = []

    # Travel trajectories
    results_dir = TRAVEL_DIR / "results"
    if results_dir.exists():
        for model_lang_dir in sorted(results_dir.iterdir()):
            if not model_lang_dir.is_dir():
                continue
            name = model_lang_dir.name
            # Parse model_lang pattern
            for lang in ("en", "zh"):
                if name.endswith(f"_{lang}"):
                    model = name[: -(len(lang) + 1)]
                    break
            else:
                continue
            if model_filter and model != model_filter:
                continue
            traj_dir = model_lang_dir / "trajectories"
            # Handle double-nested directories (e.g., model_en/model_en/trajectories/)
            if not traj_dir.exists():
                nested = list(model_lang_dir.glob("*/trajectories"))
                if nested:
                    traj_dir = nested[0]
                    model_lang_dir = traj_dir.parent
                else:
                    continue
            # Load evaluation scores if available
            eval_dir = model_lang_dir / "evaluation"
            eval_scores: Dict[str, dict] = {}
            if eval_dir.exists():
                for sf in eval_dir.glob("id_*_score.json"):
                    sd = load_json(sf)
                    if sd:
                        sid = str(sd.get("sample_id", sf.stem.replace("id_", "").replace("_score", "")))
                        eval_scores[sid] = sd.get("scores", {})

            for f in sorted(traj_dir.glob("id_*.json")):
                case_id = f.stem.replace("id_", "")
                size_kb = f.stat().st_size / 1024
                scores = eval_scores.get(case_id, {})
                entries.append({
                    "domain": "travel",
                    "model": model,
                    "subset": lang,
                    "case_id": case_id,
                    "path": str(f),
                    "size_kb": f"{size_kb:.1f}",
                    "case_key": f"travel:{lang}:{case_id}",
                    "commonsense": scores.get("commonsense_weighted_score"),
                    "personalized": scores.get("personalized_score"),
                    "composite": scores.get("composite_score"),
                    "case_acc": scores.get("case_acc"),
                })

    # Shopping messages
    db_infered_dir = SHOPPING_DIR / "database_infered"
    pattern = re.compile(r"^database_(.+?)_level([123])_(\d+)$")
    if db_infered_dir.exists():
        # Group by model+level, pick latest timestamp
        model_level_folders: Dict[Tuple[str, int], List[Tuple[int, Path]]] = {}
        for folder in sorted(db_infered_dir.iterdir()):
            if not folder.is_dir():
                continue
            m = pattern.match(folder.name)
            if not m:
                continue
            model_name, level_str, ts = m.group(1), int(m.group(2)), int(m.group(3))
            if model_filter and model_name != model_filter:
                continue
            key = (model_name, level_str)
            model_level_folders.setdefault(key, []).append((ts, folder))

        for (model_name, level), candidates in sorted(model_level_folders.items()):
            candidates.sort(key=lambda x: x[0], reverse=True)
            folder = candidates[0][1]  # latest
            for case_dir in sorted(folder.iterdir()):
                if not case_dir.is_dir() or not case_dir.name.startswith("case_"):
                    continue
                msg_file = case_dir / "messages.json"
                if not msg_file.exists():
                    continue
                case_id = case_dir.name.replace("case_", "")
                size_kb = msg_file.stat().st_size / 1024
                entries.append({
                    "domain": "shopping",
                    "model": model_name,
                    "subset": f"level {level}",
                    "case_id": case_id,
                    "path": str(msg_file),
                    "size_kb": f"{size_kb:.1f}",
                    "case_key": f"shopping:{level}:{case_id}",
                })

    return entries


# ---------------------------------------------------------------------------
# Load a single trajectory
# ---------------------------------------------------------------------------

def load_trajectory(case_key: str, model_filter: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Load a trajectory by case key (e.g., 'travel:en:42' or 'shopping:2:007')."""
    parts = case_key.split(":")
    if len(parts) != 3:
        print(f"Invalid case key: {case_key}. Expected format: domain:subset:case_id")
        return None

    domain, subset, case_id = parts

    if domain == "travel":
        lang = subset
        results_dir = TRAVEL_DIR / "results"
        if not results_dir.exists():
            return None
        # Find matching model dir
        candidates = []
        for d in results_dir.iterdir():
            if d.is_dir() and d.name.endswith(f"_{lang}"):
                m = d.name[: -(len(lang) + 1)]
                if model_filter and m != model_filter:
                    continue
                candidates.append((m, d))
        if not candidates:
            print(f"No travel results found for lang={lang}" +
                  (f", model={model_filter}" if model_filter else ""))
            return None
        if len(candidates) > 1 and not model_filter:
            models = [c[0] for c in candidates]
            print(f"Multiple models found: {models}. Use --model to specify.")
            return None
        model_name, model_dir = candidates[0]
        traj_file = model_dir / "trajectories" / f"id_{case_id}.json"
        # Handle double-nested directories
        if not traj_file.exists():
            nested = list(model_dir.glob("*/trajectories"))
            if nested:
                model_dir = nested[0].parent
                traj_file = model_dir / "trajectories" / f"id_{case_id}.json"
        if not traj_file.exists():
            print(f"Trajectory file not found: {traj_file}")
            return None
        data = load_json(traj_file)
        if data:
            data["_domain"] = "travel"
            data["_model"] = model_name
            data["_subset"] = lang
            # Load evaluation scores if available
            score_file = model_dir / "evaluation" / f"id_{case_id}_score.json"
            if score_file.exists():
                data["_eval"] = load_json(score_file)
        return data

    elif domain == "shopping":
        level = int(subset)
        db_infered_dir = SHOPPING_DIR / "database_infered"
        pattern = re.compile(r"^database_(.+?)_level([123])_(\d+)$")
        candidates = []
        if db_infered_dir.exists():
            for folder in db_infered_dir.iterdir():
                if not folder.is_dir():
                    continue
                m = pattern.match(folder.name)
                if not m:
                    continue
                mname, lv, ts = m.group(1), int(m.group(2)), int(m.group(3))
                if lv != level:
                    continue
                if model_filter and mname != model_filter:
                    continue
                candidates.append((mname, ts, folder))
        if not candidates:
            print(f"No shopping results found for level={level}" +
                  (f", model={model_filter}" if model_filter else ""))
            return None
        # Group by model, pick latest
        by_model: Dict[str, Tuple[int, Path]] = {}
        for mname, ts, folder in candidates:
            if mname not in by_model or ts > by_model[mname][0]:
                by_model[mname] = (ts, folder)
        if len(by_model) > 1 and not model_filter:
            print(f"Multiple models found: {list(by_model.keys())}. Use --model to specify.")
            return None
        model_name = list(by_model.keys())[0]
        folder = by_model[model_name][1]
        msg_file = folder / f"case_{case_id}" / "messages.json"
        if not msg_file.exists():
            print(f"Messages file not found: {msg_file}")
            return None
        raw = load_json(msg_file)
        if not raw:
            return None
        # Normalize shopping format to match travel
        messages = raw.get("messages", []) if isinstance(raw, dict) else raw
        if isinstance(raw, dict):
            data = {
                "id": case_id,
                "model": model_name,
                "messages": messages,
                "step": raw.get("step"),
                "_domain": "shopping",
                "_model": model_name,
                "_subset": f"level {level}",
            }
        else:
            # raw is a list of messages directly
            data = {
                "id": case_id,
                "model": model_name,
                "messages": raw,
                "_domain": "shopping",
                "_model": model_name,
                "_subset": f"level {level}",
            }
        return data

    print(f"Unknown domain: {domain}")
    return None


# ---------------------------------------------------------------------------
# HTML generation: CSS
# ---------------------------------------------------------------------------

def generate_css() -> str:
    return """
:root {
    --bg-primary: #faf9f7;
    --bg-secondary: #f0efe8;
    --bg-card: #ffffff;
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
.page { max-width: 1100px; margin: 0 auto; padding: 32px 24px; }
h1 { font-size: 28px; font-weight: 700; margin: 0 0 4px; letter-spacing: -0.5px; }
.subtitle { color: var(--text-secondary); font-size: 13px; margin-bottom: 24px; }

/* Header metadata */
.meta-bar {
    display: flex; flex-wrap: wrap; gap: 10px; margin: 16px 0 24px;
    padding: 16px 20px; background: var(--bg-card); border: 1px solid var(--border-color);
    border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.04);
}
.meta-item {
    display: flex; align-items: center; gap: 6px; padding: 4px 12px;
    background: var(--bg-secondary); border-radius: 6px; font-size: 13px;
}
.meta-item .label { color: var(--text-muted); font-size: 11px; text-transform: uppercase;
                    letter-spacing: 0.5px; font-weight: 600; }
.meta-item .value { font-weight: 700; }

/* Turn navigator */
.layout { display: flex; gap: 20px; }
.nav-sidebar {
    position: sticky; top: 20px; align-self: flex-start;
    width: 200px; flex-shrink: 0; max-height: calc(100vh - 40px);
    overflow-y: auto; padding: 12px; background: var(--bg-card);
    border: 1px solid var(--border-color); border-radius: 10px;
    box-shadow: 0 1px 3px rgba(0,0,0,0.04); font-size: 12px;
}
.nav-sidebar h4 { margin: 0 0 8px; font-size: 11px; text-transform: uppercase;
                   letter-spacing: 0.6px; color: var(--text-muted); }
.nav-item {
    display: block; padding: 4px 8px; margin: 1px 0; border-radius: 4px;
    text-decoration: none; color: var(--text-secondary); cursor: pointer;
    white-space: nowrap; overflow: hidden; text-overflow: ellipsis;
    border-left: 3px solid transparent; transition: all 0.15s;
}
.nav-item:hover { background: var(--bg-secondary); color: var(--text-primary); }
.nav-item.nav-system { border-left-color: #9b958f; }
.nav-item.nav-user { border-left-color: var(--accent-blue); }
.nav-item.nav-assistant { border-left-color: var(--accent-terra); }
.nav-item.nav-tool { border-left-color: var(--accent-green); }
.main-content { flex: 1; min-width: 0; }

/* Turn blocks */
.turn-separator {
    display: flex; align-items: center; gap: 12px; margin: 24px 0 8px;
    font-size: 12px; font-weight: 700; color: var(--text-muted);
    text-transform: uppercase; letter-spacing: 0.8px;
}
.turn-separator::after {
    content: ''; flex: 1; height: 1px; background: var(--border-color);
}
.msg-block {
    margin: 0 0 2px; padding: 12px 16px;
    border-left: 4px solid; background: var(--bg-card);
    border: 1px solid var(--border-light); border-radius: 8px;
    margin-bottom: 6px;
}
.msg-block.role-system { border-left: 4px solid #9b958f; }
.msg-block.role-user { border-left: 4px solid var(--accent-blue); }
.msg-block.role-assistant { border-left: 4px solid var(--accent-terra); }
.msg-block.role-tool { border-left: 4px solid var(--accent-green); }

.msg-header {
    display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
}
.role-badge {
    font-size: 11px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.5px; padding: 2px 8px; border-radius: 4px;
}
.role-badge.rb-system { background: #eae8e3; color: #6b6560; }
.role-badge.rb-user { background: #dbeafe; color: #1d4ed8; }
.role-badge.rb-assistant { background: #fde8e0; color: #c96442; }
.role-badge.rb-tool { background: #d1fae5; color: #047857; }
.msg-index { font-size: 11px; color: var(--text-muted); }
.char-badge {
    font-size: 10px; color: var(--text-muted); background: var(--bg-secondary);
    padding: 1px 6px; border-radius: 3px; margin-left: auto;
}

/* Content blocks */
.content-block {
    white-space: pre-wrap; word-break: break-word;
    font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace;
    font-size: 12.5px; line-height: 1.65; padding: 8px 0;
}
.reasoning-block {
    color: var(--text-muted); font-style: italic;
    border-left: 2px solid var(--text-muted); padding: 8px 12px; margin: 6px 0;
    font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px; line-height: 1.6;
    background: #fafaf8;
}
.tool-call-block {
    background: var(--bg-secondary); border-radius: 6px; padding: 10px 14px;
    margin: 6px 0; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px;
}
.tool-call-header {
    display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
}
.fn-name { color: var(--accent-terra); font-weight: 700; font-size: 13px; }
.call-id { color: var(--text-muted); font-size: 10px; }
.tool-args {
    white-space: pre-wrap; word-break: break-word; font-size: 12px;
    background: rgba(255,255,255,0.6); padding: 8px 10px; border-radius: 4px;
    max-height: 400px; overflow-y: auto;
}
.tool-result-block {
    background: #f0faf3; border-radius: 6px; padding: 10px 14px;
    margin: 6px 0; font-family: 'SF Mono', 'Fira Code', monospace; font-size: 12px;
}
.tool-result-header {
    display: flex; align-items: center; gap: 8px; margin-bottom: 6px;
    font-size: 11px; font-weight: 600; color: var(--accent-green);
    text-transform: uppercase; letter-spacing: 0.5px;
}

/* Collapsible long content */
.collapsible-content { position: relative; }
.collapsible-content.collapsed { max-height: 300px; overflow: hidden; }
.collapsible-content.collapsed::after {
    content: ''; position: absolute; bottom: 0; left: 0; right: 0; height: 60px;
    background: linear-gradient(transparent, var(--bg-card));
    pointer-events: none;
}
.expand-btn {
    display: block; width: 100%; padding: 6px; margin-top: 4px;
    background: var(--bg-secondary); border: 1px solid var(--border-light);
    border-radius: 4px; cursor: pointer; font-size: 11px; color: var(--accent-blue);
    text-align: center; font-weight: 600;
}
.expand-btn:hover { background: #e8e7e0; }

/* Copy button */
.copy-btn {
    font-size: 10px; padding: 2px 6px; cursor: pointer;
    background: var(--bg-card); border: 1px solid var(--border-light);
    border-radius: 3px; color: var(--text-muted); margin-left: 8px;
}
.copy-btn:hover { background: var(--bg-secondary); color: var(--text-primary); }

/* Index page */
.index-table { width: 100%; border-collapse: collapse; margin: 16px 0; }
.index-table th, .index-table td {
    padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border-light);
    font-size: 13px;
}
.index-table th {
    background: var(--bg-secondary); font-weight: 600; font-size: 11px;
    text-transform: uppercase; letter-spacing: 0.6px; color: var(--text-secondary);
    position: sticky; top: 0;
}
.index-table tbody tr:hover { background: #f0efe8; }
.index-table a { color: var(--accent-blue); text-decoration: none; font-weight: 600; }
.index-table a:hover { text-decoration: underline; }
.controls {
    display: flex; gap: 12px; align-items: center; margin: 16px 0; flex-wrap: wrap;
}
.controls select, .controls input {
    background: var(--bg-card); color: var(--text-primary);
    border: 1px solid var(--border-color); border-radius: 8px;
    padding: 8px 14px; font-size: 13px;
}
.controls select:focus, .controls input:focus {
    outline: none; border-color: var(--accent-terra);
    box-shadow: 0 0 0 3px rgba(201,100,66,0.12);
}
.count-badge {
    font-size: 12px; color: var(--text-muted); padding: 4px 10px;
    background: var(--bg-secondary); border-radius: 6px;
}

/* Final plan */
.final-plan {
    background: #fffdf0; border: 1px solid #e8e0c0; border-radius: 8px;
    padding: 16px; margin: 16px 0; white-space: pre-wrap; word-break: break-word;
    font-size: 13px; line-height: 1.7;
}
.final-plan-header {
    font-size: 12px; font-weight: 700; text-transform: uppercase;
    letter-spacing: 0.6px; color: var(--accent-yellow); margin-bottom: 8px;
}

@media (max-width: 900px) {
    .layout { flex-direction: column; }
    .nav-sidebar { position: static; width: 100%; max-height: 200px; }
}
"""


# ---------------------------------------------------------------------------
# HTML generation: JavaScript
# ---------------------------------------------------------------------------

def generate_js() -> str:
    return """
function toggleCollapse(btn) {
    const block = btn.previousElementSibling;
    if (block.classList.contains('collapsed')) {
        block.classList.remove('collapsed');
        btn.textContent = 'Collapse';
    } else {
        block.classList.add('collapsed');
        btn.textContent = 'Expand (' + block.dataset.fullLen + ' chars)';
    }
}

function copyText(btn) {
    const pre = btn.closest('.tool-call-block, .tool-result-block, .msg-block').querySelector('.copyable');
    if (pre) {
        navigator.clipboard.writeText(pre.textContent).then(() => {
            const orig = btn.textContent;
            btn.textContent = 'Copied!';
            setTimeout(() => btn.textContent = orig, 1200);
        });
    }
}

// Index page filtering
function filterIndex() {
    const domain = document.getElementById('filter-domain')?.value || '';
    const model = document.getElementById('filter-model')?.value || '';
    const search = (document.getElementById('filter-search')?.value || '').toLowerCase();
    const rows = document.querySelectorAll('.index-table tbody tr');
    let visible = 0;
    rows.forEach(r => {
        const d = r.dataset.domain || '';
        const m = r.dataset.model || '';
        const text = r.textContent.toLowerCase();
        const show = (!domain || d === domain) && (!model || m === model) && (!search || text.includes(search));
        r.style.display = show ? '' : 'none';
        if (show) visible++;
    });
    const badge = document.getElementById('count-badge');
    if (badge) badge.textContent = visible + ' trajectories';
}

// Smooth scroll for nav
function scrollToMsg(id) {
    const el = document.getElementById(id);
    if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
}
"""


# ---------------------------------------------------------------------------
# HTML generation: Render a single trajectory
# ---------------------------------------------------------------------------

COLLAPSE_THRESHOLD = 1000  # chars before collapsing

def render_trajectory_html(data: Dict[str, Any]) -> str:
    """Generate self-contained HTML for a single trajectory."""
    domain = data.get("_domain", "unknown")
    model = data.get("_model", data.get("model", "unknown"))
    subset = data.get("_subset", "")
    case_id = data.get("id", "unknown")
    messages = data.get("messages", [])
    elapsed = data.get("elapsed_time")
    token_usage = data.get("token_usage", {})
    final_plan = data.get("final_plan", "")
    query = data.get("query", "")

    parts = []
    parts.append("<!DOCTYPE html>")
    parts.append(f"<html lang='en'><head><meta charset='utf-8'>")
    parts.append(f"<title>Trajectory: {esc(model)} / {esc(domain)} / {esc(str(case_id))}</title>")
    parts.append(f"<style>{generate_css()}</style>")
    parts.append("</head><body>")
    parts.append("<div class='page'>")

    # Header
    parts.append(f"<h1>Trajectory Viewer</h1>")
    parts.append(f"<div class='subtitle'>{esc(domain.title())} Planning &mdash; Case {esc(str(case_id))}</div>")

    # Metadata bar
    parts.append("<div class='meta-bar'>")
    meta_items = [
        ("Model", model),
        ("Domain", domain.title()),
        ("Subset", subset),
        ("Case ID", str(case_id)),
        ("Messages", str(len(messages))),
    ]
    if elapsed is not None:
        meta_items.append(("Time", f"{elapsed:.1f}s"))
    if token_usage:
        meta_items.append(("Prompt Tokens", f"{token_usage.get('prompt_tokens', 0):,}"))
        meta_items.append(("Completion Tokens", f"{token_usage.get('completion_tokens', 0):,}"))
        meta_items.append(("Total Tokens", f"{token_usage.get('total_tokens', 0):,}"))
    for label, value in meta_items:
        parts.append(f"<div class='meta-item'><span class='label'>{esc(label)}</span>"
                     f"<span class='value'>{esc(value)}</span></div>")
    parts.append("</div>")

    # Query (if present)
    if query:
        parts.append("<details open><summary style='font-weight:600; font-size:13px; cursor:pointer;'>"
                     "User Query</summary>")
        parts.append(f"<div style='padding:12px 16px; white-space:pre-wrap; word-break:break-word; "
                     f"font-size:13px; line-height:1.7;'>{esc(query)}</div>")
        parts.append("</details>")

    # Final plan (if present)
    if final_plan and final_plan != "Reached max LLM calls without final answer.":
        parts.append("<details><summary style='font-weight:600; font-size:13px; cursor:pointer;'>"
                     "Final Plan</summary>")
        parts.append(f"<div class='final-plan'>{esc(final_plan)}</div>")
        parts.append("</details>")

    # Evaluation scores (if available)
    eval_data = data.get("_eval")
    if eval_data:
        scores = eval_data.get("scores", {})
        composite = scores.get("composite_score")
        case_acc = scores.get("case_acc")
        commonsense_w = scores.get("commonsense_weighted_score")
        personalized = scores.get("personalized_score")

        # Overall scores bar
        pass_label = ("<span style='color:var(--accent-green); font-weight:700;'>PASS</span>"
                      if case_acc and case_acc >= 1.0 else
                      "<span style='color:var(--accent-red); font-weight:700;'>FAIL</span>")
        parts.append("<details open><summary style='font-weight:600; font-size:13px; cursor:pointer;'>"
                     f"Evaluation Scores &mdash; {pass_label}</summary>")
        parts.append("<div style='padding:16px;'>")

        # Summary scores
        parts.append("<div style='display:flex; gap:16px; flex-wrap:wrap; margin-bottom:16px;'>")
        for label, val in [("Composite", composite), ("Commonsense", commonsense_w), ("Personalized", personalized)]:
            if val is not None:
                pct = f"{val * 100:.0f}%"
                color = "var(--accent-green)" if val >= 0.8 else "var(--accent-yellow)" if val >= 0.5 else "var(--accent-red)"
                parts.append(f"<div style='background:var(--bg-secondary); padding:10px 20px; border-radius:8px; text-align:center;'>"
                             f"<div style='font-size:11px; color:var(--text-muted); text-transform:uppercase; letter-spacing:0.5px;'>{label}</div>"
                             f"<div style='font-size:24px; font-weight:700; color:{color};'>{pct}</div></div>")
        parts.append("</div>")

        # Commonsense dimension breakdown
        cs_dims = eval_data.get("commonsense_dimension_details", {})
        if cs_dims:
            parts.append("<h4 style='margin:16px 0 8px; font-size:13px; color:var(--text-secondary);'>Commonsense Dimensions</h4>")
            parts.append("<table style='font-size:13px; margin:0;'><thead><tr>"
                         "<th>Dimension</th><th>Check</th><th>Result</th><th>Message</th>"
                         "</tr></thead><tbody>")
            for dim_name, dim_data in cs_dims.items():
                checks = dim_data.get("checks", [])
                dim_score = dim_data.get("score", 0)
                dim_color = "var(--accent-green)" if dim_score >= 1.0 else "var(--accent-yellow)" if dim_score >= 0.5 else "var(--accent-red)"
                for ci, check in enumerate(checks):
                    passed = check.get("passed", False)
                    icon = "<span style='color:var(--accent-green); font-weight:700;'>PASS</span>" if passed else "<span style='color:var(--accent-red); font-weight:700;'>FAIL</span>"
                    msg = check.get("message") or ""
                    if len(msg) > 150:
                        msg = msg[:147] + "..."
                    dim_label = f"<span style='font-weight:600; color:{dim_color};'>{esc(dim_name)}</span> <span style='font-size:11px; color:var(--text-muted);'>({dim_data.get('passed',0)}/{dim_data.get('total',0)})</span>" if ci == 0 else ""
                    parts.append(f"<tr><td>{dim_label}</td>"
                                 f"<td style='font-family:monospace; font-size:12px;'>{esc(check.get('name', ''))}</td>"
                                 f"<td style='text-align:center;'>{icon}</td>"
                                 f"<td style='font-size:11px; color:var(--text-muted); max-width:400px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;' title='{esc(msg)}'>{esc(msg)}</td></tr>")
            parts.append("</tbody></table>")

        # Personalized constraints breakdown
        pers_data = eval_data.get("personalized_dimension_score", {})
        pers_constraints = pers_data.get("constraints", {})
        if pers_constraints:
            parts.append("<h4 style='margin:16px 0 8px; font-size:13px; color:var(--text-secondary);'>Personalized Constraints</h4>")
            parts.append("<table style='font-size:13px; margin:0;'><thead><tr>"
                         "<th>Constraint</th><th>Result</th><th>Message</th>"
                         "</tr></thead><tbody>")
            for cname, cdata in pers_constraints.items():
                passed = cdata.get("passed", False)
                icon = "<span style='color:var(--accent-green); font-weight:700;'>PASS</span>" if passed else "<span style='color:var(--accent-red); font-weight:700;'>FAIL</span>"
                msg = cdata.get("message") or ""
                if len(msg) > 150:
                    msg = msg[:147] + "..."
                parts.append(f"<tr><td style='font-family:monospace; font-size:12px;'>{esc(cname)}</td>"
                             f"<td style='text-align:center;'>{icon}</td>"
                             f"<td style='font-size:11px; color:var(--text-muted); max-width:400px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap;' title='{esc(msg)}'>{esc(msg)}</td></tr>")
            parts.append("</tbody></table>")

        parts.append("</div></details>")

    # Layout: sidebar + messages
    parts.append("<div class='layout'>")

    # Nav sidebar
    parts.append("<div class='nav-sidebar'>")
    parts.append("<h4>Messages</h4>")
    turn_num = 0
    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        role = msg.get("role", "unknown")
        if role == "assistant":
            turn_num += 1
        # Build label
        if role == "tool":
            tool_name = msg.get("name", "")
            label = f"#{i} tool: {tool_name}" if tool_name else f"#{i} tool"
        elif role == "assistant":
            tc = msg.get("tool_calls", [])
            if tc:
                fn_names = [t.get("function", {}).get("name", "?") for t in tc]
                label = f"#{i} asst -> {', '.join(fn_names)}"
            else:
                label = f"#{i} assistant"
        else:
            label = f"#{i} {role}"
        parts.append(f"<a class='nav-item nav-{esc(role)}' onclick=\"scrollToMsg('msg-{i}')\" "
                     f"title='{esc(label)}'>{esc(label)}</a>")
    parts.append("</div>")

    # Main content
    parts.append("<div class='main-content'>")

    turn_num = 0
    for i, msg in enumerate(messages):
        if not isinstance(msg, dict):
            continue
        role = msg.get("role", "unknown")

        # Turn separator on assistant messages
        if role == "assistant":
            turn_num += 1
            parts.append(f"<div class='turn-separator' id='turn-{turn_num}'>Turn {turn_num}</div>")

        role_css = f"role-{role}" if role in ("system", "user", "assistant", "tool") else ""
        parts.append(f"<div class='msg-block {role_css}' id='msg-{i}'>")

        # Header row
        parts.append("<div class='msg-header'>")
        rb_cls = f"rb-{role}" if role in ("system", "user", "assistant", "tool") else ""
        role_label = role.upper()
        if role == "tool":
            tool_name = msg.get("name", "")
            tool_call_id = msg.get("tool_call_id", "")
            role_label = f"TOOL: {tool_name}" if tool_name else "TOOL"
        parts.append(f"<span class='role-badge {rb_cls}'>{esc(role_label)}</span>")
        parts.append(f"<span class='msg-index'>msg #{i}</span>")

        # Char count for content
        content = msg.get("content", "") or ""
        reasoning = msg.get("reasoning_content", "") or ""
        total_chars = len(content) + len(reasoning)
        for tc in msg.get("tool_calls", []):
            total_chars += len(tc.get("function", {}).get("arguments", ""))
        if total_chars > 0:
            parts.append(f"<span class='char-badge'>{total_chars:,} chars</span>")
        parts.append("</div>")  # msg-header

        # Tool call ID for tool messages
        if role == "tool" and msg.get("tool_call_id"):
            parts.append(f"<div style='font-size:10px; color:var(--text-muted); margin-bottom:4px;'>"
                         f"call_id: {esc(msg['tool_call_id'])}</div>")

        # Reasoning content
        if reasoning:
            is_long = len(reasoning) > COLLAPSE_THRESHOLD
            cls = "collapsible-content collapsed" if is_long else "collapsible-content"
            parts.append(f"<div style='margin-bottom:6px;'>"
                         f"<div style='font-size:10px; font-weight:600; color:var(--text-muted); "
                         f"text-transform:uppercase; letter-spacing:0.5px; margin-bottom:2px;'>"
                         f"Reasoning ({len(reasoning):,} chars)</div>")
            parts.append(f"<div class='{cls}' data-full-len='{len(reasoning)}'>"
                         f"<div class='reasoning-block copyable'>{esc(reasoning)}</div></div>")
            if is_long:
                parts.append(f"<button class='expand-btn' onclick='toggleCollapse(this)'>"
                             f"Expand ({len(reasoning):,} chars)</button>")
            parts.append("</div>")

        # Tool calls
        for tc in msg.get("tool_calls", []):
            func = tc.get("function", {})
            fn_name = func.get("name", "unknown")
            fn_args_raw = func.get("arguments", "")
            fn_args = pretty_json(fn_args_raw)
            tc_id = tc.get("id", "")

            parts.append("<div class='tool-call-block'>")
            parts.append("<div class='tool-call-header'>")
            parts.append(f"<span class='fn-name'>{esc(fn_name)}</span>")
            if tc_id:
                parts.append(f"<span class='call-id'>{esc(tc_id)}</span>")
            parts.append(f"<button class='copy-btn' onclick='copyText(this)'>Copy</button>")
            parts.append("</div>")

            is_long = len(fn_args) > COLLAPSE_THRESHOLD
            cls = "collapsible-content collapsed" if is_long else "collapsible-content"
            parts.append(f"<div class='{cls}' data-full-len='{len(fn_args)}'>"
                         f"<pre class='tool-args copyable'>{esc(fn_args)}</pre></div>")
            if is_long:
                parts.append(f"<button class='expand-btn' onclick='toggleCollapse(this)'>"
                             f"Expand ({len(fn_args):,} chars)</button>")
            parts.append("</div>")

        # Content
        if content:
            if role == "tool":
                # Render tool result with pretty JSON
                pretty_content = pretty_json(content)
                is_long = len(pretty_content) > COLLAPSE_THRESHOLD
                cls = "collapsible-content collapsed" if is_long else "collapsible-content"
                parts.append("<div class='tool-result-block'>")
                parts.append("<div class='tool-result-header'>Result"
                             f"<span style='font-size:10px; color:var(--text-muted); font-weight:400; "
                             f"text-transform:none; letter-spacing:0;'>"
                             f"({len(content):,} chars)</span>"
                             f"<button class='copy-btn' onclick='copyText(this)'>Copy</button></div>")
                parts.append(f"<div class='{cls}' data-full-len='{len(pretty_content)}'>"
                             f"<pre class='tool-args copyable'>{esc(pretty_content)}</pre></div>")
                if is_long:
                    parts.append(f"<button class='expand-btn' onclick='toggleCollapse(this)'>"
                                 f"Expand ({len(content):,} chars)</button>")
                parts.append("</div>")
            else:
                # Regular content
                is_long = len(content) > COLLAPSE_THRESHOLD and role == "system"
                cls = "collapsible-content collapsed" if is_long else "collapsible-content"
                parts.append(f"<div class='{cls}' data-full-len='{len(content)}'>"
                             f"<div class='content-block copyable'>{esc(content)}</div></div>")
                if is_long:
                    parts.append(f"<button class='expand-btn' onclick='toggleCollapse(this)'>"
                                 f"Expand ({len(content):,} chars)</button>")

        parts.append("</div>")  # msg-block

    parts.append("</div>")  # main-content
    parts.append("</div>")  # layout

    parts.append(f"<script>{generate_js()}</script>")
    parts.append("</div></body></html>")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# HTML generation: Index page
# ---------------------------------------------------------------------------

def render_index_html(entries: List[Dict[str, str]]) -> str:
    """Generate index page listing all available trajectories."""
    parts = []
    parts.append("<!DOCTYPE html>")
    parts.append("<html lang='en'><head><meta charset='utf-8'>")
    parts.append("<title>DeepPlanning Trajectory Index</title>")
    parts.append(f"<style>{generate_css()}</style>")
    parts.append("</head><body>")
    parts.append("<div class='page'>")
    parts.append("<h1>Trajectory Index</h1>")
    parts.append(f"<div class='subtitle'>Found {len(entries)} trajectories. "
                 f"Click a case to generate its detailed viewer.</div>")

    # Filters
    domains = sorted(set(e["domain"] for e in entries))
    models = sorted(set(e["model"] for e in entries))

    parts.append("<div class='controls'>")
    parts.append("<select id='filter-domain' onchange='filterIndex()'>"
                 "<option value=''>All Domains</option>")
    for d in domains:
        parts.append(f"<option value='{esc(d)}'>{esc(d.title())}</option>")
    parts.append("</select>")

    parts.append("<select id='filter-model' onchange='filterIndex()'>"
                 "<option value=''>All Models</option>")
    for m in models:
        parts.append(f"<option value='{esc(m)}'>{esc(m)}</option>")
    parts.append("</select>")

    parts.append("<input id='filter-search' type='text' placeholder='Search...' oninput='filterIndex()'>")
    parts.append(f"<span class='count-badge' id='count-badge'>{len(entries)} trajectories</span>")
    parts.append("</div>")

    # Table
    parts.append("<table class='index-table'><thead><tr>")
    for h in ["Domain", "Model", "Subset", "Case ID", "Commonsense", "Personalized", "Composite", "Pass", "Size (KB)"]:
        parts.append(f"<th>{h}</th>")
    parts.append("</tr></thead><tbody>")

    for e in entries:
        html_file = trajectory_filename(e['case_key'], e['model'])
        parts.append(f"<tr data-domain='{esc(e['domain'])}' data-model='{esc(e['model'])}'>")
        parts.append(f"<td>{esc(e['domain'].title())}</td>")
        parts.append(f"<td>{esc(e['model'])}</td>")
        parts.append(f"<td>{esc(e['subset'])}</td>")
        parts.append(f"<td><a href='{esc(html_file)}'><strong>{esc(e['case_id'])}</strong></a></td>")
        # Scores
        for key in ("commonsense", "personalized", "composite"):
            val = e.get(key)
            if val is not None:
                pct = f"{val * 100:.0f}%"
                color = "var(--accent-green)" if val >= 0.8 else "var(--accent-yellow)" if val >= 0.5 else "var(--accent-red)" if val > 0 else "var(--text-muted)"
                parts.append(f"<td style='text-align:right; font-weight:600; color:{color};'>{pct}</td>")
            else:
                parts.append("<td style='text-align:right; color:var(--text-muted);'>—</td>")
        # Pass/fail
        case_acc = e.get("case_acc")
        if case_acc is not None:
            if case_acc >= 1.0:
                parts.append("<td style='text-align:center;'><span style='color:var(--accent-green); font-weight:700;'>PASS</span></td>")
            else:
                parts.append("<td style='text-align:center;'><span style='color:var(--accent-red); font-weight:700;'>FAIL</span></td>")
        else:
            parts.append("<td style='text-align:center; color:var(--text-muted);'>—</td>")
        parts.append(f"<td style='text-align:right;'>{esc(e['size_kb'])}</td>")
        parts.append("</tr>")

    parts.append("</tbody></table>")
    parts.append(f"<script>{generate_js()}</script>")
    parts.append("</div></body></html>")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="DeepPlanning Trajectory Viewer")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--index", action="store_true",
                       help="Generate index page listing all trajectories")
    group.add_argument("--case", type=str,
                       help="View a specific case (e.g., travel:en:42, shopping:2:007)")
    parser.add_argument("--model", type=str, default=None,
                        help="Filter by model name")
    parser.add_argument("--output", type=str, default=None,
                        help="Custom output path for HTML file")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(exist_ok=True)

    if args.index:
        entries = scan_trajectories(model_filter=args.model)
        if not entries:
            print("No trajectories found. Run benchmarks first.")
            sys.exit(1)

        # Auto-generate all per-case HTML files
        generated = 0
        for e in entries:
            data = load_trajectory(e['case_key'], model_filter=e['model'])
            if not data:
                continue
            html_content = render_trajectory_html(data)
            html_file = trajectory_filename(e['case_key'], e['model'])
            case_path = OUTPUT_DIR / html_file
            with open(case_path, "w", encoding="utf-8") as f:
                f.write(html_content)
            generated += 1

        # Generate index page
        html_content = render_index_html(entries)
        out_path = Path(args.output) if args.output else OUTPUT_DIR / "index.html"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"Index generated: {out_path} ({len(entries)} trajectories, {generated} HTML files)")

    elif args.case:
        data = load_trajectory(args.case, model_filter=args.model)
        if not data:
            sys.exit(1)
        html_content = render_trajectory_html(data)
        if args.output:
            out_path = Path(args.output)
        else:
            safe_name = args.case.replace(":", "_")
            model_suffix = f"_{args.model}" if args.model else f"_{data.get('_model', '')}"
            out_path = OUTPUT_DIR / f"trajectory_{safe_name}{model_suffix}.html"
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        n_msgs = len(data.get("messages", []))
        print(f"Trajectory viewer generated: {out_path} ({n_msgs} messages)")


if __name__ == "__main__":
    main()
