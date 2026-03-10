"""
Shared utilities for the data construction pipeline.
"""
import csv
import json
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple


def load_json(path: str) -> Any:
    """Load a JSON file."""
    with open(path, 'r', encoding='utf-8') as f:
        return json.load(f)


def save_json(data: Any, path: str):
    """Save data as JSON."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def count_csv_rows(path: str) -> int:
    """Count non-header rows in a CSV file."""
    if not os.path.exists(path):
        return 0
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(reader, None)  # skip header
        return sum(1 for _ in reader)


def read_csv_as_dicts(path: str) -> List[Dict[str, str]]:
    """Read a CSV file into a list of dicts."""
    if not os.path.exists(path):
        return []
    with open(path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv_from_dicts(rows: List[Dict[str, str]], path: str, fieldnames: Optional[List[str]] = None):
    """Write a list of dicts to a CSV file."""
    if not rows:
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    if fieldnames is None:
        fieldnames = list(rows[0].keys())
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def parse_datetime(dt_str: str) -> Optional[datetime]:
    """Parse a datetime string like '2025-11-12 07:03:00'."""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def format_datetime(dt: datetime) -> str:
    """Format datetime as '2025-11-12 07:03:00'."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def format_time(dt: datetime) -> str:
    """Format datetime as 'HH:MM'."""
    return dt.strftime("%H:%M")


def replace_date_in_datetime(dt_str: str, new_date: str) -> str:
    """
    Replace the date portion of a datetime string.
    '2025-11-12 07:03:00' + '2026-01-15' -> '2026-01-15 07:03:00'
    Handles midnight crossover for arrival times.
    """
    dt = parse_datetime(dt_str)
    if dt is None:
        return dt_str
    new_dt = datetime.strptime(new_date, "%Y-%m-%d")
    result = dt.replace(year=new_dt.year, month=new_dt.month, day=new_dt.day)
    return format_datetime(result)


def date_add_days(date_str: str, days: int) -> str:
    """Add days to a date string. '2025-11-12' + 1 -> '2025-11-13'."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return (dt + timedelta(days=days)).strftime("%Y-%m-%d")


def weekday_from_date(date_str: str) -> int:
    """Get weekday from date string (0=Mon, 6=Sun)."""
    return datetime.strptime(date_str, "%Y-%m-%d").weekday()


def weekday_name(weekday: int) -> str:
    """0=Monday, ... 6=Sunday."""
    names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return names[weekday]


def time_to_minutes(time_str: str) -> int:
    """Convert 'HH:MM' to minutes since midnight."""
    parts = time_str.strip().split(":")
    return int(parts[0]) * 60 + int(parts[1])


def minutes_to_time(minutes: int) -> str:
    """Convert minutes since midnight to 'HH:MM'."""
    h = minutes // 60
    m = minutes % 60
    return f"{h:02d}:{m:02d}"


def parse_tool_json_output(output: str) -> Any:
    """Parse JSON output from a tool call. Handles tool error messages."""
    try:
        return json.loads(output)
    except (json.JSONDecodeError, TypeError):
        return None


def coord_str(lat: float, lon: float) -> str:
    """Format coordinates as 'lat,lon' string for distance_matrix lookup."""
    return f"{lat},{lon}"


def parse_coord_str(s: str) -> Tuple[float, float]:
    """Parse 'lat,lon' string into (lat, lon) tuple."""
    parts = s.split(",")
    return float(parts[0]), float(parts[1])


def safe_float(val: Any, default: float = 0.0) -> float:
    """Safely convert to float."""
    try:
        v = float(val)
        if v != v:  # NaN check
            return default
        return v
    except (ValueError, TypeError):
        return default


def safe_int(val: Any, default: int = 0) -> int:
    """Safely convert to int."""
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def clamp(val: float, lo: float, hi: float) -> float:
    """Clamp val to [lo, hi]."""
    return max(lo, min(hi, val))
