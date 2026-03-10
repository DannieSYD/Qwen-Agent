"""
Stage 1: Database Fork & Perturb

Copies an existing database and applies perturbations (dates, prices,
ratings, seat status, tags) to create a new unique task database.
"""
import os
import random
import shutil
from typing import Dict, List, Optional

from .config import DB_FILES, DatabaseIndex, DbEntry, TaskConfig
from .utils import (
    read_csv_as_dicts, write_csv_from_dicts, safe_float, safe_int,
    replace_date_in_datetime, date_add_days, clamp,
)


def fork_database(
    source_entry: DbEntry,
    task_config: TaskConfig,
    output_db_dir: str,
    rng: Optional[random.Random] = None,
) -> str:
    """
    Fork a source database and apply perturbations.

    Args:
        source_entry: The source database entry to fork from.
        task_config: Configuration for the new task.
        output_db_dir: Directory to write the new database to.

    Returns:
        Path to the new database directory.
    """
    if rng is None:
        rng = random.Random()

    # Copy the entire database directory
    if os.path.exists(output_db_dir):
        shutil.rmtree(output_db_dir)
    shutil.copytree(source_entry.db_path, output_db_dir)

    # Apply perturbations
    _redate_transport(output_db_dir, task_config, rng)
    _perturb_hotels(output_db_dir, task_config, rng)
    _perturb_attractions(output_db_dir, task_config, rng)
    _perturb_restaurants(output_db_dir, task_config, rng)
    _perturb_transport_seats(output_db_dir, task_config, rng)

    return output_db_dir


def select_source_db(
    index: DatabaseIndex,
    task_config: TaskConfig,
    rng: Optional[random.Random] = None,
) -> DbEntry:
    """
    Select a source database for the given task config.

    Picks from databases matching the desired route.
    Prefers databases with entity counts appropriate for the difficulty level.
    """
    if rng is None:
        rng = random.Random()

    route = (task_config.origin, task_config.dest)
    candidates = index.by_route.get(route, [])

    if not candidates:
        raise ValueError(f"No source database found for route {route}")

    # For now, pick randomly. Could be smarter about difficulty matching.
    return rng.choice(candidates)


# ============================================================================
# Re-dating transport
# ============================================================================

def _redate_transport(db_dir: str, config: TaskConfig, rng: random.Random):
    """Re-date all transport CSVs to match the new task dates."""
    # Determine original dates from source (read first data row)
    for csv_name in ["trains", "flights"]:
        csv_path = os.path.join(db_dir, DB_FILES[csv_name])
        if not os.path.exists(csv_path):
            continue

        rows = read_csv_as_dicts(csv_path)
        if not rows:
            continue

        # Find original outbound and inbound dates
        original_dates = set()
        for row in rows:
            dep_date = row.get("dep_date", "")
            if dep_date:
                original_dates.add(dep_date)

        if not original_dates:
            continue

        original_dates = sorted(original_dates)
        # Assume first date is outbound, last is inbound
        orig_outbound = original_dates[0]
        orig_inbound = original_dates[-1] if len(original_dates) > 1 else original_dates[0]

        # Map original dates to new dates
        date_mapping = {
            orig_outbound: config.depart_date,
            orig_inbound: config.return_date,
        }
        # For multi-day trips with intermediate dates
        if len(original_dates) > 2:
            for i, od in enumerate(original_dates):
                if od not in date_mapping:
                    new_date = date_add_days(config.depart_date, i)
                    date_mapping[od] = new_date

        # Apply date mapping
        for row in rows:
            old_dep_date = row.get("dep_date", "")
            new_dep_date = date_mapping.get(old_dep_date, old_dep_date)
            row["dep_date"] = new_dep_date

            # Update dep_datetime and arr_datetime
            if row.get("dep_datetime"):
                row["dep_datetime"] = replace_date_in_datetime(
                    row["dep_datetime"], new_dep_date
                )
            if row.get("arr_datetime"):
                # Arrival might be on a different day for overnight trips
                # For simplicity, use same date as departure
                # (the time-of-day is preserved, which is what matters)
                row["arr_datetime"] = replace_date_in_datetime(
                    row["arr_datetime"], new_dep_date
                )

        fieldnames = list(rows[0].keys())
        write_csv_from_dicts(rows, csv_path, fieldnames)


# ============================================================================
# Hotel perturbation
# ============================================================================

def _perturb_hotels(db_dir: str, config: TaskConfig, rng: random.Random):
    """Perturb hotel prices, scores, and services."""
    csv_path = os.path.join(db_dir, DB_FILES["hotels"])
    if not os.path.exists(csv_path):
        return

    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return

    difficulty = config.difficulty
    # Perturbation magnitude: easy=large, hard=small
    price_pct = {1: 0.20, 2: 0.12, 3: 0.05}[difficulty]
    score_range = {1: 0.3, 2: 0.2, 3: 0.1}[difficulty]

    for row in rows:
        # Perturb price
        price = safe_float(row.get("price", 0))
        if price > 0:
            factor = 1.0 + rng.uniform(-price_pct, price_pct)
            new_price = round(price * factor)
            row["price"] = str(max(50, new_price))

        # Perturb score slightly
        score = safe_float(row.get("score", 0))
        if score > 0:
            delta = rng.uniform(-score_range, score_range)
            new_score = round(clamp(score + delta, 3.0, 5.0), 1)
            row["score"] = str(new_score)

        # Randomly toggle a service (10% chance per hotel)
        services_str = row.get("services", "")
        if services_str and rng.random() < 0.10:
            services = [s.strip() for s in services_str.split(";") if s.strip()]
            if services and rng.random() < 0.5:
                # Remove a random service
                services.pop(rng.randrange(len(services)))
            else:
                # Add a random service not already present
                from .config import KNOWN_HOTEL_SERVICES
                available = [s for s in KNOWN_HOTEL_SERVICES if s not in services]
                if available:
                    services.append(rng.choice(available))
            row["services"] = ";".join(services)

    # Ensure no two hotels of the same star level have identical prices
    _ensure_unique_prices_per_star(rows, rng)

    fieldnames = list(rows[0].keys())
    write_csv_from_dicts(rows, csv_path, fieldnames)


def _ensure_unique_prices_per_star(rows: List[Dict], rng: random.Random):
    """Ensure no two hotels of the same star have the exact same price."""
    from collections import defaultdict
    star_prices: Dict[str, List[int]] = defaultdict(list)

    for i, row in enumerate(rows):
        star = row.get("hotel_star", "")
        price = safe_int(row.get("price", 0))
        key = (star, price)
        star_prices[star].append((price, i))

    for star, price_indices in star_prices.items():
        seen_prices = set()
        for price, idx in price_indices:
            while price in seen_prices:
                price += rng.choice([-1, 1]) * rng.randint(1, 10)
                price = max(50, price)
            seen_prices.add(price)
            rows[idx]["price"] = str(price)


# ============================================================================
# Attraction perturbation
# ============================================================================

def _perturb_attractions(db_dir: str, config: TaskConfig, rng: random.Random):
    """Perturb attraction ratings and ticket prices."""
    csv_path = os.path.join(db_dir, DB_FILES["attractions"])
    if not os.path.exists(csv_path):
        return

    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return

    for row in rows:
        # Perturb rating slightly
        rating = safe_float(row.get("rating", 0))
        if rating > 0:
            delta = rng.uniform(-0.1, 0.1)
            new_rating = round(clamp(rating + delta, 3.0, 5.0), 1)
            row["rating"] = str(new_rating)

        # Perturb ticket price slightly
        ticket = safe_float(row.get("ticket_price", 0))
        if ticket > 0:
            factor = 1.0 + rng.uniform(-0.10, 0.10)
            new_ticket = round(ticket * factor, 1)
            row["ticket_price"] = str(max(0, new_ticket))

    # Ensure unique ratings per attraction type for top-N constraints
    _ensure_unique_ratings_per_type(rows, rng)

    fieldnames = list(rows[0].keys())
    write_csv_from_dicts(rows, csv_path, fieldnames)


def _ensure_unique_ratings_per_type(rows: List[Dict], rng: random.Random):
    """Ensure no two attractions of the same type have the exact same rating."""
    from collections import defaultdict
    type_ratings: Dict[str, set] = defaultdict(set)

    for row in rows:
        atype = row.get("attraction_type", "")
        rating = safe_float(row.get("rating", 0))

        while rating in type_ratings[atype]:
            rating = round(rating + rng.choice([-0.1, 0.1]), 1)
            rating = clamp(rating, 3.0, 5.0)

        type_ratings[atype].add(rating)
        row["rating"] = str(rating)


# ============================================================================
# Restaurant perturbation
# ============================================================================

def _perturb_restaurants(db_dir: str, config: TaskConfig, rng: random.Random):
    """Perturb restaurant prices, ratings, and tags."""
    csv_path = os.path.join(db_dir, DB_FILES["restaurants"])
    if not os.path.exists(csv_path):
        return

    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return

    for row in rows:
        # Perturb price_per_person
        price = safe_float(row.get("price_per_person", 0))
        if price > 0:
            factor = 1.0 + rng.uniform(-0.15, 0.15)
            new_price = round(price * factor, 1)
            row["price_per_person"] = str(max(10, new_price))

        # Perturb rating slightly
        rating = safe_float(row.get("rating", 0))
        if rating > 0:
            delta = rng.uniform(-0.2, 0.2)
            new_rating = round(clamp(rating + delta, 3.0, 5.0), 1)
            row["rating"] = str(new_rating)

        # Randomly toggle a tag (5% chance per restaurant)
        tags_str = row.get("tags", "")
        if tags_str and rng.random() < 0.05:
            tags = [t.strip() for t in tags_str.split(";") if t.strip()]
            if tags and rng.random() < 0.5 and len(tags) > 1:
                tags.pop(rng.randrange(len(tags)))
            else:
                from .config import KNOWN_RESTAURANT_TAGS
                available = [t for t in KNOWN_RESTAURANT_TAGS if t not in tags]
                if available:
                    tags.append(rng.choice(available))
            row["tags"] = ";".join(tags)

    fieldnames = list(rows[0].keys())
    write_csv_from_dicts(rows, csv_path, fieldnames)


# ============================================================================
# Transport seat status perturbation
# ============================================================================

def _perturb_transport_seats(db_dir: str, config: TaskConfig, rng: random.Random):
    """Redistribute seat_status values across transport options."""
    for csv_name in ["trains", "flights"]:
        csv_path = os.path.join(db_dir, DB_FILES[csv_name])
        if not os.path.exists(csv_path):
            continue

        rows = read_csv_as_dicts(csv_path)
        if not rows:
            continue

        for row in rows:
            seat_status = row.get("seat_status", "")
            # Only perturb numeric seat status values
            try:
                status_val = int(seat_status)
                # Redistribute: some low (1-3), some medium (4-6), some high (7-10)
                new_status = rng.choices(
                    population=[rng.randint(1, 3), rng.randint(4, 6), rng.randint(7, 10)],
                    weights=[0.3, 0.3, 0.4],
                    k=1,
                )[0]
                row["seat_status"] = str(new_status)
            except (ValueError, TypeError):
                pass  # Keep non-numeric values (e.g., "Sufficient") as-is

        fieldnames = list(rows[0].keys())
        write_csv_from_dicts(rows, csv_path, fieldnames)
