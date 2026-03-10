#!/usr/bin/env python3
"""
Test script for Stage 0: Database Indexing
==========================================

Stage 0 is the "reconnaissance" stage. Before generating anything, the pipeline
needs to understand what raw material it has to work with.

INPUT:  120 existing task databases + their query metadata JSON
OUTPUT: A DatabaseIndex object — a structured catalog of every database

The 120 databases come from the released DeepPlanning benchmark. Each one is a
"sandbox" for a single travel planning task: a set of 7 CSVs containing flights,
trains, hotels, attractions, restaurants, location coordinates, and a distance
matrix. Each sandbox is specific to a route (e.g., Hefei → Nanjing).

Stage 0 reads all of them, counts their entities, extracts metadata (hotel brands,
star levels, attraction types), and organizes everything by route. This lets later
stages quickly answer questions like:
  - "Which databases serve the Shanghai → Xiamen route?"
  - "Does this database have enough attractions for a 5-day trip?"
  - "Does this route have flights, or only trains?"

Run with:
    cd /data1/dannie/projects/Qwen-Agent/benchmark/deepplanning
    /data1/dannie/anaconda3/envs/deepplanning/bin/python -m travelplanning.datagen.test_stage0
"""

import os
import sys
import json

# ============================================================================
# Setup
# ============================================================================

PROJECT_DIR = "/data1/dannie/projects/Qwen-Agent/benchmark/deepplanning"
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)


def main():
    # ========================================================================
    # PART 1: What does the raw data look like?
    # ========================================================================
    # Before running Stage 0, let's look at what it consumes.

    print("=" * 70)
    print("PART 1: Understanding the Raw Inputs")
    print("=" * 70)

    # --- 1a: The query JSON ---
    # This file has 120 entries — one per task in the released benchmark.
    # Each entry has a natural language query + structured metadata.

    from travelplanning.datagen.config import DATABASE_EN_DIR, QUERY_EN_PATH, DB_FILES
    from travelplanning.datagen.utils import load_json, read_csv_as_dicts, count_csv_rows

    queries = load_json(QUERY_EN_PATH)
    print(f"\nQuery JSON: {QUERY_EN_PATH}")
    print(f"  Total entries: {len(queries)}")
    print(f"\n  First entry's metadata:")
    meta = queries[0]["meta_info"]
    for key in ["org", "dest", "days", "people_number", "room_number", "depart_date", "return_date"]:
        print(f"    {key}: {meta[key]}")
    print(f"    hard_constraints: {list(meta['hard_constraints'].keys())}")
    print(f"\n  First entry's query (first 200 chars):")
    print(f"    \"{queries[0]['query'][:200]}...\"")

    # --- 1b: The database directories ---
    # Each id_X directory is a self-contained sandbox with 7 CSV files.

    print(f"\nDatabase directory: {DATABASE_EN_DIR}")
    db_dirs = sorted([d for d in os.listdir(DATABASE_EN_DIR) if d.startswith("id_")])
    print(f"  Total database directories: {len(db_dirs)}")
    print(f"  First 5: {db_dirs[:5]}")
    print(f"  Last 5:  {db_dirs[-5:]}")

    # --- 1c: Inside one database ---
    # Let's look at id_0 (Hefei → Nanjing, 2-day trip)

    sample_db = os.path.join(DATABASE_EN_DIR, "id_0")
    print(f"\n  Inside {sample_db}:")
    print(f"  {'CSV File':<45} {'Rows':>6}  {'What it contains'}")
    print(f"  {'-'*45} {'-'*6}  {'-'*30}")

    descriptions = {
        "flights":         "Flights on the route (may not exist)",
        "trains":          "Trains on the route",
        "hotels":          "Hotels at the destination city",
        "attractions":     "Tourist attractions to visit",
        "restaurants":     "Restaurants near attractions",
        "locations":       "Lat/lon for every named place",
        "distance_matrix": "Taxi time+cost between all POI pairs",
    }
    for csv_name, csv_rel_path in DB_FILES.items():
        csv_path = os.path.join(sample_db, csv_rel_path)
        rows = count_csv_rows(csv_path)
        desc = descriptions.get(csv_name, "")
        print(f"  {csv_rel_path:<45} {rows:>6}  {desc}")

    # --- 1d: Sample rows from key CSVs ---
    print(f"\n  Sample hotel row (id_0):")
    hotels = read_csv_as_dicts(os.path.join(sample_db, DB_FILES["hotels"]))
    h = hotels[0]
    for key in ["city", "name", "hotel_star", "price", "score", "brand", "services"]:
        print(f"    {key}: {h.get(key, '')}")

    print(f"\n  Sample attraction row (id_0):")
    attrs = read_csv_as_dicts(os.path.join(sample_db, DB_FILES["attractions"]))
    a = attrs[0]
    for key in ["attraction_name", "attraction_type", "rating", "opening_time",
                "closing_time", "closing_dates", "min_visit_hours", "max_visit_hours",
                "ticket_price", "latitude", "longitude"]:
        print(f"    {key}: {a.get(key, '')}")

    print(f"\n  Sample train row (id_0):")
    trains = read_csv_as_dicts(os.path.join(sample_db, DB_FILES["trains"]))
    t = trains[0]
    for key in ["origin_city", "destination_city", "dep_date", "train_no", "train_type",
                "dep_datetime", "arr_datetime", "duration", "seat_class", "seat_status", "price"]:
        print(f"    {key}: {t.get(key, '')}")

    # ========================================================================
    # PART 2: Running Stage 0
    # ========================================================================

    print("\n\n" + "=" * 70)
    print("PART 2: Running Stage 0 — build_db_index()")
    print("=" * 70)

    # This is the actual Stage 0 call. It scans all 120 databases and builds
    # a DatabaseIndex object.
    from travelplanning.datagen.stage0_db_index import build_db_index, print_index_summary

    index = build_db_index()

    # ========================================================================
    # PART 3: Exploring the Index
    # ========================================================================

    print("\n\n" + "=" * 70)
    print("PART 3: What's Inside the Index?")
    print("=" * 70)

    # --- 3a: Top-level statistics ---
    print(f"\n  Total databases indexed: {len(index.entries)}")
    print(f"  Unique routes:           {len(index.all_routes)}")
    print(f"  Origin cities ({len(index.origin_cities)}):      {sorted(index.origin_cities)}")
    print(f"  Destination cities ({len(index.dest_cities)}): {sorted(index.dest_cities)}")

    # --- 3b: Route distribution ---
    # Some routes have multiple databases (from different original tasks),
    # others have just one. This affects how many unique tasks we can generate
    # per route.
    print(f"\n  Route distribution (databases per route):")
    route_counts = sorted(
        [(route, len(entries)) for route, entries in index.by_route.items()],
        key=lambda x: -x[1]
    )
    for (orig, dest), count in route_counts[:10]:
        print(f"    {orig:>12} → {dest:<12}: {count} database(s)")
    print(f"    ... ({len(route_counts)} routes total)")

    # --- 3c: Deep dive into one DbEntry ---
    # Each entry is a DbEntry dataclass. Let's inspect one.
    entry = index.entries[0]
    print(f"\n  Deep dive — DbEntry for id_{entry.task_id}:")
    print(f"    Route:          {entry.origin} → {entry.dest}")
    print(f"    Original trip:  {entry.days} days, {entry.people_number} people, {entry.room_number} room(s)")
    print(f"    Database path:  {entry.db_path}")
    print(f"    Entity counts:")
    print(f"      Hotels:       {entry.num_hotels}")
    print(f"      Attractions:  {entry.num_attractions}")
    print(f"      Restaurants:  {entry.num_restaurants}")
    print(f"      Trains:       {entry.num_trains}")
    print(f"      Flights:      {entry.num_flights}")
    print(f"    Has flights:    {entry.has_flights}")
    print(f"    Has trains:     {entry.has_trains}")
    print(f"    Hotel brands:   {sorted(entry.hotel_brands)}")
    print(f"    Hotel stars:    {sorted(entry.hotel_stars)}")
    print(f"    Attraction types: {sorted(entry.attraction_types)}")
    print(f"    Max feasible days: {entry.max_feasible_days}")
    print(f"      (= ({entry.num_attractions} + 2) // 2, because each day needs ~2 unique attractions)")

    # --- 3d: Databases WITHOUT flights ---
    # This is practically important: some routes only have trains. Stage 2
    # must handle this gracefully.
    no_flights = [e for e in index.entries if not e.has_flights]
    no_trains = [e for e in index.entries if not e.has_trains]
    both = [e for e in index.entries if e.has_flights and e.has_trains]
    print(f"\n  Transport availability:")
    print(f"    Both trains + flights: {len(both)} databases")
    print(f"    Trains only:           {len(no_flights)} databases")
    print(f"    Flights only:          {len(no_trains)} databases")

    # --- 3e: Attraction count distribution ---
    # This determines max trip length. Most DBs have 9-10 attractions,
    # meaning max ~5-6 day trips.
    from collections import Counter
    attr_dist = Counter(e.num_attractions for e in index.entries)
    print(f"\n  Attraction count distribution:")
    for count, num_dbs in sorted(attr_dist.items()):
        max_days = (count + 2) // 2
        bar = "#" * num_dbs
        print(f"    {count:>2} attractions → max {max_days}-day trip : {bar} ({num_dbs} DBs)")

    # --- 3f: How later stages use the index ---
    # Example: "I want to generate a 4-day trip. Which routes support that?"
    print(f"\n  Example query: 'Which routes support a 4-day trip?'")
    print(f"  (Need ≥ 2*4 - 2 = 6 unique attractions)")
    feasible_routes = set()
    for route, entries in index.by_route.items():
        if any(e.max_feasible_days >= 4 for e in entries):
            feasible_routes.add(route)
    print(f"    Answer: {len(feasible_routes)} / {len(index.all_routes)} routes")

    print(f"\n  Example query: 'Which routes support a 7-day trip?'")
    print(f"  (Need ≥ 2*7 - 2 = 12 unique attractions)")
    feasible_7 = set()
    for route, entries in index.by_route.items():
        if any(e.max_feasible_days >= 7 for e in entries):
            feasible_7.add(route)
    print(f"    Answer: {len(feasible_7)} / {len(index.all_routes)} routes")
    if feasible_7:
        print(f"    Routes: {sorted(feasible_7)[:5]}...")

    # ========================================================================
    # PART 4: The full summary view
    # ========================================================================

    print("\n")
    print_index_summary(index)


if __name__ == "__main__":
    main()
