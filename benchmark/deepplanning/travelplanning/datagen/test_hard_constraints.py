#!/usr/bin/env python3
"""
Test script: How Hard Constraints Actually Work
================================================

This script traces the FULL chain for one real task:
  1. What the user sees (natural language query)
  2. What the evaluator has (hard_constraints metadata)
  3. What the evaluator actually checks (shockingly simple name matching)
  4. WHY the name matching is sufficient (the database was rigged)

The key insight: hard constraint evaluation does NOT read the database.
It just checks whether the agent's plan contains the right entity NAMES.
The correctness of those names was guaranteed at task-construction time
by manipulating the database CSVs.

Run with:
    cd /data1/dannie/projects/Qwen-Agent/benchmark/deepplanning
    /data1/dannie/anaconda3/envs/deepplanning/bin/python -m travelplanning.datagen.test_hard_constraints
"""

import os
import sys
import json

PROJECT_DIR = "/data1/dannie/projects/Qwen-Agent/benchmark/deepplanning"
sys.path.insert(0, PROJECT_DIR)
os.chdir(PROJECT_DIR)


def main():
    from travelplanning.datagen.utils import load_json, read_csv_as_dicts, safe_float, safe_int
    from travelplanning.datagen.config import DATABASE_EN_DIR, QUERY_EN_PATH, DB_FILES
    from travelplanning.evaluation.constraints_hard import eval_hard

    # ========================================================================
    # STEP 1: Load one real task (task 0: Hefei → Nanjing, 2 days)
    # ========================================================================
    print("=" * 70)
    print("STEP 1: What the USER sees (natural language query)")
    print("=" * 70)

    queries = load_json(QUERY_EN_PATH)
    task = queries[0]
    meta = task["meta_info"]

    print(f"\nQuery:\n  \"{task['query'][:500]}...\"\n")

    # ========================================================================
    # STEP 2: What the EVALUATOR has (structured hard_constraints)
    # ========================================================================
    print("=" * 70)
    print("STEP 2: What the EVALUATOR has (hard_constraints in meta_info)")
    print("=" * 70)

    print(f"\nThe evaluator NEVER reads the query text.")
    print(f"It only looks at meta_info['hard_constraints'] — a structured dict.\n")

    hard_constraints = meta["hard_constraints"]
    for cname, cdata in hard_constraints.items():
        print(f"  Constraint: {cname}")
        for k, v in cdata.items():
            val_str = str(v)
            if len(val_str) > 80:
                val_str = val_str[:80] + "..."
            print(f"    {k}: {val_str}")
        print()

    # ========================================================================
    # STEP 3: What the evaluator ACTUALLY CHECKS
    # ========================================================================
    print("=" * 70)
    print("STEP 3: What the evaluator ACTUALLY CHECKS (just name matching!)")
    print("=" * 70)

    print("""
    The evaluator's logic per constraint type:

    train_seat_status:
        → Extract all trains from plan (type='travel_intercity_public', mode='train')
        → Check: is 'G7798' in the plan's train numbers? (outbound)
        → Check: is 'G3031' in the plan's train numbers? (inbound)
        → That's it. No database access.

    hotel_star_service_required:
        → Extract all hotels from plan (daily_plans[].accommodation.name)
        → Check: is 'Orange Hotel Nanjing Confucius Temple Scenic Area' in hotel names?
        → That's it. Doesn't verify it's actually 3-star or has a pool.

    restaurant_specific_tag_nearby:
        → Extract all restaurants from plan (type='meal', details.name)
        → Check: is 'Six Dynasties Pine Teahouse' in restaurant names?
        → That's it. Doesn't verify it's near Laomendong or has birthday packages.

    attraction_must_visit_named:
        → Extract all attractions from plan (type='attraction', details.name)
        → Check: is 'Nanjing Deji Plaza' in attraction names?
        → Check: is 'Nanjing City Wall Taicheng Scenic Area' in attraction names?
        → That's it.

    budget_constraint:
        → Sum all costs from the plan (transport*people, hotel*rooms, meals*people, etc.)
        → Check: is total ≤ 3000?
        → This one actually computes something.
    """)

    # ========================================================================
    # STEP 4: PROVE it — run the evaluator with a fake plan
    # ========================================================================
    print("=" * 70)
    print("STEP 4: PROVE IT — run evaluator with fake plans")
    print("=" * 70)

    # A plan that has the RIGHT entity names → passes
    correct_plan = {
        "daily_plans": [
            {
                "accommodation": {"name": "Orange Hotel Nanjing Confucius Temple Scenic Area"},
                "activities": [
                    {"type": "travel_intercity_public",
                     "details": {"mode": "train", "number": "G7798", "cost": 100}},
                    {"type": "attraction",
                     "details": {"name": "Nanjing Deji Plaza", "cost": 0}},
                    {"type": "attraction",
                     "details": {"name": "Nanjing City Wall Taicheng Scenic Area", "cost": 30}},
                    {"type": "meal",
                     "details": {"name": "Six Dynasties Pine Teahouse", "cost": 50}},
                ]
            },
            {
                "accommodation": None,
                "activities": [
                    {"type": "attraction",
                     "details": {"name": "Some Other Place", "cost": 20}},
                    {"type": "travel_intercity_public",
                     "details": {"mode": "train", "number": "G3031", "cost": 100}},
                ]
            }
        ],
        "budget_summary": {"total_estimated_budget": 2500}
    }

    # A plan with WRONG entity names → fails
    wrong_plan = {
        "daily_plans": [
            {
                "accommodation": {"name": "Hilton Nanjing"},  # WRONG hotel
                "activities": [
                    {"type": "travel_intercity_public",
                     "details": {"mode": "train", "number": "G7798", "cost": 100}},
                    {"type": "attraction",
                     "details": {"name": "Nanjing Deji Plaza", "cost": 0}},
                    # MISSING: Nanjing City Wall Taicheng Scenic Area
                    {"type": "meal",
                     "details": {"name": "Some Random Restaurant", "cost": 50}},  # WRONG restaurant
                ]
            },
            {
                "accommodation": None,
                "activities": [
                    {"type": "travel_intercity_public",
                     "details": {"mode": "train", "number": "G3031", "cost": 100}},
                ]
            }
        ],
        "budget_summary": {"total_estimated_budget": 2500}
    }

    print("\n--- Evaluating CORRECT plan (right entity names): ---")
    results_correct = eval_hard(correct_plan, meta)
    for cname, (passed, msg) in results_correct.items():
        status = "PASS" if passed else f"FAIL: {msg}"
        print(f"  {cname}: {status}")

    print("\n--- Evaluating WRONG plan (wrong entity names): ---")
    results_wrong = eval_hard(wrong_plan, meta)
    for cname, (passed, msg) in results_wrong.items():
        status = "PASS" if passed else f"FAIL: {msg}"
        print(f"  {cname}: {status}")

    # ========================================================================
    # STEP 5: WHY name matching is sufficient — the database is rigged
    # ========================================================================
    print("\n\n" + "=" * 70)
    print("STEP 5: WHY this works — the database is RIGGED")
    print("=" * 70)

    print("""
    The evaluator's name-checking seems naive. Why doesn't it verify that
    'Orange Hotel' actually IS the cheapest 3-star with a pool?

    Because the database was CONSTRUCTED to make it the ONLY valid answer.
    Let's verify this for task 0:
    """)

    db_path = os.path.join(DATABASE_EN_DIR, "id_0")

    # --- Verify hotel constraint ---
    print("  --- hotel_star_service_required ---")
    print("  Constraint: 3-star hotel with Swimming Pool")
    print(f"  Expected answer: Orange Hotel Nanjing Confucius Temple Scenic Area\n")

    hotels = read_csv_as_dicts(os.path.join(db_path, DB_FILES["hotels"]))
    print(f"  All 3-star hotels in database:")
    for h in hotels:
        if safe_int(h.get("hotel_star", 0)) == 3:
            services = h.get("services", "")
            has_pool = "Swimming Pool" in services
            marker = " ✓ HAS POOL (this is the only valid answer)" if has_pool else ""
            print(f"    {h['name']}")
            print(f"      services: {services}{marker}")
    print()

    # --- Verify train constraint ---
    print("  --- train_seat_status ---")
    print("  Constraint: 3 travelers need trains with ≥3 remaining seats")
    print(f"  Expected answers: G7798 (outbound), G3031 (inbound)\n")

    trains = read_csv_as_dicts(os.path.join(db_path, DB_FILES["trains"]))

    # Outbound trains (Hefei → Nanjing on 2025-11-12)
    outbound = [t for t in trains
                if t.get("origin_city") == "Hefei"
                and t.get("destination_city") == "Nanjing"
                and t.get("dep_date") == "2025-11-12"]
    print(f"  Outbound trains (Hefei→Nanjing, Nov 12):")
    for t in outbound[:8]:
        seats = t.get("seat_status", "?")
        try:
            seats_int = int(seats)
            enough = seats_int >= 3
        except ValueError:
            enough = True  # "Sufficient"
        marker = " ✓" if enough else ""
        marker2 = " ← CORRECT ANSWER" if t["train_no"] == "G7798" else ""
        print(f"    {t['train_no']:>8}  seats={seats:<12} enough_for_3={enough}{marker}{marker2}")
    if len(outbound) > 8:
        print(f"    ... ({len(outbound)} total)")

    # Inbound trains (Nanjing → Hefei on 2025-11-13)
    inbound = [t for t in trains
               if t.get("origin_city") == "Nanjing"
               and t.get("destination_city") == "Hefei"
               and t.get("dep_date") == "2025-11-13"]
    print(f"\n  Inbound trains (Nanjing→Hefei, Nov 13):")
    for t in inbound[:8]:
        seats = t.get("seat_status", "?")
        try:
            seats_int = int(seats)
            enough = seats_int >= 3
        except ValueError:
            enough = True
        marker = " ✓" if enough else ""
        marker2 = " ← CORRECT ANSWER" if t["train_no"] == "G3031" else ""
        print(f"    {t['train_no']:>8}  seats={seats:<12} enough_for_3={enough}{marker}{marker2}")
    if len(inbound) > 8:
        print(f"    ... ({len(inbound)} total)")

    # --- Verify restaurant constraint ---
    print(f"\n  --- restaurant_specific_tag_nearby ---")
    print(f"  Constraint: Restaurant near 'Laomendong' with 'Birthday Package' tag")
    print(f"  Expected answer: Six Dynasties Pine Teahouse\n")

    restaurants = read_csv_as_dicts(os.path.join(db_path, DB_FILES["restaurants"]))
    near_laomendong = [r for r in restaurants
                       if r.get("nearby_attraction_name", "").strip() == "Laomendong"]
    print(f"  Restaurants near 'Laomendong' ({len(near_laomendong)} total):")
    for r in near_laomendong:
        tags = r.get("tags", "")
        has_birthday = "Birthday Package" in tags
        marker = " ✓ HAS TAG (only valid answer)" if has_birthday else ""
        print(f"    {r['restaurant_name']}")
        print(f"      tags: {tags}{marker}")
    print()

    # ========================================================================
    # STEP 6: The mapping chain — complete picture
    # ========================================================================
    print("=" * 70)
    print("STEP 6: The Complete Chain")
    print("=" * 70)

    print("""
    TASK CONSTRUCTION TIME (by benchmark authors, or our pipeline):
    ┌─────────────────────────────────────────────────────────────────────┐
    │ 1. Pick solution entities:                                         │
    │    hotel = "Orange Hotel...", train = G7798, restaurant = "Six..." │
    │                                                                     │
    │ 2. Rig the database:                                               │
    │    - Remove "Swimming Pool" from all other 3-star hotels           │
    │    - Set seat_status < 3 on all other outbound trains              │
    │    - Remove "Birthday Package" tag from other Laomendong restaurants│
    │                                                                     │
    │ 3. Record the answers in hard_constraints:                         │
    │    {                                                                │
    │      "hotel_star_service_required": {"hotel_name": "Orange..."},   │
    │      "train_seat_status": {"outbound_train_no": "G7798", ...},    │
    │      "restaurant_specific_tag_nearby": {"restaurant_name": "Six.."│}
    │    }                                                                │
    │                                                                     │
    │ 4. Write natural language query:                                   │
    │    "I'd like a 3-star hotel with swimming pool..."                 │
    │    (mentions the PROPERTY, not the NAME)                           │
    └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    AGENT EXECUTION TIME:
    ┌─────────────────────────────────────────────────────────────────────┐
    │ Agent reads query → uses tools to search database → discovers      │
    │ that "Orange Hotel" is the only 3-star with pool → picks it        │
    │                                                                     │
    │ Agent's plan: {"accommodation": {"name": "Orange Hotel..."}}       │
    └─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    EVALUATION TIME:
    ┌─────────────────────────────────────────────────────────────────────┐
    │ eval_hard() reads plan and hard_constraints                        │
    │                                                                     │
    │ Check: "Orange Hotel..." in plan's hotel names?  → YES → PASS     │
    │                                                                     │
    │ (Does NOT re-verify against database. Trusts the rigging.)         │
    └─────────────────────────────────────────────────────────────────────┘

    So the "mapping" from query to evaluation is:

    Query text          →  (implicit, for the agent to reason about)
    hard_constraints    →  (explicit, stored alongside the query)
    Database rigging    →  (guarantees only one answer satisfies the constraint)
    eval_hard()         →  (just checks if the plan has the right names)

    The constraint key (e.g., "hotel_star_service_required") is NOT parsed
    from the query. It's metadata attached to the task at creation time.
    The evaluator dispatches on the key prefix:
      "hotel_*"       → check hotel_name in plan
      "train_*"       → check train_no in plan
      "flight_*"      → check flight_no in plan
      "restaurant_*"  → check restaurant_name in plan
      "attraction_*"  → check attraction_names in plan
      "budget_*"      → compute total cost from plan
    """)


if __name__ == "__main__":
    main()
