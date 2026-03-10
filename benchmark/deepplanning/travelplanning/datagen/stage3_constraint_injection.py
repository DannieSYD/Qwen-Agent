"""
Stage 3: Constraint Selection & Injection

Selects 4-5 hard constraints from the reference solution and modifies
the database to ensure each constraint uniquely identifies the chosen entity.
"""
import os
import random
from typing import Any, Dict, List, Optional, Tuple

from .config import (
    TRANSPORT_CONSTRAINTS, HOTEL_CONSTRAINTS, RESTAURANT_CONSTRAINTS,
    ATTRACTION_CONSTRAINTS, DB_FILES, TaskConfig,
)
from .utils import read_csv_as_dicts, write_csv_from_dicts, safe_float, safe_int


# ============================================================================
# Extraction helpers — pull entities from the solution
# ============================================================================

def _extract_solution_entities(solution: Dict) -> Dict[str, Any]:
    """Extract key entities from the reference solution."""
    entities = {
        "outbound_transport": None,  # {mode, number, cost, ...}
        "return_transport": None,
        "hotel": None,  # {name, price, ...}
        "restaurants": [],  # [{name, cost, meal_type, nearby_attraction, ...}]
        "attractions": [],  # [{name, city, cost, ...}]
    }

    for day in solution.get("daily_plans", []):
        # Hotel
        accom = day.get("accommodation")
        if accom and isinstance(accom, dict) and accom.get("name"):
            entities["hotel"] = accom

        for act in day.get("activities", []):
            atype = act.get("type", "")
            details = act.get("details", {})

            if atype == "travel_intercity_public":
                info = {
                    "mode": details.get("mode", ""),
                    "number": details.get("number", ""),
                    "cost": safe_float(details.get("cost", 0)),
                    "from": details.get("from", ""),
                    "to": details.get("to", ""),
                }
                # Determine outbound vs return by day position
                if entities["outbound_transport"] is None:
                    entities["outbound_transport"] = info
                else:
                    entities["return_transport"] = info

            elif atype == "meal":
                entities["restaurants"].append({
                    "name": details.get("name", ""),
                    "cost": safe_float(details.get("cost", 0)),
                    "meal_type": details.get("meal_type", ""),
                })

            elif atype == "attraction":
                entities["attractions"].append({
                    "name": details.get("name", ""),
                    "city": details.get("city", ""),
                    "cost": safe_float(details.get("cost", 0)),
                })

    return entities


# ============================================================================
# Constraint selection
# ============================================================================

def select_constraints(
    solution: Dict,
    db_dir: str,
    config: TaskConfig,
    rng: random.Random,
    budget_prob: float = 0.15,
) -> Dict[str, Dict]:
    """
    Select 4-5 hard constraints compatible with the solution.

    Returns:
        hard_constraints dict matching the existing task format.
    """
    entities = _extract_solution_entities(solution)
    constraints = {}

    # 1. Transport constraint
    transport_c = _select_transport_constraint(entities, db_dir, config, rng)
    if transport_c:
        constraints.update(transport_c)

    # 2. Hotel constraint
    hotel_c = _select_hotel_constraint(entities, db_dir, config, rng)
    if hotel_c:
        constraints.update(hotel_c)

    # 3. Restaurant constraint
    restaurant_c = _select_restaurant_constraint(entities, db_dir, config, rng)
    if restaurant_c:
        constraints.update(restaurant_c)

    # 4. Attraction constraint
    attraction_c = _select_attraction_constraint(entities, db_dir, config, rng)
    if attraction_c:
        constraints.update(attraction_c)

    # 5. Optional budget constraint
    if rng.random() < budget_prob:
        budget_c = _select_budget_constraint(solution, config)
        if budget_c:
            constraints.update(budget_c)

    return constraints


# ============================================================================
# Transport constraint selection + injection
# ============================================================================

def _select_transport_constraint(
    entities: Dict, db_dir: str, config: TaskConfig, rng: random.Random,
) -> Optional[Dict[str, Dict]]:
    """Select and inject a transport constraint."""
    outbound = entities.get("outbound_transport")
    ret = entities.get("return_transport")

    if not outbound:
        return None

    mode = outbound["mode"]
    ret_mode = ret["mode"] if ret else mode
    csv_name = "trains" if mode == "train" else "flights"
    csv_path = os.path.join(db_dir, DB_FILES[csv_name])

    if not os.path.exists(csv_path):
        return None

    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return None

    # Choose constraint type based on available data
    if mode == "train":
        constraint_type = rng.choice([
            "train_seat_status", "train_cheapest_direct",
            "train_earliest_departure_direct", "train_latest_arrival_direct",
        ])
    else:
        constraint_type = rng.choice([
            "flight_seat_status", "flight_cheapest_direct",
            "flight_earliest_departure_direct",
        ])

    no_key = "train_no" if mode == "train" else "flight_no"
    # Only include return transport if it uses the same mode
    ret_same_mode = ret if (ret and ret_mode == mode) else None

    if constraint_type.endswith("seat_status"):
        # Ensure chosen transport has enough seats, others don't
        for row in rows:
            if row.get(no_key) == outbound["number"]:
                row["seat_status"] = str(max(config.people_number, safe_int(row.get("seat_status", 5))))
            elif ret_same_mode and row.get(no_key) == ret_same_mode["number"]:
                row["seat_status"] = str(max(config.people_number, safe_int(row.get("seat_status", 5))))
            else:
                # Set to less than people_number for most others
                if rng.random() < 0.7:
                    row["seat_status"] = str(rng.randint(1, max(1, config.people_number - 1)))

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))

        constraint_data = {
            "constraint_context": f"{config.people_number} travelers, need to select transport with sufficient remaining tickets",
            "people_number": config.people_number,
        }
        if mode == "train":
            constraint_data["outbound_train_no"] = outbound["number"]
            if ret_same_mode:
                constraint_data["inbound_train_no"] = ret_same_mode["number"]
            # Store seat status for verification
            for row in rows:
                if row.get(no_key) == outbound["number"]:
                    constraint_data["outbound_seat_status"] = row["seat_status"]
                if ret_same_mode and row.get(no_key) == ret_same_mode["number"]:
                    constraint_data["inbound_seat_status"] = row["seat_status"]
        else:
            constraint_data["outbound_flight_no"] = outbound["number"]
            if ret_same_mode:
                constraint_data["inbound_flight_no"] = ret_same_mode["number"]

        return {constraint_type: constraint_data}

    elif constraint_type.endswith("cheapest_direct"):
        # Make chosen transport the cheapest direct option
        chosen_price = outbound["cost"]
        outbound_rows = [r for r in rows if r.get("origin_city") == config.origin
                         and r.get("destination_city") == config.dest
                         and r.get("segment_index", "1") == "1"]

        for row in outbound_rows:
            if row.get(no_key) != outbound["number"]:
                current_price = safe_float(row.get("price", 0))
                if current_price <= chosen_price:
                    row["price"] = str(round(chosen_price + rng.uniform(10, 50), 1))

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))

        constraint_data = {
            "constraint_context": f"Please select the cheapest direct {mode} for the outbound trip",
        }
        if mode == "train":
            constraint_data["outbound_train_no"] = outbound["number"]
        else:
            constraint_data["outbound_flight_no"] = outbound["number"]

        return {constraint_type: constraint_data}

    elif constraint_type.endswith("earliest_departure_direct"):
        # Make chosen transport the earliest departure
        # (Already selected for early departure in Stage 2, just ensure it's unique)
        constraint_data = {
            "constraint_context": f"Please select the earliest departing direct {mode}",
        }
        if mode == "train":
            constraint_data["outbound_train_no"] = outbound["number"]
        else:
            constraint_data["outbound_flight_no"] = outbound["number"]

        return {constraint_type: constraint_data}

    elif constraint_type.endswith("latest_arrival_direct") and ret_same_mode:
        constraint_data = {
            "constraint_context": f"Please select the latest arriving direct {mode} for the return trip",
        }
        if mode == "train":
            constraint_data["inbound_train_no"] = ret_same_mode["number"]
        else:
            constraint_data["inbound_flight_no"] = ret_same_mode["number"]

        return {constraint_type: constraint_data}

    # Fallback if the chosen constraint couldn't be applied (e.g., latest_arrival
    # with mismatched modes), try earliest_departure instead
    if constraint_type.endswith("latest_arrival_direct"):
        constraint_data = {
            "constraint_context": f"Please select the earliest departing direct {mode}",
        }
        if mode == "train":
            constraint_data["outbound_train_no"] = outbound["number"]
        else:
            constraint_data["outbound_flight_no"] = outbound["number"]
        fallback_type = f"{mode}_earliest_departure_direct"
        return {fallback_type: constraint_data}

    return None


# ============================================================================
# Hotel constraint selection + injection
# ============================================================================

def _select_hotel_constraint(
    entities: Dict, db_dir: str, config: TaskConfig, rng: random.Random,
) -> Optional[Dict[str, Dict]]:
    """Select and inject a hotel constraint."""
    hotel = entities.get("hotel")
    if not hotel or not hotel.get("name"):
        return None

    hotel_name = hotel["name"]
    hotel_price = safe_float(hotel.get("price_per_night", hotel.get("price", 0)))
    csv_path = os.path.join(db_dir, DB_FILES["hotels"])

    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return None

    # Find the chosen hotel's row
    chosen_row = None
    for row in rows:
        if row.get("name") == hotel_name:
            chosen_row = row
            break

    if chosen_row is None:
        return None

    chosen_star = safe_int(chosen_row.get("hotel_star", 0))
    chosen_brand = chosen_row.get("brand", "").strip()
    chosen_score = safe_float(chosen_row.get("score", 0))
    services = [s.strip() for s in chosen_row.get("services", "").split(";") if s.strip()]

    constraint_type = rng.choice([
        "hotel_star_service_required",
        "hotel_highest_rated",
        "hotel_cheapest_brand",
        "hotel_cheapest_star",
    ])

    # Fallback if no brand
    if constraint_type == "hotel_cheapest_brand" and not chosen_brand:
        constraint_type = "hotel_highest_rated"

    # Fallback if no services
    if constraint_type == "hotel_star_service_required" and not services:
        constraint_type = "hotel_highest_rated"

    if constraint_type == "hotel_star_service_required":
        required_service = rng.choice(services)
        # Remove this service from other hotels of the same star
        for row in rows:
            if row.get("name") != hotel_name and safe_int(row.get("hotel_star", 0)) == chosen_star:
                row_services = [s.strip() for s in row.get("services", "").split(";") if s.strip()]
                if required_service in row_services:
                    row_services.remove(required_service)
                    row["services"] = ";".join(row_services)

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))
        return {"hotel_star_service_required": {
            "constraint_context": f"Please select a {chosen_star}-star hotel that offers {required_service}",
            "constraint_type": "superlative_star_service_required",
            "hotel_name": hotel_name,
            "required_service": required_service,
            "hotel_star": chosen_star,
            "hotel_price": hotel_price,
        }}

    elif constraint_type == "hotel_highest_rated":
        # Make chosen hotel the highest rated
        for row in rows:
            if row.get("name") != hotel_name:
                if safe_float(row.get("score", 0)) >= chosen_score:
                    row["score"] = str(round(chosen_score - rng.uniform(0.1, 0.3), 1))

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))
        return {"hotel_highest_rated": {
            "constraint_context": "Please select the highest rated hotel",
            "constraint_type": "superlative_highest_rated",
            "hotel_name": hotel_name,
            "hotel_price": hotel_price,
            "hotel_score": chosen_score,
        }}

    elif constraint_type == "hotel_cheapest_brand":
        # Make chosen hotel cheapest in its brand
        for row in rows:
            if row.get("name") != hotel_name and row.get("brand", "").strip() == chosen_brand:
                if safe_float(row.get("price", 0)) <= hotel_price:
                    row["price"] = str(round(hotel_price + rng.uniform(20, 80)))

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))
        return {"hotel_cheapest_brand": {
            "constraint_context": f"Please select the cheapest hotel among the \"{chosen_brand}\" brand",
            "constraint_type": "superlative_cheapest_brand",
            "hotel_name": hotel_name,
            "hotel_price": hotel_price,
        }}

    elif constraint_type == "hotel_cheapest_star":
        # Make chosen hotel cheapest at its star level
        for row in rows:
            if row.get("name") != hotel_name and safe_int(row.get("hotel_star", 0)) == chosen_star:
                if safe_float(row.get("price", 0)) <= hotel_price:
                    row["price"] = str(round(hotel_price + rng.uniform(20, 80)))

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))
        return {"hotel_cheapest_star": {
            "constraint_context": f"Please select the cheapest {chosen_star}-star hotel",
            "constraint_type": "superlative_cheapest_star",
            "hotel_name": hotel_name,
            "hotel_price": hotel_price,
            "hotel_star": chosen_star,
        }}

    return None


# ============================================================================
# Restaurant constraint selection + injection
# ============================================================================

def _select_restaurant_constraint(
    entities: Dict, db_dir: str, config: TaskConfig, rng: random.Random,
) -> Optional[Dict[str, Dict]]:
    """Select and inject a restaurant constraint."""
    restaurants = entities.get("restaurants", [])
    if not restaurants:
        return None

    # Pick a restaurant from the solution
    chosen = rng.choice(restaurants)
    restaurant_name = chosen["name"]
    restaurant_cost = chosen["cost"]

    csv_path = os.path.join(db_dir, DB_FILES["restaurants"])
    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return None

    # Find the chosen restaurant's row and its nearby attraction
    chosen_row = None
    for row in rows:
        if row.get("restaurant_name") == restaurant_name:
            chosen_row = row
            break

    if chosen_row is None:
        return None

    nearby_attraction = chosen_row.get("nearby_attraction_name", "")
    tags = [t.strip() for t in chosen_row.get("tags", "").split(";") if t.strip()]
    rating = safe_float(chosen_row.get("rating", 0))

    constraint_type = rng.choice([
        "restaurant_specific_tag_nearby",
        "restaurant_highest_rated",
        "restaurant_must_eat_named",
    ])

    # Fallback if no tags
    if constraint_type == "restaurant_specific_tag_nearby" and not tags:
        constraint_type = "restaurant_highest_rated"

    if constraint_type == "restaurant_specific_tag_nearby":
        required_tag = rng.choice(tags)
        # Remove this tag from other restaurants near the same attraction
        for row in rows:
            if (row.get("restaurant_name") != restaurant_name
                    and row.get("nearby_attraction_name") == nearby_attraction):
                row_tags = [t.strip() for t in row.get("tags", "").split(";") if t.strip()]
                if required_tag in row_tags:
                    row_tags.remove(required_tag)
                    row["tags"] = ";".join(row_tags)

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))
        return {"restaurant_specific_tag_nearby": {
            "constraint_context": f"Arrange a meal at a restaurant near '{nearby_attraction}' with {required_tag} service required",
            "constraint_type": "superlative_specific_tag_nearby",
            "attraction_name": nearby_attraction,
            "restaurant_name": restaurant_name,
            "required_tag": required_tag,
            "price_per_person": restaurant_cost,
            "restaurant_rating": rating,
        }}

    elif constraint_type == "restaurant_highest_rated":
        # Make chosen restaurant highest rated near its attraction
        for row in rows:
            if (row.get("restaurant_name") != restaurant_name
                    and row.get("nearby_attraction_name") == nearby_attraction):
                if safe_float(row.get("rating", 0)) >= rating:
                    row["rating"] = str(round(rating - rng.uniform(0.1, 0.3), 1))

        write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))
        return {"restaurant_highest_rated": {
            "constraint_context": f"Arrange a meal at the highest rated restaurant near '{nearby_attraction}'",
            "constraint_type": "superlative_highest_rated",
            "attraction_name": nearby_attraction,
            "restaurant_name": restaurant_name,
            "price_per_person": restaurant_cost,
            "restaurant_rating": rating,
        }}

    elif constraint_type == "restaurant_must_eat_named":
        return {"restaurant_must_eat_named": {
            "constraint_context": f"Must eat at '{restaurant_name}'",
            "constraint_type": "superlative_must_eat_named",
            "restaurant_name": restaurant_name,
            "price_per_person": restaurant_cost,
        }}

    return None


# ============================================================================
# Attraction constraint selection + injection
# ============================================================================

def _select_attraction_constraint(
    entities: Dict, db_dir: str, config: TaskConfig, rng: random.Random,
) -> Optional[Dict[str, Dict]]:
    """Select and inject an attraction constraint."""
    attractions = entities.get("attractions", [])
    if not attractions:
        return None

    csv_path = os.path.join(db_dir, DB_FILES["attractions"])
    rows = read_csv_as_dicts(csv_path)
    if not rows:
        return None

    constraint_type = rng.choice([
        "attraction_must_visit_named",
        "attraction_all_of_type",
        "attraction_type_highest_rated",
    ])

    if constraint_type == "attraction_must_visit_named":
        # Pick 1-3 attractions from the solution
        num_to_pick = min(len(attractions), rng.randint(1, 3))
        chosen_attrs = rng.sample(attractions, num_to_pick)
        names = [a["name"] for a in chosen_attrs]
        ratings = []
        for name in names:
            for row in rows:
                if row.get("attraction_name") == name:
                    ratings.append(safe_float(row.get("rating", 0)))
                    break

        return {"attraction_must_visit_named": {
            "constraint_context": f"The itinerary must include visits to {', '.join(repr(n) for n in names)}",
            "constraint_type": "superlative_must_visit_named",
            "attraction_names": names,
            "attraction_ratings": ratings,
        }}

    elif constraint_type == "attraction_all_of_type":
        # Find a type where all attractions of that type are in the solution
        solution_names = {a["name"] for a in attractions}
        type_groups: Dict[str, List[str]] = {}
        for row in rows:
            atype = row.get("attraction_type", "")
            aname = row.get("attraction_name", "")
            if atype:
                type_groups.setdefault(atype, []).append(aname)

        # Find types where all attractions are in solution
        valid_types = []
        for atype, names in type_groups.items():
            if all(n in solution_names for n in names):
                valid_types.append(atype)

        if valid_types:
            chosen_type = rng.choice(valid_types)
            names = type_groups[chosen_type]
            ratings = []
            for name in names:
                for row in rows:
                    if row.get("attraction_name") == name:
                        ratings.append(safe_float(row.get("rating", 0)))
                        break

            return {"attraction_all_of_type": {
                "constraint_context": f"Must visit all '{chosen_type}' type attractions",
                "constraint_type": "superlative_all_of_type",
                "attraction_names": names,
                "attraction_type": chosen_type,
                "attraction_ratings": ratings,
            }}

        # Fallback to must_visit_named
        return _select_attraction_constraint_fallback(attractions, rows, rng)

    elif constraint_type == "attraction_type_highest_rated":
        # Pick an attraction from solution and make it highest rated of its type
        chosen = rng.choice(attractions)
        chosen_name = chosen["name"]

        # Find its type and rating
        chosen_type = ""
        chosen_rating = 0.0
        for row in rows:
            if row.get("attraction_name") == chosen_name:
                chosen_type = row.get("attraction_type", "")
                chosen_rating = safe_float(row.get("rating", 0))
                break

        if chosen_type:
            # Ensure it's the highest rated of its type
            for row in rows:
                if (row.get("attraction_name") != chosen_name
                        and row.get("attraction_type") == chosen_type):
                    if safe_float(row.get("rating", 0)) >= chosen_rating:
                        row["rating"] = str(round(chosen_rating - rng.uniform(0.1, 0.2), 1))

            write_csv_from_dicts(rows, csv_path, list(rows[0].keys()))

            return {"attraction_type_highest_rated": {
                "constraint_context": f"Must visit the highest rated '{chosen_type}' attraction",
                "constraint_type": "superlative_type_highest_rated",
                "attraction_names": [chosen_name],
                "attraction_type": chosen_type,
                "attraction_ratings": [chosen_rating],
            }}

    return _select_attraction_constraint_fallback(attractions, rows, rng)


def _select_attraction_constraint_fallback(
    attractions: List[Dict], rows: List[Dict], rng: random.Random,
) -> Dict[str, Dict]:
    """Fallback: use must_visit_named."""
    num = min(len(attractions), rng.randint(1, 2))
    chosen = rng.sample(attractions, num)
    names = [a["name"] for a in chosen]
    ratings = []
    for name in names:
        for row in rows:
            if row.get("attraction_name") == name:
                ratings.append(safe_float(row.get("rating", 0)))
                break
    return {"attraction_must_visit_named": {
        "constraint_context": f"The itinerary must include visits to {', '.join(repr(n) for n in names)}",
        "constraint_type": "superlative_must_visit_named",
        "attraction_names": names,
        "attraction_ratings": ratings,
    }}


# ============================================================================
# Budget constraint
# ============================================================================

def _select_budget_constraint(
    solution: Dict, config: TaskConfig,
) -> Optional[Dict[str, Dict]]:
    """Add a budget constraint with some slack."""
    budget = solution.get("budget_summary", {})
    total = safe_float(budget.get("total_estimated_budget", 0))

    if total <= 0:
        return None

    # Set max_budget with 5-15% slack based on difficulty
    slack = {1: 0.20, 2: 0.10, 3: 0.05}.get(config.difficulty, 0.10)
    max_budget = int(total * (1 + slack))

    # Round to nearest 100 for natural-looking budget
    max_budget = ((max_budget + 50) // 100) * 100

    return {"budget_constraint": {
        "max_budget": max_budget,
    }}


# ============================================================================
# Main entry point
# ============================================================================

def inject_constraints(
    solution: Dict,
    db_dir: str,
    config: TaskConfig,
    rng: Optional[random.Random] = None,
    budget_prob: float = 0.15,
) -> Dict[str, Dict]:
    """
    Select and inject constraints for a task.

    Modifies the database CSVs in-place and returns the hard_constraints dict.
    """
    if rng is None:
        rng = random.Random()

    return select_constraints(solution, db_dir, config, rng, budget_prob)
