"""
Stage 2: Solution Construction

Builds a valid multi-day travel itinerary by programmatically querying
the forked database using the existing tool implementations.
The generated solution must pass all commonsense constraint checks.
"""
import json
import math
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import TaskConfig
from .utils import (
    safe_float, safe_int, time_to_minutes, minutes_to_time,
    parse_tool_json_output, coord_str, parse_coord_str,
    format_time, read_csv_as_dicts,
)


# ============================================================================
# Tool initialization helpers
# ============================================================================

def _init_tools(db_dir: str, language: str = "en") -> Dict[str, Any]:
    """
    Initialize all travel planning tools pointing to the given database.

    Returns dict of tool_name -> tool_instance.
    """
    from travelplanning.tools.train_query_tool import TrainQueryTool
    from travelplanning.tools.flight_query_tool import FlightQueryTool
    from travelplanning.tools.hotel_query_tool import HotelQueryTool
    from travelplanning.tools.attraction_query_tool import (
        AttractionDetailsQueryTool, AttractionRecommendTool,
    )
    from travelplanning.tools.restaurant_query_tool import (
        RestaurantRecommendTool, RestaurantDetailsQueryTool,
    )
    from travelplanning.tools.location_search_tool import LocationSearchTool
    from travelplanning.tools.roadroute_query_tool import RoadRouteInfoQueryTool

    cfg_base = {"language": language, "load_schema": False}

    tools = {
        "train": TrainQueryTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "trains", "trains.csv")
        }),
        "flight": FlightQueryTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "flights", "flights.csv")
        }),
        "hotel": HotelQueryTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "hotels", "hotels.csv")
        }),
        "attraction_details": AttractionDetailsQueryTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "attractions", "attractions.csv")
        }),
        "attraction_recommend": AttractionRecommendTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "attractions", "attractions.csv")
        }),
        "restaurant_recommend": RestaurantRecommendTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "restaurants", "restaurants.csv")
        }),
        "restaurant_details": RestaurantDetailsQueryTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "restaurants", "restaurants.csv")
        }),
        "location": LocationSearchTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "locations", "locations_coords.csv")
        }),
        "road_route": RoadRouteInfoQueryTool(cfg={
            **cfg_base, "database_path": os.path.join(db_dir, "transportation", "distance_matrix.csv")
        }),
    }
    return tools


# ============================================================================
# Data querying via tools
# ============================================================================

def _query_transport(tools: Dict, origin: str, dest: str, date: str) -> Dict[str, List[Dict]]:
    """Query available trains and flights for a route on a date."""
    result = {"trains": [], "flights": []}

    # Query trains
    try:
        train_out = tools["train"].call({
            "origin": origin, "destination": dest, "depDate": date
        })
        parsed = parse_tool_json_output(train_out)
        if parsed and isinstance(parsed, list):
            for route_data in parsed:
                if isinstance(route_data, list):
                    for seg in route_data:
                        result["trains"].append(seg)
                elif isinstance(route_data, dict):
                    result["trains"].append(route_data)
    except Exception:
        pass  # No trains available for this route

    # Query flights (may not exist for some routes)
    try:
        flight_out = tools["flight"].call({
            "origin": origin, "destination": dest, "depDate": date
        })
        parsed = parse_tool_json_output(flight_out)
        if parsed and isinstance(parsed, list):
            for route_data in parsed:
                if isinstance(route_data, dict):
                    result["flights"].append(route_data)
    except Exception:
        pass  # No flights available for this route

    return result


def _query_hotels(tools: Dict, dest: str, checkin: str, checkout: str) -> List[Dict]:
    """Query available hotels."""
    out = tools["hotel"].call({
        "destination": dest, "checkinDate": checkin, "checkoutDate": checkout
    })
    parsed = parse_tool_json_output(out)
    return parsed if isinstance(parsed, list) else []


def _query_attractions(tools: Dict, city: str) -> List[Dict]:
    """Query all attractions in a city. Returns parsed list from tool text output."""
    # Use CSV directly since recommend tool returns text, not JSON
    csv_path = tools["attraction_recommend"].database_path
    rows = read_csv_as_dicts(csv_path)
    attractions = []
    for row in rows:
        attractions.append({
            "name": row.get("attraction_name", ""),
            "type": row.get("attraction_type", ""),
            "rating": safe_float(row.get("rating", 0)),
            "opening_time": row.get("opening_time", "09:00"),
            "closing_time": row.get("closing_time", "17:00"),
            "closing_dates": row.get("closing_dates", ""),
            "min_visit_hours": safe_float(row.get("min_visit_hours", 1)),
            "max_visit_hours": safe_float(row.get("max_visit_hours", 2)),
            "ticket_price": safe_float(row.get("ticket_price", 0)),
            "latitude": row.get("latitude", ""),
            "longitude": row.get("longitude", ""),
            "city": row.get("city", city),
        })
    return attractions


def _query_restaurants_near(tools: Dict, lat: str, lon: str) -> List[Dict]:
    """Query restaurants near a coordinate by reading CSV directly.

    Uses query_latitude/query_longitude matching (same logic as the tool)
    but reads from CSV to ensure price consistency with the validator.
    """
    csv_path = tools["restaurant_recommend"].database_path
    rows = read_csv_as_dicts(csv_path)
    results = []
    lat_str = str(lat).strip()
    lon_str = str(lon).strip()
    for row in rows:
        q_lat = str(row.get("query_latitude", "")).strip()
        q_lon = str(row.get("query_longitude", "")).strip()
        if q_lat == lat_str and q_lon == lon_str:
            price_raw = row.get("price_per_person", "0")
            results.append({
                "name": row.get("restaurant_name", ""),
                "latitude": str(row.get("latitude", "")),
                "longitude": str(row.get("longitude", "")),
                "price_per_person": int(round(safe_float(price_raw))),
                "cuisine": row.get("cuisine", ""),
                "opening_time": row.get("opening_time", ""),
                "closing_time": row.get("closing_time", ""),
                "nearby_attraction_name": row.get("nearby_attraction_name", ""),
                "rating": str(row.get("rating", "4.5")),
                "tags": row.get("tags", ""),
            })
    return results


def _query_road_route(tools: Dict, origin_coord: str, dest_coord: str) -> Optional[Dict]:
    """Query road route between two coordinate strings ('lat,lon')."""
    out = tools["road_route"].call({
        "origin": origin_coord, "destination": dest_coord,
    })
    parsed = parse_tool_json_output(out)
    return parsed if isinstance(parsed, dict) else None


def _query_location(tools: Dict, place_name: str) -> Optional[Tuple[str, str]]:
    """Get (lat, lon) for a place name."""
    out = tools["location"].call({"place_name": place_name})
    parsed = parse_tool_json_output(out)
    if parsed and isinstance(parsed, dict):
        lat = parsed.get("latitude", "")
        lon = parsed.get("longitude", "")
        if lat and lon:
            return (lat, lon)
    return None


# ============================================================================
# Transport selection helpers
# ============================================================================

def _extract_transport_info(route_data: Dict, mode: str) -> Optional[Dict]:
    """
    Extract transport info from a route data dict.
    Returns dict with: number, dep_time, arr_time, duration, from_station,
    to_station, cost, seat_status, mode.
    """
    # Find the first segment key (e.g., "Segment 1")
    seg_key = None
    for k in route_data:
        if k.startswith("Segment") or k.startswith("第"):
            seg_key = k
            break

    if not seg_key:
        return None

    seg = route_data[seg_key]
    price = safe_float(route_data.get("price", 0))

    # Extract seat status
    seat_status = seg.get("Remaining Seats", seg.get("剩余票数量", ""))

    dep_dt = seg.get("depDateTime", "")
    arr_dt = seg.get("arrDateTime", "")

    dep_time = dep_dt.split(" ")[-1][:5] if " " in dep_dt else ""
    arr_time = arr_dt.split(" ")[-1][:5] if " " in arr_dt else ""

    transport_no = seg.get("marketingTransportNo", "")

    return {
        "mode": mode,
        "number": transport_no,
        "dep_time": dep_time,
        "arr_time": arr_time,
        "duration": safe_int(seg.get("duration", 0)),
        "from_station": seg.get("depStationName", ""),
        "to_station": seg.get("arrStationName", ""),
        "cost": price,
        "seat_status": seat_status,
        "dep_datetime": dep_dt,
        "arr_datetime": arr_dt,
    }


def _select_outbound_transport(
    transport_data: Dict[str, List], people: int, rng: random.Random
) -> Optional[Dict]:
    """Select a suitable outbound transport (morning departure, sufficient seats).

    Uses progressive relaxation:
      Tier 1: Direct, before 10:00
      Tier 2: Direct, before 14:00
      Tier 3: Direct, any time before 18:00
      Tier 4: Any route (including transfers), any time before 18:00
    """
    def _collect_candidates(max_dep: int, allow_transfer: bool) -> List[Dict]:
        cands = []
        for mode in ["trains", "flights"]:
            for route_data in transport_data[mode]:
                info = _extract_transport_info(route_data, "train" if mode == "trains" else "flight")
                if info is None or not info["dep_time"]:
                    continue
                dep_minutes = time_to_minutes(info["dep_time"])
                if dep_minutes > max_dep:
                    continue
                # Check seat availability
                try:
                    seats = int(info["seat_status"])
                    if seats < people:
                        continue
                except (ValueError, TypeError):
                    pass  # "Sufficient" or similar — OK
                if not allow_transfer:
                    num_segments = sum(1 for k in route_data if k.startswith("Segment") or k.startswith("第"))
                    if num_segments > 1:
                        continue
                cands.append(info)
        return cands

    for max_dep, allow_xfer in [(600, False), (840, False), (1080, False), (1080, True)]:
        candidates = _collect_candidates(max_dep, allow_xfer)
        if candidates:
            return rng.choice(candidates)

    return None


def _select_return_transport(
    transport_data: Dict[str, List], people: int, rng: random.Random
) -> Optional[Dict]:
    """Select a suitable return transport (evening departure, sufficient seats).

    Uses progressive relaxation:
      Tier 1: Direct, after 16:00
      Tier 2: Direct, after 14:00
      Tier 3: Direct, after 10:00
      Tier 4: Any route (including transfers), after 10:00
    """
    def _collect_candidates(min_dep: int, allow_transfer: bool) -> List[Dict]:
        cands = []
        for mode in ["trains", "flights"]:
            for route_data in transport_data[mode]:
                info = _extract_transport_info(route_data, "train" if mode == "trains" else "flight")
                if info is None or not info["dep_time"]:
                    continue
                dep_minutes = time_to_minutes(info["dep_time"])
                if dep_minutes < min_dep:
                    continue
                try:
                    seats = int(info["seat_status"])
                    if seats < people:
                        continue
                except (ValueError, TypeError):
                    pass
                if not allow_transfer:
                    num_segments = sum(1 for k in route_data if k.startswith("Segment") or k.startswith("第"))
                    if num_segments > 1:
                        continue
                cands.append(info)
        return cands

    for min_dep, allow_xfer in [(960, False), (840, False), (600, False), (600, True)]:
        candidates = _collect_candidates(min_dep, allow_xfer)
        if candidates:
            return rng.choice(candidates)

    return None


# ============================================================================
# Schedule building
# ============================================================================

def _is_attraction_open(attraction: Dict, date_str: str) -> bool:
    """Check if an attraction is open on the given date."""
    closing_dates = attraction.get("closing_dates", "").strip()
    if not closing_dates:
        return True

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    day_name = dt.strftime("%A")

    # closing_dates can be "Monday", "Monday;Tuesday", etc.
    closed_days = [d.strip() for d in closing_dates.replace(",", ";").split(";")]
    return day_name not in closed_days


def _get_opening_minutes(attraction: Dict) -> Tuple[int, int]:
    """Get opening and closing times in minutes."""
    open_str = attraction.get("opening_time", "09:00").strip()
    close_str = attraction.get("closing_time", "17:00").strip()

    if "24" in open_str.lower() or "all" in open_str.lower():
        return 0, 1440  # 24 hours

    try:
        open_min = time_to_minutes(open_str)
    except (ValueError, IndexError):
        open_min = 540  # 09:00 default

    try:
        close_min = time_to_minutes(close_str)
    except (ValueError, IndexError):
        close_min = 1020  # 17:00 default

    if close_min <= open_min:
        close_min = 1440  # assume midnight

    return open_min, close_min


def _get_city_travel_duration(tools: Dict, from_coords: str, to_coords: str) -> int:
    """Get travel duration in minutes between two POIs. Returns default if not found."""
    route = _query_road_route(tools, from_coords, to_coords)
    if route:
        return max(5, safe_int(route.get("duration_in_minutes", 10)))
    return 15  # default fallback


def _get_city_travel_cost(tools: Dict, from_coords: str, to_coords: str) -> float:
    """Get travel cost between two POIs."""
    route = _query_road_route(tools, from_coords, to_coords)
    if route:
        return safe_float(route.get("cost", 0))
    return 0


def _build_restaurant_price_index(db_dir: str) -> Dict[str, int]:
    """Build name→rounded_price index matching the validator's load_restaurant_index.

    The validator keeps the LAST occurrence when a name appears multiple times.
    """
    csv_path = os.path.join(db_dir, "restaurants", "restaurants.csv")
    rows = read_csv_as_dicts(csv_path)
    index = {}
    for row in rows:
        name = (row.get("restaurant_name") or "").strip()
        if not name:
            continue
        price_raw = row.get("price_per_person", "0")
        index[name] = int(round(safe_float(price_raw)))
    return index


def _build_activities_for_day(
    day_num: int,
    total_days: int,
    attractions_today: List[Dict],
    restaurants_pool: Dict[str, List[Dict]],
    restaurant_price_index: Dict[str, int],
    hotel: Dict,
    outbound_transport: Optional[Dict],
    return_transport: Optional[Dict],
    tools: Dict,
    config: TaskConfig,
    used_restaurants: set,
    date_str: str,
    rng: random.Random,
) -> Tuple[List[Dict], List[str]]:
    """
    Build activities list for a single day using event-based scheduling.

    Builds an ordered event list [attraction, meal, attraction, meal, ...] then
    schedules each event sequentially with travel_city segments in between.

    Returns:
        (activities_list, newly_used_restaurant_names)
    """
    activities = []
    new_used = []
    is_first_day = (day_num == 1)
    is_last_day = (day_num == total_days)

    # Determine day time boundaries
    if is_first_day and outbound_transport:
        arr_time = outbound_transport["arr_time"]
        arr_min = time_to_minutes(arr_time)
        if outbound_transport["mode"] == "flight":
            start_min = arr_min + 40
        else:
            start_min = arr_min + 10

        activities.append({
            "type": "travel_intercity_public",
            "time_slot": f"{outbound_transport['dep_time']}-{outbound_transport['arr_time']}",
            "details": {
                "mode": outbound_transport["mode"],
                "number": outbound_transport["number"],
                "from": outbound_transport["from_station"],
                "to": outbound_transport["to_station"],
                "cost": outbound_transport["cost"],
            }
        })

        buffer_end = minutes_to_time(start_min)
        activities.append({
            "type": "buffer",
            "time_slot": f"{outbound_transport['arr_time']}-{buffer_end}",
            "details": {"description": "Arrive and exit station"}
        })
    else:
        start_min = 480  # 08:00

    if is_last_day and return_transport:
        dep_time = return_transport["dep_time"]
        # Reserve time for: travel to station (~40 min) + station buffer (~10 min)
        end_min = time_to_minutes(dep_time) - 60
    else:
        end_min = 1260  # 21:00

    # Determine how many meals based on essential_meal_coverage rules:
    # - Arrival day (arrive < 10:00): 2 meals
    # - Arrival day (10:00-16:00): 1+ meal
    # - Non-intercity (middle) day: 2 meals
    # - Departure day (depart > 15:00): 1+ meal
    # - Departure day (depart < 10:00): 0 meals
    available_time = end_min - start_min
    meals_needed = []

    if is_first_day and outbound_transport:
        arr_min_val = time_to_minutes(outbound_transport["arr_time"])
        if arr_min_val < 600:  # arrive < 10:00 → must have 2 meals
            needs_two = True
        elif arr_min_val < 960:  # arrive 10:00-16:00 → at least 1
            needs_two = (available_time >= 360)  # 2 if enough time
        else:
            needs_two = False
    elif is_last_day and return_transport:
        dep_min_val = time_to_minutes(return_transport["dep_time"])
        if dep_min_val >= 900:  # depart >= 15:00 → at least 1 meal
            needs_two = (available_time >= 360)
        elif dep_min_val >= 600:  # depart 10:00-15:00 → 0-1 meal
            needs_two = False
        else:
            needs_two = False
    else:
        # Non-intercity (middle) day → must have 2 meals
        needs_two = True

    if needs_two and available_time >= 300:
        third = available_time // 3
        meal1_time = start_min + third
        meal2_time = start_min + 2 * third
        # Ensure gap >= 180 min (meal ~60-90 min + travel + buffer)
        if meal2_time - meal1_time >= 180:
            meals_needed.append(("lunch", meal1_time))
            meals_needed.append(("dinner", meal2_time))
        else:
            meals_needed.append(("lunch", (start_min + end_min) // 2))
    elif available_time >= 150:  # >= 2.5 hours → 1 meal
        meals_needed.append(("lunch", (start_min + end_min) // 2))

    # Build ordered event list: interleave attractions and meals
    # Strategy: attr, attr, MEAL, attr, attr, MEAL, ...
    events = []  # list of ("attraction", attr_dict) or ("meal", meal_type, ideal_time)
    attrs_remaining = list(attractions_today)
    meals_remaining = list(meals_needed)

    # Place attractions evenly, insert meals between groups
    if len(meals_remaining) == 0:
        for attr in attrs_remaining:
            events.append(("attraction", attr))
    elif len(meals_remaining) == 1:
        # Split attractions into two halves with meal in between
        half = max(1, len(attrs_remaining) // 2)
        for attr in attrs_remaining[:half]:
            events.append(("attraction", attr))
        events.append(("meal", meals_remaining[0][0], meals_remaining[0][1]))
        for attr in attrs_remaining[half:]:
            events.append(("attraction", attr))
    else:
        # 2 meals: split attractions into 3 groups
        n = len(attrs_remaining)
        g1 = max(1, n // 3)
        g2 = max(1, (n - g1) // 2)
        for attr in attrs_remaining[:g1]:
            events.append(("attraction", attr))
        events.append(("meal", meals_remaining[0][0], meals_remaining[0][1]))
        for attr in attrs_remaining[g1:g1+g2]:
            events.append(("attraction", attr))
        events.append(("meal", meals_remaining[1][0], meals_remaining[1][1]))
        for attr in attrs_remaining[g1+g2:]:
            events.append(("attraction", attr))

    # Schedule events sequentially
    current_min = start_min
    hotel_lat, hotel_lon = hotel.get("latitude", ""), hotel.get("longitude", "")
    hotel_coords = f"{hotel_lat},{hotel_lon}" if hotel_lat and hotel_lon else ""
    current_coords = hotel_coords

    if is_first_day and outbound_transport:
        station_name = outbound_transport["to_station"]
        loc = _query_location(tools, station_name)
        if loc:
            current_coords = f"{loc[0]},{loc[1]}"

    last_meal_end = 0  # Track end of last meal for 120-min gap rule

    for event in events:
        if event[0] == "attraction":
            attr = event[1]
            attr_lat = attr.get("latitude", "")
            attr_lon = attr.get("longitude", "")
            attr_coords = f"{attr_lat},{attr_lon}" if attr_lat and attr_lon else ""

            # Compute travel (don't emit yet)
            travel_dur = 0
            travel_cost = 0.0
            if current_coords and attr_coords and current_coords != attr_coords:
                travel_dur = _get_city_travel_duration(tools, current_coords, attr_coords)
                travel_cost = _get_city_travel_cost(tools, current_coords, attr_coords)

            # Check feasibility with travel included
            open_min, close_min = _get_opening_minutes(attr)
            attr_start = max(current_min + travel_dur, open_min)

            min_hours = safe_float(attr.get("min_visit_hours", 0.5))
            max_hours = safe_float(attr.get("max_visit_hours", 2.0))
            if min_hours <= 0:
                min_hours = 0.5
            if max_hours < min_hours:
                max_hours = min_hours + 0.5
            min_visit_dur = int(min_hours * 60)

            if attr_start + min_visit_dur > end_min or attr_start + min_visit_dur > close_min:
                continue
            if attr_start >= end_min - 30:
                break

            # Use shorter visits when many events need to fit
            remaining_events = len(events) - events.index(event) - 1
            remaining_time = end_min - attr_start
            if remaining_events > 0 and remaining_time < remaining_events * 120:
                # Tight schedule — use minimum visit duration
                visit_dur = min_visit_dur
            else:
                visit_dur = int(rng.uniform(min_hours, max_hours) * 60)
                visit_dur = max(min_visit_dur, visit_dur)
            attr_end = min(attr_start + visit_dur, close_min, end_min)
            if attr_end - attr_start < min_visit_dur:
                continue

            # Feasibility OK — now emit travel + buffer + attraction
            if travel_dur > 0:
                activities.append({
                    "type": "travel_city",
                    "time_slot": f"{minutes_to_time(current_min)}-{minutes_to_time(current_min + travel_dur)}",
                    "details": {
                        "from": "previous location",
                        "to": attr["name"],
                        "distance": "",
                        "duration": f"{travel_dur} min",
                        "cost": travel_cost,
                    }
                })
                current_min += travel_dur

            if attr_start > current_min:
                activities.append({
                    "type": "buffer",
                    "time_slot": f"{minutes_to_time(current_min)}-{minutes_to_time(attr_start)}",
                    "details": {"description": f"Wait for {attr['name']} to open"}
                })

            activities.append({
                "type": "attraction",
                "time_slot": f"{minutes_to_time(attr_start)}-{minutes_to_time(attr_end)}",
                "details": {
                    "name": attr["name"],
                    "city": attr.get("city", config.dest),
                    "cost": attr.get("ticket_price", 0),
                }
            })
            current_min = attr_end
            current_coords = attr_coords

        elif event[0] == "meal":
            meal_type = event[1]
            ideal_time = event[2]

            # Build list of candidate locations: last attraction, then all
            # attractions for today, then hotel
            candidate_locs = []
            for prev_event in reversed(events[:events.index(event)]):
                if prev_event[0] == "attraction":
                    candidate_locs.append((
                        prev_event[1].get("latitude", ""),
                        prev_event[1].get("longitude", ""),
                    ))
            # Add remaining attractions as fallback
            for ev in events:
                if ev[0] == "attraction":
                    loc = (ev[1].get("latitude", ""), ev[1].get("longitude", ""))
                    if loc not in candidate_locs:
                        candidate_locs.append(loc)
            # Always try hotel as last resort
            if (hotel_lat, hotel_lon) not in candidate_locs:
                candidate_locs.append((hotel_lat, hotel_lon))

            # Compute earliest meal start considering 120-min gap and opening hours
            earliest_meal_start = max(current_min, last_meal_end + 120)

            # Try each candidate location until we find a restaurant
            restaurant = None
            near_lat, near_lon = hotel_lat, hotel_lon
            for c_lat, c_lon in candidate_locs:
                if not c_lat or not c_lon:
                    continue
                restaurant = _pick_restaurant(
                    tools, c_lat, c_lon, restaurants_pool,
                    earliest_meal_start, end_min, used_restaurants, rng,
                )
                if restaurant:
                    near_lat, near_lon = c_lat, c_lon
                    break
            if restaurant is None:
                continue

            r_lat = restaurant.get("latitude", "")
            r_lon = restaurant.get("longitude", "")
            r_coords = f"{r_lat},{r_lon}" if r_lat and r_lon else ""

            # Compute travel (don't emit yet)
            travel_dur = 0
            travel_cost = 0.0
            if current_coords and r_coords and current_coords != r_coords:
                travel_dur = _get_city_travel_duration(tools, current_coords, r_coords)
                travel_cost = _get_city_travel_cost(tools, current_coords, r_coords)

            # Compute actual meal_start respecting all constraints
            meal_start = max(earliest_meal_start, current_min + travel_dur)
            r_open = restaurant.get("opening_time", "")
            if r_open:
                try:
                    r_open_min = time_to_minutes(r_open)
                    meal_start = max(meal_start, r_open_min)
                except (ValueError, IndexError):
                    pass

            if meal_start >= end_min - 30:
                continue

            meal_dur = rng.randint(60, 90)
            meal_end = meal_start + meal_dur

            # Respect restaurant closing time and end_min
            r_close = restaurant.get("closing_time", "")
            if r_close:
                try:
                    r_close_min = time_to_minutes(r_close)
                    if r_close_min <= meal_start:
                        r_close_min = 1440
                    meal_end = min(meal_end, r_close_min)
                except (ValueError, IndexError):
                    pass
            meal_end = min(meal_end, end_min)
            if meal_end - meal_start < 30:
                continue

            # Feasibility OK — now emit travel + buffer + meal
            if travel_dur > 0:
                activities.append({
                    "type": "travel_city",
                    "time_slot": f"{minutes_to_time(current_min)}-{minutes_to_time(current_min + travel_dur)}",
                    "details": {
                        "from": "previous location",
                        "to": restaurant["name"],
                        "distance": "",
                        "duration": f"{travel_dur} min",
                        "cost": travel_cost,
                    }
                })
                current_min += travel_dur

            if meal_start > current_min:
                activities.append({
                    "type": "buffer",
                    "time_slot": f"{minutes_to_time(current_min)}-{minutes_to_time(meal_start)}",
                    "details": {"description": "Wait before meal"}
                })
                current_min = meal_start

            r_name = restaurant["name"]
            r_price = restaurant_price_index.get(r_name, safe_float(restaurant.get("price_per_person", 0)))
            activities.append({
                "type": "meal",
                "time_slot": f"{minutes_to_time(meal_start)}-{minutes_to_time(meal_end)}",
                "details": {
                    "meal_type": meal_type,
                    "name": r_name,
                    "cost": r_price,
                }
            })
            used_restaurants.add(restaurant["name"])
            new_used.append(restaurant["name"])
            current_min = meal_end
            last_meal_end = meal_end
            if r_coords:
                current_coords = r_coords

    # City travel to return station or hotel
    if is_last_day and return_transport:
        station_name = return_transport["from_station"]
        dep_min = time_to_minutes(return_transport["dep_time"])
        loc = _query_location(tools, station_name)
        if loc and current_coords:
            station_coords = f"{loc[0]},{loc[1]}"
            travel_dur = _get_city_travel_duration(tools, current_coords, station_coords)
            travel_cost = _get_city_travel_cost(tools, current_coords, station_coords)
            # Travel right after last activity (no idle gap)
            travel_start_min = current_min
            travel_end_min = travel_start_min + travel_dur
            activities.append({
                "type": "travel_city",
                "time_slot": f"{minutes_to_time(travel_start_min)}-{minutes_to_time(travel_end_min)}",
                "details": {
                    "from": "last activity",
                    "to": station_name,
                    "distance": "",
                    "duration": f"{travel_dur} min",
                    "cost": travel_cost,
                }
            })
            current_min = travel_end_min

            # Add buffer at station if there's waiting time before departure
            if current_min < dep_min:
                activities.append({
                    "type": "buffer",
                    "time_slot": f"{minutes_to_time(current_min)}-{return_transport['dep_time']}",
                    "details": {"description": "Wait at station for departure"}
                })

        activities.append({
            "type": "travel_intercity_public",
            "time_slot": f"{return_transport['dep_time']}-{return_transport['arr_time']}",
            "details": {
                "mode": return_transport["mode"],
                "number": return_transport["number"],
                "from": return_transport["from_station"],
                "to": return_transport["to_station"],
                "cost": return_transport["cost"],
            }
        })
    elif not is_last_day:
        # Travel to hotel and check in
        if current_coords and hotel_coords and current_coords != hotel_coords:
            travel_dur = _get_city_travel_duration(tools, current_coords, hotel_coords)
            travel_cost = _get_city_travel_cost(tools, current_coords, hotel_coords)
            travel_start = minutes_to_time(current_min)
            current_min += travel_dur
            travel_end = minutes_to_time(current_min)
            activities.append({
                "type": "travel_city",
                "time_slot": f"{travel_start}-{travel_end}",
                "details": {
                    "from": "last activity",
                    "to": hotel["name"],
                    "distance": "",
                    "duration": f"{travel_dur} min",
                    "cost": travel_cost,
                }
            })

        # Hotel check-in / rest
        hotel_start = minutes_to_time(current_min)
        hotel_end = minutes_to_time(min(current_min + 30, 1380))
        activities.append({
            "type": "hotel",
            "time_slot": f"{hotel_start}-{hotel_end}",
            "details": {
                "activity": "check-in" if is_first_day else "rest",
                "name": hotel["name"],
            }
        })

    return activities, new_used


def _pick_restaurant(
    tools: Dict,
    lat: str, lon: str,
    restaurants_pool: Dict[str, List[Dict]],
    earliest_start: int,
    end_of_day: int,
    used: set,
    rng: random.Random,
) -> Optional[Dict]:
    """Pick an available restaurant near the given coordinates.

    Accepts restaurants that will be open at any point between
    earliest_start and end_of_day (the scheduling code adjusts
    meal_start to the restaurant opening time).
    """
    coord_key = f"{lat},{lon}"

    # Load restaurants for this coordinate if not cached
    if coord_key not in restaurants_pool:
        nearby = _query_restaurants_near(tools, lat, lon)
        restaurants_pool[coord_key] = nearby

    candidates = []
    for r in restaurants_pool.get(coord_key, []):
        name = r.get("name", "")
        if name in used:
            continue

        # Check if restaurant will be open during our time window
        open_str = r.get("opening_time", "")
        close_str = r.get("closing_time", "")
        if open_str and close_str:
            try:
                open_min = time_to_minutes(open_str)
                close_min = time_to_minutes(close_str)
                if close_min <= open_min:
                    close_min = 1440
                # Restaurant must close after earliest_start and open before end_of_day
                if close_min <= earliest_start or open_min >= end_of_day:
                    continue
            except (ValueError, IndexError):
                pass

        candidates.append(r)

    if not candidates:
        return None

    return rng.choice(candidates)


# ============================================================================
# Main solution builder
# ============================================================================

def build_solution(
    db_dir: str,
    config: TaskConfig,
    rng: Optional[random.Random] = None,
    max_retries: int = 10,
) -> Optional[Dict]:
    """
    Build a valid travel plan solution.

    Args:
        db_dir: Path to the task's database directory.
        config: Task configuration.
        rng: Random number generator.
        max_retries: Maximum attempts to build a valid solution.

    Returns:
        Solution dict matching eval_converted.py format, or None on failure.
    """
    if rng is None:
        rng = random.Random()

    tools = _init_tools(db_dir, language="en")

    # Pre-check: enough attractions for the trip duration?
    # Min needed: 1 (first day) + 2*(days-2) (middle days) + 1 (last day) = 2*days - 2
    attractions = _query_attractions(tools, config.dest)
    min_attrs_needed = max(2, 2 * config.days - 2)
    if len(attractions) < min_attrs_needed:
        return None

    # Pre-query transport (same for all retries — no point re-querying)
    outbound_data = _query_transport(tools, config.origin, config.dest, config.depart_date)
    return_data = _query_transport(tools, config.dest, config.origin, config.return_date)

    # Early exit if no transport at all
    has_outbound = any(outbound_data[m] for m in outbound_data)
    has_return = any(return_data[m] for m in return_data)
    if not has_outbound or not has_return:
        return None

    for attempt in range(max_retries):
        solution = _try_build_solution(tools, config, db_dir, rng,
                                       outbound_data=outbound_data,
                                       return_data=return_data)
        if solution is None:
            continue

        # Validate with commonsense checks
        if _validate_solution(solution, config, db_dir):
            return solution

    return None


def _try_build_solution(
    tools: Dict, config: TaskConfig, db_dir: str, rng: random.Random,
    outbound_data: Optional[Dict] = None,
    return_data: Optional[Dict] = None,
) -> Optional[Dict]:
    """Single attempt to build a solution."""

    origin = config.origin
    dest = config.dest
    days = config.days
    depart_date = config.depart_date
    return_date = config.return_date
    people = config.people_number
    rooms = config.room_number

    # Step 1: Transport options (pre-queried or query now)
    if outbound_data is None:
        outbound_data = _query_transport(tools, origin, dest, depart_date)
    if return_data is None:
        return_data = _query_transport(tools, dest, origin, return_date)

    outbound = _select_outbound_transport(outbound_data, people, rng)
    ret = _select_return_transport(return_data, people, rng)

    if outbound is None or ret is None:
        return None

    # Step 2: Select hotel
    hotels = _query_hotels(tools, dest, depart_date, return_date)
    if not hotels:
        return None

    hotel = rng.choice(hotels)
    hotel_price = safe_float(hotel.get("price", 0))

    # Step 3: Query and assign attractions
    attractions = _query_attractions(tools, dest)
    if not attractions:
        return None

    # Filter attractions open during trip
    available_attractions = []
    for attr in attractions:
        # Check if open on at least one day of the trip
        for d in range(days):
            day_date = _date_add(depart_date, d)
            if _is_attraction_open(attr, day_date):
                available_attractions.append(attr)
                break

    if not available_attractions:
        return None

    rng.shuffle(available_attractions)

    # Distribute attractions across days using a reservation system:
    # 1. Determine minimum attractions per day based on validator rules
    # 2. Reserve minimums first, then distribute remaining
    attraction_assignment: Dict[int, List[Dict]] = {}

    # Compute per-day minimum requirements and targets
    day_mins = {}
    day_targets = {}
    for day_num in range(1, days + 1):
        is_first = day_num == 1
        is_last = day_num == days

        # Minimum required by validator
        min_req = 0
        if is_first and outbound:
            arr_h = time_to_minutes(outbound["arr_time"]) / 60.0
            if arr_h < 12.0:  # arrival < 12:00 → ≥1 attraction
                min_req = 1
        if is_last and ret:
            dep_h = time_to_minutes(ret["dep_time"]) / 60.0
            if dep_h > 16.0:  # departure > 16:00 → ≥1 attraction
                min_req = max(min_req, 1)
        if not is_first and not is_last:
            # Non-intercity day: ≥2 attractions (or ≥4h, but safer to target 2)
            min_req = 2

        day_mins[day_num] = min_req

        # Ideal target
        if is_first:
            avail_hours = 7
        elif is_last:
            avail_hours = 9
        else:
            avail_hours = 12
        day_targets[day_num] = max(min_req, min(4, avail_hours // 3))

    # Total minimum needed
    total_min = sum(day_mins.values())
    total_available = len(available_attractions)

    # If not enough attractions even for minimums, reduce targets proportionally
    if total_available < total_min:
        # Best effort — distribute what we have, prioritizing middle days
        pass

    # Phase 1: Reserve minimums for each day
    attr_idx = 0
    for day_num in range(1, days + 1):
        day_date = _date_add(depart_date, day_num - 1)
        min_req = day_mins[day_num]
        day_attrs = []
        while len(day_attrs) < min_req and attr_idx < len(available_attractions):
            attr = available_attractions[attr_idx]
            attr_idx += 1
            if _is_attraction_open(attr, day_date):
                day_attrs.append(attr)
        attraction_assignment[day_num] = day_attrs

    # Phase 2: Fill up to targets with remaining attractions
    for day_num in range(1, days + 1):
        day_date = _date_add(depart_date, day_num - 1)
        target = day_targets[day_num]
        current = attraction_assignment[day_num]
        while len(current) < target and attr_idx < len(available_attractions):
            attr = available_attractions[attr_idx]
            attr_idx += 1
            if _is_attraction_open(attr, day_date):
                current.append(attr)

    # Check if we have enough unique attractions for the trip
    total_assigned = sum(len(v) for v in attraction_assignment.values())
    if total_assigned < total_min:
        return None  # Not enough attractions for this trip duration

    # Step 4: Build daily plans
    daily_plans = []
    used_restaurants: set = set()
    restaurants_pool: Dict[str, List[Dict]] = {}
    restaurant_price_index = _build_restaurant_price_index(db_dir)

    for day_num in range(1, days + 1):
        day_date = _date_add(depart_date, day_num - 1)
        is_first = day_num == 1
        is_last = day_num == days

        current_city = dest
        if is_first:
            current_city = f"from {origin} to {dest}"
        elif is_last:
            current_city = f"from {dest} to {origin}"

        attrs_today = attraction_assignment.get(day_num, [])

        activities, new_used = _build_activities_for_day(
            day_num=day_num,
            total_days=days,
            attractions_today=attrs_today,
            restaurants_pool=restaurants_pool,
            restaurant_price_index=restaurant_price_index,
            hotel=hotel,
            outbound_transport=outbound if is_first else None,
            return_transport=ret if is_last else None,
            tools=tools,
            config=config,
            used_restaurants=used_restaurants,
            date_str=day_date,
            rng=rng,
        )

        accommodation = None
        if not is_last:
            accommodation = {
                "name": hotel.get("name", ""),
                "price_per_night": hotel_price,
            }

        daily_plans.append({
            "day_number": day_num,
            "current_city": current_city,
            "accommodation": accommodation,
            "activities": activities,
        })

    # Step 5: Compute budget
    budget = _compute_budget(daily_plans, people, rooms, hotel_price, days)

    return {
        "daily_plans": daily_plans,
        "budget_summary": budget,
    }


def _compute_budget(
    daily_plans: List[Dict], people: int, rooms: int,
    hotel_price: float, days: int,
) -> Dict[str, Any]:
    """Compute budget summary from the plan."""
    transport_cost = 0.0
    meals_cost = 0.0
    attractions_cost = 0.0
    taxis_needed = max(1, math.ceil(people / 4))

    for day in daily_plans:
        for act in day.get("activities", []):
            atype = act.get("type", "")
            details = act.get("details", {})
            cost = safe_float(details.get("cost", 0))

            if atype == "travel_intercity_public":
                transport_cost += cost * people
            elif atype == "travel_city":
                transport_cost += cost * taxis_needed
            elif atype == "meal":
                meals_cost += cost * people
            elif atype == "attraction":
                attractions_cost += cost * people

    accommodation_cost = hotel_price * rooms * (days - 1)
    total = transport_cost + accommodation_cost + meals_cost + attractions_cost

    return {
        "transportation": round(transport_cost, 2),
        "accommodation": round(accommodation_cost, 2),
        "meals": round(meals_cost, 2),
        "attractions_and_tickets": round(attractions_cost, 2),
        "other": 0,
        "total_estimated_budget": round(total, 2),
        "currency": "CNY",
    }


def _validate_solution(solution: Dict, config: TaskConfig, db_dir: str) -> bool:
    """Validate solution passes commonsense checks."""
    try:
        from travelplanning.evaluation.constraints_commonsense import eval_commonsense

        meta = {
            "org": config.origin,
            "dest": [config.dest],
            "days": config.days,
            "depart_date": config.depart_date,
            "return_date": config.return_date,
            "people_number": config.people_number,
            "room_number": config.room_number,
        }

        results = eval_commonsense(solution, meta, database_dir=Path(db_dir))

        # Check all results
        for check_name, (passed, msg) in results.items():
            if passed is not None and not passed:
                return False
        return True
    except Exception as e:
        print(f"[Stage 2] Validation error: {e}")
        return False


def _date_add(date_str: str, days: int) -> str:
    """Add days to a date string."""
    dt = datetime.strptime(date_str, "%Y-%m-%d")
    return (dt + timedelta(days=days)).strftime("%Y-%m-%d")
