"""
Export WorkingMemory contents as a clean dict for solver code.

The exported dict is injected as `data` into the LLM-generated solver code,
so the LLM can reference `data["hotels"]`, `data["routes"]`, etc.
"""

from typing import Any, Dict, List, Optional


def _clean(val: Any) -> Any:
    """Convert '?' sentinel values to None."""
    if val == '?':
        return None
    return val


def _safe_float(val: Any, default: float = 0.0) -> Optional[float]:
    if val == '?' or val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


def _safe_int(val: Any, default: int = 0) -> Optional[int]:
    if val == '?' or val is None:
        return None
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default


def export_memory_as_dict(memory) -> Dict[str, Any]:
    """
    Convert WorkingMemory into a clean dict for solver code.

    Args:
        memory: WorkingMemory instance

    Returns:
        Dict with keys: trip_meta, constraints, outbound_transport,
        inbound_transport, hotels, attractions, restaurants, routes
    """
    result: Dict[str, Any] = {}

    # --- Trip metadata ---
    if memory.trip_meta is not None:
        meta = memory.trip_meta
        result["trip_meta"] = {
            "origin": meta.origin,
            "destinations": meta.destinations or [],
            "days": meta.days,
            "depart_date": meta.depart_date,
            "return_date": meta.return_date,
            "people": meta.people_number,
            "rooms": meta.room_number,
        }
    else:
        result["trip_meta"] = {
            "origin": "", "destinations": [], "days": 0,
            "depart_date": "", "return_date": "",
            "people": 1, "rooms": 1,
        }

    # --- Constraints ---
    constraints_list = []
    for c in (memory.constraints or []):
        constraints_list.append({
            "variable": c.variable,
            "type": c.type,
            "operator": c.operator,
            "value": c.value,
            "certainty": c.certainty,
            "schedule_sensitivity": c.schedule_sensitivity,
            "layer": c.layer,
            "source": getattr(c, 'source', 'explicit'),
            "raw_text": getattr(c, 'raw_text', ''),
        })
    result["constraints"] = constraints_list

    # --- Transport (split into outbound / inbound) ---
    meta = result["trip_meta"]
    depart_date = meta["depart_date"]  # e.g. "2025-11-12"
    return_date = meta["return_date"]  # e.g. "2025-11-13"
    origin = meta["origin"]
    destinations = meta["destinations"]

    outbound = []
    inbound = []

    for transport_list, key_field in [(memory.flights, "flight_no"), (memory.trains, "train_no")]:
        mode = "flight" if key_field == "flight_no" else "train"
        for entry in transport_list:
            entry_origin = entry.get('origin', '')
            entry_dest = entry.get('dest', '')
            entry_date = entry.get('date', '')

            for opt in entry.get('options', []):
                option_dict = {
                    "id": opt.get(key_field, ''),
                    "mode": mode,
                    "dep_time": opt.get('dep_time', ''),
                    "arr_time": opt.get('arr_time', ''),
                    "dep_station": opt.get('dep_station', ''),
                    "arr_station": opt.get('arr_station', ''),
                    "price": _safe_float(opt.get('price', 0)),
                    "duration": _safe_int(opt.get('duration', 0)),
                    "seat_class": _clean(opt.get('seat_class', '')),
                }

                # Classify as outbound or inbound based on date and direction
                is_outbound = False
                is_inbound = False

                if depart_date and entry_date == depart_date:
                    # Check direction: origin city -> destination
                    if _city_match(entry_origin, origin) or any(_city_match(entry_dest, d) for d in destinations):
                        is_outbound = True
                if return_date and entry_date == return_date:
                    if any(_city_match(entry_origin, d) for d in destinations) or _city_match(entry_dest, origin):
                        is_inbound = True

                # If can't classify by date/direction, use date as fallback
                if not is_outbound and not is_inbound:
                    if entry_date == depart_date:
                        is_outbound = True
                    elif entry_date == return_date:
                        is_inbound = True

                if is_outbound:
                    outbound.append(option_dict)
                if is_inbound:
                    inbound.append(option_dict)

    result["outbound_transport"] = outbound
    result["inbound_transport"] = inbound

    # --- Hotels ---
    hotels = []
    for h in memory.hotels:
        hotels.append({
            "name": h.get('name', ''),
            "city": h.get('city', ''),
            "price": _safe_float(h.get('price')),
            "star": _safe_int(h.get('star')),
            "rating": _safe_float(h.get('rating')),
            "address": _clean(h.get('address', '')),
            "services": h.get('services', []),
            "decoration_time": _safe_int(h.get('decoration_time')),
            "brand": _clean(h.get('brand', '')),
        })
    result["hotels"] = hotels

    # --- Attractions (with details) ---
    attractions = {}
    for name, d in memory.attractions_detail.items():
        attractions[name] = {
            "price": _safe_float(d.get('price', 0)),
            "open": _clean(d.get('open', '08:00')),
            "close": _clean(d.get('close', '22:00')),
            "visit_min_hrs": _safe_float(d.get('visit_min', 1.0)),
            "visit_max_hrs": _safe_float(d.get('visit_max', 2.0)),
            "type": _clean(d.get('type', '')),
            "rating": _safe_float(d.get('rating')),
            "closed_dates": _clean(d.get('closed_dates', '')),
        }
    # Also include summary-only attractions (no details queried)
    for a in memory.attractions_summary:
        name = a.get('name', '')
        if name and name not in attractions:
            attractions[name] = {
                "price": None,
                "open": None,
                "close": None,
                "visit_min_hrs": None,
                "visit_max_hrs": None,
                "type": a.get('type', ''),
                "rating": None,
                "closed_dates": None,
                "_no_details": True,
            }
    result["attractions"] = attractions

    # --- Restaurants ---
    restaurants = []
    seen_names = set()
    for r in memory.restaurants:
        name = r.get('name', '')
        if name in seen_names:
            continue
        seen_names.add(name)
        restaurants.append({
            "name": name,
            "price_per_person": _safe_float(r.get('price_per_person')),
            "cuisine": _clean(r.get('cuisine', '')),
            "open": _clean(r.get('opening_time', '')),
            "close": _clean(r.get('closing_time', '')),
            "rating": _safe_float(r.get('rating')),
            "near": _clean(r.get('near_attraction', '')),
            "tags": r.get('tags', []),
        })
    result["restaurants"] = restaurants

    # --- Routes ---
    routes = {}
    for r in memory.routes:
        origin_name = r.get('origin_name', '')
        dest_name = r.get('dest_name', '')
        if origin_name and dest_name:
            key = f"{origin_name} -> {dest_name}"
            dist_m = r.get('distance_m', 0)
            dist_km = round(dist_m / 1000, 1) if isinstance(dist_m, (int, float)) else None
            routes[key] = {
                "duration_min": _safe_int(r.get('duration_min', 0)),
                "distance_km": dist_km,
                "cost": _safe_float(r.get('cost', 0)),
            }
    result["routes"] = routes

    return result


def _city_match(name: str, city: str) -> bool:
    """Fuzzy check if a transport origin/dest matches a city name."""
    if not name or not city:
        return False
    name_l = name.lower()
    city_l = city.lower()
    return city_l in name_l or name_l in city_l
