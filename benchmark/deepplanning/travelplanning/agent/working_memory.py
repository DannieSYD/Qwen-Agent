"""
Working Memory for Travel Planning Agent

Maintains a structured data store that accumulates tool results across turns.
After each tool call, the raw response is parsed and stored in the memory.
The model sees a clean, organized summary instead of raw JSON/text.

Design: Option C — replaces raw tool output with structured memory snapshot.
"""

import json
import re
import math
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple


class WorkingMemory:
    """Structured working memory that accumulates tool results."""

    def __init__(self, language: str = 'en'):
        self.language = language

        # Accumulated data stores
        self.flights: List[Dict[str, Any]] = []       # Each: {origin, dest, date, options: [{flight_no, dep_time, arr_time, dep_station, arr_station, duration, price, seat_class}]}
        self.trains: List[Dict[str, Any]] = []         # Same structure as flights
        self.hotels: List[Dict[str, Any]] = []         # Each: {name, city, price, star, rating, lat, lon, address}
        self.attractions_summary: List[Dict[str, Any]] = []  # From recommend_attractions: {name, type, description}
        self.attractions_detail: Dict[str, Dict[str, Any]] = {}  # From query_attraction_details: name -> {price, hours, visit_min, visit_max, lat, lon, rating, closed_dates}
        self.locations: Dict[str, Dict[str, float]] = {}  # name -> {lat, lon}
        self.restaurants: List[Dict[str, Any]] = []    # Each: {name, price_per_person, cuisine, hours, rating, lat, lon, near_attraction}
        self.restaurant_details: Dict[str, Dict[str, Any]] = {}  # name -> full details
        self.routes: List[Dict[str, Any]] = []         # Each: {origin, dest, distance_m, duration_min, cost}

        # Track which queries have been made
        self.queries_made: List[str] = []

        # Phase 1: Extracted constraints and trip metadata
        self.trip_meta = None            # TripMeta instance (set by constraint_extractor)
        self.constraints: List[Any] = [] # List[ConstraintTuple] (set by constraint_extractor)
        self._constraints_rendered: str = ""  # Pre-rendered constraint block

    def set_constraints(self, trip_meta, constraints, rendered: str = "") -> None:
        """Store extracted constraints and optional pre-rendered text."""
        self.trip_meta = trip_meta
        self.constraints = constraints
        self._constraints_rendered = rendered

    def process_tool_result(self, tool_name: str, arguments: str, result: str) -> str:
        """
        Parse a tool result and update memory. Returns acknowledgment string.

        Args:
            tool_name: Name of the tool that was called
            arguments: JSON string of the tool call arguments
            result: Raw result string from the tool

        Returns:
            Brief acknowledgment of what was stored (replaces raw output)
        """
        try:
            args = json.loads(arguments) if arguments else {}
        except json.JSONDecodeError:
            args = {}

        self.queries_made.append(f"{tool_name}({json.dumps(args, ensure_ascii=False)})")

        parser = {
            'query_flight_info': self._parse_flights,
            'query_train_info': self._parse_trains,
            'query_hotel_info': self._parse_hotels,
            'recommend_attractions': self._parse_recommend_attractions,
            'query_attraction_details': self._parse_attraction_details,
            'search_location': self._parse_location,
            'recommend_restaurants': self._parse_restaurants,
            'query_restaurant_details': self._parse_restaurant_details,
            'query_road_route_info': self._parse_route,
        }

        handler = parser.get(tool_name)
        if handler:
            try:
                ack = handler(args, result)
                return ack
            except Exception as e:
                return f"[Memory] Failed to parse {tool_name} result: {e}. Raw: {result[:200]}"

        return result  # Unknown tool, pass through raw

    def _parse_flights(self, args: dict, result: str) -> str:
        """Parse flight query results."""
        origin = args.get('origin', '?')
        dest = args.get('destination', '?')
        date = args.get('depDate', '?')

        try:
            data = json.loads(result)
        except json.JSONDecodeError:
            return f"[Stored] Flights {origin}→{dest} ({date}): No results or parse error"

        if not isinstance(data, list) or len(data) == 0:
            return f"[Stored] Flights {origin}→{dest} ({date}): No flights found"

        options = []
        for route in data:
            if not isinstance(route, dict):
                continue
            price = route.get('price', 0)
            # Find first segment for basic info
            seg_key = 'Segment 1' if 'Segment 1' in route else '第1段'
            seg = route.get(seg_key, {})
            if not seg:
                # Try to find any segment key
                for k, v in route.items():
                    if isinstance(v, dict) and 'depDateTime' in v:
                        seg = v
                        break

            option = {
                'flight_no': seg.get('marketingTransportNo', '?'),
                'dep_time': seg.get('depDateTime', '?'),
                'arr_time': seg.get('arrDateTime', '?'),
                'dep_station': seg.get('depStationName', '?'),
                'arr_station': seg.get('arrStationName', '?'),
                'duration': seg.get('duration', '?'),
                'price': price,
                'seat_class': seg.get('seatClassName', '?'),
            }
            options.append(option)

        entry = {'origin': origin, 'dest': dest, 'date': date, 'options': options}
        self.flights.append(entry)

        summary_lines = [f"[Stored] Flights {origin}→{dest} ({date}): {len(options)} options"]
        for o in options:
            summary_lines.append(f"  {o['flight_no']}: {o['dep_time']}→{o['arr_time']}, {o['dep_station']}→{o['arr_station']}, {o['duration']}min, ¥{o['price']}/person")
        return "\n".join(summary_lines)

    def _parse_trains(self, args: dict, result: str) -> str:
        """Parse train query results."""
        origin = args.get('origin', '?')
        dest = args.get('destination', '?')
        date = args.get('depDate', '?')

        try:
            data = json.loads(result)
        except json.JSONDecodeError:
            return f"[Stored] Trains {origin}→{dest} ({date}): No results or parse error"

        # Trains have double-array structure [[{...}]]
        if isinstance(data, list) and len(data) > 0 and isinstance(data[0], list):
            routes = data[0]  # Unwrap outer array
        elif isinstance(data, list):
            routes = data
        else:
            return f"[Stored] Trains {origin}→{dest} ({date}): No trains found"

        options = []
        for route in routes:
            if not isinstance(route, dict):
                continue
            price = route.get('price', 0)
            seg_key = 'Segment 1' if 'Segment 1' in route else '第1段'
            seg = route.get(seg_key, {})
            if not seg:
                for k, v in route.items():
                    if isinstance(v, dict) and 'depDateTime' in v:
                        seg = v
                        break

            option = {
                'train_no': seg.get('marketingTransportNo', '?'),
                'dep_time': seg.get('depDateTime', '?'),
                'arr_time': seg.get('arrDateTime', '?'),
                'dep_station': seg.get('depStationName', '?'),
                'arr_station': seg.get('arrStationName', '?'),
                'duration': seg.get('duration', '?'),
                'price': price,
                'seat_class': seg.get('seatClassName', '?'),
            }
            options.append(option)

        entry = {'origin': origin, 'dest': dest, 'date': date, 'options': options}
        self.trains.append(entry)

        summary_lines = [f"[Stored] Trains {origin}→{dest} ({date}): {len(options)} options"]
        for o in options:
            summary_lines.append(f"  {o['train_no']}: {o['dep_time']}→{o['arr_time']}, {o['dep_station']}→{o['arr_station']}, {o['duration']}min, ¥{o['price']}/person")
        return "\n".join(summary_lines)

    def _parse_hotels(self, args: dict, result: str) -> str:
        """Parse hotel query results."""
        city = args.get('destination', '?')

        try:
            data = json.loads(result)
        except json.JSONDecodeError:
            return f"[Stored] Hotels in {city}: No results or parse error"

        if not isinstance(data, list):
            return f"[Stored] Hotels in {city}: No hotels found"

        for h in data:
            if not isinstance(h, dict):
                continue
            hotel = {
                'name': h.get('name', '?'),
                'city': city,
                'price': h.get('price', '?'),
                'star': h.get('hotelStar', '?'),
                'rating': h.get('score', '?'),
                'lat': h.get('latitude', '?'),
                'lon': h.get('longitude', '?'),
                'address': h.get('address', '?'),
            }
            self.hotels.append(hotel)
            # Also store coordinates in locations
            if hotel['lat'] != '?' and hotel['lon'] != '?':
                self.locations[hotel['name']] = {
                    'lat': hotel['lat'],
                    'lon': hotel['lon']
                }

        summary_lines = [f"[Stored] Hotels in {city}: {len(data)} options"]
        for h in self.hotels:
            if h['city'] == city:
                summary_lines.append(f"  {h['name']}: ¥{h['price']}/night, {h['star']}-star, rating {h['rating']}, coords=({h['lat']}, {h['lon']})")
        return "\n".join(summary_lines)

    def _parse_recommend_attractions(self, args: dict, result: str) -> str:
        """Parse attraction recommendations (text format)."""
        city = args.get('city', '?')

        # Parse text format: "Name, Description. This is a Type type attraction"
        attractions = []
        for line in result.split('\n'):
            line = line.strip()
            if not line or line.startswith('Recommended') or line.startswith('推荐'):
                continue

            # Extract name (before first comma)
            if ',' in line or '，' in line:
                sep = ',' if ',' in line else '，'
                name = line.split(sep)[0].strip()
            else:
                name = line.strip()

            # Extract type
            attr_type = '?'
            type_match_en = re.search(r'This is a (.+?) type attraction', line)
            type_match_zh = re.search(r'这是一个(.+?)类型的景点', line)
            if type_match_en:
                attr_type = type_match_en.group(1)
            elif type_match_zh:
                attr_type = type_match_zh.group(1)

            if name:
                entry = {'name': name, 'city': city, 'type': attr_type}
                attractions.append(entry)
                self.attractions_summary.append(entry)

        summary_lines = [f"[Stored] Attractions in {city}: {len(attractions)} found"]
        for a in attractions:
            detail_status = "✓ details queried" if a['name'] in self.attractions_detail else "⚠ details NOT yet queried"
            summary_lines.append(f"  {a['name']} ({a['type']}) [{detail_status}]")
        return "\n".join(summary_lines)

    def _parse_attraction_details(self, args: dict, result: str) -> str:
        """Parse attraction detail query (text format)."""
        name = args.get('attraction_name', '?')

        detail = {
            'name': name,
            'price': '?', 'open': '?', 'close': '?',
            'visit_min': '?', 'visit_max': '?',
            'lat': '?', 'lon': '?', 'rating': '?',
            'closed_dates': '?', 'type': '?',
        }

        # Parse key-value lines (supports both EN and ZH)
        for line in result.split('\n'):
            line = line.strip()

            # Ticket price
            if 'Ticket Price' in line or '门票价格' in line:
                match = re.search(r'[\d.]+', line)
                if match:
                    detail['price'] = match.group()

            # Opening hours
            if ('Opening Hours' in line or '开放时间' in line):
                time_matches = re.findall(r'\d{2}:\d{2}', line)
                if len(time_matches) >= 2:
                    detail['open'] = time_matches[0]
                    detail['close'] = time_matches[1]

            # Visit duration
            if 'Minimum Visit' in line or '最短游玩' in line:
                match = re.search(r'[\d.]+', line)
                if match:
                    detail['visit_min'] = match.group()
            if 'Maximum Visit' in line or '最长游玩' in line:
                match = re.search(r'[\d.]+', line)
                if match:
                    detail['visit_max'] = match.group()

            # Coordinates
            if 'Latitude' in line or '纬度' in line:
                lat_match = re.search(r'(?:Latitude|纬度)\s*[：:]\s*([\d.]+)', line)
                lon_match = re.search(r'(?:Longitude|经度)\s*[：:]\s*([\d.]+)', line)
                if not lat_match:
                    lat_match = re.search(r'(?:Latitude|纬度)\s+([\d.]+)', line)
                if not lon_match:
                    lon_match = re.search(r'(?:Longitude|经度)\s+([\d.]+)', line)
                if lat_match:
                    detail['lat'] = lat_match.group(1)
                if lon_match:
                    detail['lon'] = lon_match.group(1)
            elif 'Coordinates' in line or '经纬度' in line or '坐标' in line:
                nums = re.findall(r'[\d.]+', line)
                if len(nums) >= 2:
                    detail['lat'] = nums[0]
                    detail['lon'] = nums[1]

            # Rating
            if 'Rating' in line or '评分' in line:
                match = re.search(r'([\d.]+)', line)
                if match:
                    detail['rating'] = match.group(1)

            # Closed dates
            if 'Closed' in line or '闭馆' in line:
                parts = line.split(':', 1) if ':' in line else line.split('：', 1)
                if len(parts) > 1:
                    detail['closed_dates'] = parts[1].strip()

            # Type
            if 'Attraction Type' in line or '景点类型' in line:
                parts = line.split(':', 1) if ':' in line else line.split('：', 1)
                if len(parts) > 1:
                    detail['type'] = parts[1].strip()

        self.attractions_detail[name] = detail
        # Store coordinates
        if detail['lat'] != '?' and detail['lon'] != '?':
            self.locations[name] = {'lat': detail['lat'], 'lon': detail['lon']}

        return (f"[Stored] Attraction detail: {name}\n"
                f"  Price: ¥{detail['price']}/person, Hours: {detail['open']}-{detail['close']}, "
                f"Visit: {detail['visit_min']}-{detail['visit_max']}hrs, "
                f"Rating: {detail['rating']}, Coords: ({detail['lat']}, {detail['lon']}), "
                f"Closed: {detail['closed_dates']}")

    def _parse_location(self, args: dict, result: str) -> str:
        """Parse location search result."""
        place_name = args.get('place_name', '?')

        try:
            data = json.loads(result)
        except json.JSONDecodeError:
            return f"[Stored] Location '{place_name}': Not found"

        if 'not found' in result.lower() or 'error' in result.lower():
            return f"[Stored] Location '{place_name}': Not found — use exact names from other tool results"

        lat = data.get('latitude', '?')
        lon = data.get('longitude', '?')

        if lat != '?' and lon != '?':
            self.locations[place_name] = {'lat': lat, 'lon': lon}

        return f"[Stored] Location '{place_name}': coords=({lat}, {lon})"

    def _parse_restaurants(self, args: dict, result: str) -> str:
        """Parse restaurant recommendations."""
        near = args.get('near', None)
        lat = args.get('latitude', '?')
        lon = args.get('longitude', '?')

        # Determine location label
        if near:
            near_location = near
        else:
            near_location = self._find_location_by_coords(lat, lon)

        # Strip resolution note prefix if present (from compound tool)
        result_body = result
        if result.startswith('[Auto-resolved') or result.startswith('[已自动解析'):
            # The first line is the resolution note, rest is JSON
            lines = result.split('\n', 1)
            if len(lines) > 1:
                result_body = lines[1]

        if 'No recommended' in result_body or '未找到' in result_body or 'not found' in result_body.lower() or 'Location' in result_body and 'not found' in result_body:
            # Suggest exact coordinates from known locations
            suggestions = self._suggest_nearby_coords(lat, lon)
            location_info = near or f"({lat}, {lon})"
            msg = f"[Stored] Restaurants near {location_info}"
            if near_location and not near:
                msg += f" [{near_location}]"
            msg += ": NONE FOUND"
            if suggestions:
                msg += f"\n  💡 Try these exact coordinates from your queried locations:\n{suggestions}"
            return msg

        try:
            data = json.loads(result_body)
        except json.JSONDecodeError:
            location_info = near or f"({lat}, {lon})"
            return f"[Stored] Restaurants near {location_info}: Parse error"

        if not isinstance(data, list):
            location_info = near or f"({lat}, {lon})"
            return f"[Stored] Restaurants near {location_info}: No results"

        for r in data:
            if not isinstance(r, dict):
                continue
            restaurant = {
                'name': r.get('name', '?'),
                'price_per_person': r.get('price_per_person', '?'),
                'cuisine': r.get('cuisine', '?'),
                'opening_time': r.get('opening_time', '?'),
                'closing_time': r.get('closing_time', '?'),
                'rating': r.get('rating', '?'),
                'lat': r.get('latitude', '?'),
                'lon': r.get('longitude', '?'),
                'near_attraction': r.get('nearby_attraction_name', '?'),
                'tags': r.get('tags', []),
            }
            self.restaurants.append(restaurant)
            # Store coordinates
            if restaurant['lat'] != '?' and restaurant['lon'] != '?':
                self.locations[restaurant['name']] = {
                    'lat': restaurant['lat'],
                    'lon': restaurant['lon']
                }

        location_info = near or f"({lat}, {lon})"
        location_label = f" [{near_location}]" if near_location and not near else ""
        summary_lines = [f"[Stored] Restaurants near {location_info}{location_label}: {len(data)} found"]
        for r in data:
            name = r.get('name', '?')
            price = r.get('price_per_person', '?')
            cuisine = r.get('cuisine', '?')
            hours = f"{r.get('opening_time', '?')}-{r.get('closing_time', '?')}"
            summary_lines.append(f"  {name}: ¥{price}/person, {cuisine}, {hours}")
        return "\n".join(summary_lines)

    def _parse_restaurant_details(self, args: dict, result: str) -> str:
        """Parse restaurant detail query."""
        name = args.get('restaurant_name', '?')

        try:
            data = json.loads(result)
        except json.JSONDecodeError:
            return f"[Stored] Restaurant detail '{name}': Parse error"

        if 'message' in data and 'not found' in str(data.get('message', '')).lower():
            return f"[Stored] Restaurant detail '{name}': Not found"

        detail = {
            'name': data.get('name', name),
            'price_per_person': data.get('price_per_person', '?'),
            'cuisine': data.get('cuisine', '?'),
            'opening_time': data.get('opening_time', '?'),
            'closing_time': data.get('closing_time', '?'),
            'rating': data.get('rating', '?'),
            'lat': data.get('latitude', '?'),
            'lon': data.get('longitude', '?'),
            'near_attraction': data.get('nearby_attraction_name', '?'),
            'tags': data.get('tags', []),
        }
        self.restaurant_details[name] = detail
        if detail['lat'] != '?' and detail['lon'] != '?':
            self.locations[name] = {'lat': detail['lat'], 'lon': detail['lon']}

        tags_str = ', '.join(detail['tags']) if detail['tags'] else 'none'
        return (f"[Stored] Restaurant detail: {name}\n"
                f"  ¥{detail['price_per_person']}/person, {detail['cuisine']}, "
                f"Hours: {detail['opening_time']}-{detail['closing_time']}, "
                f"Rating: {detail['rating']}, Tags: {tags_str}")

    def _parse_route(self, args: dict, result: str) -> str:
        """Parse road route query result."""
        origin = args.get('origin', '?')
        dest = args.get('destination', '?')
        origin_place = args.get('origin_place', None)
        destination_place = args.get('destination_place', None)

        # Strip resolution note prefix if present (from compound tool)
        result_body = result
        if result.startswith('[Auto-resolved') or result.startswith('[已自动解析'):
            # Resolution notes may be multiple lines before JSON
            lines = result.split('\n')
            json_start = 0
            for i, line in enumerate(lines):
                if line.strip().startswith('{'):
                    json_start = i
                    break
            result_body = '\n'.join(lines[json_start:])

        try:
            data = json.loads(result_body)
        except json.JSONDecodeError:
            label = f"{origin_place or origin} → {destination_place or dest}"
            return f"[Stored] Route {label}: Parse error or not found"

        # Use place names if provided, otherwise resolve from coordinates
        origin_name = origin_place or self._find_location_by_coords_str(origin)
        dest_name = destination_place or self._find_location_by_coords_str(dest)

        # Get actual coordinates from the result
        actual_origin = data.get('origin', origin)
        actual_dest = data.get('destination', dest)

        route = {
            'origin_coords': actual_origin,
            'dest_coords': actual_dest,
            'origin_name': origin_name,
            'dest_name': dest_name,
            'distance_m': data.get('distance_in_meters', '?'),
            'duration_min': data.get('duration_in_minutes', '?'),
            'cost': data.get('cost', '?'),
        }
        self.routes.append(route)

        origin_label = f"{origin_name} " if origin_name else ""
        dest_label = f"{dest_name} " if dest_name else ""

        distance_km = f"{int(route['distance_m'])/1000:.1f}km" if isinstance(route['distance_m'], (int, float)) else '?'

        return (f"[Stored] Route: {origin_label}({actual_origin}) → {dest_label}({actual_dest})\n"
                f"  Distance: {distance_km}, Duration: {route['duration_min']}min, Cost: ¥{route['cost']}")

    def _find_location_by_coords(self, lat: str, lon: str) -> Optional[str]:
        """Find a location name by approximate coordinate match."""
        try:
            lat_f = float(lat)
            lon_f = float(lon)
        except (ValueError, TypeError):
            return None

        for name, coords in self.locations.items():
            try:
                if abs(float(coords['lat']) - lat_f) < 0.001 and abs(float(coords['lon']) - lon_f) < 0.001:
                    return name
            except (ValueError, TypeError):
                continue
        return None

    def _find_location_by_coords_str(self, coord_str: str) -> Optional[str]:
        """Find location name from 'lat,lon' coordinate string."""
        parts = coord_str.split(',')
        if len(parts) == 2:
            return self._find_location_by_coords(parts[0].strip(), parts[1].strip())
        return None

    def _suggest_nearby_coords(self, lat: str, lon: str) -> str:
        """Suggest known coordinates near a failed query."""
        if not self.locations:
            return ""
        lines = []
        for name, coords in self.locations.items():
            lines.append(f"    {name}: ({coords['lat']}, {coords['lon']})")
        return "\n".join(lines[:8])  # Show up to 8 suggestions

    @staticmethod
    def _meal_slot_tag(opening: str, closing: str) -> str:
        """Return a meal-slot tag like ' [lunch+dinner]' based on hours."""
        try:
            open_h = int(opening.split(':')[0])
            close_h = int(closing.split(':')[0])
            close_m = int(closing.split(':')[1]) if ':' in closing else 0
        except (ValueError, IndexError):
            return ''
        # Lunch: restaurant must be open during 11:00-14:00 window
        lunch_ok = open_h < 14 and (close_h > 11 or (close_h == 11 and close_m > 0))
        # Dinner: restaurant must be open during 17:00-21:00 window
        dinner_ok = open_h < 21 and (close_h > 17 or (close_h == 17 and close_m > 0))
        if lunch_ok and dinner_ok:
            return ' [lunch+dinner]'
        elif lunch_ok:
            return ' [lunch only]'
        elif dinner_ok:
            return ' [dinner only]'
        return ''

    def autocorrect_travel_city(self, plan_text: str) -> str:
        """Auto-correct travel_city lines in a plan using stored route data.

        For each travel_city line, look up the origin→dest pair in self.routes
        and replace distance, duration, and cost with the stored values.
        Also adjusts the end-time of the activity to match the corrected duration.
        """
        if not self.routes or not plan_text:
            return plan_text

        # Build lookup: (origin_name, dest_name) -> route dict
        # Also build reverse lookup (dest→origin) since routes may be one-way
        route_lookup = {}
        for r in self.routes:
            o = r.get('origin_name', '')
            d = r.get('dest_name', '')
            if o and d:
                route_lookup[(o, d)] = r
                # Don't auto-add reverse — asymmetric routes are common

        # Pattern for travel_city lines:
        # HH:MM-HH:MM | travel_city | Origin - Destination, Xkm, Ymin, ¥Z
        travel_city_re = re.compile(
            r'^(\d{2}:\d{2})-(\d{2}:\d{2})\s*\|\s*travel_city\s*\|\s*(.+?)\s*-\s*(.+?),\s*[\d.]+km,\s*(\d+)min,\s*¥([\d.]+)',
            re.MULTILINE
        )

        def _replace_line(match):
            start_time = match.group(1)
            origin = match.group(2).strip()  # group(2) is old end time, but we also need origin
            # Actually re-parse — the regex groups are positional
            return match.group(0)  # fallback

        # More careful line-by-line approach
        lines = plan_text.split('\n')
        corrected_lines = []
        for line in lines:
            m = travel_city_re.match(line.strip())
            if m:
                start_str = m.group(1)
                _end_str = m.group(2)
                origin = m.group(3).strip()
                dest = m.group(4).strip()

                route = route_lookup.get((origin, dest)) or route_lookup.get((dest, origin))
                if route:
                    dur = int(route['duration_min']) if isinstance(route['duration_min'], (int, float)) else None
                    dist_m = route['distance_m']
                    cost = route['cost']
                    if dur is not None and isinstance(dist_m, (int, float)):
                        dist_km = f"{dist_m / 1000:.1f}km"
                        # Compute new end time
                        sh, sm = int(start_str[:2]), int(start_str[3:])
                        total_min = sh * 60 + sm + dur
                        eh, em = divmod(total_min, 60)
                        if eh >= 24:
                            eh = 23; em = 59  # clamp to end of day
                        new_end = f"{eh:02d}:{em:02d}"
                        leading = line[:len(line) - len(line.lstrip())]
                        corrected_lines.append(
                            f"{leading}{start_str}-{new_end} | travel_city | {origin} - {dest}, {dist_km}, {dur}min, ¥{cost}"
                        )
                        continue
            corrected_lines.append(line)

        return '\n'.join(corrected_lines)

    def _render_routes_matrix(self) -> str:
        """Render routes as a compact distance matrix grouped by origin."""
        # Build lookup: (origin_name, dest_name) -> route info
        from collections import OrderedDict
        by_origin = OrderedDict()  # origin -> [(dest, duration, dist_km, cost)]
        for r in self.routes:
            origin = r['origin_name'] or r['origin_coords']
            dest = r['dest_name'] or r['dest_coords']
            dur = r['duration_min']
            dist = f"{int(r['distance_m'])/1000:.1f}" if isinstance(r['distance_m'], (int, float)) else '?'
            cost = r['cost']
            by_origin.setdefault(origin, []).append((dest, dur, dist, cost))

        lines = ["ROUTES (from → to: duration, distance, cost):"]
        for origin, dests in by_origin.items():
            entries = [f"{d[0]} {d[1]}min/{d[2]}km/¥{d[3]}" for d in dests]
            lines.append(f"  From {origin}:")
            # Wrap entries ~2 per line to keep it readable
            row = []
            for e in entries:
                row.append(e)
                if len(row) == 2:
                    lines.append(f"    {' | '.join(row)}")
                    row = []
            if row:
                lines.append(f"    {' | '.join(row)}")
        return "\n".join(lines)

    def render_snapshot(self) -> str:
        """
        Render the current memory state as a compact, readable string.
        This gets appended to tool responses so the model always sees current state.
        """
        sections = []

        # Flights
        if self.flights:
            lines = ["FLIGHTS:"]
            for entry in self.flights:
                lines.append(f"  {entry['origin']}→{entry['dest']} ({entry['date']}): {len(entry['options'])} options")
                for o in entry['options']:
                    lines.append(f"    {o['flight_no']}: {o['dep_time']}→{o['arr_time']}, {o['dep_station']}→{o['arr_station']}, {o['duration']}min, ¥{o['price']}/person")
            sections.append("\n".join(lines))

        # Trains
        if self.trains:
            lines = ["TRAINS:"]
            for entry in self.trains:
                lines.append(f"  {entry['origin']}→{entry['dest']} ({entry['date']}): {len(entry['options'])} options")
                for o in entry['options']:
                    lines.append(f"    {o['train_no']}: {o['dep_time']}→{o['arr_time']}, {o['dep_station']}→{o['arr_station']}, {o['duration']}min, ¥{o['price']}/person")
            sections.append("\n".join(lines))

        # Hotels
        if self.hotels:
            lines = ["HOTELS:"]
            for h in self.hotels:
                lines.append(f"  {h['name']}: ¥{h['price']}/night, {h['star']}-star, rating {h['rating']}, coords=({h['lat']}, {h['lon']})")
            sections.append("\n".join(lines))

        # Attractions
        if self.attractions_summary or self.attractions_detail:
            lines = ["ATTRACTIONS:"]
            # Show all known attractions with detail status
            all_names = set()
            for a in self.attractions_summary:
                all_names.add(a['name'])
            for name in self.attractions_detail:
                all_names.add(name)

            for name in sorted(all_names):
                if name in self.attractions_detail:
                    d = self.attractions_detail[name]
                    lines.append(f"  {name}: ¥{d['price']}/person, {d['open']}-{d['close']}, visit {d['visit_min']}-{d['visit_max']}hrs, coords=({d['lat']}, {d['lon']})")
                else:
                    # Only from recommend_attractions, no details yet
                    summary = next((a for a in self.attractions_summary if a['name'] == name), {})
                    lines.append(f"  {name} ({summary.get('type', '?')}): ⚠ DETAILS NOT QUERIED — call query_attraction_details(\"{name}\")")
            sections.append("\n".join(lines))

        # Restaurants
        if self.restaurants:
            lines = ["RESTAURANTS:"]
            seen = set()
            for r in self.restaurants:
                if r['name'] in seen:
                    continue
                seen.add(r['name'])
                meal_tag = self._meal_slot_tag(r['opening_time'], r['closing_time'])
                lines.append(f"  {r['name']}: ¥{r['price_per_person']}/person, {r['cuisine']}, {r['opening_time']}-{r['closing_time']}{meal_tag}, near {r['near_attraction']}")
            sections.append("\n".join(lines))

        # Locations (compact)
        if self.locations:
            lines = ["KNOWN COORDINATES:"]
            for name, coords in self.locations.items():
                lines.append(f"  {name}: ({coords['lat']}, {coords['lon']})")
            sections.append("\n".join(lines))

        # Routes — render as compact distance matrix
        if self.routes:
            sections.append(self._render_routes_matrix())

        # Constraints (Phase 1) — show at top if available
        if self._constraints_rendered:
            sections.insert(0, self._constraints_rendered)

        if not sections:
            return "═══ WORKING MEMORY: Empty (no data collected yet) ═══"

        header = "═══ WORKING MEMORY (all collected data) ═══"
        footer = "═══ END WORKING MEMORY ═══"
        return f"{header}\n" + "\n\n".join(sections) + f"\n{footer}"

    # ------------------------------------------------------------------
    # assemble_day: deterministic day-plan builder
    # ------------------------------------------------------------------

    def assemble_day(self, args: Dict[str, Any]) -> str:
        """Build a fully-formatted day plan from a sequence of activities.

        The model specifies WHAT to do (which places, which order).
        This method computes all timestamps, durations, distances, and costs
        deterministically from stored working-memory data.

        Args (JSON from model):
            day: int — day number (1-indexed)
            current_city: str — e.g. "from Shanghai to Beijing" or "Beijing"
            accommodation: str — hotel name (or "-" for departure day)
            accommodation_price: str — e.g. "¥200/room/night" (or "-")
            activities: list of dicts, each with:
                type: "intercity" | "attraction" | "meal" | "hotel" | "buffer"
                --- for intercity ---
                transport_type: "train" | "flight"
                id: str — train/flight number (e.g. "G7798")
                --- for attraction ---
                name: str — exact attraction name from memory
                --- for meal ---
                meal_type: "Lunch" | "Dinner"
                restaurant: str — exact restaurant name from memory
                --- for hotel ---
                action: "Check-in" | "Check-out" | "Rest"
                --- for buffer ---
                description: str — e.g. "Deplaning, baggage claim"
                duration_min: int — buffer duration in minutes

        Returns:
            Formatted day plan text, or error string starting with "ERROR:"
        """
        try:
            return self._assemble_day_impl(args)
        except Exception as e:
            return f"ERROR: {e}"

    def _assemble_day_impl(self, args: Dict[str, Any]) -> str:
        day_num = args.get('day', 1)
        current_city = args.get('current_city', '')
        accommodation = args.get('accommodation', '-')
        accommodation_price = args.get('accommodation_price', '-')
        activities = args.get('activities', [])

        if not activities:
            return "ERROR: No activities provided."

        errors = []
        lines = []
        lines.append(f"Day {day_num}:")
        lines.append(f"Current City: {current_city}")
        lines.append(f"Accommodation: {accommodation}, {accommodation_price}" if accommodation != '-' else "Accommodation: -")

        cursor = None  # datetime — current time
        current_location = None  # name of where we are

        # If the first activity is not intercity, we need a default start time.
        # Try to infer the date from any known train/flight, else use a fallback.
        default_morning = None
        for entry in (self.trains + self.flights):
            for opt in entry.get('options', []):
                try:
                    dt = datetime.strptime(opt['dep_time'], '%Y-%m-%d %H:%M:%S')
                    default_morning = dt.replace(hour=7, minute=0, second=0)
                    break
                except (ValueError, KeyError):
                    continue
            if default_morning:
                break
        if default_morning is None:
            default_morning = datetime(2025, 11, 12, 7, 0, 0)  # fallback

        for i, act in enumerate(activities):
            act_type = act.get('type', '')

            # If cursor is still None and this isn't an intercity arrival,
            # start the day at 07:00 from the hotel.
            if cursor is None and act_type != 'intercity':
                cursor = default_morning
                if accommodation != '-':
                    current_location = accommodation

            if act_type == 'intercity':
                result = self._assemble_intercity(act, cursor)
                if result.get('error'):
                    errors.append(result['error'])
                    continue
                lines.append(result['line'])
                cursor = result['end_time']
                current_location = result['end_location']

            elif act_type == 'buffer':
                desc = act.get('description', 'Buffer')
                dur = int(act.get('duration_min', 30))
                if cursor is None:
                    errors.append(f"Buffer '{desc}' has no preceding activity to set start time.")
                    continue
                start = cursor
                end = start + timedelta(minutes=dur)
                lines.append(f"{self._fmt(start)}-{self._fmt(end)} | buffer | {desc}")
                cursor = end
                # location unchanged

            elif act_type == 'hotel':
                action = act.get('action', 'Rest')
                hotel_name = accommodation if accommodation != '-' else act.get('name', 'Hotel')
                dur = int(act.get('duration_min', 50 if action == 'Check-in' else 60))

                # If we need to travel to the hotel first
                if current_location and current_location != hotel_name:
                    travel_result = self._assemble_travel_city(current_location, hotel_name, cursor)
                    if travel_result.get('error'):
                        errors.append(travel_result['error'])
                    else:
                        lines.append(travel_result['line'])
                        cursor = travel_result['end_time']
                        current_location = hotel_name

                if cursor is None:
                    errors.append(f"Hotel '{action}' has no start time.")
                    continue
                start = cursor
                end = start + timedelta(minutes=dur)
                lines.append(f"{self._fmt(start)}-{self._fmt(end)} | hotel | {action}, {hotel_name}")
                cursor = end
                current_location = hotel_name

            elif act_type == 'attraction':
                name = act.get('name', '')
                detail = self.attractions_detail.get(name)
                if not detail:
                    errors.append(f"Attraction '{name}' not found in memory. Call query_attraction_details first.")
                    continue

                # Travel to attraction
                if current_location and current_location != name:
                    travel_result = self._assemble_travel_city(current_location, name, cursor)
                    if travel_result.get('error'):
                        errors.append(travel_result['error'])
                    else:
                        lines.append(travel_result['line'])
                        cursor = travel_result['end_time']

                if cursor is None:
                    errors.append(f"Attraction '{name}' has no start time.")
                    continue

                # Duration: use midpoint of min-max range
                visit_min = float(detail.get('visit_min', 1))
                visit_max = float(detail.get('visit_max', 2))
                visit_hrs = round((visit_min + visit_max) / 2, 1)
                # Clamp to within opening hours
                open_time = self._parse_time(detail.get('open', '08:00'), cursor, default_h=0, default_m=0)
                close_time = self._parse_time(detail.get('close', '22:00'), cursor, default_h=23, default_m=59)

                start = max(cursor, open_time)
                end = start + timedelta(hours=visit_hrs)
                if end > close_time:
                    end = close_time
                    actual_hrs = (end - start).total_seconds() / 3600
                    if actual_hrs < visit_min:
                        errors.append(f"Attraction '{name}' closes at {detail.get('close')} — not enough time (need {visit_min}h, only {actual_hrs:.1f}h available).")

                price_str = f"¥{detail.get('price', 0)}/person"
                lines.append(f"{self._fmt(start)}-{self._fmt(end)} | attraction | {name}, {price_str}")
                cursor = end
                current_location = name

            elif act_type == 'meal':
                meal_type = act.get('meal_type', 'Lunch')
                restaurant = act.get('restaurant', '')

                # Look up restaurant data
                r_data = self._find_restaurant(restaurant)
                if not r_data:
                    errors.append(f"Restaurant '{restaurant}' not found in memory.")
                    continue

                # Travel to restaurant
                if current_location and current_location != restaurant:
                    travel_result = self._assemble_travel_city(current_location, restaurant, cursor)
                    if travel_result.get('error'):
                        errors.append(travel_result['error'])
                    else:
                        lines.append(travel_result['line'])
                        cursor = travel_result['end_time']

                if cursor is None:
                    errors.append(f"Meal at '{restaurant}' has no start time.")
                    continue

                # Check business hours
                r_open = self._parse_time(r_data.get('opening_time', '08:00'), cursor, default_h=0, default_m=0)
                r_close = self._parse_time(r_data.get('closing_time', '22:00'), cursor, default_h=23, default_m=59)
                start = max(cursor, r_open)
                dur_min = int(act.get('duration_min', 60))
                dur_min = max(60, min(120, dur_min))  # clamp 1-2 hours
                end = start + timedelta(minutes=dur_min)

                if start >= r_close:
                    errors.append(f"Restaurant '{restaurant}' closes at {r_data.get('closing_time')} — cannot schedule {meal_type} at {self._fmt(start)}.")
                    continue
                if end > r_close:
                    end = r_close
                    if (end - start).total_seconds() < 3600:
                        errors.append(f"Restaurant '{restaurant}' closes at {r_data.get('closing_time')} — not enough time for {meal_type}.")
                        continue

                price_str = f"¥{r_data.get('price_per_person', '?')}/person"
                lines.append(f"{self._fmt(start)}-{self._fmt(end)} | meal | {meal_type}, {restaurant}, {price_str}")
                cursor = end
                current_location = restaurant

            else:
                errors.append(f"Unknown activity type: '{act_type}'")

        result = "\n".join(lines)
        if errors:
            result += "\n\n⚠️ ASSEMBLY ERRORS:\n" + "\n".join(f"  - {e}" for e in errors)
        return result

    def _assemble_intercity(self, act: Dict, cursor: Optional[datetime]) -> Dict:
        """Assemble an intercity transport line from memory."""
        transport_type = act.get('transport_type', 'train')
        transport_id = act.get('id', '')

        # Search in flights or trains
        source = self.trains if transport_type == 'train' else self.flights
        for entry in source:
            for opt in entry.get('options', []):
                key = 'train_no' if transport_type == 'train' else 'flight_no'
                if opt.get(key) == transport_id:
                    dep = datetime.strptime(opt['dep_time'], '%Y-%m-%d %H:%M:%S')
                    arr = datetime.strptime(opt['arr_time'], '%Y-%m-%d %H:%M:%S')
                    label = 'train' if transport_type == 'train' else 'flight'
                    line = (
                        f"{self._fmt(dep)}-{self._fmt(arr)} | travel_intercity_public | "
                        f"{label} {transport_id}, {opt['dep_station']} - {opt['arr_station']}, "
                        f"¥{opt['price']}/person"
                    )
                    return {'line': line, 'end_time': arr, 'end_location': opt['arr_station'], 'dep_time': dep}

        return {'error': f"{transport_type.capitalize()} '{transport_id}' not found in memory."}

    def _assemble_travel_city(self, origin: str, dest: str, cursor: Optional[datetime]) -> Dict:
        """Look up a route between two places and format a travel_city line."""
        if cursor is None:
            return {'error': f"Cannot compute travel from '{origin}' to '{dest}' — no current time."}

        # Search routes in both directions
        route = None
        for r in self.routes:
            o = r.get('origin_name', '')
            d = r.get('dest_name', '')
            if (o == origin and d == dest) or (o == dest and d == origin):
                route = r
                break

        if not route:
            return {'error': f"No route found: {origin} → {dest}. Query query_road_route_info first."}

        dur = int(route['duration_min']) if isinstance(route['duration_min'], (int, float)) else 10
        dist_m = route['distance_m'] if isinstance(route['distance_m'], (int, float)) else 0
        cost = route['cost'] if isinstance(route['cost'], (int, float)) else 0
        dist_km = f"{dist_m / 1000:.1f}km"

        start = cursor
        end = start + timedelta(minutes=dur)
        line = f"{self._fmt(start)}-{self._fmt(end)} | travel_city | {origin} - {dest}, {dist_km}, {dur}min, ¥{cost}"
        return {'line': line, 'end_time': end}

    def _find_restaurant(self, name: str) -> Optional[Dict]:
        """Find a restaurant by name in memory."""
        # Check detailed first, then list
        if name in self.restaurant_details:
            return self.restaurant_details[name]
        for r in self.restaurants:
            if r['name'] == name:
                return r
        return None

    @staticmethod
    def _fmt(dt: datetime) -> str:
        """Format datetime as HH:MM."""
        return dt.strftime('%H:%M')

    @staticmethod
    def _parse_time(time_str: str, reference_date: datetime, default_h: int = 8, default_m: int = 0) -> datetime:
        """Parse HH:MM string into a datetime on the same date as reference."""
        try:
            h, m = int(time_str.split(':')[0]), int(time_str.split(':')[1])
        except (ValueError, IndexError):
            h, m = default_h, default_m
        return reference_date.replace(hour=h, minute=m, second=0, microsecond=0)
