"""
Working Memory for Travel Planning Agent

Maintains a structured data store that accumulates tool results across turns.
After each tool call, the raw response is parsed and stored in the memory.
The model sees a clean, organized summary instead of raw JSON/text.

Design: Option C — replaces raw tool output with structured memory snapshot.
"""

import json
import re
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
                lines.append(f"  {r['name']}: ¥{r['price_per_person']}/person, {r['cuisine']}, {r['opening_time']}-{r['closing_time']}, near {r['near_attraction']}")
            sections.append("\n".join(lines))

        # Locations (compact)
        if self.locations:
            lines = ["KNOWN COORDINATES:"]
            for name, coords in self.locations.items():
                lines.append(f"  {name}: ({coords['lat']}, {coords['lon']})")
            sections.append("\n".join(lines))

        # Routes
        if self.routes:
            lines = ["ROUTES:"]
            for r in self.routes:
                origin_label = r['origin_name'] or r['origin_coords']
                dest_label = r['dest_name'] or r['dest_coords']
                dist = f"{int(r['distance_m'])/1000:.1f}km" if isinstance(r['distance_m'], (int, float)) else '?'
                lines.append(f"  {origin_label} → {dest_label}: {dist}, {r['duration_min']}min, ¥{r['cost']}")
            sections.append("\n".join(lines))

        if not sections:
            return "═══ WORKING MEMORY: Empty (no data collected yet) ═══"

        header = "═══ WORKING MEMORY (all collected data) ═══"
        footer = "═══ END WORKING MEMORY ═══"
        return f"{header}\n" + "\n\n".join(sections) + f"\n{footer}"
