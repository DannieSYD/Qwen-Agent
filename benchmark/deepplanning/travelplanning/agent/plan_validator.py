"""
Plan Validator - Post-hoc validation of generated plans against tool query results.

Extracts entities from the plan text (hotels, restaurants, attractions, flights, trains)
and cross-references them against entities returned by tool calls in the message history.
Returns a validation report listing any hallucinated (unqueried) entities.

Validation levels:
1. Entity name check: is the entity name found in tool results?
2. Detail query check: was query_attraction_details called for attractions used in the plan?
3. Price check: does the price in the plan match the price from tool results?

This module is designed to be called programmatically after plan generation,
enabling an automatic correction loop.
"""

import json
import re
from typing import Dict, List, Set, Tuple, Optional, Any


# =============================================================================
# Entity extraction from tool responses
# =============================================================================

def extract_tool_entities(messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Extract all entity names and price data from tool call responses.

    Returns:
        Dict with:
        - 'hotels', 'attractions', 'restaurants', 'flights', 'trains': sets of names
        - 'flight_routes', 'train_routes': sets of route strings
        - 'prices': dict of {entity_name: price} for price validation
        - 'attraction_details_queried': set of attraction names with detail queries
    """
    entities = {
        'hotels': set(),
        'attractions': set(),
        'restaurants': set(),
        'flights': set(),
        'trains': set(),
        'flight_routes': set(),
        'train_routes': set(),
    }

    # Price tracking: {entity_name: price_value}
    prices = {
        'hotels': {},        # name -> price per night
        'attractions': {},   # name -> ticket price
        'restaurants': {},   # name -> price per person
        'flights': {},       # flight_no -> price per person
        'trains': {},        # train_no -> price per person
    }

    # Track which attractions had detail queries (not just recommendations)
    attraction_details_queried = set()

    for msg in messages:
        role = msg.get('role') if isinstance(msg, dict) else getattr(msg, 'role', None)
        if role != 'tool':
            continue

        tool_name = msg.get('name', '') if isinstance(msg, dict) else getattr(msg, 'name', '')
        content = msg.get('content', '') if isinstance(msg, dict) else getattr(msg, 'content', '')

        if not content or not isinstance(content, str):
            continue

        # Check if this is working memory format (starts with "[Stored]")
        is_memory_format = content.strip().startswith('[Stored]') or content.strip().startswith('[Memory]')

        if is_memory_format:
            # Strip the working memory snapshot (everything after ═══ WORKING MEMORY)
            # The snapshot is appended to every response but only the [Stored] part is tool-specific
            memory_separator = '═══ WORKING MEMORY'
            if memory_separator in content:
                content = content[:content.index(memory_separator)].strip()
            # Parse working memory format for all tool types
            _extract_from_memory_format(tool_name, content, entities, prices, attraction_details_queried)
            continue

        # Try to parse as JSON (original format)
        try:
            data = json.loads(content)
        except (json.JSONDecodeError, TypeError):
            data = None

        if tool_name == 'query_hotel_info' and data:
            _extract_hotel_info(data, entities['hotels'], prices['hotels'])

        elif tool_name == 'recommend_attractions':
            _extract_attraction_names(content, data, entities['attractions'])

        elif tool_name == 'query_attraction_details':
            _extract_attraction_details(content, data, entities['attractions'],
                                        prices['attractions'], attraction_details_queried)

        elif tool_name in ('recommend_restaurants', 'query_restaurant_details'):
            _extract_restaurant_info(content, data, entities['restaurants'], prices['restaurants'])

        elif tool_name == 'query_flight_info' and data:
            _extract_flight_info(data, entities['flights'], entities['flight_routes'], prices['flights'])

        elif tool_name == 'query_train_info' and data:
            _extract_train_info(data, entities['trains'], entities['train_routes'], prices['trains'])

    return {
        **entities,
        'prices': prices,
        'attraction_details_queried': attraction_details_queried,
    }


def _extract_from_memory_format(
    tool_name: str,
    content: str,
    entities: Dict[str, Set[str]],
    prices: Dict[str, Dict[str, float]],
    attraction_details_queried: Set[str],
):
    """
    Extract entities from working memory formatted tool responses.

    Working memory format examples:
      [Stored] Hotels in Nanjing: 4 options
        Home Inn: ¥441/night, 3-star, rating 4.5, coords=(32.038, 118.786)
      [Stored] Trains Hefei→Nanjing (2025-11-12): 3 options
        G7222: 2025-11-12 06:36→07:24, Hefei South→Nanjing South, 48min, ¥73.0/person
      [Stored] Attractions in Nanjing: 5 found
        Nanjing Museum (Historical) [⚠ details NOT yet queried]
      [Stored] Attraction detail: Nanjing Museum
        Price: ¥0.0/person, Hours: 09:00-17:00, Visit: 2.0-3.0hrs
      [Stored] Restaurants near (lat, lon) [Hotel Name]: 4 found
        Restaurant Name: ¥100.0/person, Chinese, 11:00-21:00
      [Stored] Flights Shanghai→Xiamen (2025-11-12): 2 options
        MU5653: 2025-11-12 07:00→09:10, Shanghai Hongqiao→Xiamen Gaoqi, 130min, ¥500/person
    """
    lines = content.split('\n')

    if tool_name == 'query_hotel_info':
        for line in lines:
            line = line.strip()
            if line.startswith('[Stored]'):
                continue
            # "Hotel Name: ¥441/night, 3-star, ..."
            match = re.match(r'(.+?):\s*[¥￥]([\d.]+)/night', line)
            if match:
                name = match.group(1).strip()
                if name:
                    entities['hotels'].add(name)
                    try:
                        prices['hotels'][name] = float(match.group(2))
                    except (ValueError, TypeError):
                        pass

    elif tool_name == 'recommend_attractions':
        for line in lines:
            line = line.strip()
            if line.startswith('[Stored]'):
                continue
            # "Attraction Name (Type) [status]"
            match = re.match(r'(.+?)\s*\(', line)
            if match:
                name = match.group(1).strip()
                if name:
                    entities['attractions'].add(name)

    elif tool_name == 'query_attraction_details':
        # "[Stored] Attraction detail: Name"
        for line in lines:
            line = line.strip()
            if line.startswith('[Stored] Attraction detail:'):
                name = line.replace('[Stored] Attraction detail:', '').strip()
                if name:
                    entities['attractions'].add(name)
                    attraction_details_queried.add(name)
            # "Price: ¥80.0/person"
            price_match = re.search(r'Price:\s*[¥￥]([\d.]+)', line)
            if price_match and name:
                try:
                    prices['attractions'][name] = float(price_match.group(1))
                except (ValueError, TypeError):
                    pass

    elif tool_name in ('recommend_restaurants', 'query_restaurant_details'):
        for line in lines:
            line = line.strip()
            if line.startswith('[Stored]'):
                continue
            # "Restaurant Name: ¥100.0/person, cuisine, hours"
            match = re.match(r'(.+?):\s*[¥￥]([\d.]+)/person', line)
            if match:
                name = match.group(1).strip()
                if name:
                    entities['restaurants'].add(name)
                    try:
                        prices['restaurants'][name] = float(match.group(2))
                    except (ValueError, TypeError):
                        pass

    elif tool_name == 'query_flight_info':
        for line in lines:
            line = line.strip()
            if line.startswith('[Stored]'):
                continue
            # "MU5653: 2025-11-12 07:00→09:10, ..., ¥500/person"
            match = re.match(r'(\S+):\s*\d{4}-\d{2}-\d{2}', line)
            if match:
                flight_no = match.group(1).strip()
                if flight_no:
                    entities['flights'].add(flight_no)
                    price_match = re.search(r'[¥￥]([\d.]+)/person', line)
                    if price_match:
                        try:
                            prices['flights'][flight_no] = float(price_match.group(1))
                        except (ValueError, TypeError):
                            pass

    elif tool_name == 'query_train_info':
        for line in lines:
            line = line.strip()
            if line.startswith('[Stored]'):
                continue
            # "G7222: 2025-11-12 06:36→07:24, ..., ¥73.0/person"
            match = re.match(r'(\S+):\s*\d{4}-\d{2}-\d{2}', line)
            if match:
                train_no = match.group(1).strip()
                if train_no:
                    entities['trains'].add(train_no)
                    price_match = re.search(r'[¥￥]([\d.]+)/person', line)
                    if price_match:
                        try:
                            prices['trains'][train_no] = float(price_match.group(1))
                        except (ValueError, TypeError):
                            pass


def _extract_hotel_info(data: Any, hotel_set: Set[str], hotel_prices: Dict[str, float]):
    """Extract hotel names and prices from query_hotel_info response."""
    items = data if isinstance(data, list) else [data] if isinstance(data, dict) else []
    for item in items:
        if isinstance(item, dict) and 'name' in item:
            name = item['name'].strip()
            if name:
                hotel_set.add(name)
                price_str = item.get('price', '')
                try:
                    hotel_prices[name] = float(price_str)
                except (ValueError, TypeError):
                    pass


def _extract_attraction_names(content: str, data: Any, attraction_set: Set[str]):
    """Extract attraction names from recommend_attractions response (names only, no prices)."""
    # recommend_attractions returns text: "Name, Description. This is a..."
    for pattern in [
        r'(?:Recommended attractions|推荐的景点有)[：:]?\s*\n(.+)',
    ]:
        match = re.search(pattern, content, re.DOTALL)
        if match:
            lines = match.group(1).strip().split('\n')
            for line in lines:
                line = line.strip()
                if line:
                    name = line.split(',')[0].split('，')[0].strip()
                    if name:
                        attraction_set.add(name)

    # Also try JSON format
    if data and isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                for key in ('attraction_name', 'name'):
                    if key in item:
                        name = str(item[key]).strip()
                        if name:
                            attraction_set.add(name)


def _extract_attraction_details(content: str, data: Any, attraction_set: Set[str],
                                  attraction_prices: Dict[str, float],
                                  details_queried: Set[str]):
    """Extract attraction names and prices from query_attraction_details response."""
    name = None
    price = None

    # query_attraction_details returns formatted text:
    # "Attraction Name：Suzhou Museum" or "景点名称：故宫博物院"
    for pattern in [
        r'(?:Attraction Name|景点名称)[：:](.+)',
    ]:
        match = re.search(pattern, content)
        if match:
            name = match.group(1).strip()

    # Extract ticket price: "Ticket Price：60.0 yuan/person" or "门票价格：60.0 元/人"
    for pattern in [
        r'(?:Ticket Price|门票价格)[：:]\s*([\d.]+)',
    ]:
        match = re.search(pattern, content)
        if match:
            try:
                price = float(match.group(1))
            except (ValueError, TypeError):
                pass

    if name:
        attraction_set.add(name)
        details_queried.add(name)
        if price is not None:
            attraction_prices[name] = price

    # Also try JSON format
    if data and isinstance(data, dict):
        n = data.get('attraction_name') or data.get('name')
        if n:
            n = str(n).strip()
            attraction_set.add(n)
            details_queried.add(n)
            p = data.get('ticket_price') or data.get('price')
            if p is not None:
                try:
                    attraction_prices[n] = float(p)
                except (ValueError, TypeError):
                    pass


def _extract_restaurant_info(content: str, data: Any, restaurant_set: Set[str],
                              restaurant_prices: Dict[str, float]):
    """Extract restaurant names and prices from recommend_restaurants or query_restaurant_details."""
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                name = str(item.get('name', '') or item.get('restaurant_name', '')).strip()
                if name:
                    restaurant_set.add(name)
                    price_str = item.get('price_per_person') or item.get('average_cost') or item.get('price')
                    if price_str is not None:
                        try:
                            restaurant_prices[name] = float(price_str)
                        except (ValueError, TypeError):
                            pass
    elif isinstance(data, dict):
        name = str(data.get('name', '') or data.get('restaurant_name', '')).strip()
        if name:
            restaurant_set.add(name)
            price_str = data.get('price_per_person') or data.get('average_cost') or data.get('price')
            if price_str is not None:
                try:
                    restaurant_prices[name] = float(price_str)
                except (ValueError, TypeError):
                    pass

    # query_restaurant_details may return text format
    if not data:
        for pattern in [
            r'(?:Restaurant Name|餐厅名称)[：:](.+)',
        ]:
            match = re.search(pattern, content)
            if match:
                name = match.group(1).strip()
                if name:
                    restaurant_set.add(name)
                    # Try to find price
                    for pp in [r'(?:Per Person Price|Average Cost|人均消费|人均价格)[：:]\s*([\d.]+)']:
                        pm = re.search(pp, content)
                        if pm:
                            try:
                                restaurant_prices[name] = float(pm.group(1))
                            except (ValueError, TypeError):
                                pass


def _extract_flight_info(data: Any, flight_set: Set[str], route_set: Set[str],
                          flight_prices: Dict[str, float]):
    """Extract flight numbers, routes, and prices from query_flight_info response."""
    if not isinstance(data, list):
        return
    for route in data:
        if not isinstance(route, dict):
            continue
        for key, segment in route.items():
            if not isinstance(segment, dict):
                continue
            flight_no = segment.get('marketingTransportNo', '')
            if flight_no:
                flight_no = flight_no.strip()
                flight_set.add(flight_no)
                price = segment.get('price') or segment.get('minPrice')
                if price is not None:
                    try:
                        flight_prices[flight_no] = float(price)
                    except (ValueError, TypeError):
                        pass
            dep = segment.get('depStationName', '')
            arr = segment.get('arrStationName', '')
            if dep and arr:
                route_set.add(f"{dep} -> {arr}")


def _extract_train_info(data: Any, train_set: Set[str], route_set: Set[str],
                         train_prices: Dict[str, float]):
    """Extract train numbers, routes, and prices from query_train_info response."""
    if not isinstance(data, list):
        return
    for item in data:
        if not isinstance(item, (list, dict)):
            continue
        segments = item if isinstance(item, list) else [item]
        for route in segments:
            if not isinstance(route, dict):
                continue
            for key, segment in route.items():
                if not isinstance(segment, dict):
                    continue
                train_no = segment.get('trainNo', '') or segment.get('marketingTransportNo', '')
                if train_no:
                    train_no = train_no.strip()
                    train_set.add(train_no)
                    price = segment.get('price') or segment.get('minPrice')
                    if price is not None:
                        try:
                            train_prices[train_no] = float(price)
                        except (ValueError, TypeError):
                            pass
                dep = segment.get('depStationName', '')
                arr = segment.get('arrStationName', '')
                if dep and arr:
                    route_set.add(f"{dep} -> {arr}")


# =============================================================================
# Entity + price extraction from plan text
# =============================================================================

def extract_plan_entities(plan_text: str) -> Dict[str, Any]:
    """
    Extract entity names and prices referenced in the generated plan text.

    Returns:
        Dict with:
        - 'hotels', 'attractions', 'restaurants', 'flights', 'trains': sets of names
        - 'prices': dict of {entity_type: {name: price}}
    """
    entities = {
        'hotels': set(),
        'attractions': set(),
        'restaurants': set(),
        'flights': set(),
        'trains': set(),
    }

    prices = {
        'hotels': {},
        'attractions': {},
        'restaurants': {},
        'flights': {},
        'trains': {},
    }

    for line in plan_text.split('\n'):
        line = line.strip()

        # Hotel: "Accommodation: Hotel Name, ¥xxx/room/night"
        acc_match = re.match(
            r'Accommodation:\s*(.+?)(?:,|，)\s*[¥￥]([\d.]+)',
            line
        )
        if acc_match:
            name = acc_match.group(1).strip()
            if name and name != '-':
                entities['hotels'].add(name)
                try:
                    prices['hotels'][name] = float(acc_match.group(2))
                except (ValueError, TypeError):
                    pass
            continue

        # Activities: "HH:MM-HH:MM | type | details"
        act_match = re.match(r'\d{2}:\d{2}-\d{2}:\d{2}\s*\|?\s*(.+)', line)
        if not act_match:
            continue

        rest = act_match.group(1).strip()
        parts = [p.strip() for p in re.split(r'\s*\|\s*', rest)]

        if len(parts) < 2:
            continue

        act_type = parts[0]
        detail = parts[1] if len(parts) >= 2 else ''

        if act_type == 'attraction':
            # "Attraction Name, ¥60/person"
            attr_parts = re.split(r'[,，]', detail)
            name = attr_parts[0].strip()
            if name:
                entities['attractions'].add(name)
                # Extract price
                price_match = re.search(r'[¥￥]([\d.]+)', detail)
                if price_match:
                    try:
                        prices['attractions'][name] = float(price_match.group(1))
                    except (ValueError, TypeError):
                        pass

        elif act_type == 'meal':
            # "Lunch, Restaurant Name, ¥100/person"
            meal_parts = re.split(r'[,，]', detail)
            if len(meal_parts) >= 2:
                name = meal_parts[1].strip()
                if name:
                    entities['restaurants'].add(name)
                    price_match = re.search(r'[¥￥]([\d.]+)', detail)
                    if price_match:
                        try:
                            prices['restaurants'][name] = float(price_match.group(1))
                        except (ValueError, TypeError):
                            pass

        elif act_type == 'travel_intercity_public':
            # "flight CA1234, Dep - Arr, ¥650"
            flight_match = re.match(r'flight\s+(\S+)', detail)
            train_match = re.match(r'train\s+(\S+)', detail)
            if flight_match:
                no = flight_match.group(1).rstrip(',').rstrip('，')
                entities['flights'].add(no)
                price_match = re.search(r'[¥￥]([\d.]+)', detail)
                if price_match:
                    try:
                        prices['flights'][no] = float(price_match.group(1))
                    except (ValueError, TypeError):
                        pass
            elif train_match:
                no = train_match.group(1).rstrip(',').rstrip('，')
                entities['trains'].add(no)
                price_match = re.search(r'[¥￥]([\d.]+)', detail)
                if price_match:
                    try:
                        prices['trains'][no] = float(price_match.group(1))
                    except (ValueError, TypeError):
                        pass

        elif act_type == 'hotel':
            # "Check-in, Hotel Name"
            hotel_parts = re.split(r'[,，]', detail)
            if len(hotel_parts) >= 2:
                name = hotel_parts[1].strip()
                if name:
                    entities['hotels'].add(name)

    return {**entities, 'prices': prices}


# =============================================================================
# Validation
# =============================================================================

def validate_plan(
    plan_text: str,
    messages: List[Dict[str, Any]],
    language: str = 'en',
) -> Dict[str, Any]:
    """
    Validate a generated plan against tool query results.

    Checks:
    1. Entity names: are all plan entities found in tool results?
    2. Detail queries: were attractions queried with query_attraction_details?
    3. Prices: do plan prices match tool result prices?

    Returns:
        {
            'valid': bool,
            'hallucinated': {entity_type: [names]},
            'missing_details': [attraction names without detail queries],
            'price_mismatches': [{name, type, plan_price, tool_price}],
            'total_issues': int,
            'summary': str,
            'plan_entities': {...},
            'tool_entities': {...},
        }
    """
    tool_data = extract_tool_entities(messages)
    plan_data = extract_plan_entities(plan_text)

    # --- Check 1: Entity name hallucination ---
    hallucinated = {}
    total_hallucinated = 0

    for entity_type in ['hotels', 'attractions', 'restaurants', 'flights', 'trains']:
        plan_set = plan_data.get(entity_type, set())
        tool_set = tool_data.get(entity_type, set())
        invalid = plan_set - tool_set
        if invalid:
            hallucinated[entity_type] = sorted(invalid)
            total_hallucinated += len(invalid)
        else:
            hallucinated[entity_type] = []

    # --- Check 2: Attraction detail queries ---
    # Attractions in plan should have query_attraction_details called, not just recommend_attractions
    details_queried = tool_data.get('attraction_details_queried', set())
    plan_attractions = plan_data.get('attractions', set())
    # Only flag attractions that exist in tool results but lack detail queries
    # (hallucinated ones are already caught above)
    known_attractions = plan_attractions - set(hallucinated.get('attractions', []))
    missing_details = sorted(known_attractions - details_queried)

    # --- Check 3: Price mismatches ---
    price_mismatches = []
    tool_prices = tool_data.get('prices', {})
    plan_prices = plan_data.get('prices', {})

    for entity_type in ['hotels', 'attractions', 'restaurants', 'flights', 'trains']:
        plan_p = plan_prices.get(entity_type, {})
        tool_p = tool_prices.get(entity_type, {})

        for name, plan_price in plan_p.items():
            if name in tool_p:
                tool_price = tool_p[name]
                # Allow small float tolerance
                if abs(plan_price - tool_price) > 0.5:
                    price_mismatches.append({
                        'name': name,
                        'type': entity_type,
                        'plan_price': plan_price,
                        'tool_price': tool_price,
                    })

    # --- Aggregate ---
    total_issues = total_hallucinated + len(missing_details) + len(price_mismatches)
    is_valid = total_issues == 0

    # --- Build summary ---
    summary = _build_summary(
        language, is_valid, hallucinated, missing_details, price_mismatches
    )

    return {
        'valid': is_valid,
        'hallucinated': hallucinated,
        'total_hallucinated': total_hallucinated,
        'missing_details': missing_details,
        'price_mismatches': price_mismatches,
        'total_issues': total_issues,
        'summary': summary,
        'plan_entities': {k: sorted(v) if isinstance(v, set) else v for k, v in plan_data.items()},
        'tool_entities': {k: sorted(v) if isinstance(v, set) else v for k, v in tool_data.items()},
    }


def _build_summary(
    language: str,
    is_valid: bool,
    hallucinated: Dict[str, List[str]],
    missing_details: List[str],
    price_mismatches: List[Dict],
) -> str:
    """Build a human-readable validation summary."""
    if is_valid:
        return "All entities in the plan match tool query results with correct prices."

    if language == 'zh':
        lines = ["计划验证失败。发现以下问题："]
        type_labels = {'hotels': '酒店', 'attractions': '景点', 'restaurants': '餐厅', 'flights': '航班', 'trains': '火车'}
    else:
        lines = ["Plan validation failed. Issues found:"]
        type_labels = {'hotels': 'Hotels', 'attractions': 'Attractions', 'restaurants': 'Restaurants', 'flights': 'Flights', 'trains': 'Trains'}

    # Hallucinated entities
    has_hallucinated = any(v for v in hallucinated.values())
    if has_hallucinated:
        if language == 'zh':
            lines.append("\n【未查询的实体】以下实体不在你的工具查询结果中：")
        else:
            lines.append("\n[UNQUERIED ENTITIES] The following entities were NOT found in your tool query results:")
        for entity_type, invalid_list in hallucinated.items():
            if invalid_list:
                lines.append(f"  - {type_labels[entity_type]}: {', '.join(invalid_list)}")

    # Missing detail queries
    if missing_details:
        if language == 'zh':
            lines.append(f"\n【缺少详情查询】以下景点只通过 recommend_attractions 推荐，但你没有调用 query_attraction_details 查询详情（开放时间、门票价格、游览时长）。请先查询详情再使用：")
        else:
            lines.append(f"\n[MISSING DETAIL QUERIES] The following attractions were only listed by recommend_attractions, but you did not call query_attraction_details for them (to get opening hours, ticket prices, visit duration). Query their details before using them:")
        lines.append(f"  - {', '.join(missing_details)}")

    # Price mismatches
    if price_mismatches:
        if language == 'zh':
            lines.append(f"\n【价格不匹配】以下实体的价格与工具查询结果不一致：")
        else:
            lines.append(f"\n[PRICE MISMATCHES] The following entities have prices that don't match your tool query results:")
        for pm in price_mismatches:
            label = type_labels.get(pm['type'], pm['type'])
            if language == 'zh':
                lines.append(f"  - {label} \"{pm['name']}\": 计划中 ¥{pm['plan_price']}，工具返回 ¥{pm['tool_price']}")
            else:
                lines.append(f"  - {label} \"{pm['name']}\": plan has ¥{pm['plan_price']}, tool returned ¥{pm['tool_price']}")

    # Action guidance
    if language == 'zh':
        lines.append("\n请修正以上问题：")
        if has_hallucinated:
            lines.append("- 对于未查询的实体，请先调用工具查询，或替换为已查询的实体。")
        if missing_details:
            lines.append("- 对于缺少详情的景点，请调用 query_attraction_details 查询详情。")
        if price_mismatches:
            lines.append("- 对于价格不匹配的实体，请使用工具返回的正确价格。")
        lines.append("- 修正后请重新在 <plan></plan> 标签内生成完整计划。")
    else:
        lines.append("\nPlease fix the issues above:")
        if has_hallucinated:
            lines.append("- For unqueried entities: call the appropriate tool first, or replace with entities you have already queried.")
        if missing_details:
            lines.append("- For missing details: call query_attraction_details for those attractions.")
        if price_mismatches:
            lines.append("- For price mismatches: use the exact prices returned by the tools.")
        lines.append("- After fixing, regenerate the complete plan within <plan></plan> tags.")

    return '\n'.join(lines)


def build_correction_message(validation_result: Dict[str, Any], language: str = 'en') -> str:
    """
    Build a user message to send back to the agent for plan correction.

    Args:
        validation_result: Output from validate_plan()
        language: Language code

    Returns:
        Correction message string
    """
    summary = validation_result['summary']

    if language == 'zh':
        msg = f"""你的计划未通过自动验证。

{summary}"""
    else:
        msg = f"""Your plan did not pass automatic validation.

{summary}"""

    return msg
