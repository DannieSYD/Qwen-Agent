"""
Custom Agent implementation - Framework-independent
Uses universal LLM calling for multiple providers
"""
import json
import os
import sys
import time
import re
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

try:
    from .call_llm import call_llm
except ImportError:
    from call_llm import call_llm


def _load_dotenv_for_module() -> None:
    """Load .env file so API keys are available before agent instantiation."""
    try:
        domain_root = Path(__file__).resolve().parent.parent
        project_root = domain_root.parent
        dotenv_path = project_root / '.env' if (project_root / '.env').exists() else domain_root / '.env'
        if not dotenv_path.exists():
            return
        for line in dotenv_path.read_text(encoding='utf-8').splitlines():
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            key, val = line.split('=', 1)
            key, val = key.strip(), val.strip().strip('"').strip("'")
            if key and key not in os.environ:
                os.environ[key] = val
    except Exception:
        pass


def _data_summary_for_model(solver_data: Dict[str, Any]) -> str:
    """Build a compact schema summary of the pre-loaded data dict for the model."""
    lines = ["=== PRE-LOADED `data` DICT (do NOT redefine `data`) ==="]

    meta = solver_data.get("trip_meta", {})
    lines.append(f"data['trip_meta']: {json.dumps(meta, default=str)}")

    n_c = len(solver_data.get("constraints", []))
    lines.append(f"data['constraints']: {n_c} constraints")

    for key in ("outbound_transport", "inbound_transport"):
        items = solver_data.get(key, [])
        lines.append(f"data['{key}']: {len(items)} options")
        for t in items[:2]:
            lines.append(f"  {t.get('id','')} dep={t.get('dep_time','')} price={t.get('price','')}")

    hotels = solver_data.get("hotels", [])
    lines.append(f"data['hotels']: {len(hotels)} hotels")
    for h in hotels[:3]:
        svc = h.get('services', [])
        lines.append(f"  {h.get('name','')[:50]}: ¥{h.get('price','')}, {h.get('star','')}★, services={svc}")
    if len(hotels) > 3:
        lines.append(f"  ... ({len(hotels)} total)")

    attrs = solver_data.get("attractions", {})
    lines.append(f"data['attractions']: {len(attrs)} attractions (keys: {list(attrs.keys())[:5]})")

    rests = solver_data.get("restaurants", [])
    lines.append(f"data['restaurants']: {len(rests)} restaurants")
    for r in rests[:3]:
        lines.append(f"  {r.get('name','')[:40]}: ¥{r.get('price_per_person','')}/pp, tags={r.get('tags',[])}")
    if len(rests) > 3:
        lines.append(f"  ... ({len(rests)} total)")

    routes = solver_data.get("routes", {})
    lines.append(f"data['routes']: {len(routes)} routes")

    lines.append("=== END data summary ===\n")
    return "\n".join(lines)


# v3 schema (legacy): LLM writes solver code
RUN_SOLVER_SCHEMA_V3 = {
    "type": "function",
    "function": {
        "name": "run_solver",
        "description": (
            "Execute Python code that uses OR-Tools CP-SAT to build an optimized "
            "travel plan. The variable `data` is pre-loaded with all working memory "
            "data (flights, trains, hotels, attractions, restaurants, routes, "
            "constraints, trip metadata). Pre-imported: ortools.sat.python.cp_model, "
            "datetime, timedelta, json, math. Your code should print() the final "
            "plan text. If the solver finds the model infeasible, print an error "
            "explaining which constraints conflict."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "code": {
                    "type": "string",
                    "description": (
                        "Python code to execute. Access working memory via the `data` "
                        "dict. Must print() the complete plan text as output."
                    )
                }
            },
            "required": ["code"]
        }
    }
}

# v4 schema: fixed CP-SAT template, returns entity selection for LLM to arrange
RUN_SOLVER_SCHEMA = {
    "type": "function",
    "function": {
        "name": "run_solver",
        "description": (
            "Run the CP-SAT optimizer to SELECT entities for your travel plan. "
            "Automatically reads all Working Memory data (transport, hotels, "
            "attractions, restaurants, routes) and extracted constraints. Returns "
            "the selected entities — you then arrange them into a day-by-day plan. "
            "No arguments needed — just call this when you have gathered enough data. "
            "If it returns feedback about missing data, query that data and call again."
        ),
        "parameters": {
            "type": "object",
            "properties": {},
            "required": []
        }
    }
}

SCHEDULE_DAY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "schedule_day",
        "description": (
            "Given a set of entities already selected for a specific day "
            "(attractions, optionally a lunch and dinner restaurant, plus "
            "transit times between every pair of them), return a timed "
            "schedule with correct ordering, business-hour compliance, "
            "meal-window compliance, and transit/buffer insertion. "
            "Call once per day of the trip. "
            "If the response is INFEASIBLE, read unsat_core, adjust your "
            "selection (e.g., re-assign a day, swap a restaurant, pick a "
            "later inbound train), and call again. Limit 3 retries per day."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "payload": {
                    "type": "object",
                    "description": (
                        "See spec 2026-04-24-scheduling-model-design.md section 5 for full schema. "
                        "Required keys: day_index (int), weekday (str), "
                        "arrival (object|null), departure (object|null), "
                        "start_location (str), end_location (str), "
                        "attractions (list of POI objects), "
                        "lunch_restaurant (object|null), dinner_restaurant (object|null), "
                        "transits (object mapping \"('A', 'B')\" string keys to "
                        "{duration_min: int})."
                    ),
                },
            },
            "required": ["payload"],
        },
    },
}

RESOLVE_CONSTRAINT_SCHEMA = {
    "type": "function",
    "function": {
        "name": "resolve_constraint",
        "description": (
            "Resolve a fuzzy constraint by mapping the user's term to the actual "
            "database value. Use this when run_solver reports SOLVER_FUZZY_UNRESOLVED "
            "and shows available values. For example, if the user said 'birthday set "
            "menu' but the database has 'Birthday Package', call this to map it. "
            "After resolving all fuzzy constraints, call run_solver again."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "variable": {
                    "type": "string",
                    "description": "The constraint variable (e.g., 'restaurant.tag', 'hotel.service')"
                },
                "original_value": {
                    "type": "string",
                    "description": "The original value from the constraint that had no match"
                },
                "resolved_value": {
                    "type": "string",
                    "description": "The database value that best matches the user's intent"
                }
            },
            "required": ["variable", "original_value", "resolved_value"]
        }
    }
}

ASSEMBLE_DAY_SCHEMA = {
    "type": "function",
    "function": {
        "name": "assemble_day",
        "description": (
            "Build a fully-formatted day plan with correct timestamps, travel times, "
            "distances, and costs. You specify the sequence of activities; the tool "
            "computes all times deterministically from working memory data. "
            "Auto-inserts travel_city segments between locations. "
            "Returns formatted day text or errors if data is missing."
        ),
        "parameters": {
            "type": "object",
            "properties": {
                "day": {"type": "integer", "description": "Day number (1-indexed)"},
                "current_city": {"type": "string", "description": "e.g. 'from Shanghai to Beijing' or 'Beijing'"},
                "accommodation": {"type": "string", "description": "Hotel name from memory, or '-' for departure day"},
                "accommodation_price": {"type": "string", "description": "e.g. '¥200/room/night', or '-'"},
                "activities": {
                    "type": "array",
                    "description": "Ordered list of activities for the day",
                    "items": {
                        "type": "object",
                        "properties": {
                            "type": {
                                "type": "string",
                                "enum": ["intercity", "attraction", "meal", "hotel", "buffer"],
                                "description": "Activity type"
                            },
                            "transport_type": {"type": "string", "enum": ["train", "flight"], "description": "For intercity: train or flight"},
                            "id": {"type": "string", "description": "For intercity: train/flight number (e.g. 'G7798')"},
                            "name": {"type": "string", "description": "For attraction: exact name from memory"},
                            "meal_type": {"type": "string", "enum": ["Lunch", "Dinner"], "description": "For meal: Lunch or Dinner"},
                            "restaurant": {"type": "string", "description": "For meal: exact restaurant name from memory"},
                            "action": {"type": "string", "enum": ["Check-in", "Check-out", "Rest"], "description": "For hotel activity"},
                            "description": {"type": "string", "description": "For buffer: description text"},
                            "duration_min": {"type": "integer", "description": "For buffer/hotel/meal: duration in minutes"}
                        },
                        "required": ["type"]
                    }
                }
            },
            "required": ["day", "current_city", "accommodation", "activities"]
        }
    }
}


class ToolsFnAgent:
    """
    Lightweight function-calling Agent (framework-independent):
    - Loads tool schemas from tools/tool_schema.json
    - Dynamically loads tool classes (BaseTravelTool subclasses)
    - Iteratively calls LLM and executes tool_calls until final answer
    """

    def __init__(self,
                 model: str,
                 sample_id: Optional[str] = None,
                 database_base_path: Optional[str] = None,
                 tool_schema_path: Optional[str] = None,
                 language: str = 'zh') -> None:
        """
        Initialize Agent
        
        Args:
            model: Model name (must exist in models_config.json)
            sample_id: Sample ID for database path resolution
            database_base_path: Base path to database directory
            tool_schema_path: Path to tool schema JSON file
            language: Language code ('zh' or 'en')
        """
        self._load_env_from_dotenv()
        
        self.model = model
        self.language = language
        
        default_schema = Path(__file__).resolve().parent.parent / 'tools' / f'tool_schema_{language}.json'
        self.tool_schema_path = tool_schema_path or str(default_schema)
        
        self.sample_id = sample_id
        if database_base_path:
            self.database_base_path = Path(database_base_path)
        else:
            project_root = Path(__file__).resolve().parent.parent
            self.database_base_path = project_root / 'database' / f'database_{language}'

        self.tools_schema = self._load_tool_schemas()
        self.openai_tools = self._build_openai_tools(self.tools_schema)
        self.tool_instances = self._load_tool_instances()
        
        if not Path(self.tool_schema_path).exists():
            raise FileNotFoundError(f"Tool schema not found: {self.tool_schema_path}")

    def _load_env_from_dotenv(self) -> None:
        """
        Load environment variables from .env file
        
        Searches for .env in the following order:
        1. Domain directory (travelplanning/)
        2. Project root (parent of domain)
        """
        try:
            # Try domain directory first
            domain_root = Path(__file__).resolve().parent.parent
            domain_dotenv = domain_root / '.env'
            
            # Try project root
            project_root = domain_root.parent
            project_dotenv = project_root / '.env'
            
            # Use project root .env if it exists, otherwise domain .env
            dotenv_path = project_dotenv if project_dotenv.exists() else domain_dotenv
            
            if not dotenv_path.exists():
                return
            
            for line in dotenv_path.read_text(encoding='utf-8').splitlines():
                line = line.strip()
                if not line or line.startswith('#') or '=' not in line:
                    continue
                key, val = line.split('=', 1)
                key = key.strip()
                val = val.strip().strip('"').strip("'")
                if key and (key not in os.environ):
                    os.environ[key] = val
        except Exception:
            pass

    def _load_tool_schemas(self) -> List[Dict[str, Any]]:
        """Load tool schemas from JSON file"""
        path = Path(self.tool_schema_path)
        with open(path, 'r', encoding='utf-8') as f:
            raw = json.load(f)
        if isinstance(raw, list):
            return raw
        if isinstance(raw, dict) and 'tools' in raw and isinstance(raw['tools'], list):
            return raw['tools']
        return [raw]

    def _build_openai_tools(self, schemas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Build OpenAI tools format
        - If schema is already {type:function, function:{...}}, use as-is
        - Otherwise wrap as function definition
        """
        tools: List[Dict[str, Any]] = []
        for s in schemas:
            if isinstance(s, dict) and s.get('type') == 'function' and isinstance(s.get('function'), dict):
                tools.append(s)
                continue
            if not isinstance(s, dict):
                continue
            func = {
                "name": s.get('name'),
                "description": s.get('description', ''),
                "parameters": s.get('parameters', {}),
            }
            if func["name"]:
                tools.append({"type": "function", "function": func})
        return tools

    def _build_tool_config(self, tool_cls) -> Dict[str, Any]:
        """Build tool configuration with database path and language"""
        cfg = {
            'language': self.language  # Pass language to tool instance
        }
        
        if self.sample_id is None:
            return cfg
        
        sample_db_path = self.database_base_path / f'id_{self.sample_id}'
        tool_name = getattr(tool_cls, 'name', '')
        
        db_mapping = {
            'query_train_info': 'trains/trains.csv',
            'query_flight_info': 'flights/flights.csv',
            'query_hotel_info': 'hotels/hotels.csv',
            'query_attraction_details': 'attractions/attractions.csv',
            'recommend_attractions': 'attractions/attractions.csv',
            'search_location': 'locations/locations_coords.csv',
            'query_road_route_info': 'transportation/distance_matrix.csv',
            'recommend_restaurants': 'restaurants/restaurants.csv',
            'query_restaurant_details': 'restaurants/restaurants.csv',
        }

        if tool_name in db_mapping:
            db_path = sample_db_path / db_mapping[tool_name]
            if db_path.exists():
                cfg['database_path'] = str(db_path)

        # Pass location database path to compound tools (restaurants, road routes)
        # so they can internally resolve place names → coordinates
        if tool_name in ('recommend_restaurants', 'query_road_route_info'):
            location_db_path = sample_db_path / 'locations' / 'locations_coords.csv'
            if location_db_path.exists():
                cfg['location_database_path'] = str(location_db_path)

        return cfg
    
    def _load_tool_instances(self) -> Dict[str, Any]:
        """Dynamically load tool instances"""
        instances: Dict[str, Any] = {}
        tools_dir = Path(__file__).resolve().parent.parent / 'tools'

        sys.path.insert(0, str(tools_dir.parent))
        sys.path.insert(0, str(tools_dir))

        try:
            import tools  # noqa: F401
        except Exception as e:
            print(f"⚠️  Failed to import tools package: {e}")
            return instances

        try:
            import importlib
            tools_mod = importlib.import_module('tools.base_travel_tool')
            base_tool_cls = getattr(tools_mod, 'BaseTravelTool', None)
        except Exception as e:
            print(f"⚠️  Failed to import BaseTravelTool: {e}")
            return instances

        if base_tool_cls is None:
            print("⚠️  BaseTravelTool class not found in tools.base_travel_tool")
            return instances

        for cls in base_tool_cls.__subclasses__():
            try:
                tool_cfg = self._build_tool_config(cls)
                inst = cls(cfg=tool_cfg)
                inst_name = getattr(inst, 'name', None) or getattr(cls, 'name', None)
                if inst_name:
                    instances[inst_name] = inst
            except Exception as e:
                print(f"⚠️  Failed to instantiate tool {cls.__name__}: {e}")
                continue

        if not instances:
            print(f"⚠️  No tool instances loaded! Subclasses found: {[c.__name__ for c in base_tool_cls.__subclasses__()]}")

        return instances

    def _exec_tool(self, name: str, arguments_json: str) -> str:
        """Execute tool call"""
        inst = self.tool_instances.get(name)
        if not inst:
            return json.dumps({"error": f"tool '{name}' not found"}, ensure_ascii=False)
        
        try:
            args = json.loads(arguments_json) if arguments_json else {}
        except Exception:
            args = {}
        
        try:
            res = inst.call(args)
            return res if isinstance(res, str) else json.dumps(res, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"error": str(e)}, ensure_ascii=False)

    def _auto_query_routes(self, tool_name: str, arguments_json: str, memory) -> str:
        """
        After certain tool calls, automatically query road routes between
        newly-discovered locations and existing known locations (hotel, attractions).
        This gives the model travel-time info for free (Option C).

        Returns a string summarizing auto-queried routes, or empty string.
        """
        if tool_name not in ('recommend_restaurants', 'query_attraction_details',
                             'query_hotel_info', 'recommend_attractions'):
            return ""

        route_tool = self.tool_instances.get('query_road_route_info')
        if not route_tool:
            return ""

        # Determine which new locations to compute routes for
        try:
            args = json.loads(arguments_json) if arguments_json else {}
        except json.JSONDecodeError:
            args = {}

        # Only compute routes between hotels (anchors) and attractions (targets).
        # Skip hotel-hotel and attraction-attraction pairs to avoid context bloat.
        anchors = {}  # name -> "lat,lon" (hotels only)
        for h in memory.hotels:
            if h.get('lat', '?') != '?' and h.get('lon', '?') != '?':
                anchors[h['name']] = f"{h['lat']},{h['lon']}"

        targets = {}  # name -> "lat,lon" (attractions only)
        for name, d in memory.attractions_detail.items():
            if d.get('lat', '?') != '?' and d.get('lon', '?') != '?':
                targets[name] = f"{d['lat']},{d['lon']}"

        if tool_name == 'recommend_restaurants':
            # Also add: nearby attraction → each restaurant
            near = args.get('near', None)
            if near and near in memory.locations:
                loc = memory.locations[near]
                anchors[near] = f"{loc['lat']},{loc['lon']}"
            for r in memory.restaurants:
                if r.get('lat', '?') != '?' and r.get('lon', '?') != '?':
                    targets[r['name']] = f"{r['lat']},{r['lon']}"

        if not anchors or not targets:
            return ""

        # Compute routes between anchors and targets (skip duplicates)
        existing_routes = set()
        for r in memory.routes:
            existing_routes.add((r.get('origin_coords', ''), r.get('dest_coords', '')))

        route_lines = []
        queries_done = 0
        MAX_AUTO_ROUTES = 20  # Cap to avoid explosion

        for anchor_name, anchor_coord in anchors.items():
            for target_name, target_coord in targets.items():
                if anchor_name == target_name:
                    continue
                if anchor_coord == target_coord:
                    continue
                if (anchor_coord, target_coord) in existing_routes:
                    continue
                if queries_done >= MAX_AUTO_ROUTES:
                    break

                try:
                    result = route_tool.call({
                        'origin': anchor_coord,
                        'destination': target_coord
                    })
                    # Parse and store in memory
                    ack = memory.process_tool_result(
                        'query_road_route_info',
                        json.dumps({
                            'origin': anchor_coord,
                            'destination': target_coord,
                            'origin_place': anchor_name,
                            'destination_place': target_name
                        }),
                        result
                    )
                    route_lines.append(ack)
                    queries_done += 1
                except Exception:
                    continue
            if queries_done >= MAX_AUTO_ROUTES:
                break

        if route_lines:
            header = f"[Auto-computed {queries_done} travel routes]"
            return header + "\n" + "\n".join(route_lines)
        return ""

    def _auto_query_attraction_details(self, tool_name: str, memory) -> str:
        """
        After recommend_attractions, automatically query details for all
        attractions that don't have details yet. This ensures coordinates
        are available for route computation and the model has full info.

        Returns a string summarizing auto-queried details, or empty string.
        """
        if tool_name != 'recommend_attractions':
            return ""

        detail_tool = self.tool_instances.get('query_attraction_details')
        if not detail_tool:
            return ""

        # Find attractions from recommendations that lack details
        unqueried = []
        for entry in memory.attractions_summary:
            name = entry.get('name', '')
            if name and name not in memory.attractions_detail:
                unqueried.append(name)

        if not unqueried:
            return ""

        detail_lines = []
        for attr_name in unqueried:
            try:
                result = detail_tool.call({'attraction_name': attr_name})
                ack = memory.process_tool_result(
                    'query_attraction_details',
                    json.dumps({'attraction_name': attr_name}),
                    result
                )
                detail_lines.append(ack)
            except Exception:
                continue

        if detail_lines:
            header = f"[Auto-queried {len(detail_lines)} attraction details]"
            return header + "\n" + "\n".join(detail_lines)
        return ""

    def _call_llm(self, messages: List[Any], tools: Optional[List[Dict[str, Any]]] = None):
        """Call LLM with unified handling for all models"""
        # Pass messages directly - OpenAI SDK can handle both dict and object formats
        return call_llm(
            config_name=self.model,
            messages=messages,
            tools=tools
        )

    def _detect_tool_calls(self, assistant_message) -> List[Dict[str, Any]]:
        """Detect and normalize tool calls from structured API response or embedded tags."""
        import uuid

        tool_calls = getattr(assistant_message, 'tool_calls', None)
        calls: List[Dict[str, Any]] = []

        # Primary: structured tool_calls from API
        if tool_calls:
            for idx, tc in enumerate(tool_calls):
                try:
                    tool_call_id = tc.id
                    if tool_call_id is None or not tool_call_id:
                        tool_call_id = f"call_{uuid.uuid4().hex[:24]}"
                    calls.append({
                        'id': tool_call_id,
                        'name': tc.function.name,
                        'arguments': tc.function.arguments,
                    })
                except Exception:
                    continue
            return calls

        # Fallback: parse <tool_call>...</tool_call> tags from content
        # (some models/servers emit tool calls as text instead of structured response)
        content = getattr(assistant_message, 'content', '') or ''
        if '<tool_call>' in content:
            matches = re.findall(r'<tool_call>\s*(.*?)\s*</tool_call>', content, re.DOTALL)
            for match in matches:
                try:
                    tc_data = json.loads(match)
                except json.JSONDecodeError:
                    # The code field often has unescaped quotes — extract name and
                    # arguments separately using regex
                    try:
                        name_match = re.search(r'"name"\s*:\s*"([^"]+)"', match)
                        args_match = re.search(r'"arguments"\s*:\s*\{(.*)\}\s*$', match, re.DOTALL)
                        if name_match and args_match:
                            name = name_match.group(1)
                            # Extract code from arguments — find "code": "..." boundary
                            args_raw = '{' + args_match.group(1) + '}'
                            code_match = re.search(r'"code"\s*:\s*"', args_raw)
                            if code_match:
                                code_start = code_match.end()
                                # Code ends at the last "} — find it by rfind
                                code_end = args_raw.rfind('"')
                                code_str = args_raw[code_start:code_end]
                                # Unescape basic sequences
                                code_str = code_str.replace('\\n', '\n').replace('\\t', '\t').replace('\\"', '"')
                                tc_data = {'name': name, 'arguments': {'code': code_str}}
                            else:
                                continue
                        else:
                            continue
                    except Exception:
                        continue

                name = tc_data.get('name', '')
                arguments = tc_data.get('arguments', {})
                if isinstance(arguments, dict):
                    arguments = json.dumps(arguments)
                if name:
                    calls.append({
                        'id': f"call_{uuid.uuid4().hex[:24]}",
                        'name': name,
                        'arguments': arguments,
                    })

        return calls

    def _extract_plan_content(self, text: str) -> str:
        """Extract content from <plan>...</plan> tags, with fallback for truncated output."""
        if not text:
            return ""

        # Remove <think>...</think> sections
        think_end_matches = list(re.finditer(r"</think>", text, flags=re.IGNORECASE))
        if think_end_matches:
            last_think_end = think_end_matches[-1]
            text = text[last_think_end.end():]

        # Extract <plan>...</plan>
        matches = re.findall(r"<plan>(.*?)</plan>", text, flags=re.DOTALL | re.IGNORECASE)
        if matches:
            cleaned = [m.strip() for m in matches if m.strip()]
            return "\n\n".join(cleaned) if cleaned else ""

        # Fallback: extract from last <plan> to end of text (truncated response)
        last_plan_idx = text.lower().rfind('<plan>')
        if last_plan_idx >= 0:
            content = text[last_plan_idx + 6:].strip()
            if content:
                return content

        return ""

    def _message_to_dict(self, msg) -> Dict[str, Any]:
        """Convert message object to serializable dictionary"""
        if isinstance(msg, dict):
            return msg
        
        msg_dict: Dict[str, Any] = {}
        
        # Extract role
        if hasattr(msg, 'role'):
            msg_dict['role'] = msg.role
        elif hasattr(msg, 'get'):
            msg_dict['role'] = msg.get('role', 'assistant')
        else:
            msg_dict['role'] = 'assistant'
        
        # Extract content
        if hasattr(msg, 'content'):
            msg_dict['content'] = msg.content or ''
        elif isinstance(msg, dict) and 'content' in msg:
            msg_dict['content'] = msg['content'] or ''
        else:
            msg_dict['content'] = ''
        
        # Extract tool_calls if present
        tool_calls = getattr(msg, 'tool_calls', None)
        if tool_calls:
            calls_list = []
            for tc in tool_calls:
                try:
                    tool_call_id = getattr(tc, 'id', None) or ''
                    call_dict = {
                        'id': tool_call_id,
                        'type': 'function',
                        'function': {
                            'name': getattr(tc.function, 'name', '') if hasattr(tc, 'function') else '',
                            'arguments': getattr(tc.function, 'arguments', '') if hasattr(tc, 'function') else ''
                        }
                    }
                    calls_list.append(call_dict)
                except Exception:
                    continue
            if calls_list:
                msg_dict['tool_calls'] = calls_list
        
        # Preserve reasoning_content if present
        if hasattr(msg, 'reasoning_content') and msg.reasoning_content:
            msg_dict['reasoning_content'] = msg.reasoning_content
        
        return msg_dict

    def _serialize_messages(self, messages: List[Any]) -> List[Dict[str, Any]]:
        """Convert all messages in list to serializable dictionaries"""
        serialized = []
        for msg in messages:
            serialized.append(self._message_to_dict(msg))
        return serialized

    @staticmethod
    def _compress_previous_msgs(messages: list) -> None:
        """Compress older messages to save context window space.

        Applied **in-place** before each LLM call.  Two concerns:

        1. **Assistant reasoning bloat** — Thinking models emit verbose
           chain-of-thought directly in ``content``.  For past turns this is
           no longer useful.
        2. **Repeated memory snapshots** — Each tool result includes a full
           ``═══ WORKING MEMORY … ═══`` snapshot.  Only the most recent
           snapshot matters; older ones are redundant.

        Rules:
        - Assistant msgs with tool_calls → truncate content to 200 chars.
        - Assistant msgs with <plan> → keep only the <plan> block.
        - Tool msgs → strip working-memory snapshot from all but the last
          tool message (the latest snapshot is kept intact).
        - Strips ``<think>`` blocks and orphan ``</think>`` tags everywhere.
        """
        _MEMORY_RE = re.compile(
            r'\n*═══ WORKING MEMORY.*?═══ END WORKING MEMORY ═══',
            re.DOTALL,
        )

        # --- Find the index of the LAST tool message so we preserve its snapshot ---
        last_tool_idx = -1
        for i in range(len(messages) - 1, -1, -1):
            m = messages[i]
            role = m.get('role') if isinstance(m, dict) else getattr(m, 'role', None)
            if role == 'tool':
                last_tool_idx = i
                break

        for idx, msg in enumerate(messages):
            if isinstance(msg, dict):
                role = msg.get('role')
                content = msg.get('content')
            else:
                role = getattr(msg, 'role', None)
                content = getattr(msg, 'content', None)

            if not content:
                continue

            # --- Tool messages: strip old memory snapshots ---
            if role == 'tool' and idx != last_tool_idx:
                stripped = _MEMORY_RE.sub('', content).strip()
                if stripped:
                    if isinstance(msg, dict):
                        msg['content'] = stripped
                    else:
                        msg.content = stripped
                continue

            if role != 'assistant':
                continue

            # --- Assistant messages ---
            has_tc = (msg.get('tool_calls') if isinstance(msg, dict)
                      else getattr(msg, 'tool_calls', None))
            has_tc = bool(has_tc)

            # Strip <think>…</think> and orphan </think>
            cleaned = re.sub(r'<think>.*?</think>\s*', '', content, flags=re.DOTALL | re.IGNORECASE)
            cleaned = re.sub(r'<think>.*', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
            cleaned = re.sub(r'</think>\s*', '', cleaned, flags=re.IGNORECASE)
            cleaned = cleaned.strip()

            if has_tc:
                # Content is reasoning only — truncate aggressively
                if len(cleaned) > 200:
                    cleaned = cleaned[:200] + '…[reasoning truncated]'
            else:
                # If there's a <plan> block, keep only that
                plan_match = re.search(r'(<plan>.*?</plan>)', cleaned, flags=re.DOTALL | re.IGNORECASE)
                if plan_match:
                    cleaned = plan_match.group(1)

            if isinstance(msg, dict):
                msg['content'] = cleaned or content
            else:
                msg.content = cleaned or content

    @staticmethod
    def _compact_tool_output(tool_name: str, raw_output: str) -> str:
        """Strip coordinates and non-essential fields from tool outputs for harness_v1.

        Keeps only fields the model needs for planning decisions. Removes latitude,
        longitude, coordinates, IDs, and addresses that add noise."""
        # Remove auto-resolved coordinate lines
        lines = raw_output.split('\n')
        lines = [l for l in lines if not l.strip().startswith('[Auto-resolved location')]
        output = '\n'.join(lines).strip()

        try:
            parsed = json.loads(output)
        except (json.JSONDecodeError, TypeError):
            # Text format (e.g., attraction details) — strip coordinate lines
            result_lines = []
            for line in output.split('\n'):
                lower = line.lower()
                if any(kw in lower for kw in ('latitude', 'longitude', 'coordinates', 'attraction id')):
                    continue
                if 'address' in lower and 'nan' in lower:
                    continue
                result_lines.append(line)
            return '\n'.join(result_lines).strip()

        # JSON format — strip fields from dicts
        strip_keys = {'latitude', 'longitude', 'address', 'id'}

        def clean_dict(d: dict) -> dict:
            return {k: v for k, v in d.items() if k.lower() not in strip_keys}

        if isinstance(parsed, list):
            cleaned = [clean_dict(item) if isinstance(item, dict) else item for item in parsed]
        elif isinstance(parsed, dict):
            cleaned = clean_dict(parsed)
            # For road route, also strip origin/destination coordinate strings
            if tool_name == 'query_road_route_info':
                cleaned.pop('origin', None)
                cleaned.pop('destination', None)
        else:
            return output

        return json.dumps(cleaned, ensure_ascii=False, indent=2)

    def run(self,
            user_query: str,
            system_prompt: Optional[str] = None,
            max_llm_calls: int = 100,
            enable_memory: bool = False,
            compact_outputs: bool = False,
            extracted_constraints: Optional[tuple] = None,
            enable_solver: bool = False,
            solver_version: str = 'v3') -> Tuple[str, List[Dict[str, Any]], Dict[str, int]]:
        """
        Agent main loop: Call LLM → Execute tools → Repeat until final answer

        Args:
            user_query: User query
            system_prompt: System prompt
            max_llm_calls: Maximum LLM calls
            enable_memory: If True, use working memory to replace raw tool outputs
                           with structured summaries + accumulated memory snapshot
            compact_outputs: If True, strip coordinates and non-essential fields from
                           tool outputs before passing to the model (harness_v1)
            extracted_constraints: Optional (trip_meta, constraints, rendered_str) tuple
                           from Phase 1 constraint extraction. If provided, stored in
                           working memory and rendered in the memory snapshot.
            enable_solver: If True, add run_solver tool
            solver_version: 'v3' (LLM writes code), 'v4' (fixed CP-SAT template), or 'v5' (intra-day CP-SAT scheduler)

        Returns:
            (final_plan, messages, token_usage): Final plan, complete message history, and token usage
        """
        # Initialize working memory if enabled
        memory = None
        if enable_memory:
            try:
                from working_memory import WorkingMemory
            except ImportError:
                from agent.working_memory import WorkingMemory
            memory = WorkingMemory(language=self.language)

            # Phase 1: Store extracted constraints in working memory
            if extracted_constraints is not None:
                trip_meta, constraints, rendered = extracted_constraints
                memory.set_constraints(trip_meta, constraints, rendered)

        # When memory is enabled, add extra tools based on solver version.
        # v4: run_solver + resolve_constraint (no assemble_day — LLM arranges plan)
        # v3: run_solver + assemble_day
        # no solver: assemble_day only
        tools_for_llm = self.openai_tools
        if enable_memory:
            extra_tools = []
            if enable_solver:
                if solver_version == 'v5':
                    extra_tools.append(SCHEDULE_DAY_SCHEMA)
                elif solver_version == 'v4':
                    extra_tools.append(RUN_SOLVER_SCHEMA)
                    extra_tools.append(RESOLVE_CONSTRAINT_SCHEMA)
                else:
                    extra_tools.append(RUN_SOLVER_SCHEMA_V3)
                    extra_tools.append(ASSEMBLE_DAY_SCHEMA)
            else:
                extra_tools.append(ASSEMBLE_DAY_SCHEMA)
            tools_for_llm = self.openai_tools + extra_tools

        messages: List[Dict[str, Any]] = []
        if system_prompt:
            messages.append({"role": "system", "content": system_prompt})
        messages.append({"role": "user", "content": user_query})

        # Inject extracted constraints so the LLM sees them before the first tool call
        if memory and memory._constraints_rendered:
            messages.append({
                "role": "user",
                "content": (
                    "Before you start querying, here are the extracted constraints "
                    "from the user's request. Use these to guide your tool calls "
                    "(e.g., filter hotels by star rating, find restaurants near "
                    "specific locations, pick the right transport mode).\n\n"
                    + memory.render_snapshot()
                ),
            })

        total_prompt_tokens = 0
        total_completion_tokens = 0
        total_tokens = 0
        empty_plan_retries = 0
        solver_was_called = False  # Track whether run_solver has been invoked
        solver_succeeded = False  # Track whether run_solver returned a valid plan
        solver_nudge_count = 0  # Limit how many times we nudge for solver usage
        solver_result_obj = None  # SolverResult for faithfulness checking (v4)
        solver_data_obj = None   # Solver data dict for faithfulness checking (v4)
        faithfulness_retries = 0  # Track faithfulness correction rounds

        llm_budget = max_llm_calls

        while llm_budget > 0:
            llm_budget -= 1

            # Compress reasoning in older assistant messages to free context
            if compact_outputs:
                self._compress_previous_msgs(messages)

            resp = self._call_llm(messages=messages, tools=tools_for_llm)

            # Accumulate token usage
            usage = getattr(resp, 'usage', None)
            if usage:
                total_prompt_tokens += getattr(usage, 'prompt_tokens', 0) or 0
                total_completion_tokens += getattr(usage, 'completion_tokens', 0) or 0
                total_tokens += getattr(usage, 'total_tokens', 0) or 0

            msg = resp.choices[0].message
            calls = self._detect_tool_calls(msg)

            # Retry detection if content has <tool_call> tags but no calls found
            if not calls:
                _c = getattr(msg, 'content', None) or ''
                if '<tool_call>' in _c:
                    calls = self._detect_tool_calls(msg)

            messages.append(msg)
            if calls:
                # Execute tool calls
                for call in calls:
                    # --- Special handling: assemble_day is a memory-side tool ---
                    if call['name'] == 'assemble_day' and memory:
                        try:
                            args = json.loads(call['arguments']) if call['arguments'] else {}
                        except json.JSONDecodeError:
                            args = {}
                        tool_content = memory.assemble_day(args)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": tool_content,
                        })
                        continue

                    # --- Special handling: resolve_constraint updates a constraint value ---
                    if call['name'] == 'resolve_constraint' and memory:
                        try:
                            args = json.loads(call['arguments']) if call['arguments'] else {}
                        except json.JSONDecodeError:
                            args = {}
                        variable = args.get('variable', '')
                        original = args.get('original_value', '')
                        resolved = args.get('resolved_value', '')
                        # Update the constraint in memory
                        # Try exact match first, then fuzzy match on original_value
                        updated_list = []
                        original_lower = str(original).lower()
                        for c in memory.constraints:
                            if c.variable != variable:
                                continue
                            c_val = str(c.value).lower()
                            if c_val == original_lower:
                                # Exact match
                                c.value = resolved
                                updated_list.append(c_val)
                                break
                        if not updated_list:
                            # Fuzzy match: resolve ALL constraints with this variable
                            # whose value is a substring of original or vice versa
                            # (handles "washing machine and dryer" matching both
                            #  "washing machine" and "dryer" separately)
                            for c in memory.constraints:
                                if c.variable != variable:
                                    continue
                                c_val = str(c.value).lower()
                                if c_val in original_lower or original_lower in c_val:
                                    old_val = c.value
                                    c.value = resolved
                                    updated_list.append(old_val)
                        if updated_list:
                            tool_content = (
                                f"Constraint(s) updated: {variable} value(s) "
                                f"{updated_list} → '{resolved}'. Call run_solver again."
                            )
                        else:
                            tool_content = (
                                f"No constraint found with variable='{variable}' and "
                                f"value='{original}'. Available constraints: "
                                + ", ".join(f"{c.variable}={c.value}" for c in memory.constraints)
                            )
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": tool_content,
                        })
                        continue

                    # --- Special handling: schedule_day (v5 intra-day scheduler) ---
                    if call['name'] == 'schedule_day':
                        try:
                            from solver.day_scheduler import schedule_day
                        except ImportError:
                            from agent.solver.day_scheduler import schedule_day
                        payload = call['arguments'].get('payload', {})
                        if isinstance(payload, str):
                            try:
                                payload = json.loads(payload)
                            except Exception:
                                pass
                        result = schedule_day(payload)
                        solver_was_called = True
                        if result.get("status") == "FEASIBLE":
                            solver_succeeded = True
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                        continue

                    # --- Special handling: run_solver ---
                    if call['name'] == 'run_solver' and memory:
                        solver_was_called = True
                        try:
                            from solver.data_export import export_memory_as_dict
                        except ImportError:
                            from agent.solver.data_export import export_memory_as_dict
                        solver_data = export_memory_as_dict(memory)

                        if solver_version == 'v4':
                            # v4: CP-SAT selects entities, LLM arranges them
                            try:
                                from solver.cp_template import CPSATEntitySelector
                                from solver.executor import format_solver_selection
                            except ImportError:
                                from agent.solver.cp_template import CPSATEntitySelector
                                from agent.solver.executor import format_solver_selection

                            selector = CPSATEntitySelector(solver_data)
                            sel_result = selector.solve()

                            _FAIL_STATUSES = ("MISSING_DATA", "INFEASIBLE", "ERROR")
                            if sel_result.status in _FAIL_STATUSES:
                                tool_content = sel_result.feedback
                            elif sel_result.feedback and "SOLVER_FUZZY_UNRESOLVED" in sel_result.feedback:
                                tool_content = sel_result.feedback
                            else:
                                # Solver succeeded — format selection for LLM
                                tool_content = format_solver_selection(sel_result, solver_data)
                                solver_result_obj = sel_result
                                solver_data_obj = solver_data
                                solver_succeeded = True
                        else:
                            # v3: LLM-generated code
                            try:
                                args = json.loads(call['arguments']) if call['arguments'] else {}
                            except json.JSONDecodeError:
                                args = {}
                            try:
                                from solver.executor import run_solver_code
                            except ImportError:
                                from agent.solver.executor import run_solver_code
                            tool_content = run_solver_code(
                                code=args.get('code', ''),
                                data=solver_data,
                            )

                        if solver_version == 'v4':
                            # v4: selection or feedback — either way, pass to LLM
                            # and let it continue (write plan or query more data)
                            # Working memory is already visible via regular tool responses;
                            messages.append({
                                "role": "tool",
                                "tool_call_id": call['id'],
                                "name": call['name'],
                                "content": tool_content,
                            })
                            continue
                        else:
                            # v3: old behavior — return plan directly on success
                            data_summary = _data_summary_for_model(solver_data)
                            _SOLVER_FAIL_PREFIXES = ('SOLVER_ERROR', 'SOLVER_INFEASIBLE', 'SOLVER_FEEDBACK', 'SOLVER_FUZZY_UNRESOLVED')
                            solver_succeeded = not any(tool_content.startswith(p) for p in _SOLVER_FAIL_PREFIXES)
                            if solver_succeeded:
                                messages.append({
                                    "role": "tool",
                                    "tool_call_id": call['id'],
                                    "name": call['name'],
                                    "content": data_summary + tool_content,
                                })
                                token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
                                return tool_content, messages, token_usage, memory

                            messages.append({
                                "role": "tool",
                                "tool_call_id": call['id'],
                                "name": call['name'],
                                "content": data_summary + tool_content,
                            })
                            continue

                    tool_result = self._exec_tool(call['name'], call['arguments'])

                    if memory:
                        # Process through working memory BEFORE compacting,
                        # so memory gets full data (coordinates, etc.)
                        processed = memory.process_tool_result(
                            call['name'], call['arguments'], tool_result
                        )

                    # Compact tool output if enabled (strip coords, non-essential fields)
                    if compact_outputs:
                        tool_result = self._compact_tool_output(call['name'], tool_result)

                    if memory:

                        # --- Auto-query attraction details after recommend_attractions ---
                        auto_detail_lines = self._auto_query_attraction_details(call['name'], memory)

                        # --- Auto-attach route info (Option C) ---
                        # After restaurants or attraction details, auto-query routes
                        # between known locations so the model gets travel times for free
                        auto_route_lines = self._auto_query_routes(call['name'], call['arguments'], memory)

                        # Replace raw output with: acknowledgment + auto-details + auto-routes + full memory snapshot
                        tool_content = processed
                        if auto_detail_lines:
                            tool_content += "\n\n" + auto_detail_lines
                        if auto_route_lines:
                            tool_content += "\n\n" + auto_route_lines
                        tool_content += "\n\n" + memory.render_snapshot()
                    else:
                        tool_content = tool_result

                    messages.append({
                        "role": "tool",
                        "tool_call_id": call['id'],
                        "name": call['name'],
                        "content": tool_content,
                    })
                continue

            # No tool calls → try to extract plan
            final_content = self._extract_plan_content(msg.content or '')

            # Guard: if solver is enabled but hasn't produced a valid plan, reject manual plans
            if final_content and enable_solver and not solver_succeeded and solver_nudge_count < 3 and llm_budget > 0:
                solver_nudge_count += 1
                if solver_version == 'v5':
                    messages.append({
                        "role": "user",
                        "content": (
                            "REJECTED: You wrote the plan manually. You MUST call `schedule_day` "
                            "for each day of the trip before assembling the final plan.\n\n"
                            "For each day, call `schedule_day(payload)` with the day's entities, "
                            "transit graph, and day_index. Stitch the per-day results into the "
                            "final plan yourself. Call `schedule_day` now."
                        ),
                    })
                elif solver_version == 'v4':
                    messages.append({
                        "role": "user",
                        "content": (
                            "REJECTED: You wrote the plan manually. You MUST call `run_solver` (no arguments).\n\n"
                            "If `run_solver` returned SOLVER_FEEDBACK about missing data, query that data first, "
                            "then call `run_solver` again.\n"
                            "If it returned SOLVER_FUZZY_UNRESOLVED, call `resolve_constraint` to map fuzzy "
                            "values, then call `run_solver` again.\n\n"
                            "Do NOT write plans manually. Call `run_solver` now."
                        ),
                    })
                else:
                    messages.append({
                        "role": "user",
                        "content": (
                            "REJECTED: You wrote the plan manually. You MUST call the `run_solver` tool.\n\n"
                            "Here is a minimal template — adapt it to your data:\n"
                            "```python\n"
                            "model = cp_model.CpModel()\n"
                            "solver = cp_model.CpSolver()\n"
                            "# Boolean selection: one hotel, one outbound, one inbound\n"
                            "h_sel = [model.NewBoolVar(f'h{i}') for i in range(len(data['hotels']))]\n"
                            "model.AddExactlyOne(h_sel)\n"
                            "ob_sel = [model.NewBoolVar(f'ob{i}') for i in range(len(data['outbound_transport']))]\n"
                            "model.AddExactlyOne(ob_sel)\n"
                            "ib_sel = [model.NewBoolVar(f'ib{i}') for i in range(len(data['inbound_transport']))]\n"
                            "model.AddExactlyOne(ib_sel)\n"
                            "# Budget constraint: sum of selected costs <= budget\n"
                            "# ... add constraints, solve, then print plan using solver.Value()\n"
                            "```\n"
                            "Call `run_solver` with your complete code NOW."
                        ),
                    })
                continue

            if final_content:
                # v4 faithfulness check: ensure all solver-selected entities appear
                if solver_version == 'v4' and solver_result_obj and faithfulness_retries < 2:
                    try:
                        from solver.executor import check_plan_faithfulness
                    except ImportError:
                        from agent.solver.executor import check_plan_faithfulness
                    faith_feedback = check_plan_faithfulness(final_content, solver_result_obj, solver_data_obj)
                    if faith_feedback and llm_budget > 0:
                        faithfulness_retries += 1
                        messages.append({"role": "user", "content": faith_feedback})
                        continue

                token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
                return final_content, messages, token_usage, memory

            # Empty plan — nudge the model
            empty_plan_retries += 1
            if empty_plan_retries <= 2 and llm_budget > 0:
                if enable_solver and not solver_was_called:
                    # Solver mode: nudge to call run_solver, not write manually
                    messages.append({
                        "role": "user",
                        "content": (
                            "Do not explain. Just call `run_solver` now with your Python code. "
                            "Do not output any text — only make a tool call to `run_solver`."
                        ),
                    })
                else:
                    messages.append({
                        "role": "user",
                        "content": (
                            "Your response did not contain a travel plan in <plan>...</plan> tags. "
                            "Please output your complete travel plan now, enclosed in <plan>...</plan> tags."
                        ),
                    })
                continue

            # Exhausted retries — return empty
            token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
            return "", messages, token_usage, memory

        token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
        return "Reached max LLM calls without final answer.", messages, token_usage, memory

    def continue_run(self,
                     messages: List[Any],
                     max_llm_calls: int = 20,
                     compact_outputs: bool = False,
                     memory=None) -> Tuple[str, List[Any], Dict[str, int]]:
        """
        Continue agent loop from existing message history.
        Used for correction rounds after validation.

        Args:
            messages: Existing message history (will be mutated in-place)
            max_llm_calls: Maximum additional LLM calls
            compact_outputs: If True, strip coordinates from tool outputs
            memory: Optional WorkingMemory instance for assemble_day support

        Returns:
            (final_plan, messages, token_usage): Same as run()
        """
        total_prompt_tokens = 0
        total_completion_tokens = 0
        total_tokens = 0

        # Include assemble_day (and run_solver if available) when memory is present
        tools_for_llm = self.openai_tools
        if memory is not None:
            extra_tools = [ASSEMBLE_DAY_SCHEMA, RUN_SOLVER_SCHEMA]
            tools_for_llm = self.openai_tools + extra_tools

        llm_budget = max_llm_calls

        while llm_budget > 0:
            llm_budget -= 1

            # Compress reasoning in older assistant messages to free context
            if compact_outputs:
                self._compress_previous_msgs(messages)

            resp = self._call_llm(messages=messages, tools=tools_for_llm)

            usage = getattr(resp, 'usage', None)
            if usage:
                total_prompt_tokens += getattr(usage, 'prompt_tokens', 0) or 0
                total_completion_tokens += getattr(usage, 'completion_tokens', 0) or 0
                total_tokens += getattr(usage, 'total_tokens', 0) or 0

            msg = resp.choices[0].message
            calls = self._detect_tool_calls(msg)
            messages.append(msg)
            if calls:
                for call in calls:
                    # --- Special handling: schedule_day (v5 intra-day scheduler) ---
                    if call['name'] == 'schedule_day':
                        try:
                            from solver.day_scheduler import schedule_day
                        except ImportError:
                            from agent.solver.day_scheduler import schedule_day
                        payload = call['arguments'].get('payload', {})
                        if isinstance(payload, str):
                            try:
                                payload = json.loads(payload)
                            except Exception:
                                pass
                        result = schedule_day(payload)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": json.dumps(result, ensure_ascii=False),
                        })
                        continue

                    # Handle assemble_day via memory (same as run())
                    if call['name'] == 'assemble_day' and memory:
                        try:
                            args = json.loads(call['arguments']) if call['arguments'] else {}
                        except json.JSONDecodeError:
                            args = {}
                        tool_content = memory.assemble_day(args)
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": tool_content,
                        })
                        continue

                    # Handle run_solver via solver executor (same as run())
                    if call['name'] == 'run_solver' and memory:
                        try:
                            from solver.data_export import export_memory_as_dict
                        except ImportError:
                            from agent.solver.data_export import export_memory_as_dict
                        solver_data = export_memory_as_dict(memory)
                        # Use v4 template in _finalize_plan context
                        try:
                            from solver.executor import run_solver_template
                        except ImportError:
                            from agent.solver.executor import run_solver_template
                        tool_content = run_solver_template(
                            data=solver_data,
                            memory=memory,
                        )
                        data_summary = _data_summary_for_model(solver_data)
                        # Auto-extract successful solver output as final plan
                        if not tool_content.startswith('SOLVER_ERROR') and not tool_content.startswith('SOLVER_INFEASIBLE') and not tool_content.startswith('SOLVER_FEEDBACK'):
                            messages.append({
                                "role": "tool",
                                "tool_call_id": call['id'],
                                "name": call['name'],
                                "content": data_summary + tool_content,
                            })
                            token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
                            return tool_content, messages, token_usage
                        messages.append({
                            "role": "tool",
                            "tool_call_id": call['id'],
                            "name": call['name'],
                            "content": data_summary + tool_content,
                        })
                        continue

                    tool_result = self._exec_tool(call['name'], call['arguments'])
                    if compact_outputs:
                        tool_result = self._compact_tool_output(call['name'], tool_result)
                    messages.append({
                        "role": "tool",
                        "tool_call_id": call['id'],
                        "name": call['name'],
                        "content": tool_result,
                    })
                continue

            final_content = self._extract_plan_content(msg.content or '')
            token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
            return final_content, messages, token_usage

        token_usage = {"prompt_tokens": total_prompt_tokens, "completion_tokens": total_completion_tokens, "total_tokens": total_tokens}
        return "Reached max LLM calls without final answer.", messages, token_usage


def run_agent_inference(
    model: str,
    language: str,
    test_data_path: Path,
    database_dir: Path,
    tool_schema_path: Path,
    output_dir: Path,
    workers: int = 10,
    max_llm_calls: int = 100,
    rerun_ids: Optional[List[int]] = None,
    prompt_variant: str = 'default',
) -> Dict[str, Any]:
    """
    Run agent inference (batch processing)
    
    Args:
        model: Configuration name from models_config.json
        language: Language code ('zh' or 'en')
        test_data_path: Path to test data JSON file
        database_dir: Base path to database directory
        tool_schema_path: Path to tool schema JSON file
        output_dir: Output directory for results
        workers: Number of parallel workers
        max_llm_calls: Maximum LLM calls per sample
        rerun_ids: Optional list of specific IDs to rerun. If None, run all samples.
        prompt_variant: Prompt variant to use ('default' or 'explore')

    Returns:
        Results summary dict
    """
    with open(test_data_path, 'r', encoding='utf-8') as f:
        test_data = json.load(f)
    
    # Filter samples if rerun_ids is specified
    if rerun_ids is not None:
        rerun_ids_set = set(str(id) for id in rerun_ids)  # Convert to strings for comparison
        original_count = len(test_data)
        test_data = [s for s in test_data if str(s.get('id')) in rerun_ids_set]
        print(f"  🔄 Filtered {original_count} samples to {len(test_data)} samples for rerun")
        
        if len(test_data) == 0:
            print(f"  ⚠️  Warning: No samples found matching the specified IDs")
            return {
                'total': 0,
                'success': 0,
                'failed': 0,
                'elapsed_time': 0,
                'results': []
            }
    
    print(f"\n{'='*80}")
    print(f"Agent Inference")
    print(f"{'='*80}")
    print(f"Model: {model}")
    print(f"Language: {language}")
    print(f"Samples: {len(test_data)}")
    print(f"Workers: {workers}")
    print(f"{'='*80}\n")
    
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / 'trajectories').mkdir(exist_ok=True)
    (output_dir / 'reports').mkdir(exist_ok=True)

    # Load .env early so API keys are available for constraint extraction
    _load_dotenv_for_module()

    _GUIDED_VARIANTS = ('guided', 'guided_memory', 'harness_v1', 'harness_v2', 'harness_v3', 'harness_v4', 'harness_v5')

    if prompt_variant in _GUIDED_VARIANTS:
        try:
            from prompts_guided import get_system_prompt
        except ImportError:
            from agent.prompts_guided import get_system_prompt
    elif prompt_variant == 'explore':
        try:
            from prompts_explore import get_system_prompt
        except ImportError:
            from agent.prompts_explore import get_system_prompt
    else:
        try:
            from prompts import get_system_prompt
        except ImportError:
            from agent.prompts import get_system_prompt

    # Import validator for guided variants
    if prompt_variant in _GUIDED_VARIANTS:
        try:
            from plan_validator import validate_plan, build_correction_message
        except ImportError:
            from agent.plan_validator import validate_plan, build_correction_message

    # Phase 1: Import constraint extractor for harness_v2+ (NOT harness_v5 — v5 parses NL itself)
    enable_constraint_extraction = (prompt_variant in ('harness_v2', 'harness_v3', 'harness_v4'))
    if enable_constraint_extraction:
        try:
            from constraint_extractor import extract_constraints, render_constraints_for_prompt
        except ImportError:
            from agent.constraint_extractor import extract_constraints, render_constraints_for_prompt

    # Enable working memory for guided_memory, harness_v1+
    enable_memory = (prompt_variant in ('guided_memory', 'harness_v1', 'harness_v2', 'harness_v3', 'harness_v4', 'harness_v5'))
    # Enable compact outputs for harness_v1+
    compact_outputs = (prompt_variant in ('harness_v1', 'harness_v2', 'harness_v3', 'harness_v4', 'harness_v5'))
    # Enable solver tool for harness_v3+
    enable_solver = (prompt_variant in ('harness_v3', 'harness_v4', 'harness_v5'))
    # Solver version: v5 = intra-day CP-SAT scheduler, v4 = fixed template, v3 = LLM writes code
    if prompt_variant == 'harness_v5':
        solver_version = 'v5'
    elif prompt_variant == 'harness_v4':
        solver_version = 'v4'
    else:
        solver_version = 'v3'
    
    print_lock = Lock()
    results = []
    
    def process_sample(sample):
        sample_id_raw = sample.get('id', 'unknown')
        sample_id = f"id_{sample_id_raw}" if str(sample_id_raw).isdigit() else str(sample_id_raw)
        query = sample.get('query', '')
        
        try:
            with print_lock:
                print(f"\n🚀 Processing sample: {sample_id}")
            
            agent = ToolsFnAgent(
                model=model,
                sample_id=sample_id_raw,
                database_base_path=database_dir,
                tool_schema_path=str(tool_schema_path),
                language=language
            )
            
            if prompt_variant == 'harness_v5':
                system_prompt = get_system_prompt(language, variant='solver_v5')
            elif prompt_variant in ('harness_v3', 'harness_v4'):
                system_prompt = get_system_prompt(language, variant='solver' if prompt_variant == 'harness_v3' else 'solver_v4')
            else:
                system_prompt = get_system_prompt(language)
            start_time = time.time()

            # Phase 1: Extract constraints before running agent
            extracted_constraints = None
            if enable_constraint_extraction:
                try:
                    trip_meta, constraints = extract_constraints(query, model)
                    rendered = render_constraints_for_prompt(trip_meta, constraints)
                    extracted_constraints = (trip_meta, constraints, rendered)
                    with print_lock:
                        print(f"  📋 {sample_id}: Extracted {len(constraints)} constraints")
                except Exception as e:
                    with print_lock:
                        print(f"  ⚠️  {sample_id}: Constraint extraction failed: {e}")

            final_plan, full_messages, token_usage, memory = agent.run(
                user_query=query,
                system_prompt=system_prompt,
                max_llm_calls=max_llm_calls,
                enable_memory=enable_memory,
                compact_outputs=compact_outputs,
                extracted_constraints=extracted_constraints,
                enable_solver=enable_solver,
                solver_version=solver_version,
            )

            # Validation loop for guided variants (max 2 correction rounds)
            # Skip for harness_v4/v5: faithfulness check is built into the main run() loop
            if prompt_variant in _GUIDED_VARIANTS and prompt_variant not in ('harness_v4', 'harness_v5') and final_plan and final_plan != "Reached max LLM calls without final answer.":
                max_corrections = 2
                for correction_round in range(max_corrections):
                    serialized_for_validation = agent._serialize_messages(full_messages)
                    validation = validate_plan(final_plan, serialized_for_validation, language)

                    if validation['valid']:
                        with print_lock:
                            print(f"  ✅ {sample_id}: Plan passed validation (round {correction_round})")
                        break

                    with print_lock:
                        print(f"  🔄 {sample_id}: Validation found {validation['total_hallucinated']} hallucinated entities (round {correction_round + 1}/{max_corrections})")

                    # Send correction message and continue from existing messages
                    correction_msg = build_correction_message(validation, language)
                    full_messages.append({"role": "user", "content": correction_msg})

                    # Continue agent from current message history
                    corrected_plan, full_messages, extra_usage = agent.continue_run(
                        messages=full_messages,
                        max_llm_calls=max(20, max_llm_calls // 4),
                        compact_outputs=compact_outputs,
                        memory=memory,
                    )

                    # Accumulate token usage
                    token_usage['prompt_tokens'] += extra_usage.get('prompt_tokens', 0)
                    token_usage['completion_tokens'] += extra_usage.get('completion_tokens', 0)
                    token_usage['total_tokens'] += extra_usage.get('total_tokens', 0)

                    if corrected_plan and corrected_plan != "Reached max LLM calls without final answer.":
                        final_plan = corrected_plan

            # Auto-correct travel_city durations using working memory routes
            if memory is not None and final_plan:
                final_plan = memory.autocorrect_travel_city(final_plan)

            elapsed = time.time() - start_time

            # Ensure messages are serializable before writing
            serialized_messages = agent._serialize_messages(full_messages)

            result = {
                'id': sample_id,
                'query': query,
                'model': model,
                'language': language,
                'final_plan': final_plan,
                'messages': serialized_messages,  # Use serialized messages
                'elapsed_time': elapsed,
                'success': True,
                'token_usage': token_usage,
            }
            
            trajectory_file = output_dir / 'trajectories' / f'{sample_id}.json'
            try:
                with open(trajectory_file, 'w', encoding='utf-8') as f:
                    json.dump(result, f, indent=2, ensure_ascii=False, default=str)
            except TypeError as e:
                with print_lock:
                    print(f"⚠️  Sample {sample_id}: JSON serialization error: {e}")
                    print(f"   Attempting to identify problematic message...")
                # Try to identify which message causes the problem
                for i, msg in enumerate(serialized_messages):
                    try:
                        json.dumps(msg, ensure_ascii=False)
                    except TypeError as msg_err:
                        with print_lock:
                            print(f"   Message {i} cannot be serialized: {msg_err}")
                            print(f"   Message type: {type(msg)}")
                            if hasattr(msg, '__dict__'):
                                print(f"   Message attrs: {list(msg.__dict__.keys())}")
                raise
            
            # Save working memory snapshot if available
            if memory is not None:
                (output_dir / 'memory_snapshots').mkdir(exist_ok=True)
                memory_file = output_dir / 'memory_snapshots' / f'{sample_id}.txt'
                with open(memory_file, 'w', encoding='utf-8') as f:
                    f.write(memory.render_snapshot())

            if final_plan:
                plan_file = output_dir / 'reports' / f'{sample_id}.txt'
                with open(plan_file, 'w', encoding='utf-8') as f:
                    f.write(final_plan)
            else:
                with print_lock:
                    print(f"⚠️  Sample {sample_id}: No plan extracted")
            
            with print_lock:
                print(f"✅ Sample {sample_id} completed in {elapsed:.2f}s")
            
            return result
            
        except Exception as e:
            with print_lock:
                print(f"❌ Sample {sample_id} failed: {e}")
            
            return {
                'id': sample_id,
                'query': query,
                'success': False,
                'error': str(e),
            }
    
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_sample, sample) for sample in test_data]
        for future in as_completed(futures):
            result = future.result()
            results.append(result)
    
    success_count = sum(1 for r in results if r['success'])
    
    return {
        'total': len(results),
        'success': success_count,
        'failed': len(results) - success_count,
        'results': results
    }


if __name__ == '__main__':
    """Simple test"""
    import argparse
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--model', default='qwen-plus', help='Configuration name from models_config.json')
    parser.add_argument('--language', default='zh', help='Language: zh or en')
    args = parser.parse_args()
    
    base_dir = Path(__file__).parent.parent
    test_output_dir = base_dir / 'results' / 'test'
    
    result = run_agent_inference(
        model=args.model,
        language=args.language,
        test_data_path=base_dir / 'data' / f'travelplanning_query_{args.language}.json',
        database_dir=base_dir / 'database' / f'database_{args.language}',
        tool_schema_path=base_dir / 'tools' / f'tool_schema_{args.language}.json',
        output_dir=test_output_dir,
        workers=2,
    )
    print(f"\nTest completed: {result['success']}/{result['total']} succeeded")
