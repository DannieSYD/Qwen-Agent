"""
Parameterized CP-SAT solver template for entity selection.

Takes structured data dict + constraint tuples and compiles them into a CP-SAT
model. Selects optimal transport, hotel, attractions, and restaurants subject
to constraints. No LLM-generated code — the model structure is fixed.

Constraint compilation is generic: based on (variable, operator, value) tuples,
NOT on evaluation-specific constraint type names.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple
import math


@dataclass
class SolverResult:
    """Result of CP-SAT entity selection."""
    status: str  # "OPTIMAL" | "FEASIBLE" | "INFEASIBLE" | "MISSING_DATA" | "ERROR"
    outbound: Optional[Dict] = None
    inbound: Optional[Dict] = None
    hotel: Optional[Dict] = None
    attractions: List[str] = field(default_factory=list)
    restaurants: List[Dict] = field(default_factory=list)
    total_cost: int = 0
    cost_breakdown: Dict[str, int] = field(default_factory=dict)
    feedback: str = ""


# ── Field mapping: constraint variable suffix → entity dict key ──────────
# Used by _get_entity_field to extract the right value from an entity dict.
_FIELD_MAP = {
    "star": "star",
    "price": "price",
    "service": "services",
    "brand": "brand",
    "name": "name",
    "tag": "tags",
    "cuisine": "cuisine",
    "location": "near",
    "mode": "mode",
    "seat_class": "seat_class",
    "time": "dep_time",
    "rating": "rating",
    "type": "type",
    "id": "id",
}


def _safe_int(val, default=0) -> int:
    """Convert a value to int, rounding floats."""
    if val is None:
        return default
    try:
        return int(round(float(val)))
    except (ValueError, TypeError):
        return default


def _check_constraint(entity_val: Any, operator: str, target_val: Any) -> bool:
    """
    Generic constraint check: does entity_val satisfy (operator, target_val)?

    Works for scalar fields, list fields (services, tags), and strings.
    Returns True if the constraint is satisfied or if data is unknown (optimistic).
    """
    if entity_val is None:
        return True  # unknown data → don't exclude (optimistic)

    # ── List fields (services, tags): membership check ──
    if isinstance(entity_val, list):
        target_str = str(target_val).lower()
        entity_strs = [str(v).lower() for v in entity_val]
        if operator in ("=", "in"):
            return any(target_str in s for s in entity_strs)
        return True

    # ── Numeric comparisons ──
    try:
        ev = float(entity_val)
        tv = float(target_val)
        if operator == "=":
            return abs(ev - tv) < 0.01
        elif operator == "<=":
            return ev <= tv + 0.01
        elif operator == ">=":
            return ev >= tv - 0.01
        elif operator == "<":
            return ev < tv
        elif operator == ">":
            return ev > tv
    except (ValueError, TypeError):
        pass

    # ── String comparisons ──
    ev_str = str(entity_val).lower()
    tv_str = str(target_val).lower()

    if operator == "=":
        return ev_str == tv_str or tv_str in ev_str or ev_str in tv_str
    elif operator == "in":
        if isinstance(target_val, list):
            return ev_str in [str(v).lower() for v in target_val]
        return tv_str in ev_str
    elif operator == "near":
        return tv_str in ev_str or ev_str in tv_str

    return True  # unknown operator → don't exclude


class CPSATEntitySelector:
    """
    Generic parameterized CP-SAT solver for entity selection.

    Compiles constraint tuples into CP-SAT constraints using variable prefix
    (entity category) and suffix (entity field). No domain-specific constraint
    type names are referenced.
    """

    def __init__(self, data: Dict[str, Any]):
        self.data = data
        self.meta = data.get("trip_meta", {})
        self.constraints = data.get("constraints", [])
        self.people = self.meta.get("people", 1)
        self.rooms = self.meta.get("rooms", 1)
        self.days = self.meta.get("days", 2)

        self.outbound_raw = data.get("outbound_transport", [])
        self.inbound_raw = data.get("inbound_transport", [])
        self.hotels_raw = data.get("hotels", [])
        self.attractions_raw = data.get("attractions", {})  # dict name→details
        self.restaurants_raw = data.get("restaurants", [])
        self.routes = data.get("routes", {})

    # ──────────────────────────────────────────────────────────────────────
    # Public API
    # ──────────────────────────────────────────────────────────────────────

    def solve(self) -> SolverResult:
        """Run entity selection. Returns SolverResult."""
        # Phase 0: data completeness
        missing = self._check_data_completeness()
        if missing:
            return SolverResult(
                status="MISSING_DATA",
                feedback="SOLVER_FEEDBACK: Missing data — query these before calling run_solver:\n" + missing,
            )

        # Phase 1: pre-filter entities by precise constraints
        ob_feasible = self._filter_entities(self.outbound_raw, "transport.outbound")
        ib_feasible = self._filter_entities(self.inbound_raw, "transport.inbound")
        h_feasible = self._filter_entities(self.hotels_raw, "hotel")
        r_feasible = self._filter_entities(self.restaurants_raw, "restaurant")

        # Also apply generic "transport" prefix constraints to both directions
        ob_feasible = self._filter_entities(ob_feasible, "transport")
        ib_feasible = self._filter_entities(ib_feasible, "transport")

        # Check if filtering emptied any category
        empty_feedback = self._check_filtered_empty(
            ob_feasible, ib_feasible, h_feasible, r_feasible
        )
        if empty_feedback:
            return SolverResult(status="INFEASIBLE", feedback=empty_feedback)

        # Phase 2: build and solve CP-SAT model
        try:
            return self._build_and_solve(ob_feasible, ib_feasible, h_feasible, r_feasible)
        except Exception as e:
            return SolverResult(status="ERROR", feedback=f"SOLVER_ERROR: {e}")

    # ──────────────────────────────────────────────────────────────────────
    # Data completeness check
    # ──────────────────────────────────────────────────────────────────────

    def _check_data_completeness(self) -> str:
        """Return missing-data description, or empty string if complete."""
        issues = []
        if not self.outbound_raw:
            dest = ", ".join(self.meta.get("destinations", [])) or "destination"
            issues.append(f"No outbound transport. Query flights/trains from {self.meta.get('origin', '?')} to {dest} on {self.meta.get('depart_date', '?')}.")
        if not self.inbound_raw:
            dest = ", ".join(self.meta.get("destinations", [])) or "destination"
            issues.append(f"No inbound transport. Query flights/trains from {dest} to {self.meta.get('origin', '?')} on {self.meta.get('return_date', '?')}.")
        if not self.hotels_raw:
            dest = ", ".join(self.meta.get("destinations", [])) or "destination"
            issues.append(f"No hotels found. Query hotels in {dest}.")
        if not self.attractions_raw:
            dest = ", ".join(self.meta.get("destinations", [])) or "destination"
            issues.append(f"No attractions found. Use recommend_attractions for {dest}, then query_attraction_details.")

        # Check attractions without details
        no_details = [n for n, d in self.attractions_raw.items() if d.get("_no_details")]
        if no_details:
            issues.append(f"Attractions without details (need query_attraction_details): {no_details[:5]}")

        if not self.restaurants_raw:
            issues.append("No restaurants found. Use recommend_restaurants for the destination.")

        # Check must-visit attractions are in data
        for c in self.constraints:
            if c.get("variable") in ("attraction.must_visit", "attraction.name") and c.get("operator") in ("=", "in"):
                names = c["value"] if isinstance(c["value"], list) else [c["value"]]
                for name in names:
                    if name not in self.attractions_raw:
                        issues.append(f"Must-visit attraction '{name}' not found. Query attraction details for it.")

        return "\n".join(f"- {i}" for i in issues) if issues else ""

    # ──────────────────────────────────────────────────────────────────────
    # Pre-filtering
    # ──────────────────────────────────────────────────────────────────────

    # Constraints that mean "at least one must satisfy" rather than "all must satisfy".
    # These are handled in _compile_constraints, NOT pre-filtering.
    _AT_LEAST_ONE_VARS = {
        "attraction.must_visit", "attraction.name", "attraction.type",
        "restaurant.tag", "restaurant.cuisine", "restaurant.location",
        "budget.total",
    }

    def _filter_entities(self, entities: list, var_prefix: str) -> list:
        """
        Remove entities that fail any precise constraint with matching prefix.
        Only filters constraints that apply to ALL entities in the category
        (e.g., hotel.star=3 means all selected hotels must be 3-star).
        """
        relevant = [
            c for c in self.constraints
            if c.get("variable", "").startswith(var_prefix)
            and c.get("certainty", "precise") == "precise"
            and c.get("variable", "") not in self._AT_LEAST_ONE_VARS
        ]
        if not relevant:
            return list(entities)

        feasible = []
        for entity in entities:
            passes = True
            for c in relevant:
                suffix = c["variable"].split(".")[-1]
                field_key = _FIELD_MAP.get(suffix, suffix)
                entity_val = entity.get(field_key)
                if not _check_constraint(entity_val, c["operator"], c["value"]):
                    passes = False
                    break
            if passes:
                feasible.append(entity)
        return feasible

    def _check_filtered_empty(self, ob, ib, hotels, restaurants) -> str:
        """Check if pre-filtering emptied any required category."""
        issues = []
        for label, feasible, raw, prefix in [
            ("outbound transport", ob, self.outbound_raw, "transport.outbound"),
            ("inbound transport", ib, self.inbound_raw, "transport.inbound"),
            ("hotels", hotels, self.hotels_raw, "hotel"),
            ("restaurants", restaurants, self.restaurants_raw, "restaurant"),
        ]:
            if len(raw) > 0 and len(feasible) == 0:
                relevant = [
                    c for c in self.constraints
                    if c.get("variable", "").startswith(prefix)
                ]
                desc = "; ".join(
                    f"{c['variable']} {c['operator']} {c['value']}" for c in relevant
                )
                issues.append(
                    f"No {label} satisfies constraints ({desc}). "
                    f"Had {len(raw)} options, all filtered out. "
                    f"Query more {label} or check constraint extraction."
                )
        if issues:
            return "SOLVER_INFEASIBLE: Constraint filtering eliminated all options.\n" + "\n".join(f"- {i}" for i in issues)
        return ""

    # ──────────────────────────────────────────────────────────────────────
    # CP-SAT model
    # ──────────────────────────────────────────────────────────────────────

    def _build_and_solve(self, ob_list, ib_list, h_list, r_list) -> SolverResult:
        from ortools.sat.python import cp_model

        model = cp_model.CpModel()
        nights = max(self.days - 1, 1)

        # ── Boolean selection variables ──
        ob_vars = [model.NewBoolVar(f"ob_{i}") for i in range(len(ob_list))]
        ib_vars = [model.NewBoolVar(f"ib_{i}") for i in range(len(ib_list))]
        h_vars = [model.NewBoolVar(f"h_{i}") for i in range(len(h_list))]

        model.AddExactlyOne(ob_vars)
        model.AddExactlyOne(ib_vars)
        model.AddExactlyOne(h_vars)

        # Attractions: BoolVar per attraction (1=visit, 0=skip)
        attr_names = list(self.attractions_raw.keys())
        a_vars = [model.NewBoolVar(f"a_{i}") for i in range(len(attr_names))]

        # Restaurants: BoolVar per restaurant (1=selected, 0=not)
        r_vars = [model.NewBoolVar(f"r_{i}") for i in range(len(r_list))]

        # ── Cost computation (integer yuan) ──
        MAX_COST = 10_000_000

        ob_cost = model.NewIntVar(0, MAX_COST, "ob_cost")
        model.Add(ob_cost == sum(
            ob_vars[i] * _safe_int(ob_list[i].get("price", 0)) * self.people
            for i in range(len(ob_list))
        ))

        ib_cost = model.NewIntVar(0, MAX_COST, "ib_cost")
        model.Add(ib_cost == sum(
            ib_vars[i] * _safe_int(ib_list[i].get("price", 0)) * self.people
            for i in range(len(ib_list))
        ))

        hotel_cost = model.NewIntVar(0, MAX_COST, "hotel_cost")
        model.Add(hotel_cost == sum(
            h_vars[i] * _safe_int(h_list[i].get("price", 0)) * self.rooms * nights
            for i in range(len(h_list))
        ))

        attr_cost = model.NewIntVar(0, MAX_COST, "attr_cost")
        if attr_names:
            model.Add(attr_cost == sum(
                a_vars[i] * _safe_int(
                    (self.attractions_raw[attr_names[i]].get("price") or 0)
                ) * self.people
                for i in range(len(attr_names))
            ))
        else:
            model.Add(attr_cost == 0)

        meal_cost = model.NewIntVar(0, MAX_COST, "meal_cost")
        if r_list:
            model.Add(meal_cost == sum(
                r_vars[i] * _safe_int(r_list[i].get("price_per_person", 0)) * self.people
                for i in range(len(r_list))
            ))
        else:
            model.Add(meal_cost == 0)

        # City transport estimate: flat per-day estimate (not in CP-SAT scope)
        city_transport_estimate = self.days * 30 * self.people  # rough ¥30/day/person

        total_cost = model.NewIntVar(0, 99_000_000, "total_cost")
        model.Add(total_cost == ob_cost + ib_cost + hotel_cost + attr_cost + meal_cost + city_transport_estimate)

        # ── Structural constraints ──
        self._add_structural_constraints(model, a_vars, attr_names, r_vars, r_list)

        # ── Compile constraint tuples into CP-SAT ──
        self._compile_constraints(
            model, ob_vars, ib_vars, h_vars, a_vars, r_vars,
            ob_list, ib_list, h_list, attr_names, r_list,
            total_cost,
        )

        # ── Objective: minimize cost ──
        model.Minimize(total_cost)

        # ── Solve ──
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = 30.0
        status = solver.Solve(model)

        if status in (cp_model.OPTIMAL, cp_model.FEASIBLE):
            return self._extract_result(
                solver, status, ob_vars, ib_vars, h_vars, a_vars, r_vars,
                ob_list, ib_list, h_list, attr_names, r_list,
                total_cost,
                {"ob_cost": ob_cost, "ib_cost": ib_cost, "hotel_cost": hotel_cost,
                 "attr_cost": attr_cost, "meal_cost": meal_cost},
                city_transport_estimate,
            )
        else:
            return SolverResult(
                status="INFEASIBLE",
                feedback=self._diagnose_infeasibility(ob_list, ib_list, h_list, r_list, attr_names),
            )

    def _add_structural_constraints(self, model, a_vars, attr_names, r_vars, r_list):
        """Domain-structural constraints (always true for travel planning)."""
        # Minimum meals: ~2 per full day, ~1 per travel day
        full_days = max(self.days - 2, 0)
        # Arrival + departure days get ~1 meal each, full days get 2
        min_meals = max(full_days * 2 + 2, 2)
        max_meals = self.days * 2

        if r_list:
            num_restaurants = model.NewIntVar(0, len(r_list), "num_r")
            model.Add(num_restaurants == sum(r_vars))
            model.Add(num_restaurants >= min(min_meals, len(r_list)))
            model.Add(num_restaurants <= min(max_meals, len(r_list)))

        # Minimum attractions: need enough to cover each day.
        # Full sightseeing days need ≥2. Arrival/departure days need ≥1 if time allows.
        # Conservative: at least days * 1 (one per day), capped by available.
        if attr_names:
            min_attractions = max(full_days * 2 + 2, self.days + 1)
            model.Add(sum(a_vars) >= min(min_attractions, len(attr_names)))

    def _compile_constraints(
        self, model, ob_vars, ib_vars, h_vars, a_vars, r_vars,
        ob_list, ib_list, h_list, attr_names, r_list, total_cost,
    ):
        """
        Compile constraint tuples into CP-SAT constraints.

        Generic: uses variable prefix for entity category, operator for constraint type.
        """
        for c in self.constraints:
            variable = c.get("variable", "")
            operator = c.get("operator", "=")
            value = c.get("value")
            certainty = c.get("certainty", "precise")

            # ── Budget constraint ──
            if variable.startswith("budget"):
                if operator == "<=" and value is not None:
                    model.Add(total_cost <= _safe_int(value))
                elif operator == ">=" and value is not None:
                    model.Add(total_cost >= _safe_int(value))
                continue

            # ── Must-visit attractions ──
            if variable in ("attraction.must_visit", "attraction.name") and operator in ("=", "in"):
                names = value if isinstance(value, list) else [value]
                for name in names:
                    for j, aname in enumerate(attr_names):
                        if aname == name or name.lower() in aname.lower() or aname.lower() in name.lower():
                            model.Add(a_vars[j] == 1)
                continue

            # ── Attraction type constraint (visit all of a type) ──
            if variable == "attraction.type" and operator in ("=", "in"):
                target_type = str(value).lower()
                for j, aname in enumerate(attr_names):
                    a_detail = self.attractions_raw.get(aname, {})
                    a_type = str(a_detail.get("type", "")).lower()
                    if target_type in a_type or a_type in target_type:
                        model.Add(a_vars[j] == 1)
                continue

            # ── Restaurant "at least one" constraints (tag, cuisine, location) ──
            if variable.startswith("restaurant.") and r_list:
                suffix = variable.split(".")[-1]
                if suffix in ("tag", "cuisine", "location"):
                    field_key = _FIELD_MAP.get(suffix, suffix)
                    matching = []
                    for j, r in enumerate(r_list):
                        r_val = r.get(field_key)
                        if _check_constraint(r_val, operator, value):
                            matching.append(j)
                    if matching:
                        # At least one matching restaurant must be selected
                        model.Add(sum(r_vars[j] for j in matching) >= 1)
                continue

            # ── Transport ID constraints (specific train/flight number) ──
            if variable.startswith("transport") and "id" in variable.split(".")[-1:]:
                target_id = str(value)
                if "outbound" in variable:
                    for j, t in enumerate(ob_list):
                        if t.get("id") == target_id:
                            model.Add(ob_vars[j] == 1)
                elif "inbound" in variable:
                    for j, t in enumerate(ib_list):
                        if t.get("id") == target_id:
                            model.Add(ib_vars[j] == 1)
                continue

            # ── Remaining precise constraints are already handled by pre-filtering ──
            # Fuzzy constraints: no hard CP-SAT constraint, just pre-filter optimistically

    # ──────────────────────────────────────────────────────────────────────
    # Result extraction
    # ──────────────────────────────────────────────────────────────────────

    def _extract_result(
        self, solver, status, ob_vars, ib_vars, h_vars, a_vars, r_vars,
        ob_list, ib_list, h_list, attr_names, r_list, total_cost, cost_vars,
        city_transport_est,
    ) -> SolverResult:
        from ortools.sat.python import cp_model

        status_str = "OPTIMAL" if status == cp_model.OPTIMAL else "FEASIBLE"

        # Find selected entities
        sel_ob = next((ob_list[i] for i in range(len(ob_list)) if solver.Value(ob_vars[i])), None)
        sel_ib = next((ib_list[i] for i in range(len(ib_list)) if solver.Value(ib_vars[i])), None)
        sel_hotel = next((h_list[i] for i in range(len(h_list)) if solver.Value(h_vars[i])), None)
        sel_attrs = [attr_names[i] for i in range(len(attr_names)) if solver.Value(a_vars[i])]
        sel_restaurants = [r_list[i] for i in range(len(r_list)) if solver.Value(r_vars[i])]

        breakdown = {
            "transport": solver.Value(cost_vars["ob_cost"]) + solver.Value(cost_vars["ib_cost"]),
            "hotel": solver.Value(cost_vars["hotel_cost"]),
            "meals": solver.Value(cost_vars["meal_cost"]),
            "attractions": solver.Value(cost_vars["attr_cost"]),
            "city_transport": city_transport_est,
        }

        return SolverResult(
            status=status_str,
            outbound=sel_ob,
            inbound=sel_ib,
            hotel=sel_hotel,
            attractions=sel_attrs,
            restaurants=sel_restaurants,
            total_cost=solver.Value(total_cost),
            cost_breakdown=breakdown,
        )

    # ──────────────────────────────────────────────────────────────────────
    # Infeasibility diagnosis
    # ──────────────────────────────────────────────────────────────────────

    def _diagnose_infeasibility(self, ob_list, ib_list, h_list, r_list, attr_names) -> str:
        """Produce structured feedback when CP-SAT returns INFEASIBLE."""
        issues = []

        # Check budget feasibility
        budget_constraints = [
            c for c in self.constraints
            if c.get("variable", "").startswith("budget") and c.get("operator") == "<="
        ]
        if budget_constraints:
            max_budget = min(_safe_int(c["value"], 999999) for c in budget_constraints)
            min_cost = self._estimate_min_cost(ob_list, ib_list, h_list, r_list)
            if min_cost > max_budget:
                issues.append(
                    f"Budget infeasible: cheapest feasible combination costs ~¥{min_cost} "
                    f"but budget is ¥{max_budget}. Query cheaper options."
                )

        # Check must-visit attractions need enough days
        must_visit = []
        for c in self.constraints:
            if c.get("variable") in ("attraction.must_visit", "attraction.name"):
                names = c["value"] if isinstance(c["value"], list) else [c["value"]]
                must_visit.extend(names)
        if len(must_visit) > self.days * 3:
            issues.append(
                f"Too many must-visit attractions ({len(must_visit)}) for {self.days} days "
                f"(max ~3 per day). Some constraints may conflict."
            )

        # Check meal requirements vs available restaurants
        min_meals = max((self.days - 2) * 2 + 2, 2)
        if len(r_list) < min_meals:
            issues.append(
                f"Need ~{min_meals} restaurant selections but only {len(r_list)} available. "
                f"Query more restaurants."
            )

        if not issues:
            issues.append("Unknown infeasibility. Try querying more options to expand the search space.")

        return "SOLVER_INFEASIBLE:\n" + "\n".join(f"- {i}" for i in issues)

    def _estimate_min_cost(self, ob_list, ib_list, h_list, r_list) -> int:
        """Estimate minimum possible cost from cheapest options."""
        nights = max(self.days - 1, 1)
        min_ob = min((_safe_int(t.get("price", 0)) for t in ob_list), default=0) * self.people
        min_ib = min((_safe_int(t.get("price", 0)) for t in ib_list), default=0) * self.people
        min_h = min((_safe_int(h.get("price", 0)) for h in h_list), default=0) * self.rooms * nights

        # Cheapest N restaurants for minimum meals
        min_meals = max((self.days - 2) * 2 + 2, 2)
        r_prices = sorted(_safe_int(r.get("price_per_person", 0)) for r in r_list)
        min_meal = sum(r_prices[:min_meals]) * self.people

        return min_ob + min_ib + min_h + min_meal
