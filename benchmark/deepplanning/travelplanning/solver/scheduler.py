"""
Greedy day-by-day scheduler.

Takes selected entities from CPSATEntitySelector, assigns them to days,
builds activity sequences, and calls assemble_day() for deterministic
timestamp computation.
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime


def _parse_hour(time_str: str) -> int:
    """Extract hour from 'YYYY-MM-DD HH:MM:SS' or 'HH:MM' string."""
    if not time_str:
        return 12
    try:
        if len(time_str) > 10:
            return int(time_str.split(" ")[1].split(":")[0])
        return int(time_str.split(":")[0])
    except (IndexError, ValueError):
        return 12


class GreedyScheduler:
    """
    Given CP-SAT selected entities, build a complete day-by-day plan.

    Uses WorkingMemory.assemble_day() for deterministic scheduling.
    """

    def __init__(self, data: Dict[str, Any], selection, memory):
        """
        Args:
            data: Full data dict from export_memory_as_dict
            selection: SolverResult from CPSATEntitySelector
            memory: WorkingMemory instance (for assemble_day)
        """
        self.data = data
        self.sel = selection
        self.memory = memory
        self.meta = data["trip_meta"]
        self.days = self.meta.get("days", 2)
        self.people = self.meta.get("people", 1)
        self.rooms = self.meta.get("rooms", 1)
        self.routes = data.get("routes", {})
        self.attractions_data = data.get("attractions", {})

    def schedule(self) -> Tuple[str, str]:
        """
        Build complete plan text.

        Returns:
            (plan_text, feedback): plan text on success, feedback describes issues
        """
        if not self.sel.outbound or not self.sel.inbound or not self.sel.hotel:
            return "", "SOLVER_FEEDBACK: Missing critical selection (transport or hotel)."

        # Assign attractions to days
        attraction_schedule = self._assign_attractions_to_days()

        # Assign restaurants to meal slots
        meal_schedule = self._assign_meals_to_days(attraction_schedule)

        # Build each day
        day_plans = []
        errors = []

        for day_num in range(1, self.days + 1):
            activities = self._build_day_activities(
                day_num, attraction_schedule, meal_schedule
            )
            current_city = self._get_current_city(day_num)
            accommodation = self.sel.hotel["name"] if day_num < self.days else "-"
            accom_price = (
                f"¥{int(self.sel.hotel.get('price', 0))}/room/night"
                if day_num < self.days else "-"
            )

            args = {
                "day": day_num,
                "current_city": current_city,
                "accommodation": accommodation,
                "accommodation_price": accom_price,
                "activities": activities,
            }

            result = self.memory.assemble_day(args)
            if "ERROR:" in result:
                errors.append(f"Day {day_num}: {result}")
            day_plans.append(result)

        # Budget summary
        budget_text = self._format_budget_summary(day_plans)
        plan_text = "\n\n".join(day_plans) + "\n\n" + budget_text

        feedback = ""
        if errors:
            feedback = "SCHEDULER_WARNINGS:\n" + "\n".join(f"- {e}" for e in errors)

        return plan_text, feedback

    # ──────────────────────────────────────────────────────────────────────
    # Attraction assignment
    # ──────────────────────────────────────────────────────────────────────

    def _assign_attractions_to_days(self) -> Dict[int, List[str]]:
        """Assign selected attractions to days. Greedy nearest-first."""
        selected = list(self.sel.attractions)
        if not selected:
            return {d: [] for d in range(1, self.days + 1)}

        # Determine capacity per day based on transport times
        ob = self.sel.outbound
        ib = self.sel.inbound
        arr_hour = _parse_hour(ob.get("arr_time", "")) if ob else 12
        dep_hour = _parse_hour(ib.get("dep_time", "")) if ib else 18

        capacity = {}
        for d in range(1, self.days + 1):
            if d == 1:
                # Arrival day: fewer attractions if arriving late
                if arr_hour < 10:
                    capacity[d] = 2
                elif arr_hour < 14:
                    capacity[d] = 1
                else:
                    capacity[d] = 0
            elif d == self.days:
                # Departure day: if departing after 14:00, there's time for attractions
                if dep_hour >= 16:
                    capacity[d] = 2
                elif dep_hour >= 14:
                    capacity[d] = 1
                else:
                    capacity[d] = 0
            else:
                # Full sightseeing day
                capacity[d] = 3

        schedule = {d: [] for d in range(1, self.days + 1)}
        remaining = list(selected)

        # Round-robin distribution: give each day with capacity at least 1
        # attraction before giving any day a 2nd. This ensures Day 2 isn't empty.
        days_with_capacity = [d for d in range(1, self.days + 1) if capacity[d] > 0]

        # Pass 1: one attraction per day
        for d in days_with_capacity:
            if remaining:
                ref = self.sel.hotel.get("name", "") if self.sel.hotel else ""
                nearest = self._pick_nearest_attraction(remaining, ref)
                schedule[d].append(nearest)
                remaining.remove(nearest)

        # Pass 2+: fill remaining capacity
        for d in sorted(days_with_capacity, key=lambda d: (-capacity[d], d)):
            while remaining and len(schedule[d]) < capacity[d]:
                ref = schedule[d][-1] if schedule[d] else (self.sel.hotel.get("name", "") if self.sel.hotel else "")
                nearest = self._pick_nearest_attraction(remaining, ref)
                schedule[d].append(nearest)
                remaining.remove(nearest)

        # If attractions still remain, squeeze them into days with room
        for attr in remaining:
            best_day = min(
                range(1, self.days + 1),
                key=lambda d: len(schedule[d]),
            )
            schedule[best_day].append(attr)

        return schedule

    def _pick_nearest_attraction(self, candidates: List[str], reference: str) -> str:
        """Pick the candidate attraction nearest to reference location."""
        if not candidates:
            return ""
        if not reference:
            return candidates[0]

        best = candidates[0]
        best_dist = float("inf")

        for cand in candidates:
            # Check route distance
            key_fwd = f"{reference} -> {cand}"
            key_rev = f"{cand} -> {reference}"
            route = self.routes.get(key_fwd) or self.routes.get(key_rev)
            if route:
                dist = route.get("distance_km", 999) or 999
                if dist < best_dist:
                    best_dist = dist
                    best = cand

        return best

    # ──────────────────────────────────────────────────────────────────────
    # Restaurant/meal assignment
    # ──────────────────────────────────────────────────────────────────────

    def _assign_meals_to_days(
        self, attraction_schedule: Dict[int, List[str]]
    ) -> Dict[int, Dict[str, Optional[Dict]]]:
        """Assign selected restaurants to (day, meal_type) slots."""
        available = list(self.sel.restaurants)

        ob = self.sel.outbound
        ib = self.sel.inbound
        arr_hour = _parse_hour(ob.get("arr_time", "")) if ob else 12
        dep_hour = _parse_hour(ib.get("dep_time", "")) if ib else 18

        meal_schedule: Dict[int, Dict[str, Optional[Dict]]] = {}

        for day_num in range(1, self.days + 1):
            needs_lunch, needs_dinner = self._meal_needs(day_num, arr_hour, dep_hour)
            day_attrs = attraction_schedule.get(day_num, [])
            meals: Dict[str, Optional[Dict]] = {}

            if needs_lunch and available:
                best = self._pick_best_restaurant(available, day_attrs, "Lunch")
                meals["Lunch"] = best
                available.remove(best)

            if needs_dinner and available:
                best = self._pick_best_restaurant(available, day_attrs, "Dinner")
                meals["Dinner"] = best
                available.remove(best)

            meal_schedule[day_num] = meals

        return meal_schedule

    def _meal_needs(self, day_num: int, arr_hour: int, dep_hour: int) -> Tuple[bool, bool]:
        """Determine if a day needs lunch and/or dinner."""
        if day_num == 1:
            # Arrival day
            needs_lunch = arr_hour < 11
            needs_dinner = arr_hour < 16
            return needs_lunch, needs_dinner
        elif day_num == self.days:
            # Departure day: evaluator requires meals if departing late
            needs_lunch = dep_hour >= 13
            needs_dinner = dep_hour >= 19
            return needs_lunch, needs_dinner
        else:
            # Full day: always lunch + dinner
            return True, True

    def _pick_best_restaurant(
        self, available: List[Dict], day_attrs: List[str], meal_type: str
    ) -> Dict:
        """Pick best restaurant for a meal slot, preferring those near day's attractions."""
        if not available:
            return {}

        # Score: prefer restaurants whose "near" field matches a day attraction
        scored = []
        for r in available:
            score = 0
            near = r.get("near", "")
            if near:
                for attr in day_attrs:
                    if near.lower() in attr.lower() or attr.lower() in near.lower():
                        score += 10
                        break

            # Check if restaurant is open for this meal type
            r_open = r.get("open", "")
            r_close = r.get("close", "")
            if meal_type == "Lunch" and r_open:
                try:
                    open_h = int(r_open.split(":")[0])
                    if open_h > 14:
                        score -= 100  # dinner-only restaurant, bad for lunch
                except (ValueError, IndexError):
                    pass
            if meal_type == "Dinner" and r_close:
                try:
                    close_h = int(r_close.split(":")[0])
                    if close_h < 17:
                        score -= 100  # closes before dinner
                except (ValueError, IndexError):
                    pass

            # Prefer higher-rated
            rating = r.get("rating")
            if rating is not None:
                try:
                    score += float(rating)
                except (ValueError, TypeError):
                    pass

            scored.append((score, r))

        scored.sort(key=lambda x: -x[0])
        return scored[0][1]

    # ──────────────────────────────────────────────────────────────────────
    # Activity sequence building
    # ──────────────────────────────────────────────────────────────────────

    def _build_day_activities(
        self, day_num: int,
        attraction_schedule: Dict[int, List[str]],
        meal_schedule: Dict[int, Dict[str, Optional[Dict]]],
    ) -> List[Dict]:
        """Build ordered activity list for a single day."""
        activities = []
        day_attrs = attraction_schedule.get(day_num, [])
        day_meals = meal_schedule.get(day_num, {})

        if day_num == 1:
            # ── Arrival day ──
            ob = self.sel.outbound
            if ob:
                activities.append({
                    "type": "intercity",
                    "transport_type": ob["mode"],
                    "id": ob["id"],
                })
                buffer_min = 40 if ob["mode"] == "flight" else 20
                activities.append({
                    "type": "buffer",
                    "description": "Arrival, luggage",
                    "duration_min": buffer_min,
                })
            # Hotel check-in
            activities.append({"type": "hotel", "action": "Check-in"})

            # Attractions + meals
            self._interleave_attractions_meals(activities, day_attrs, day_meals)

            # End with hotel rest
            activities.append({"type": "hotel", "action": "Rest"})

        elif day_num == self.days:
            # ── Departure day ──
            activities.append({"type": "hotel", "action": "Check-out"})

            # Optional attractions + meals before departure
            self._interleave_attractions_meals(activities, day_attrs, day_meals)

            # Travel to station + intercity
            ib = self.sel.inbound
            if ib:
                buffer_min = 40 if ib["mode"] == "flight" else 20
                activities.append({
                    "type": "buffer",
                    "description": "Travel to station, security",
                    "duration_min": buffer_min,
                })
                activities.append({
                    "type": "intercity",
                    "transport_type": ib["mode"],
                    "id": ib["id"],
                })

        else:
            # ── Full sightseeing day ──
            # No check-out/check-in for middle days, just activities from hotel
            self._interleave_attractions_meals(activities, day_attrs, day_meals)

            # End with hotel rest
            activities.append({"type": "hotel", "action": "Rest"})

        return activities

    def _interleave_attractions_meals(
        self, activities: List[Dict],
        day_attrs: List[str],
        day_meals: Dict[str, Optional[Dict]],
    ):
        """Interleave attractions with lunch/dinner in a sensible order.

        Ensures lunch and dinner are separated by enough attractions/time
        to satisfy the >=2h gap requirement.
        """
        lunch = day_meals.get("Lunch")
        dinner = day_meals.get("Dinner")
        lunch_placed = False
        dinner_placed = False
        n_attrs = len(day_attrs)

        if lunch and dinner and n_attrs >= 2:
            # With both meals and 2+ attractions:
            # attraction(s) → lunch → attraction(s) → dinner
            # Split attractions: first half before lunch, second half before dinner
            split = max(1, n_attrs // 2)

            for i, attr_name in enumerate(day_attrs):
                activities.append({"type": "attraction", "name": attr_name})

                if not lunch_placed and i >= split - 1:
                    activities.append({
                        "type": "meal", "meal_type": "Lunch",
                        "restaurant": lunch["name"], "duration_min": 60,
                    })
                    lunch_placed = True

            if not lunch_placed and lunch:
                activities.append({
                    "type": "meal", "meal_type": "Lunch",
                    "restaurant": lunch["name"], "duration_min": 60,
                })
                lunch_placed = True

            activities.append({
                "type": "meal", "meal_type": "Dinner",
                "restaurant": dinner["name"], "duration_min": 60,
            })
            dinner_placed = True

        else:
            # Fewer constraints: place meals around attractions
            for i, attr_name in enumerate(day_attrs):
                activities.append({"type": "attraction", "name": attr_name})

                if not lunch_placed and lunch:
                    activities.append({
                        "type": "meal", "meal_type": "Lunch",
                        "restaurant": lunch["name"], "duration_min": 60,
                    })
                    lunch_placed = True

            if not lunch_placed and lunch:
                activities.append({
                    "type": "meal", "meal_type": "Lunch",
                    "restaurant": lunch["name"], "duration_min": 60,
                })

            # If both meals and not enough attractions between them,
            # insert hotel rest to ensure >=2h gap (accounting for travel time)
            if dinner and not dinner_placed and lunch:
                activities.append({
                    "type": "hotel", "action": "Rest", "duration_min": 150,
                })

            if dinner and not dinner_placed:
                activities.append({
                    "type": "meal", "meal_type": "Dinner",
                    "restaurant": dinner["name"], "duration_min": 60,
                })

    # ──────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────

    def _get_current_city(self, day_num: int) -> str:
        """Build 'Current City' string for a day."""
        origin = self.meta.get("origin", "")
        destinations = self.meta.get("destinations", [])
        dest = destinations[0] if destinations else ""

        if day_num == 1:
            return f"from {origin} to {dest}"
        elif day_num == self.days:
            return f"from {dest} to {origin}"
        else:
            return dest

    def _format_budget_summary(self, day_plans: List[str]) -> str:
        """Format budget summary from solver cost breakdown + actual city transport."""
        bd = self.sel.cost_breakdown

        # Parse actual city transport costs from assembled day plans
        city_transport = 0
        for plan_text in day_plans:
            for line in plan_text.split("\n"):
                if "travel_city" in line and "¥" in line:
                    # Extract cost from "... ¥XX" pattern
                    try:
                        cost_part = line.rsplit("¥", 1)[-1]
                        cost_val = float(cost_part.strip().rstrip(")").split("/")[0])
                        city_transport += int(cost_val) * self.people
                    except (ValueError, IndexError):
                        pass

        transport_total = bd.get("transport", 0)
        hotel_total = bd.get("hotel", 0)
        meal_total = bd.get("meals", 0)
        attraction_total = bd.get("attractions", 0)
        grand_total = transport_total + hotel_total + meal_total + attraction_total + city_transport

        lines = [
            "Budget Summary:",
            f"Transportation: ¥{transport_total}",
            f"Accommodation: ¥{hotel_total}",
            f"Meals: ¥{meal_total}",
            f"Attractions & Tickets: ¥{attraction_total}",
            f"City Transport: ¥{city_transport}",
            f"Total Estimated Budget: ¥{grand_total}",
        ]
        return "\n".join(lines)
