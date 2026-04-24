"""Unit tests for day_scheduler.schedule_day."""
from solver.day_scheduler import schedule_day


def _minimal_payload() -> dict:
    """Single attraction, no meals, no anchors — simplest feasible case."""
    return {
        "day_index": 1,
        "weekday": "Wed",
        "arrival": None,
        "departure": None,
        "start_location": "HOTEL",
        "end_location": "HOTEL",
        "attractions": [
            {
                "poi_id": "A1",
                "name": "Park",
                "open": "08:00",
                "close": "18:00",
                "stay_min": 30,
                "stay_max": 120,
            }
        ],
        "lunch_restaurant": None,
        "dinner_restaurant": None,
        "transits": {
            "('HOTEL', 'A1')": {"duration_min": 10},
            "('A1', 'HOTEL')": {"duration_min": 10},
        },
    }


def test_smoke_feasible_single_attraction():
    result = schedule_day(_minimal_payload())
    assert result["status"] == "FEASIBLE"
    assert isinstance(result["schedule"], list)
    assert any(evt["type"] == "attraction" and evt["name"] == "Park" for evt in result["schedule"])
