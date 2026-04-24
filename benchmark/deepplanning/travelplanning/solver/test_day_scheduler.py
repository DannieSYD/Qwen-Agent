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


def test_single_attraction_respects_business_hours():
    payload = _minimal_payload()
    payload["attractions"][0]["open"] = "09:00"
    payload["attractions"][0]["close"] = "17:00"
    payload["attractions"][0]["stay_min"] = 60
    payload["attractions"][0]["stay_max"] = 60

    result = schedule_day(payload)
    assert result["status"] == "FEASIBLE"
    attr_event = next(e for e in result["schedule"] if e["type"] == "attraction")
    start_min = _hhmm_to_min(attr_event["start"])
    end_min = _hhmm_to_min(attr_event["end"])
    assert 9 * 60 <= start_min
    assert end_min <= 17 * 60
    assert end_min - start_min == 60


def _hhmm_to_min(s: str) -> int:
    h, m = s.split(":")
    return int(h) * 60 + int(m)


def test_two_attractions_ordered_with_transit():
    payload = _minimal_payload()
    payload["attractions"] = [
        {"poi_id": "A1", "name": "Morning POI", "open": "08:00", "close": "12:00",
         "stay_min": 60, "stay_max": 60},
        {"poi_id": "A2", "name": "Afternoon POI", "open": "13:00", "close": "18:00",
         "stay_min": 60, "stay_max": 60},
    ]
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 10},
        "('HOTEL', 'A2')": {"duration_min": 15},
        "('A1', 'A2')": {"duration_min": 20},
        "('A2', 'A1')": {"duration_min": 20},
        "('A1', 'HOTEL')": {"duration_min": 10},
        "('A2', 'HOTEL')": {"duration_min": 15},
    }
    result = schedule_day(payload)
    assert result["status"] == "FEASIBLE"
    attractions = [e for e in result["schedule"] if e["type"] == "attraction"]
    assert [e["name"] for e in attractions] == ["Morning POI", "Afternoon POI"]
    kinds = [e["type"] for e in result["schedule"]]
    assert "transit" in kinds
    m_end = _hhmm_to_min(attractions[0]["end"])
    a_start = _hhmm_to_min(attractions[1]["start"])
    assert a_start >= m_end + 20


def test_forced_overlap_is_infeasible():
    """Two attractions whose business hours force an overlap."""
    payload = _minimal_payload()
    payload["attractions"] = [
        {"poi_id": "A1", "name": "POI1", "open": "10:00", "close": "11:30",
         "stay_min": 60, "stay_max": 60},
        {"poi_id": "A2", "name": "POI2", "open": "10:00", "close": "11:30",
         "stay_min": 60, "stay_max": 60},
    ]
    payload["transits"] = {
        "('HOTEL', 'A1')": {"duration_min": 5},
        "('HOTEL', 'A2')": {"duration_min": 5},
        "('A1', 'A2')": {"duration_min": 5},
        "('A2', 'A1')": {"duration_min": 5},
        "('A1', 'HOTEL')": {"duration_min": 5},
        "('A2', 'HOTEL')": {"duration_min": 5},
    }
    result = schedule_day(payload)
    assert result["status"] == "INFEASIBLE"


def _meal_payload() -> dict:
    """Payload with one attraction + lunch + dinner to exercise meal constraints."""
    p = _minimal_payload()
    p["attractions"] = [
        {"poi_id": "A1", "name": "Museum", "open": "09:00", "close": "21:00",
         "stay_min": 60, "stay_max": 60},
    ]
    p["lunch_restaurant"] = {
        "poi_id": "R1", "name": "Lunch R",
        "open": "11:00", "close": "14:00",
        "stay_min": 60, "stay_max": 60,
    }
    p["dinner_restaurant"] = {
        "poi_id": "R2", "name": "Dinner R",
        "open": "17:00", "close": "21:00",
        "stay_min": 60, "stay_max": 60,
    }
    p["transits"] = {
        f"('{x}', '{y}')": {"duration_min": 10}
        for x in ["HOTEL", "A1", "R1", "R2"]
        for y in ["HOTEL", "A1", "R1", "R2"]
        if x != y
    }
    return p


def test_lunch_and_dinner_in_windows_with_gap():
    result = schedule_day(_meal_payload())
    assert result["status"] == "FEASIBLE"

    meals = [e for e in result["schedule"] if e["type"] == "meal"]
    assert len(meals) == 2
    lunch = next(e for e in meals if e["name"] == "Lunch R")
    dinner = next(e for e in meals if e["name"] == "Dinner R")

    lunch_start = _hhmm_to_min(lunch["start"])
    lunch_end = _hhmm_to_min(lunch["end"])
    dinner_start = _hhmm_to_min(dinner["start"])

    assert 11 * 60 + 30 <= lunch_start <= 13 * 60 + 30
    assert 17 * 60 <= dinner_start <= 20 * 60 + 30
    assert dinner_start - lunch_end >= 120
