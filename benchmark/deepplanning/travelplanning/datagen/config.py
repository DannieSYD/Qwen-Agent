"""
Pipeline configuration, dataclasses, and constants.
"""
import os
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Set, Tuple

# ============================================================================
# Paths
# ============================================================================

_DATAGEN_DIR = os.path.dirname(os.path.abspath(__file__))
TRAVELPLANNING_DIR = os.path.dirname(_DATAGEN_DIR)
BENCHMARK_DIR = os.path.dirname(TRAVELPLANNING_DIR)

DATABASE_EN_DIR = os.path.join(TRAVELPLANNING_DIR, "database", "database_en")
QUERY_EN_PATH = os.path.join(TRAVELPLANNING_DIR, "data", "travelplanning_query_en.json")
OUTPUT_DIR = os.path.join(TRAVELPLANNING_DIR, "datagen_output", "generated")

# Tool schema path
TOOL_SCHEMA_EN_PATH = os.path.join(TRAVELPLANNING_DIR, "tools", "tool_schema_en.json")

# ============================================================================
# Database file structure
# ============================================================================

DB_FILES = {
    "flights": "flights/flights.csv",
    "trains": "trains/trains.csv",
    "hotels": "hotels/hotels.csv",
    "attractions": "attractions/attractions.csv",
    "restaurants": "restaurants/restaurants.csv",
    "locations": "locations/locations_coords.csv",
    "distance_matrix": "transportation/distance_matrix.csv",
}

# ============================================================================
# Constraint types
# ============================================================================

TRANSPORT_CONSTRAINTS = [
    # Train constraints
    "train_seat_status", "train_seat_class",
    "train_cheapest_direct", "train_shortest_duration_direct",
    "train_earliest_departure_direct", "train_latest_arrival_direct",
    "train_cheapest_train_type", "train_departure_time_range",
    # Flight constraints
    "flight_seat_status", "flight_seat_class",
    "flight_cheapest_direct", "flight_shortest_duration_direct",
    "flight_earliest_departure_direct", "flight_cheapest_airline_direct",
    "flight_cheapest_manufacturer_direct", "flight_earliest_airline_direct",
    "flight_departure_time_range", "flight_arrival_time_range",
]

HOTEL_CONSTRAINTS = [
    "hotel_cheapest_brand", "hotel_highest_rated", "hotel_cheapest_star",
    "hotel_newest_decoration", "hotel_brand_highest_rated",
    "hotel_star_highest_rated", "hotel_price_range", "hotel_star_service_required",
]

RESTAURANT_CONSTRAINTS = [
    "restaurant_cheapest_nearby_attraction", "restaurant_highest_rated",
    "restaurant_must_eat_named", "restaurant_closest_to_attraction",
    "restaurant_specific_cuisine_nearby", "restaurant_specific_tag_nearby",
]

ATTRACTION_CONSTRAINTS = [
    "attraction_must_visit_named", "attraction_all_of_type",
    "attraction_top_rated_must_visit", "attraction_all_free_attractions",
    "attraction_type_highest_rated",
]

ALL_CONSTRAINT_TYPES = (
    TRANSPORT_CONSTRAINTS + HOTEL_CONSTRAINTS +
    RESTAURANT_CONSTRAINTS + ATTRACTION_CONSTRAINTS +
    ["budget_constraint"]
)

# ============================================================================
# Attraction types
# ============================================================================

ATTRACTION_TYPES = [
    "Historical and Cultural",
    "Natural Scenery",
    "Art Exhibition",
    "City Landmark",
    "Leisure Experience",
    "Theme Park",
]

# ============================================================================
# Hotel brands and services
# ============================================================================

KNOWN_HOTEL_BRANDS = [
    "Ji Hotel", "Atour Hotel", "Marriott", "Hilton",
    "Home Inn", "Jinjiang Inn", "Hanting Hotel", "Orange Hotel",
]

KNOWN_HOTEL_SERVICES = [
    "Swimming Pool", "Gym", "SPA Service", "Robot Service",
    "TV Casting", "Washer and Dryer",
]

# ============================================================================
# Restaurant tags
# ============================================================================

KNOWN_RESTAURANT_TAGS = [
    "Must-Eat Top 10", "Birthday Package", "Private Room",
    "Outdoor Seating", "Pet-Friendly",
]

# ============================================================================
# Dataclasses
# ============================================================================

@dataclass
class DbEntry:
    """Metadata for a single existing database."""
    task_id: int
    origin: str
    dest: str
    days: int
    people_number: int
    room_number: int
    db_path: str
    num_hotels: int = 0
    num_attractions: int = 0
    num_restaurants: int = 0
    num_trains: int = 0
    num_flights: int = 0
    attraction_types: Set[str] = field(default_factory=set)
    hotel_brands: Set[str] = field(default_factory=set)
    hotel_stars: Set[int] = field(default_factory=set)
    has_flights: bool = False
    has_trains: bool = False

    @property
    def max_feasible_days(self) -> int:
        """Maximum trip days this database can support based on attraction count.
        Rule: need 2*days - 2 unique attractions minimum."""
        if self.num_attractions <= 0:
            return 0
        return (self.num_attractions + 2) // 2


@dataclass
class DatabaseIndex:
    """Index of all existing databases."""
    entries: List[DbEntry] = field(default_factory=list)
    by_route: Dict[Tuple[str, str], List[DbEntry]] = field(default_factory=dict)
    all_routes: List[Tuple[str, str]] = field(default_factory=list)
    origin_cities: Set[str] = field(default_factory=set)
    dest_cities: Set[str] = field(default_factory=set)


@dataclass
class TaskConfig:
    """Configuration for a single task to generate."""
    task_id: int
    origin: str
    dest: str
    days: int
    people_number: int
    room_number: int
    depart_date: str          # "YYYY-MM-DD"
    return_date: str          # "YYYY-MM-DD"
    depart_weekday: int       # 0=Mon ... 6=Sun
    difficulty: int           # 1=easy, 2=medium, 3=hard
    source_db_entry: Optional[DbEntry] = None


@dataclass
class PipelineConfig:
    """Configuration for the full pipeline run."""
    num_tasks: int = 1000
    workers: int = 40
    start_task_id: int = 1000
    output_dir: str = OUTPUT_DIR
    language: str = "en"
    # Difficulty distribution
    difficulty_weights: Tuple[float, float, float] = (0.4, 0.35, 0.25)
    # Retry settings
    max_retries_per_stage: int = 3
    max_retries_per_task: int = 3
    # Query generation
    query_model: str = "qwen-plus"
    # Budget constraint probability
    budget_constraint_prob: float = 0.15
