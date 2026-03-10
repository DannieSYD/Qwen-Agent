"""
Stage 0: Database Index

Catalogs the existing 120 databases without merging them.
Builds a lightweight index of routes, entity counts, and metadata
for efficient source database selection in later stages.
"""
import os
from collections import defaultdict
from typing import Dict, List, Set, Tuple

from .config import DATABASE_EN_DIR, QUERY_EN_PATH, DB_FILES, DbEntry, DatabaseIndex
from .utils import load_json, count_csv_rows, read_csv_as_dicts, safe_int


def build_db_index(
    database_dir: str = DATABASE_EN_DIR,
    query_path: str = QUERY_EN_PATH,
) -> DatabaseIndex:
    """
    Build an index of all existing databases.

    Reads the query JSON for route metadata and scans each database
    directory for entity counts and feature metadata.

    Returns:
        DatabaseIndex with all entries cataloged.
    """
    # Load query metadata for route info
    queries = load_json(query_path)
    query_by_id = {str(q["id"]): q for q in queries}

    entries: List[DbEntry] = []
    by_route: Dict[Tuple[str, str], List[DbEntry]] = defaultdict(list)
    origin_cities: Set[str] = set()
    dest_cities: Set[str] = set()

    # Scan all database directories
    db_dirs = sorted(
        [d for d in os.listdir(database_dir) if d.startswith("id_")],
        key=lambda x: int(x.split("_")[1])
    )

    for db_dir_name in db_dirs:
        task_id = int(db_dir_name.split("_")[1])
        db_path = os.path.join(database_dir, db_dir_name)

        if not os.path.isdir(db_path):
            continue

        # Get route info from query metadata
        query_data = query_by_id.get(str(task_id))
        if query_data is None:
            continue

        meta = query_data["meta_info"]
        origin = meta["org"]
        dest_list = meta["dest"]
        dest = dest_list[0] if dest_list else ""

        # Count entities in each CSV
        num_hotels = count_csv_rows(os.path.join(db_path, DB_FILES["hotels"]))
        num_attractions = count_csv_rows(os.path.join(db_path, DB_FILES["attractions"]))
        num_restaurants = count_csv_rows(os.path.join(db_path, DB_FILES["restaurants"]))
        num_trains = count_csv_rows(os.path.join(db_path, DB_FILES["trains"]))
        num_flights = count_csv_rows(os.path.join(db_path, DB_FILES["flights"]))

        # Scan hotel metadata
        hotel_brands: Set[str] = set()
        hotel_stars: Set[int] = set()
        hotels_path = os.path.join(db_path, DB_FILES["hotels"])
        if os.path.exists(hotels_path):
            for row in read_csv_as_dicts(hotels_path):
                brand = row.get("brand", "").strip()
                if brand:
                    hotel_brands.add(brand)
                star = safe_int(row.get("hotel_star", 0))
                if star > 0:
                    hotel_stars.add(star)

        # Scan attraction types
        attraction_types: Set[str] = set()
        attractions_path = os.path.join(db_path, DB_FILES["attractions"])
        if os.path.exists(attractions_path):
            for row in read_csv_as_dicts(attractions_path):
                atype = row.get("attraction_type", "").strip()
                if atype:
                    attraction_types.add(atype)

        entry = DbEntry(
            task_id=task_id,
            origin=origin,
            dest=dest,
            days=meta["days"],
            people_number=meta["people_number"],
            room_number=meta["room_number"],
            db_path=db_path,
            num_hotels=num_hotels,
            num_attractions=num_attractions,
            num_restaurants=num_restaurants,
            num_trains=num_trains,
            num_flights=num_flights,
            attraction_types=attraction_types,
            hotel_brands=hotel_brands,
            hotel_stars=hotel_stars,
            has_flights=num_flights > 0,
            has_trains=num_trains > 0,
        )

        entries.append(entry)
        route = (origin, dest)
        by_route[route].append(entry)
        origin_cities.add(origin)
        dest_cities.add(dest)

    all_routes = sorted(by_route.keys())

    index = DatabaseIndex(
        entries=entries,
        by_route=dict(by_route),
        all_routes=all_routes,
        origin_cities=origin_cities,
        dest_cities=dest_cities,
    )

    print(f"[Stage 0] Indexed {len(entries)} databases across {len(all_routes)} routes")
    print(f"  Origins: {len(origin_cities)} cities, Destinations: {len(dest_cities)} cities")

    return index


def get_source_db(index: DatabaseIndex, route: Tuple[str, str]) -> List[DbEntry]:
    """Get all source database entries for a given route."""
    return index.by_route.get(route, [])


def print_index_summary(index: DatabaseIndex):
    """Print a summary of the database index."""
    print(f"\n{'='*60}")
    print(f"Database Index Summary")
    print(f"{'='*60}")
    print(f"Total databases: {len(index.entries)}")
    print(f"Unique routes:   {len(index.all_routes)}")
    print(f"Origin cities:   {sorted(index.origin_cities)}")
    print(f"Dest cities:     {sorted(index.dest_cities)}")
    print(f"\nRoute distribution:")
    for route, entries in sorted(index.by_route.items(), key=lambda x: -len(x[1])):
        print(f"  {route[0]:>12} → {route[1]:<12}: {len(entries)} databases")
    print(f"{'='*60}\n")
