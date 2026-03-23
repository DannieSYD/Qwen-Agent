"""
Road Route Query Tool - Query distance and duration between locations (Multilingual)
"""
import os
from typing import Dict, Optional, Union

from .base_travel_tool import BaseTravelTool, register_tool


@register_tool('query_road_route_info')
class RoadRouteInfoQueryTool(BaseTravelTool):
    """Tool for querying distance and duration information between two locations (supports zh/en)

    Supports compound mode: accepts 'origin_place' and 'destination_place' parameters
    with place names, internally resolves coordinates from the location database.
    """

    # Language-specific field mappings
    LANG_FIELDS = {
        'zh': {
            'db_not_loaded': "数据库未加载",
            'not_found': lambda origin, dest: f"未找到从 {origin} 到 {dest} 的交通信息",
            'coord_not_in_range': lambda coord: f"坐标 {coord} 不在查询范围内，请检查：\n1. 坐标是否源自有效的工具查询结果，而非手动输入或编造；\n2. 坐标数值精度是否与查询结果保持完全一致,6位小数",
            'db_loaded': lambda count, path: f"✓ 交通距离数据库加载成功: {count} 条记录 (路径: {path})",
            'db_not_found': lambda path: f"⚠ 警告: 交通距离数据库未找到于 {path}",
            'place_not_found': lambda place: f"未找到地点 '{place}' 的坐标信息。请确保地点名称来自其他工具的查询结果。",
            'resolved': lambda name, coord: f"[已自动解析地点 '{name}' → 坐标 {coord}]",
        },
        'en': {
            'db_not_loaded': "Database not loaded",
            'not_found': lambda origin, dest: f"No transportation information found from {origin} to {dest}",
            'coord_not_in_range': lambda coord: f"Coordinate {coord} is not in query range, please check:\n1. Whether coordinate comes from valid tool query result, not manual input or fabrication;\n2. Whether coordinate precision is exactly consistent with query result, 6 decimal places",
            'db_loaded': lambda count, path: f"✓ Road route database loaded: {count} records (path: {path})",
            'db_not_found': lambda path: f"⚠ Warning: Road route database not found at {path}",
            'place_not_found': lambda place: f"Location '{place}' not found in coordinate database. Please ensure the place name comes from other tool query results.",
            'resolved': lambda name, coord: f"[Auto-resolved location '{name}' → coordinates {coord}]",
        }
    }

    def __init__(self, cfg: Optional[Dict] = None):
        super().__init__(cfg)
        self.database_path = cfg.get('database_path') if cfg else None

        # Get language-specific fields
        self.fields = self.LANG_FIELDS.get(self.language, self.LANG_FIELDS['en'])

        if self.database_path and os.path.exists(self.database_path):
            self.data = self.load_csv_database(self.database_path)
            self._log(self.fields['db_loaded'](len(self.data), self.database_path))
        else:
            if self.database_path:
                self._log(self.fields['db_not_found'](self.database_path))
            self.data = None

        # Load location database for compound tool resolution
        self.location_data = None
        location_db_path = cfg.get('location_database_path') if cfg else None
        if location_db_path and os.path.exists(location_db_path):
            self.location_data = self.load_csv_database(location_db_path)

    def _resolve_place_to_coord_string(self, place_name: str) -> Optional[str]:
        """Resolve a place name to a 'latitude,longitude' coordinate string.

        Returns:
            Coordinate string like '32.011682,118.787631' or None if not found
        """
        if self.location_data is None:
            return None

        col_name = 'poi_name' if 'poi_name' in self.location_data.columns else 'place_name'
        match = self.location_data[self.location_data[col_name] == place_name]

        if match.empty:
            return None

        row = match.iloc[0]
        lat = str(row.get('latitude', ''))
        lon = str(row.get('longitude', ''))
        if lat and lon:
            return f"{lat},{lon}"
        return None

    def call(self, params: Union[str, dict], **kwargs) -> str:
        """
        Execute road route query

        Args:
            params: Query parameters containing:
                - origin: (optional) Coordinate string in format "latitude,longitude"
                - destination: (optional) Coordinate string in format "latitude,longitude"
                - origin_place: (optional) Origin place name (auto-resolves to coordinates)
                - destination_place: (optional) Destination place name (auto-resolves to coordinates)
                Place names take priority over raw coordinates when both are provided.

        Returns:
            JSON string of query results
        """
        params = self._verify_json_format_args(params)

        origin = params.get('origin')
        destination = params.get('destination')
        origin_place = params.get('origin_place')
        destination_place = params.get('destination_place')

        if self.data is None:
            return self.fields['db_not_loaded']

        resolution_notes = []

        # Resolve origin place name to coordinates
        if origin_place:
            resolved = self._resolve_place_to_coord_string(origin_place)
            if resolved is None:
                return self.fields['place_not_found'](origin_place)
            origin = resolved
            resolution_notes.append(self.fields['resolved'](origin_place, origin))

        # Resolve destination place name to coordinates
        if destination_place:
            resolved = self._resolve_place_to_coord_string(destination_place)
            if resolved is None:
                return self.fields['place_not_found'](destination_place)
            destination = resolved
            resolution_notes.append(self.fields['resolved'](destination_place, destination))

        # Check if coordinates exist in database
        coord_existence_result = self._check_coordinate_existence(origin, destination)
        if coord_existence_result:
            return coord_existence_result

        # Query directly using coordinates
        query_result = self.data[
            (self.data['origin'] == origin) &
            (self.data['destination'] == destination)
        ]

        if query_result.empty:
            return self.fields['not_found'](origin, destination)

        # Build return result
        row = query_result.iloc[0]
        result = {
            "origin": row.get('origin', origin),
            "destination": row.get('destination', destination),
            "distance_in_meters": int(row.get('distance_meters', 0)),
            "duration_in_minutes": int(row.get('duration_minutes', 0)),
            "cost": int(row.get('cost', 0))
        }

        json_result = self.format_result_as_json(result)
        if resolution_notes:
            return "\n".join(resolution_notes) + "\n" + json_result
        return json_result

    def _check_coordinate_existence(self, origin: str, destination: str) -> str:
        """
        Check if coordinates exist in database

        Args:
            origin: Origin coordinate string
            destination: Destination coordinate string

        Returns:
            Error message string, empty string if no error
        """
        # Get all unique origin and destination coordinates from database
        all_origins = set(self.data['origin'].unique())
        all_destinations = set(self.data['destination'].unique())

        # Merge all coordinates
        all_coords = all_origins | all_destinations

        # Check origin coordinate
        if origin not in all_coords:
            return self.fields['coord_not_in_range'](origin)

        # Check destination coordinate
        if destination not in all_coords:
            return self.fields['coord_not_in_range'](destination)

        return ""
