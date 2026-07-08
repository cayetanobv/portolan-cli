"""Schema planning: parquet footer + GeoParquet metadata -> Iceberg columns.

The load-bearing invariant is ordinal alignment. geoparquet-io writes no
``PARQUET:field_id`` on columns, so readers (DuckDB) resolve Iceberg field-id N
to the Nth *physical* parquet column by ordinal. EVERY physical column is
therefore represented in the Iceberg schema — including covering structs and
list columns nobody queries — with top-level field-ids equal to the 1-based
physical position. Dropping one column would shift every later column's
ordinal and silently NULL them. Nested (struct/list/map child) field-ids are
allocated from 1001 up so they never collide with the ordinals.

Geometry columns are recognized from the GeoParquet ``geo`` file metadata and
declared with the Iceberg v3 geometry type carrying the native CRS spelled as
``geometry(EPSG:<code>)`` — readers match that CRS string literally. On the
manifest side they are BinaryType.

Scalar types anyone can prune on map to real Iceberg types; everything else
(dates, timestamps, decimals, blobs) degrades to string so the column still
resolves — readers that need the true value read the parquet.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DoubleType,
    FloatType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

# Nested field-ids are allocated from here so they never collide with the
# top-level ids, which must equal physical column position.
_NESTED_BASE = 1000


@dataclass(frozen=True)
class ColumnPlan:
    """One physical parquet column, planned for the Iceberg schema.

    Attributes:
        fid: Top-level field-id == 1-based physical column position.
        name: Column name.
        ice: pyiceberg type (manifest/schema side).
        json: Metadata-JSON type (str for scalars/geometry, dict for nested).
        pack: struct.pack format for bound encoding, None for string-kind.
        geometry: Whether this is a GeoParquet geometry column.
        scalar: Whether per-file min/max bounds apply.
        geo_bbox: Native-CRS [xmin, ymin, xmax, ymax] from the file's geo
            metadata, when this is a geometry column that declares one.
    """

    fid: int
    name: str
    ice: IcebergType
    json: str | dict[str, Any]
    pack: str | None
    geometry: bool
    scalar: bool
    geo_bbox: tuple[float, float, float, float] | None = None


class _Ids:
    """Monotonic allocator for nested field-ids."""

    def __init__(self) -> None:
        self.n = _NESTED_BASE

    def next(self) -> int:
        self.n += 1
        return self.n


def epsg_from_projjson(crs: dict[str, Any] | None) -> str | None:
    """PROJJSON identifier -> 'EPSG:<code>', or None when not an EPSG CRS."""
    if not crs:
        return None
    ident = crs.get("id")
    if not isinstance(ident, dict) or ident.get("authority") != "EPSG":
        return None
    code = str(ident.get("code", "")).strip()
    return f"EPSG:{code}" if code.isdigit() else None


def geo_columns(parquet_path: Path) -> dict[str, dict[str, Any]]:
    """The GeoParquet ``geo`` metadata columns dict, empty when absent."""
    meta = pq.ParquetFile(parquet_path).schema_arrow.metadata or {}
    raw = meta.get(b"geo")
    if raw is None:
        return {}
    try:
        parsed = json.loads(raw)
    except (ValueError, TypeError):
        return {}
    columns = parsed.get("columns") if isinstance(parsed, dict) else None
    return columns if isinstance(columns, dict) else {}


def _scalar_kind(arrow_type: pa.DataType) -> tuple[str, IcebergType, str | None]:
    """Map an arrow scalar type to (json type, pyiceberg type, pack code).

    Anything unrecognized degrades to string so the column still resolves.
    """
    if pa.types.is_boolean(arrow_type):
        return "boolean", BooleanType(), "<?"
    if (
        pa.types.is_int8(arrow_type)
        or pa.types.is_int16(arrow_type)
        or pa.types.is_int32(arrow_type)
        or pa.types.is_uint8(arrow_type)
        or pa.types.is_uint16(arrow_type)
    ):
        return "int", IntegerType(), "<i"
    if (
        pa.types.is_int64(arrow_type)
        or pa.types.is_uint32(arrow_type)
        or pa.types.is_uint64(arrow_type)
    ):
        return "long", LongType(), "<q"
    if pa.types.is_float32(arrow_type):
        return "float", FloatType(), "<f"
    if pa.types.is_float64(arrow_type):
        return "double", DoubleType(), "<d"
    return "string", StringType(), None


def _is_nested(arrow_type: pa.DataType) -> bool:
    return bool(
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
        or pa.types.is_struct(arrow_type)
        or pa.types.is_map(arrow_type)
    )


def _build_type(arrow_type: pa.DataType, ids: _Ids) -> tuple[IcebergType, str | dict[str, Any]]:
    """Recursively map an arrow type to (pyiceberg type, metadata-JSON type)."""
    if pa.types.is_struct(arrow_type):
        return _build_struct(arrow_type, ids)
    if pa.types.is_map(arrow_type):
        return _build_map(arrow_type, ids)
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        elem_id = ids.next()
        e_ice, e_json = _build_type(arrow_type.value_type, ids)
        return (
            ListType(element_id=elem_id, element_type=e_ice, element_required=False),
            {"type": "list", "element-id": elem_id, "element": e_json, "element-required": False},
        )
    json_name, ice, _pack = _scalar_kind(arrow_type)
    return ice, json_name


def _build_struct(arrow_type: pa.DataType, ids: _Ids) -> tuple[IcebergType, dict[str, Any]]:
    fields_ice: list[NestedField] = []
    fields_json: list[dict[str, Any]] = []
    for child in arrow_type:
        fid = ids.next()
        c_ice, c_json = _build_type(child.type, ids)
        fields_ice.append(NestedField(fid, child.name, c_ice, required=False))
        fields_json.append({"id": fid, "name": child.name, "required": False, "type": c_json})
    return StructType(*fields_ice), {"type": "struct", "fields": fields_json}


def _build_map(arrow_type: pa.DataType, ids: _Ids) -> tuple[IcebergType, dict[str, Any]]:
    kid, vid = ids.next(), ids.next()
    k_ice, k_json = _build_type(arrow_type.key_type, ids)
    v_ice, v_json = _build_type(arrow_type.item_type, ids)
    return (
        MapType(key_id=kid, key_type=k_ice, value_id=vid, value_type=v_ice, value_required=False),
        {
            "type": "map",
            "key-id": kid,
            "key": k_json,
            "value-id": vid,
            "value": v_json,
            "value-required": False,
        },
    )


def _geometry_plan(fid: int, name: str, geo_col: dict[str, Any]) -> ColumnPlan:
    crs = epsg_from_projjson(geo_col.get("crs"))
    jtype = f"geometry({crs})" if crs else "geometry"
    bbox_raw = geo_col.get("bbox")
    bbox: tuple[float, float, float, float] | None = None
    if isinstance(bbox_raw, list) and len(bbox_raw) == 4:
        bbox = (float(bbox_raw[0]), float(bbox_raw[1]), float(bbox_raw[2]), float(bbox_raw[3]))
    return ColumnPlan(
        fid=fid,
        name=name,
        ice=BinaryType(),
        json=jtype,
        pack=None,
        geometry=True,
        scalar=False,
        geo_bbox=bbox,
    )


def plan_columns(parquet_path: Path) -> list[ColumnPlan]:
    """Inspect a parquet file and return one plan entry per physical column."""
    arrow_schema = pq.ParquetFile(parquet_path).schema_arrow
    geo_cols = geo_columns(parquet_path)
    ids = _Ids()
    cols: list[ColumnPlan] = []
    for pos, arrow_field in enumerate(arrow_schema, start=1):
        if arrow_field.name in geo_cols:
            cols.append(_geometry_plan(pos, arrow_field.name, geo_cols[arrow_field.name]))
        elif _is_nested(arrow_field.type):
            ice, jtype = _build_type(arrow_field.type, ids)
            cols.append(ColumnPlan(pos, arrow_field.name, ice, jtype, None, False, False))
        else:
            json_name, ice, pack = _scalar_kind(arrow_field.type)
            cols.append(ColumnPlan(pos, arrow_field.name, ice, json_name, pack, False, True))
    return cols


def name_mapping(cols: list[ColumnPlan]) -> list[dict[str, Any]]:
    """The ``schema.name-mapping.default`` entries, mirroring nested ids."""
    return [_name_mapping_entry(c.fid, c.name, c.json) for c in cols]


def _name_mapping_entry(fid: int, name: str, json_type: str | dict[str, Any]) -> dict[str, Any]:
    entry: dict[str, Any] = {"field-id": fid, "names": [name]}
    children = _mapping_children(json_type)
    if children:
        entry["fields"] = children
    return entry


def _mapping_children(json_type: str | dict[str, Any]) -> list[dict[str, Any]] | None:
    if not isinstance(json_type, dict):
        return None
    kind = json_type.get("type")
    if kind == "struct":
        return [_name_mapping_entry(f["id"], f["name"], f["type"]) for f in json_type["fields"]]
    if kind == "list":
        elem = _mapping_children(json_type["element"])
        entry: dict[str, Any] = {"field-id": json_type["element-id"], "names": ["element"]}
        if elem:
            entry["fields"] = elem
        return [entry]
    if kind == "map":
        return [
            {"field-id": json_type["key-id"], "names": ["key"]},
            {"field-id": json_type["value-id"], "names": ["value"]},
        ]
    return None


def max_field_id(cols: list[ColumnPlan]) -> int:
    """The highest field-id in use, top-level or nested."""
    mx = 0
    for c in cols:
        mx = max(mx, c.fid, _max_nested(c.json))
    return mx


def _max_nested(json_type: str | dict[str, Any]) -> int:
    if not isinstance(json_type, dict):
        return 0
    kind = json_type.get("type")
    if kind == "struct":
        return max((max(f["id"], _max_nested(f["type"])) for f in json_type["fields"]), default=0)
    if kind == "list":
        return max(int(json_type["element-id"]), _max_nested(json_type["element"]))
    if kind == "map":
        return max(
            int(json_type["key-id"]),
            int(json_type["value-id"]),
            _max_nested(json_type["key"]),
            _max_nested(json_type["value"]),
        )
    return 0
