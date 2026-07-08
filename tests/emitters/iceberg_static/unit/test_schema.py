"""Schema planning: parquet footer + geo metadata -> Iceberg column plan.

The load-bearing invariant is ordinal alignment: geoparquet-io writes no
``PARQUET:field_id`` on columns, so readers resolve Iceberg field-id N to the
Nth *physical* parquet column. Top-level field-ids must therefore equal the
1-based physical position, with every physical column present in the schema.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("pyiceberg")

from pyiceberg.types import (  # noqa: E402
    BinaryType,
    DoubleType,
    ListType,
    LongType,
    StringType,
    StructType,
)

from portolan_cli.emitters.iceberg_static.schema import (  # noqa: E402
    epsg_from_projjson,
    name_mapping,
    plan_columns,
)

pytestmark = pytest.mark.unit


def test_field_ids_equal_physical_position(geoparquet_file: Path) -> None:
    cols = plan_columns(geoparquet_file)
    assert [c.name for c in cols] == ["fid", "name", "height", "tags", "bbox", "geometry"]
    assert [c.fid for c in cols] == [1, 2, 3, 4, 5, 6]


def test_every_physical_column_is_planned_even_nested(geoparquet_file: Path) -> None:
    cols = plan_columns(geoparquet_file)
    by_name = {c.name: c for c in cols}
    assert isinstance(by_name["tags"].ice, ListType)
    assert isinstance(by_name["bbox"].ice, StructType)
    # Dropping either would shift the geometry column's ordinal and NULL it.
    assert by_name["geometry"].fid == 6


def test_scalar_type_mapping(geoparquet_file: Path) -> None:
    cols = {c.name: c for c in plan_columns(geoparquet_file)}
    assert isinstance(cols["fid"].ice, LongType)
    assert cols["fid"].json == "long"
    assert isinstance(cols["name"].ice, StringType)
    assert cols["name"].json == "string"
    assert isinstance(cols["height"].ice, DoubleType)
    assert cols["height"].json == "double"


def test_geometry_column_carries_native_crs(geoparquet_file: Path) -> None:
    cols = {c.name: c for c in plan_columns(geoparquet_file)}
    geom = cols["geometry"]
    assert geom.geometry is True
    # Manifest side is binary; metadata JSON side is the v3 geometry type with
    # the CRS spelled exactly as ``EPSG:<code>`` (readers match it literally).
    assert isinstance(geom.ice, BinaryType)
    assert geom.json == "geometry(EPSG:25830)"


def test_nested_field_ids_never_collide_with_ordinals(geoparquet_file: Path) -> None:
    cols = plan_columns(geoparquet_file)
    nested_ids: list[int] = []
    for c in cols:
        nested_ids.extend(_nested_ids(c.json))
    assert nested_ids, "fixture has nested columns"
    assert min(nested_ids) > 1000


def _nested_ids(json_type: object) -> list[int]:
    if not isinstance(json_type, dict):
        return []
    out: list[int] = []
    kind = json_type.get("type")
    if kind == "struct":
        for f in json_type["fields"]:
            out.append(f["id"])
            out.extend(_nested_ids(f["type"]))
    elif kind == "list":
        out.append(json_type["element-id"])
        out.extend(_nested_ids(json_type["element"]))
    elif kind == "map":
        out.extend([json_type["key-id"], json_type["value-id"]])
    return out


def test_name_mapping_covers_all_columns_and_nesting(geoparquet_file: Path) -> None:
    cols = plan_columns(geoparquet_file)
    mapping = name_mapping(cols)
    assert [m["names"] for m in mapping] == [[c.name] for c in cols]
    by_name = {m["names"][0]: m for m in mapping}
    assert by_name["bbox"]["fields"][0]["names"] == ["xmin"]
    assert by_name["tags"]["fields"][0]["names"] == ["element"]
    assert "fields" not in by_name["fid"]


def test_epsg_from_projjson() -> None:
    assert epsg_from_projjson({"id": {"authority": "EPSG", "code": 25830}}) == "EPSG:25830"
    assert epsg_from_projjson({"id": {"authority": "EPSG", "code": "4326"}}) == "EPSG:4326"
    assert epsg_from_projjson({"id": {"authority": "OGC", "code": "CRS84"}}) is None
    assert epsg_from_projjson({}) is None
    assert epsg_from_projjson(None) is None
