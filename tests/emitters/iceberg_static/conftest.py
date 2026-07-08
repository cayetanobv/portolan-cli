"""Fixtures for the iceberg-static emitter tests.

Builds a tiny but real GeoParquet file with pyarrow (WKB geometry column plus
GeoParquet ``geo`` file metadata) and a minimal SELF_CONTAINED STAC catalog
tree around it, so the emitter is exercised against the same shapes portolan
publishes, without any network or DuckDB dependency.
"""

from __future__ import annotations

import json
import struct
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

EPSG_25830_PROJJSON = {
    "type": "ProjectedCRS",
    "name": "ETRS89 / UTM zone 30N",
    "id": {"authority": "EPSG", "code": 25830},
}


def wkb_point(x: float, y: float) -> bytes:
    """Little-endian WKB for POINT(x y)."""
    return b"\x01" + struct.pack("<I", 1) + struct.pack("<dd", x, y)


def write_geoparquet(path: Path, *, n_rows: int = 3, x0: float = 440000.0) -> None:
    """Write a small GeoParquet file: scalars, a list, a struct, WKB geometry."""
    path.parent.mkdir(parents=True, exist_ok=True)
    xs = [x0 + 10.0 * i for i in range(n_rows)]
    ys = [4470000.0 + 10.0 * i for i in range(n_rows)]
    table = pa.table(
        {
            "fid": pa.array(range(1, n_rows + 1), type=pa.int64()),
            "name": pa.array([f"feature-{i}" for i in range(n_rows)], type=pa.string()),
            "height": pa.array([1.5 * i for i in range(n_rows)], type=pa.float64()),
            "tags": pa.array([["a", "b"]] * n_rows, type=pa.list_(pa.string())),
            "bbox": pa.array(
                [{"xmin": x, "ymin": y, "xmax": x, "ymax": y} for x, y in zip(xs, ys, strict=True)],
                type=pa.struct(
                    [
                        ("xmin", pa.float64()),
                        ("ymin", pa.float64()),
                        ("xmax", pa.float64()),
                        ("ymax", pa.float64()),
                    ]
                ),
            ),
            "geometry": pa.array(
                [wkb_point(x, y) for x, y in zip(xs, ys, strict=True)], type=pa.binary()
            ),
        }
    )
    geo = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
                "crs": EPSG_25830_PROJJSON,
                "bbox": [min(xs), min(ys), max(xs), max(ys)],
            }
        },
    }
    table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode()})
    pq.write_table(table, path)


@pytest.fixture
def geoparquet_file(tmp_path: Path) -> Path:
    """A standalone GeoParquet file for schema/table tests."""
    path = tmp_path / "tunnels.parquet"
    write_geoparquet(path)
    return path


def _link(rel: str, href: str) -> dict[str, str]:
    return {"rel": rel, "href": href, "type": "application/json"}


def _write_json(path: Path, obj: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2))


@pytest.fixture
def stac_catalog(tmp_path: Path) -> Path:
    """A SELF_CONTAINED catalog with three collections.

    - ``tunnels``: single collection-level GeoParquet asset (plus PMTiles)
    - ``blocks``: two items, one GeoParquet asset each (partitioned layout)
    - ``imagery``: raster collection (COG asset only) — no Iceberg table
    """
    root = tmp_path / "catalog"
    _write_json(
        root / "catalog.json",
        {
            "type": "Catalog",
            "stac_version": "1.1.0",
            "id": "fixture-catalog",
            "description": "fixture",
            "links": [
                _link("root", "./catalog.json"),
                _link("child", "./tunnels/collection.json"),
                _link("child", "./blocks/collection.json"),
                _link("child", "./imagery/collection.json"),
            ],
        },
    )

    write_geoparquet(root / "tunnels" / "tunnels.parquet")
    _write_json(
        root / "tunnels" / "collection.json",
        {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "tunnels",
            "description": "Tunnel centerlines.",
            "title": "Tunnels",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-3.8, 40.3, -3.6, 40.5]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [_link("root", "../catalog.json"), _link("parent", "../catalog.json")],
            "assets": {
                "tunnels": {
                    "href": "./tunnels.parquet",
                    "type": "application/vnd.apache.parquet",
                    "roles": ["data"],
                },
                "tunnels-tiles": {
                    "href": "./tunnels.pmtiles",
                    "type": "application/vnd.pmtiles",
                    "roles": ["visual"],
                },
            },
        },
    )
    _write_json(
        root / "tunnels" / "versions.json",
        {
            "spec_version": "1.0.0",
            "current_version": "1.2.0",
            "versions": [],
        },
    )

    for part in ("part-001", "part-002"):
        write_geoparquet(root / "blocks" / part / f"{part}.parquet")
        _write_json(
            root / "blocks" / part / "item.json",
            {
                "type": "Feature",
                "stac_version": "1.1.0",
                "id": part,
                "geometry": None,
                "properties": {"datetime": "2026-01-01T00:00:00Z"},
                "links": [_link("root", "../../catalog.json")],
                "assets": {
                    "data": {
                        "href": f"./{part}.parquet",
                        "type": "application/vnd.apache.parquet",
                        "roles": ["data"],
                    }
                },
            },
        )
    _write_json(
        root / "blocks" / "collection.json",
        {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "blocks",
            "description": "Partitioned blocks.",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-3.8, 40.3, -3.6, 40.5]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [
                _link("root", "../catalog.json"),
                _link("parent", "../catalog.json"),
                _link("item", "./part-001/item.json"),
                _link("item", "./part-002/item.json"),
            ],
            "assets": {},
        },
    )

    _write_json(
        root / "imagery" / "collection.json",
        {
            "type": "Collection",
            "stac_version": "1.1.0",
            "id": "imagery",
            "description": "A raster collection.",
            "license": "CC-BY-4.0",
            "extent": {
                "spatial": {"bbox": [[-3.8, 40.3, -3.6, 40.5]]},
                "temporal": {"interval": [[None, None]]},
            },
            "links": [_link("root", "../catalog.json"), _link("parent", "../catalog.json")],
            "assets": {
                "cog": {
                    "href": "./imagery.tif",
                    "type": "image/tiff; application=geotiff; profile=cloud-optimized",
                    "roles": ["data"],
                }
            },
        },
    )
    return root
