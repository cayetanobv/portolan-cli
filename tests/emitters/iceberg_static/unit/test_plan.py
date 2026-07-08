"""Planning from the STAC catalog itself.

The emitter derives everything from catalog.json / collection.json / item.json,
which structurally enforces the truth direction: STAC JSON -> Iceberg. One
table per collection with local parquet data assets; collection-level assets
and item-level assets (partitioned layout) both count; raster collections and
collections whose parquet is not local are skipped, never fatal.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from portolan_cli.emitters.iceberg_static.plan import plan_tables

pytestmark = pytest.mark.unit

BASE = "https://storage.example.com/fixture"


def test_one_table_per_collection_with_parquet_data(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    assert [t.name for t in plan] == ["tunnels", "blocks"]
    assert all(t.namespace == "collections" for t in plan)


def test_raster_collection_is_skipped(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    assert "imagery" not in [t.name for t in plan]


def test_single_asset_collection_has_one_part_with_published_uri(stac_catalog: Path) -> None:
    plan = {t.name: t for t in plan_tables(stac_catalog, public_base=BASE, namespace="collections")}
    tunnels = plan["tunnels"]
    assert len(tunnels.parts) == 1
    part = tunnels.parts[0]
    assert part.path == stac_catalog / "tunnels" / "tunnels.parquet"
    assert part.uri == f"{BASE}/tunnels/tunnels.parquet"


def test_item_assets_become_parts(stac_catalog: Path) -> None:
    plan = {t.name: t for t in plan_tables(stac_catalog, public_base=BASE, namespace="collections")}
    blocks = plan["blocks"]
    assert [p.uri for p in blocks.parts] == [
        f"{BASE}/blocks/part-001/part-001.parquet",
        f"{BASE}/blocks/part-002/part-002.parquet",
    ]
    assert all(p.path.is_file() for p in blocks.parts)


def test_location_uri_and_key_are_derived_from_namespace(stac_catalog: Path) -> None:
    plan = {t.name: t for t in plan_tables(stac_catalog, public_base=BASE, namespace="collections")}
    tunnels = plan["tunnels"]
    assert tunnels.key == "data/collections/tunnels"
    assert tunnels.location_uri == f"{BASE}/data/collections/tunnels"


def test_properties_carry_collection_metadata_and_version(stac_catalog: Path) -> None:
    plan = {t.name: t for t in plan_tables(stac_catalog, public_base=BASE, namespace="collections")}
    props = plan["tunnels"].properties
    assert props["title"] == "Tunnels"
    assert props["description"] == "Tunnel centerlines."
    assert props["license"] == "CC-BY-4.0"
    # From versions.json current_version: the traceability stamp.
    assert props["portolan:version"] == "1.2.0"
    # blocks has no versions.json and no title: neither key is present, empty
    # values are never written.
    blocks = plan["blocks"].properties
    assert "portolan:version" not in blocks
    assert "title" not in blocks


def test_missing_local_parquet_skips_the_collection(stac_catalog: Path) -> None:
    (stac_catalog / "tunnels" / "tunnels.parquet").unlink()
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    assert [t.name for t in plan] == ["blocks"]


def test_public_base_trailing_slash_is_normalized(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE + "/", namespace="collections")
    part = {t.name: t for t in plan}["tunnels"].parts[0]
    assert part.uri == f"{BASE}/tunnels/tunnels.parquet"
