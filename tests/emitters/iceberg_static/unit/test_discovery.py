"""STAC-side discovery: stamping the catalog with pointers to the surface.

Without this the static surface is only findable by convention. apply_discovery
writes the STAC Iceberg Extension fields onto each projected collection and a
root catalog link to v1/config. It mutates the catalog deliberately, which is
why it is a separate explicit step and never part of emit().
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.emitters.iceberg_static.discovery import (
    ICEBERG_EXTENSION_SCHEMA,
    apply_discovery,
)
from portolan_cli.emitters.iceberg_static.plan import plan_tables

pytestmark = pytest.mark.unit

BASE = "https://storage.example.com/fixture"


@pytest.fixture
def stamped(stac_catalog: Path) -> Path:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    apply_discovery(stac_catalog, plan, public_base=BASE)
    return stac_catalog


def _read(path: Path) -> dict[str, object]:
    loaded = json.loads(path.read_text())
    assert isinstance(loaded, dict)
    return loaded


def test_projected_collections_get_extension_fields(stamped: Path) -> None:
    col = _read(stamped / "tunnels" / "collection.json")
    # Vocabulary as merged upstream (stac-iceberg-extension#4): a serverless
    # catalog on object storage is catalog_type "static", with the REST prefix
    # and authorization mode a client needs to ATTACH it.
    assert col["iceberg:catalog_type"] == "static"
    assert col["iceberg:catalog_uri"] == BASE
    assert col["iceberg:rest_prefix"] == "sdi"
    assert col["iceberg:authorization_type"] == "none"
    assert col["iceberg:table_id"] == "collections.tunnels"
    assert col["iceberg:format_version"] == 3
    # Direct URL of the table's metadata.json, usable with iceberg_scan() or
    # PyIceberg StaticTable without going through the REST surface.
    assert col["iceberg:metadata_location"] == (
        f"{BASE}/data/collections/tunnels/metadata/v1.metadata.json"
    )
    # A string: Iceberg snapshot ids are 64-bit and lose precision as JSON
    # numbers. The emitter always writes snapshot 1.
    assert col["iceberg:current_snapshot_id"] == "1"
    extensions = col["stac_extensions"]
    assert isinstance(extensions, list)
    assert ICEBERG_EXTENSION_SCHEMA in extensions


def test_prefix_option_flows_into_the_stamp(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    apply_discovery(stac_catalog, plan, public_base=BASE, prefix="custom")
    col = _read(stac_catalog / "tunnels" / "collection.json")
    assert col["iceberg:rest_prefix"] == "custom"


def test_unprojected_collections_are_untouched(stamped: Path) -> None:
    col = _read(stamped / "imagery" / "collection.json")
    assert "iceberg:table_id" not in col
    assert "stac_extensions" not in col


def test_root_catalog_links_to_the_rest_config(stamped: Path) -> None:
    catalog = _read(stamped / "catalog.json")
    links = catalog["links"]
    assert isinstance(links, list)
    rest = [link for link in links if link.get("rel") == "iceberg-rest"]
    assert len(rest) == 1
    assert rest[0]["href"] == "./v1/config"
    assert rest[0]["type"] == "application/json"


def test_apply_is_idempotent(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    apply_discovery(stac_catalog, plan, public_base=BASE)
    once = (stac_catalog / "tunnels" / "collection.json").read_text()
    once_catalog = (stac_catalog / "catalog.json").read_text()
    apply_discovery(stac_catalog, plan, public_base=BASE)
    assert (stac_catalog / "tunnels" / "collection.json").read_text() == once
    assert (stac_catalog / "catalog.json").read_text() == once_catalog


def test_restamp_updates_a_moved_base(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    apply_discovery(stac_catalog, plan, public_base=BASE)
    moved = "https://mirror.example.org/fixture"
    plan_moved = plan_tables(stac_catalog, public_base=moved, namespace="collections")
    apply_discovery(stac_catalog, plan_moved, public_base=moved)
    col = _read(stac_catalog / "tunnels" / "collection.json")
    assert col["iceberg:catalog_uri"] == moved
    extensions = col["stac_extensions"]
    assert isinstance(extensions, list)
    assert extensions.count(ICEBERG_EXTENSION_SCHEMA) == 1
    catalog = _read(stac_catalog / "catalog.json")
    links = catalog["links"]
    assert isinstance(links, list)
    assert sum(1 for link in links if link.get("rel") == "iceberg-rest") == 1


def test_returns_the_paths_it_wrote(stac_catalog: Path) -> None:
    plan = plan_tables(stac_catalog, public_base=BASE, namespace="collections")
    written = apply_discovery(stac_catalog, plan, public_base=BASE)
    assert set(written) == {
        stac_catalog / "catalog.json",
        stac_catalog / "tunnels" / "collection.json",
        stac_catalog / "blocks" / "collection.json",
    }
