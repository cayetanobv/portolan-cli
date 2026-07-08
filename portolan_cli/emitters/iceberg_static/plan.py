"""Emission planning from the STAC catalog itself.

Everything is derived from ``catalog.json`` / ``collection.json`` /
``item.json``, which structurally enforces the emitter's truth direction:
STAC JSON -> Iceberg. One table is planned per collection that has local
GeoParquet data assets; collection-level assets and item-level assets (the
partitioned layout) both count. Raster collections and collections whose
parquet is not local are skipped with a warning, never fatally: the emitter
projects whatever the catalog can prove, it does not gatekeep the publish.

This module is stdlib-only (no pyiceberg) so planning and its tests run even
without the [iceberg] extra installed.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path, PurePath
from typing import Any

from portolan_cli.output import warn

PARQUET_MEDIA_TYPE = "application/vnd.apache.parquet"


@dataclass(frozen=True)
class TablePart:
    """One physical parquet file backing a table.

    Attributes:
        path: Local file the schema/bounds are read from.
        uri: Absolute URI written into the manifest (the published location).
    """

    path: Path
    uri: str


@dataclass(frozen=True)
class TablePlan:
    """One Iceberg table to stage.

    Attributes:
        namespace: Iceberg namespace the table lives in.
        name: Table name (== collection id).
        key: Object-key prefix of the table dir, relative to the public base.
        parts: Physical parquet files, one manifest data-file entry each.
        location_uri: Absolute table location (``<public_base>/<key>``).
        properties: Table properties (title, license, version stamp, ...).
    """

    namespace: str
    name: str
    key: str
    parts: list[TablePart] = field(default_factory=list)
    location_uri: str = ""
    properties: dict[str, str] = field(default_factory=dict)


def plan_tables(catalog_root: Path, *, public_base: str, namespace: str) -> list[TablePlan]:
    """Plan one table per collection with local GeoParquet data assets."""
    base = public_base.rstrip("/")
    catalog = _read_json(catalog_root / "catalog.json")
    plan: list[TablePlan] = []
    for link in catalog.get("links", []):
        if link.get("rel") != "child" or not isinstance(link.get("href"), str):
            continue
        collection_file = (catalog_root / PurePath(link["href"])).resolve()
        table = _plan_collection(collection_file, catalog_root.resolve(), base, namespace)
        if table is not None:
            plan.append(table)
    return plan


def _plan_collection(
    collection_file: Path, catalog_root: Path, base: str, namespace: str
) -> TablePlan | None:
    """Plan a single collection, or None when it has no local parquet data."""
    if not collection_file.is_file():
        warn(f"iceberg-static: {collection_file} missing, skipping")
        return None
    collection = _read_json(collection_file)
    cid = str(collection.get("id", ""))
    if not cid:
        warn(f"iceberg-static: {collection_file} has no id, skipping")
        return None
    parts = _collect_parts(collection, collection_file.parent, catalog_root, base)
    if not parts:
        # Raster/visual-only collections and not-yet-local parquet both land
        # here; neither is an error for a projection.
        return None
    key = f"data/{namespace}/{cid}"
    return TablePlan(
        namespace=namespace,
        name=cid,
        key=key,
        parts=parts,
        location_uri=f"{base}/{key}",
        properties=_properties(collection, collection_file.parent),
    )


def _collect_parts(
    collection: dict[str, Any], collection_dir: Path, catalog_root: Path, base: str
) -> list[TablePart]:
    """Parquet data assets of the collection and its items, as table parts."""
    parts = _asset_parts(collection.get("assets"), collection_dir, catalog_root, base)
    for link in collection.get("links", []):
        if link.get("rel") != "item" or not isinstance(link.get("href"), str):
            continue
        item_file = (collection_dir / PurePath(link["href"])).resolve()
        if not item_file.is_file():
            warn(f"iceberg-static: item {item_file} missing, skipping part")
            continue
        item = _read_json(item_file)
        parts.extend(_asset_parts(item.get("assets"), item_file.parent, catalog_root, base))
    return parts


def _asset_parts(assets: Any, asset_dir: Path, catalog_root: Path, base: str) -> list[TablePart]:
    parts: list[TablePart] = []
    if not isinstance(assets, dict):
        return parts
    for asset in assets.values():
        href = asset.get("href") if isinstance(asset, dict) else None
        if not isinstance(href, str) or not _is_parquet_data(asset, href):
            continue
        if "://" in href:
            warn(f"iceberg-static: absolute asset href {href} is not local, skipping part")
            continue
        local = (asset_dir / PurePath(href)).resolve()
        if not local.is_file():
            warn(f"iceberg-static: {local} not local, skipping part")
            continue
        rel = local.relative_to(catalog_root).as_posix()
        parts.append(TablePart(path=local, uri=f"{base}/{rel}"))
    return parts


def _is_parquet_data(asset: dict[str, Any], href: str) -> bool:
    """A data-role (or role-less) asset in parquet format."""
    roles = asset.get("roles")
    if isinstance(roles, list) and roles and "data" not in roles:
        return False
    return asset.get("type") == PARQUET_MEDIA_TYPE or href.endswith(".parquet")


def _properties(collection: dict[str, Any], collection_dir: Path) -> dict[str, str]:
    """Table properties from collection metadata; empty values never written."""
    props = {
        "title": str(collection.get("title") or ""),
        "description": str(collection.get("description") or ""),
        "license": str(collection.get("license") or ""),
        "portolan:version": _current_version(collection_dir),
    }
    return {k: v for k, v in props.items() if v}


def _current_version(collection_dir: Path) -> str:
    """The collection's current version from versions.json, '' when absent.

    This is the traceability stamp: it ties the emitted projection to the
    exact catalog version it was derived from.
    """
    versions_file = collection_dir / "versions.json"
    if not versions_file.is_file():
        return ""
    try:
        current = _read_json(versions_file).get("current_version")
    except (ValueError, OSError):
        return ""
    return str(current) if current else ""


def _read_json(path: Path) -> dict[str, Any]:
    parsed = json.loads(path.read_text())
    return parsed if isinstance(parsed, dict) else {}
