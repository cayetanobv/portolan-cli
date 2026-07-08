"""STAC-side discovery: point the catalog at the static Iceberg surface.

Without metadata in the STAC JSON the surface is only findable by convention
(a knowing client probes ``ATTACH <public_base>``). This module writes the
pointers that make it discoverable:

- Each projected collection gets the `STAC Iceberg Extension
  <https://github.com/portolan-sdi/stac-iceberg-extension>`_ fields:
  ``iceberg:catalog_type`` ("rest"), ``iceberg:catalog_uri`` (the public
  base), ``iceberg:table_id`` and ``iceberg:format_version``.
- The root ``catalog.json`` gets one ``rel: "iceberg-rest"`` link to
  ``./v1/config``, advertising the whole surface to clients that start at the
  catalog rather than a collection.

This deliberately MUTATES the catalog, which emitters never do — so it is an
explicit, separate step the push wiring calls after ``emit()`` and before the
metadata upload (the stamped collection.json is what gets published). Both
stamps are idempotent: re-applying over an already-stamped catalog writes
nothing, so mtime-based change detection (ADR-0017) is never tripped
spuriously.

Stdlib-only, like ``plan``: importable without the [iceberg] extra.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from portolan_cli.emitters.iceberg_static.plan import TablePlan

ICEBERG_EXTENSION_SCHEMA = (
    "https://portolan-sdi.github.io/stac-iceberg-extension/v1.0.0/schema.json"
)
REST_CONFIG_LINK_REL = "iceberg-rest"
# The emitter writes format-version 3 table metadata (see table.py).
FORMAT_VERSION = 3


def apply_discovery(catalog_root: Path, plan: list[TablePlan], *, public_base: str) -> list[Path]:
    """Stamp discovery pointers for every planned table; return written paths.

    Files whose stamped content is already current are left untouched.
    """
    base = public_base.rstrip("/")
    written: list[Path] = []
    for table in plan:
        if table.collection_file is None:
            continue
        if _stamp_collection(table.collection_file, table, base):
            written.append(table.collection_file)
    catalog_file = catalog_root / "catalog.json"
    if catalog_file.is_file() and _stamp_catalog(catalog_file):
        written.append(catalog_file)
    return written


def _stamp_collection(collection_file: Path, table: TablePlan, base: str) -> bool:
    """Write the extension fields onto one collection; True when changed."""
    original = collection_file.read_text()
    doc = json.loads(original)
    if not isinstance(doc, dict):
        return False
    extensions = doc.get("stac_extensions")
    if not isinstance(extensions, list):
        extensions = []
    if ICEBERG_EXTENSION_SCHEMA not in extensions:
        extensions = [*extensions, ICEBERG_EXTENSION_SCHEMA]
    doc["stac_extensions"] = extensions
    doc["iceberg:catalog_type"] = "rest"
    doc["iceberg:catalog_uri"] = base
    doc["iceberg:table_id"] = f"{table.namespace}.{table.name}"
    doc["iceberg:format_version"] = FORMAT_VERSION
    return _write_if_changed(collection_file, doc, original)


def _stamp_catalog(catalog_file: Path) -> bool:
    """Ensure the root catalog carries exactly one iceberg-rest link."""
    original = catalog_file.read_text()
    doc = json.loads(original)
    if not isinstance(doc, dict):
        return False
    links = doc.get("links")
    if not isinstance(links, list):
        links = []
    desired = {
        "rel": REST_CONFIG_LINK_REL,
        "href": "./v1/config",
        "type": "application/json",
        "title": "Static Iceberg REST catalog",
    }
    kept = [link for link in links if link.get("rel") != REST_CONFIG_LINK_REL]
    doc["links"] = [*kept, desired]
    return _write_if_changed(catalog_file, doc, original)


def _write_if_changed(path: Path, doc: dict[str, Any], original: str) -> bool:
    text = json.dumps(doc, indent=2)
    if text == original:
        return False
    path.write_text(text)
    return True
