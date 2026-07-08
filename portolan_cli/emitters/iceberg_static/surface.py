"""The static Iceberg REST surface: a handful of JSON files.

The surface answers the Iceberg REST catalog protocol's read endpoints as
plain objects at fixed keys relative to the catalog's public base:

    v1/config                              -> {"defaults":{}, "overrides":{"prefix": <prefix>}}
    v1/<prefix>/namespaces                 -> {"namespaces": [[ns], ...]}
    v1/<prefix>/namespaces/<ns>            -> {"namespace": [ns], "properties": {}}
    v1/<prefix>/namespaces/<ns>/tables     -> {"identifiers": [...]}
    v1/<prefix>/namespaces/<ns>/tables/<t> -> {"metadata-location": ..., "metadata": {...}, "config": {}}

Object storage can hold both the key ``v1/<p>/namespaces`` and keys under
``v1/<p>/namespaces/``, but a filesystem cannot (file vs directory), so the
surface is staged flattened (``/`` -> ``__``) with ``_surface_manifest.json``
mapping each staged filename back to its real object key. Upload restores the
real keys.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

MANIFEST_NAME = "_surface_manifest.json"


def build_surface(
    plan: Sequence[Mapping[str, str]],
    metas: Mapping[tuple[str, str], Mapping[str, Any]],
    *,
    prefix: str,
) -> dict[str, str]:
    """Build {real object key -> json text} for the REST surface.

    ``plan`` entries carry ``ns``, ``name`` and ``location_uri``; ``metas``
    holds each table's metadata dict keyed by (ns, name).
    """
    by_ns: dict[str, list[Mapping[str, str]]] = {}
    for t in plan:
        by_ns.setdefault(t["ns"], []).append(t)
    surface: dict[str, str] = {}
    surface["v1/config"] = _json({"defaults": {}, "overrides": {"prefix": prefix}})
    surface[f"v1/{prefix}/namespaces"] = _json({"namespaces": [[ns] for ns in by_ns]})
    for ns, tables in by_ns.items():
        surface[f"v1/{prefix}/namespaces/{ns}"] = _json({"namespace": [ns], "properties": {}})
        surface[f"v1/{prefix}/namespaces/{ns}/tables"] = _json(
            {"identifiers": [{"namespace": [ns], "name": t["name"]} for t in tables]}
        )
        for t in tables:
            location = f"{t['location_uri']}/metadata/v1.metadata.json"
            surface[f"v1/{prefix}/namespaces/{ns}/tables/{t['name']}"] = _json(
                {
                    "metadata-location": location,
                    "metadata": metas[(ns, t["name"])],
                    "config": {},
                }
            )
    return surface


def stage_surface(surface: Mapping[str, str], surface_root: Path) -> dict[str, str]:
    """Write each entry flattened plus the staged-name -> real-key manifest."""
    surface_root.mkdir(parents=True, exist_ok=True)
    mapping: dict[str, str] = {}
    for key, text in surface.items():
        staged = key.replace("/", "__") + ".json"
        (surface_root / staged).write_text(text)
        mapping[staged] = key
    (surface_root / MANIFEST_NAME).write_text(json.dumps(mapping, indent=2))
    return mapping


def _json(obj: Mapping[str, Any]) -> str:
    return json.dumps(obj, indent=2)
