"""REST surface: key layout and flattened staging.

Object storage can hold both the key ``v1/sdi/namespaces`` and keys under
``v1/sdi/namespaces/``, but a filesystem cannot (file vs directory). The
surface is therefore staged flattened (``/`` -> ``__``) with a manifest that
maps each staged filename back to its real object key.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from portolan_cli.emitters.iceberg_static.surface import build_surface, stage_surface

pytestmark = pytest.mark.unit


PLAN = [
    {
        "ns": "collections",
        "name": "tunnels",
        "location_uri": "https://ex.com/data/collections/tunnels",
    },
    {
        "ns": "collections",
        "name": "blocks",
        "location_uri": "https://ex.com/data/collections/blocks",
    },
]
METAS = {
    ("collections", "tunnels"): {"format-version": 3, "table-uuid": "u1"},
    ("collections", "blocks"): {"format-version": 3, "table-uuid": "u2"},
}


def test_surface_keys_follow_the_rest_spec_shape() -> None:
    surface = build_surface(PLAN, METAS, prefix="sdi")
    assert set(surface) == {
        "v1/config",
        "v1/sdi/namespaces",
        "v1/sdi/namespaces/collections",
        "v1/sdi/namespaces/collections/tables",
        "v1/sdi/namespaces/collections/tables/tunnels",
        "v1/sdi/namespaces/collections/tables/blocks",
    }


def test_config_advertises_the_prefix() -> None:
    surface = build_surface(PLAN, METAS, prefix="sdi")
    assert json.loads(surface["v1/config"]) == {"defaults": {}, "overrides": {"prefix": "sdi"}}


def test_table_endpoint_embeds_metadata_and_location() -> None:
    surface = build_surface(PLAN, METAS, prefix="sdi")
    body = json.loads(surface["v1/sdi/namespaces/collections/tables/tunnels"])
    assert body["metadata-location"] == (
        "https://ex.com/data/collections/tunnels/metadata/v1.metadata.json"
    )
    assert body["metadata"] == {"format-version": 3, "table-uuid": "u1"}
    assert body["config"] == {}


def test_namespace_listing_names_each_table() -> None:
    surface = build_surface(PLAN, METAS, prefix="sdi")
    listing = json.loads(surface["v1/sdi/namespaces/collections/tables"])
    assert listing == {
        "identifiers": [
            {"namespace": ["collections"], "name": "tunnels"},
            {"namespace": ["collections"], "name": "blocks"},
        ]
    }


def test_staging_flattens_and_manifest_round_trips(tmp_path: Path) -> None:
    surface = build_surface(PLAN, METAS, prefix="sdi")
    mapping = stage_surface(surface, tmp_path)
    manifest = json.loads((tmp_path / "_surface_manifest.json").read_text())
    assert manifest == mapping
    for staged, real_key in mapping.items():
        assert "/" not in staged
        assert (tmp_path / staged).is_file()
        assert (tmp_path / staged).read_text() == surface[real_key]
    assert sorted(mapping.values()) == sorted(surface)
