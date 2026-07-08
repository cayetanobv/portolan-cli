"""End-to-end emit over the fixture catalog (still unit-tier: tmp dirs only).

emit() is idempotent by construction: it wipes and rebuilds the whole staging
directory every run, so the projection can never drift from the catalog by
accumulation. The result maps every staged file to its real object key.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("pyiceberg")

from portolan_cli.emitters.iceberg_static.emitter import IcebergStaticEmitter  # noqa: E402

pytestmark = pytest.mark.unit

BASE = "https://storage.example.com/fixture"


@pytest.fixture
def result_root(stac_catalog: Path) -> tuple[Path, object]:
    emitter = IcebergStaticEmitter()
    result = emitter.emit(stac_catalog, public_base=BASE)
    return stac_catalog, result


def test_stages_under_portolan_dir(result_root: tuple[Path, object]) -> None:
    catalog_root, result = result_root
    staged = result.staged_root  # type: ignore[attr-defined]
    assert staged == catalog_root / ".portolan" / "emit" / "iceberg-static"
    assert staged.is_dir()


def test_object_keys_cover_tables_and_surface(result_root: tuple[Path, object]) -> None:
    _, result = result_root
    keys = set(result.object_keys.values())  # type: ignore[attr-defined]
    assert "v1/config" in keys
    assert "data/collections/tunnels/metadata/v1.metadata.json" in keys
    assert "data/collections/blocks/metadata/snap-1-manifest-list.avro" in keys
    # Every staged file exists and is mapped exactly once.
    staged_root = result.staged_root  # type: ignore[attr-defined]
    for staged, _key in result.object_keys.items():  # type: ignore[attr-defined]
        assert (staged_root / staged).is_file()


def test_emit_is_idempotent_and_wipes_stale_state(result_root: tuple[Path, object]) -> None:
    catalog_root, result = result_root
    staged_root = result.staged_root  # type: ignore[attr-defined]
    stale = staged_root / "stale-leftover.json"
    stale.write_text("{}")
    again = IcebergStaticEmitter().emit(catalog_root, public_base=BASE)
    assert not stale.exists()
    assert again.object_keys == result.object_keys  # type: ignore[attr-defined]


def test_table_metadata_references_published_parquet(result_root: tuple[Path, object]) -> None:
    _, result = result_root
    staged_root = result.staged_root  # type: ignore[attr-defined]
    meta = json.loads(
        (
            staged_root
            / "tables"
            / "data"
            / "collections"
            / "tunnels"
            / "metadata"
            / "v1.metadata.json"
        ).read_text()
    )
    assert meta["format-version"] == 3
    assert meta["location"] == f"{BASE}/data/collections/tunnels"


def test_empty_catalog_raises_typed_error(tmp_path: Path) -> None:
    from portolan_cli.errors import PortolanError

    root = tmp_path / "empty"
    root.mkdir()
    (root / "catalog.json").write_text(
        json.dumps(
            {
                "type": "Catalog",
                "stac_version": "1.1.0",
                "id": "empty",
                "description": "no collections",
                "links": [],
            }
        )
    )
    with pytest.raises(PortolanError):
        IcebergStaticEmitter().emit(root, public_base=BASE)
