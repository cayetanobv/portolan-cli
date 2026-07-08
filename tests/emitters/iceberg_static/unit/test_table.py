"""Table writing: format-v3 metadata JSON + V2 Avro manifests, zero-copy.

The table's data file is the already-published GeoParquet referenced by URI;
nothing is rewritten. The metadata must carry the name-mapping property (the
parquet has no field-ids) and a snapshot whose summary counts match the file.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest

pytest.importorskip("pyiceberg")

from portolan_cli.emitters.iceberg_static.table import TablePart, write_table  # noqa: E402

pytestmark = pytest.mark.unit


@pytest.fixture
def written(tmp_path: Path, geoparquet_file: Path) -> tuple[Path, dict[str, object]]:
    table_dir = tmp_path / "staging" / "data" / "collections" / "tunnels"
    meta = write_table(
        table_dir,
        table_id="collections.tunnels",
        parts=[TablePart(path=geoparquet_file, uri="https://example.com/tunnels/tunnels.parquet")],
        location_uri="https://example.com/data/collections/tunnels",
        properties={"title": "Tunnels", "portolan:version": "1.2.0"},
    )
    return table_dir, meta


def test_writes_metadata_and_manifests(written: tuple[Path, dict[str, object]]) -> None:
    table_dir, _ = written
    mdir = table_dir / "metadata"
    assert (mdir / "v1.metadata.json").is_file()
    assert (mdir / "snap-1-manifest.avro").is_file()
    assert (mdir / "snap-1-manifest-list.avro").is_file()


def test_metadata_is_format_version_3(written: tuple[Path, dict[str, object]]) -> None:
    table_dir, meta = written
    on_disk = json.loads((table_dir / "metadata" / "v1.metadata.json").read_text())
    assert on_disk["format-version"] == 3
    assert on_disk == meta


def test_snapshot_counts_match_parquet(
    written: tuple[Path, dict[str, object]], geoparquet_file: Path
) -> None:
    _, meta = written
    rows = pq.ParquetFile(geoparquet_file).metadata.num_rows
    snapshots = meta["snapshots"]
    assert isinstance(snapshots, list)
    summary = snapshots[0]["summary"]
    assert summary["total-records"] == str(rows)
    assert summary["total-data-files"] == "1"


def test_name_mapping_property_covers_every_column(
    written: tuple[Path, dict[str, object]], geoparquet_file: Path
) -> None:
    _, meta = written
    props = meta["properties"]
    assert isinstance(props, dict)
    mapping = json.loads(props["schema.name-mapping.default"])
    physical = [c.name for c in pq.ParquetFile(geoparquet_file).schema_arrow]
    assert [m["names"][0] for m in mapping] == physical
    assert props["title"] == "Tunnels"
    assert props["portolan:version"] == "1.2.0"


def test_manifest_list_uri_is_rooted_at_location(written: tuple[Path, dict[str, object]]) -> None:
    _, meta = written
    snapshots = meta["snapshots"]
    assert isinstance(snapshots, list)
    assert snapshots[0]["manifest-list"] == (
        "https://example.com/data/collections/tunnels/metadata/snap-1-manifest-list.avro"
    )


def test_multi_part_table_emits_one_data_file_per_part(
    tmp_path: Path, geoparquet_file: Path
) -> None:
    meta = write_table(
        tmp_path / "t",
        table_id="collections.blocks",
        parts=[
            TablePart(path=geoparquet_file, uri="https://example.com/blocks/part-001.parquet"),
            TablePart(path=geoparquet_file, uri="https://example.com/blocks/part-002.parquet"),
        ],
        location_uri="https://example.com/data/collections/blocks",
        properties={},
    )
    rows = pq.ParquetFile(geoparquet_file).metadata.num_rows
    snapshots = meta["snapshots"]
    assert isinstance(snapshots, list)
    summary = snapshots[0]["summary"]
    assert summary["total-data-files"] == "2"
    assert summary["total-records"] == str(rows * 2)


def test_deterministic_table_uuid(tmp_path: Path, geoparquet_file: Path) -> None:
    parts = [TablePart(path=geoparquet_file, uri="https://example.com/t/t.parquet")]
    a = write_table(tmp_path / "a", "ns.t", parts, "https://example.com/data/ns/t", {})
    b = write_table(tmp_path / "b", "ns.t", parts, "https://example.com/data/ns/t", {})
    assert a["table-uuid"] == b["table-uuid"]
