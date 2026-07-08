"""One table's static Iceberg files: v1.metadata.json + V2 Avro manifests.

Zero-copy: the table's data files are the already-published GeoParquet assets,
referenced by absolute URI; nothing is rewritten. pyiceberg 0.11 only writes
V1/V2 Avro manifests (write_manifest raises for version 3), but readers
(DuckDB >= 1.5.3) accept a *format-version 3* metadata JSON whose snapshot
points at V2 manifests, so a hand-written v3 metadata JSON is paired with
pyiceberg's V2 manifest writers.

Per-file lower/upper bounds are written for every scalar column (from the
parquet footer statistics) and for the geometry column (from the file's
GeoParquet ``bbox``, packed as two little-endian float64 pairs per the Iceberg
V3 geometry bound encoding) so engines can prune whole data files.
"""

from __future__ import annotations

import json
import struct
import uuid
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.manifest import (
    DataFile,
    DataFileContent,
    FileFormat,
    ManifestEntry,
    ManifestEntryStatus,
    write_manifest,
    write_manifest_list,
)
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.typedef import Record
from pyiceberg.types import NestedField

from portolan_cli.emitters.iceberg_static.plan import TablePart
from portolan_cli.emitters.iceberg_static.schema import (
    ColumnPlan,
    geo_columns,
    max_field_id,
    name_mapping,
    plan_columns,
)

__all__ = ["TablePart", "write_table"]

# A deterministic namespace so the same table id always yields the same uuid.
_UUID_NS = uuid.UUID("00000000-0000-0000-0000-0000ca7a1057")

_STRING_BOUND_MAX = 60


def write_table(
    table_dir: Path,
    table_id: str,
    parts: list[TablePart],
    location_uri: str,
    properties: dict[str, str],
) -> dict[str, Any]:
    """Write one table's metadata JSON and manifests into ``table_dir``.

    ``parts`` is a non-empty list of physical GeoParquet files: each becomes
    one manifest data-file entry with its own row count, size and bounds, so
    readers can prune whole part files. The schema is identical across parts
    and is planned once from the first.

    Returns the metadata dict (also written to ``metadata/v1.metadata.json``).
    """
    cols = plan_columns(parts[0].path)
    schema = Schema(*[NestedField(c.fid, c.name, c.ice, required=False) for c in cols], schema_id=0)
    spec = PartitionSpec(spec_id=0)
    mdir = table_dir / "metadata"
    mdir.mkdir(parents=True, exist_ok=True)
    io = PyArrowFileIO()

    entries: list[ManifestEntry] = []
    total_records = 0
    total_files_size = 0
    for part in parts:
        facts = _part_facts(part, cols)
        entries.append(
            ManifestEntry.from_args(
                status=ManifestEntryStatus.ADDED, snapshot_id=1, data_file=facts["data_file"]
            )
        )
        total_records += int(facts["records"])
        total_files_size += int(facts["size"])

    man_path = mdir / "snap-1-manifest.avro"
    with write_manifest(
        format_version=2,
        spec=spec,
        schema=schema,
        output_file=io.new_output(man_path.resolve().as_uri()),
        snapshot_id=1,
        avro_compression="null",
    ) as mw:
        for entry in entries:
            mw.add_entry(entry)
    manifest_file = mw.to_manifest_file()
    # pyiceberg records the OutputFile's location as the manifest path; rewrite
    # it to the metadata-relative location so the manifest-list points at the
    # real key, not the local staging path. Field position 0 of the record.
    manifest_file[0] = f"{location_uri}/metadata/{man_path.name}"

    list_path = mdir / "snap-1-manifest-list.avro"
    with write_manifest_list(
        format_version=2,
        output_file=io.new_output(list_path.resolve().as_uri()),
        snapshot_id=1,
        parent_snapshot_id=None,
        sequence_number=1,
        avro_compression="null",
    ) as lw:
        lw.add_manifests([manifest_file])

    meta = _metadata(
        table_id,
        location_uri,
        cols,
        list_path,
        total_records,
        properties,
        len(parts),
        total_files_size,
    )
    (mdir / "v1.metadata.json").write_text(json.dumps(meta, indent=2))
    return meta


def _part_facts(part: TablePart, cols: list[ColumnPlan]) -> dict[str, Any]:
    """Row count, byte size and the manifest DataFile for one physical part."""
    records = pq.ParquetFile(part.path).metadata.num_rows
    size = part.path.stat().st_size
    lower, upper = _bounds(part.path, cols)
    data_file = DataFile.from_args(
        content=DataFileContent.DATA,
        file_path=part.uri,
        file_format=FileFormat.PARQUET,
        partition=Record(),
        record_count=records,
        file_size_in_bytes=size,
        lower_bounds=lower,
        upper_bounds=upper,
        spec_id=0,
    )
    return {"data_file": data_file, "records": records, "size": size}


def _bounds(
    parquet_path: Path, cols: list[ColumnPlan]
) -> tuple[dict[int, bytes], dict[int, bytes]]:
    """Per-column lower/upper bounds keyed by field-id, Iceberg encoding.

    Scalar bounds aggregate the parquet footer's row-group statistics; the
    geometry bound packs the file's own native-CRS ``geo`` bbox. Struct, list
    and map columns get no bound.
    """
    stats = _footer_stats(parquet_path)
    file_geo = geo_columns(parquet_path)
    lower: dict[int, bytes] = {}
    upper: dict[int, bytes] = {}
    for c in cols:
        if c.geometry:
            bound = _geometry_bound(c, file_geo)
            if bound is not None:
                lower[c.fid], upper[c.fid] = bound
            continue
        if not c.scalar:
            continue
        lo, hi = stats.get(c.name, (None, None))
        enc_lo, enc_hi = _encode(c, lo), _encode(c, hi)
        if enc_lo is None or enc_hi is None:
            enc_lo, enc_hi = _placeholder(c)
        lower[c.fid], upper[c.fid] = enc_lo, enc_hi
    return lower, upper


def _geometry_bound(c: ColumnPlan, file_geo: dict[str, Any]) -> tuple[bytes, bytes] | None:
    """Iceberg V3 geometry bound: packed (xmin, ymin) / (xmax, ymax) doubles.

    Read from THIS file's geo metadata (each part bounds its own extent); the
    plan's bbox from the first part is the fallback for single-part tables.
    """
    col = file_geo.get(c.name, {})
    bbox_raw = col.get("bbox") if isinstance(col, dict) else None
    bbox = (
        tuple(float(v) for v in bbox_raw)
        if isinstance(bbox_raw, list) and len(bbox_raw) == 4
        else c.geo_bbox
    )
    if bbox is None:
        return None
    xmin, ymin, xmax, ymax = bbox
    return struct.pack("<dd", xmin, ymin), struct.pack("<dd", xmax, ymax)


def _footer_stats(parquet_path: Path) -> dict[str, tuple[Any, Any]]:
    """(min, max) per top-level scalar column, aggregated over row groups."""
    meta = pq.ParquetFile(parquet_path).metadata
    agg: dict[str, tuple[Any, Any]] = {}
    for rg in range(meta.num_row_groups):
        group = meta.row_group(rg)
        for ci in range(group.num_columns):
            column = group.column(ci)
            if "." in column.path_in_schema:  # nested leaf, not a top-level scalar
                continue
            st = column.statistics
            if st is None or not st.has_min_max:
                continue
            lo, hi = st.min, st.max
            prev = agg.get(column.path_in_schema)
            if prev is not None:
                lo = min(prev[0], lo)
                hi = max(prev[1], hi)
            agg[column.path_in_schema] = (lo, hi)
    return agg


def _encode(c: ColumnPlan, value: Any) -> bytes | None:
    if value is None:
        return None
    if c.pack is None:  # string-kind (incl. timestamps/dates degraded to string)
        if isinstance(value, bytes):
            return value[:_STRING_BOUND_MAX]
        return str(value).encode("utf-8")[:_STRING_BOUND_MAX]
    try:
        return struct.pack(c.pack, value)
    except (struct.error, TypeError, ValueError):
        return None


def _placeholder(c: ColumnPlan) -> tuple[bytes, bytes]:
    if c.pack is None:
        return b"", b"\xef\xbf\xbf"  # empty .. high utf-8
    return struct.pack(c.pack, 0), struct.pack(c.pack, 0)


def _metadata(
    table_id: str,
    location_uri: str,
    cols: list[ColumnPlan],
    list_path: Path,
    row_count: int,
    properties: dict[str, str],
    total_data_files: int,
    total_files_size: int,
) -> dict[str, Any]:
    """A format-version 3 table metadata dict.

    The snapshot's manifest-list URI is rooted at the table location so the
    published surface and any local verification scratch both resolve it next
    to the metadata JSON.
    """
    schema_fields = [{"id": c.fid, "name": c.name, "required": False, "type": c.json} for c in cols]
    props = {"schema.name-mapping.default": json.dumps(name_mapping(cols)), **properties}
    return {
        "format-version": 3,
        "table-uuid": str(uuid.uuid5(_UUID_NS, table_id)),
        "location": location_uri,
        "last-sequence-number": 1,
        "last-updated-ms": 0,
        "last-column-id": max_field_id(cols),
        "current-schema-id": 0,
        "schemas": [{"type": "struct", "schema-id": 0, "fields": schema_fields}],
        "default-spec-id": 0,
        "partition-specs": [{"spec-id": 0, "fields": []}],
        "last-partition-id": 999,
        "default-sort-order-id": 0,
        "sort-orders": [{"order-id": 0, "fields": []}],
        "current-snapshot-id": 1,
        "snapshots": [
            {
                "snapshot-id": 1,
                "sequence-number": 1,
                "timestamp-ms": 0,
                "schema-id": 0,
                "manifest-list": f"{location_uri}/metadata/{list_path.name}",
                "first-row-id": 0,
                "added-rows": row_count,
                "summary": {
                    "operation": "append",
                    "total-records": str(row_count),
                    "total-data-files": str(total_data_files),
                    "total-files-size": str(total_files_size),
                    "total-delete-files": "0",
                },
            }
        ],
        "snapshot-log": [],
        "metadata-log": [],
        "properties": props,
    }
