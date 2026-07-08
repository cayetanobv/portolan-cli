"""iceberg-static emitter: a static Iceberg REST catalog over published GeoParquet.

A second, read-only serialization of the STAC catalog: table metadata JSON and
Avro manifests on object storage plus a static Iceberg REST surface
(``v1/config``, namespaces, tables) that engines can ATTACH with zero servers.

Key properties (ported from the production portolan-cats generator):

- **Zero-copy.** Each table's data files are the already-published GeoParquet
  assets, referenced by absolute URI. Nothing is rewritten; readers match
  parquet columns by name through ``schema.name-mapping.default``.
- **STAC JSON is the source of truth.** The whole output is regenerated from
  ``catalog.json`` / ``collection.json`` on every run and can always be thrown
  away and rebuilt. This is the opposite arrow from the lakehouse
  ``IcebergBackend`` (where Iceberg state is the truth); the two coexist
  because this is an emitter, not a versioning backend.
- **Format-version 3 metadata with V2 Avro manifests.** pyiceberg cannot yet
  write v3 manifests, but readers (DuckDB >= 1.5.3) accept a v3 metadata JSON
  whose snapshot points at V2 manifests.

Install with: pip install portolan-cli[iceberg]
"""

from __future__ import annotations

from portolan_cli.emitters.iceberg_static.emitter import IcebergStaticEmitter

__all__ = ["IcebergStaticEmitter"]
