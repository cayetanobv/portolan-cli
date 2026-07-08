# ADR-0057: iceberg-static — a publish-time emitter, not a versioning backend

## Status

Proposed (DRAFT)

Related: ADR-0015 (two-tier versioning), ADR-0046 (Iceberg as optional extra),
PR #433 (ADR draft "Dual serialization for the STAC catalog", whose
architectural intent this ADR carries forward with a different code shape).

## Context

Two distinct "Icebergs" exist around Portolan and are easily conflated:

1. **The lakehouse backend** (`portolan_cli/backends/iceberg/`, shipped).
   A `VersioningBackend`: data is written *into* Iceberg tables managed by a
   live catalog service, Iceberg state is the source of truth, versions are
   snapshots, and `push` is deliberately blocked because `add` already
   uploads.

2. **The static projection** (proposed in PR #433, running in production in
   the portolan-cats catalogs). A tree of static JSON/Avro files conforming to
   the Iceberg REST catalog protocol, generated *from* the published STAC
   catalog, so any Iceberg-speaking engine (DuckDB, Trino, Spark) can `ATTACH`
   a published catalog and query it with SQL — zero servers.

PR #433 framed the second as a possible second backend flavor. That framing
creates three contradictions with the shipped backend: the source-of-truth
arrow (STAC→Iceberg vs Iceberg→STAC), append semantics (a projection must
always equal the catalog; the backend appends), and the push gate (the backend
blocks push; the projection must run at publish time).

## Decision

**The static projection is not a backend. It is an *emitter*: a new component
kind for publish-time derived serializations.**

- `portolan_cli/emitters/` provides `get_emitter(name)` mirroring
  `backends.get_backend` exactly: built-in names behind a lazy import with an
  install-hint error, third parties via the `portolan.emitters` entry-point
  group. The `Emitter` protocol is `emit(catalog_root, *, public_base,
  options) -> EmitResult`.
- An emitter owns no state and accepts no writes. **STAC JSON is its source of
  truth by construction**: the emitter plans everything from `catalog.json` /
  `collection.json` / `item.json`, and regenerates its whole output on every
  run (wipe-and-rebuild, no append path exists).
- The lakehouse backend is untouched: its Iceberg-is-truth semantics, its
  snapshot versioning, and the `supports_push` gate all remain exactly as
  shipped. The two flavors coexist because they are different component kinds.
- `iceberg-static` rides the existing `[iceberg]` extra (its only heavy
  dependency, pyiceberg, is already in it). Nothing runs unless explicitly
  requested; a catalog that never emits is byte-identical to today.

### The iceberg-static emitter

- **Zero-copy tables.** One Iceberg table per collection with local GeoParquet
  data assets. The table's data files are the published parquet assets
  referenced by absolute URI; nothing is rewritten. Readers match columns by
  name via `schema.name-mapping.default`.
- **Ordinal alignment invariant.** geoparquet-io writes no `PARQUET:field_id`,
  so readers resolve Iceberg field-id N to the Nth physical parquet column.
  Every physical column stays in the schema with top-level field-ids equal to
  physical position; nested ids allocate from 1001.
- **Format-version 3 metadata, V2 Avro manifests.** pyiceberg cannot yet write
  v3 manifests; readers (DuckDB ≥ 1.5.3) accept the pairing. Geometry columns
  use the v3 geometry type carrying the native CRS (`geometry(EPSG:<code>)`),
  with per-file packed-double bounds so engines can prune.
- **Static REST surface.** `v1/config` (prefix `sdi` by default), namespace
  and table endpoints, staged flattened (`/` → `__`) with a manifest because a
  filesystem cannot hold `v1/<p>/namespaces` as both file and directory.
- **Traceability.** Each table's properties carry `portolan:version` from the
  collection's `versions.json`, tying the projection to the exact catalog
  version it derives from.
- **Partitioned collections** emit one manifest data-file entry per item-level
  parquet asset, so readers prune whole partitions.

### Deferred to follow-ups (deliberately)

- Wiring into `push` (`--emit iceberg-static`, `emit:` config) and `check`.
- The discovery table (one row per collection, STAC-in-Iceberg): its name and
  schema need upstream agreement first. Production precedent uses
  `catalog.datasets` with a flattened schema; PR #433 sketches `items` with a
  STAC-fidelity core. Until pinned, this emitter stages data tables only.
- **Invoking STAC-side discovery from push.** Without metadata in the STAC
  JSON, the surface is only findable by convention (a knowing client probes
  `ATTACH <public_base>` and falls back). The stamping itself SHIPS with this
  ADR as `emitters/iceberg_static/discovery.py::apply_discovery` — per
  collection the
  [STAC Iceberg Extension](https://github.com/portolan-sdi/stac-iceberg-extension)
  fields (`iceberg:catalog_type: "static"`, `iceberg:catalog_uri:
  <public_base>`, `iceberg:rest_prefix`, `iceberg:authorization_type:
  "none"`, `iceberg:table_id`, `iceberg:format_version: 3`,
  `iceberg:metadata_location`, `iceberg:current_snapshot_id` as a string),
  plus one `rel: "iceberg-rest"` link to `./v1/config` on the root catalog.
  Because it mutates the catalog, it is an explicit separate step, never
  called by `emit()`: the push wiring invokes it after emit and before the
  metadata upload. The vocabulary matches the extension schema as merged
  (stac-iceberg-extension#4 added `format_version: 3`, the `static` catalog
  type, and the connection fields).
- Spec addendum (optional extension: MAY publish, MUST conform if published).
- Private-catalog authentication (unchanged from PR #433's scoping).

## Consequences

### Easier

- Published catalogs become SQL-queryable with zero servers, and the three
  PR #433 contradictions dissolve without code changes to the backend.
- Hosting stays in the static-files regime (a few dozen KB per catalog).
- The production portolan-cats generator can be replaced by this emitter,
  converging two implementations into one.

### Harder

- Two serializations to keep in sync. Mitigated structurally: the emitter runs
  in the same publish flow, from the same on-disk catalog state, and its
  output is fully derived.
- Reader floor: the typed v3 geometry column needs DuckDB ≥ 1.5.3-class
  engines. GeoParquet remains the universal access path; consumers keep a
  parquet fallback.
- The pyiceberg v3-manifest workaround must be revisited when pyiceberg ships
  v3 writers (isolated in `emitters/iceberg_static/table.py`).
