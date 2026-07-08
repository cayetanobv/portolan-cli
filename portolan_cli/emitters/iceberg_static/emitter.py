"""The iceberg-static emitter: plan from STAC, stage tables and REST surface.

emit() is idempotent by construction: the whole staging directory is wiped and
rebuilt from the catalog on every run, so the projection can never drift from
the STAC JSON by accumulation (there is no append path at all).

Staging layout, under ``<catalog_root>/.portolan/emit/iceberg-static/``:

    tables/data/<ns>/<collection>/metadata/v1.metadata.json
    tables/data/<ns>/<collection>/metadata/snap-1-manifest{,-list}.avro
    surface/<flattened-key>.json + _surface_manifest.json

The returned EmitResult maps every staged file to the object key it must be
uploaded under (relative to the catalog's public base).
"""

from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from portolan_cli.emitters import EmitResult
from portolan_cli.emitters.iceberg_static.plan import TablePlan, plan_tables
from portolan_cli.emitters.iceberg_static.surface import MANIFEST_NAME, build_surface, stage_surface
from portolan_cli.emitters.iceberg_static.table import write_table
from portolan_cli.errors import EmitError

DEFAULT_PREFIX = "sdi"
DEFAULT_NAMESPACE = "collections"


class IcebergStaticEmitter:
    """Stage a static Iceberg REST catalog derived from the STAC catalog."""

    name = "iceberg-static"

    def emit(
        self,
        catalog_root: Path,
        *,
        public_base: str,
        options: dict[str, str] | None = None,
    ) -> EmitResult:
        """Regenerate the full projection for the catalog at ``catalog_root``.

        Raises:
            EmitError: When the catalog has no collection with local GeoParquet
                data assets (nothing to project is a configuration mistake).
        """
        opts = options or {}
        prefix = opts.get("prefix", DEFAULT_PREFIX)
        namespace = opts.get("namespace", DEFAULT_NAMESPACE)

        plan = plan_tables(catalog_root, public_base=public_base, namespace=namespace)
        if not plan:
            raise EmitError(
                "iceberg-static: no collection with local GeoParquet data assets to project",
                catalog_root=str(catalog_root),
            )

        staged_root = catalog_root / ".portolan" / "emit" / self.name
        if staged_root.exists():
            shutil.rmtree(staged_root)
        tables_root = staged_root / "tables"
        surface_root = staged_root / "surface"

        metas = self._write_tables(plan, tables_root)
        surface = build_surface(
            [{"ns": t.namespace, "name": t.name, "location_uri": t.location_uri} for t in plan],
            metas,
            prefix=prefix,
        )
        surface_mapping = stage_surface(surface, surface_root)

        return EmitResult(
            staged_root=staged_root,
            object_keys=self._object_keys(staged_root, tables_root, surface_mapping),
            tables=[f"{t.namespace}.{t.name}" for t in plan],
        )

    @staticmethod
    def _write_tables(
        plan: list[TablePlan], tables_root: Path
    ) -> dict[tuple[str, str], dict[str, Any]]:
        metas: dict[tuple[str, str], dict[str, Any]] = {}
        for t in plan:
            metas[(t.namespace, t.name)] = write_table(
                tables_root / t.key,
                table_id=f"{t.namespace}.{t.name}",
                parts=t.parts,
                location_uri=t.location_uri,
                properties=t.properties,
            )
        return metas

    @staticmethod
    def _object_keys(
        staged_root: Path, tables_root: Path, surface_mapping: dict[str, str]
    ) -> dict[str, str]:
        """Staged file (relative to staged_root) -> remote object key."""
        keys: dict[str, str] = {}
        for path in sorted(tables_root.rglob("*")):
            if path.is_file():
                rel = path.relative_to(tables_root).as_posix()
                keys[path.relative_to(staged_root).as_posix()] = rel
        for staged, real_key in surface_mapping.items():
            keys[f"surface/{staged}"] = real_key
        # The staging manifest itself is local bookkeeping, never uploaded, so
        # it is deliberately absent from the map.
        del_key = f"surface/{MANIFEST_NAME}"
        keys.pop(del_key, None)
        return keys
