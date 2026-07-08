"""Publish-time emitters for portolan-cli.

An emitter derives an additional, read-only serialization of the catalog at
publish time. Unlike a versioning backend (``portolan_cli.backends``), an
emitter owns no state and accepts no writes: the STAC JSON catalog is always
the source of truth and every emitter output can be thrown away and rebuilt
from it. Emitters therefore regenerate their whole output on every run.

Built-in emitters:
    - "iceberg-static" — a static Iceberg REST catalog over the published
      GeoParquet (requires: pip install portolan-cli[iceberg])

Third-party plugins can register additional emitters via the
"portolan.emitters" entry point group, mirroring "portolan.backends".
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import Protocol, runtime_checkable

__all__ = ["EmitResult", "Emitter", "get_emitter"]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class EmitResult:
    """What one emitter run staged, and where each file belongs remotely.

    Attributes:
        staged_root: Local directory holding everything the run produced.
        object_keys: Map of staged file path (relative to ``staged_root``, POSIX)
            to the object key it must be uploaded under, relative to the
            catalog's public base.
        tables: Fully qualified names ("namespace.name") of the tables staged.
    """

    staged_root: Path
    object_keys: dict[str, str] = field(default_factory=dict)
    tables: list[str] = field(default_factory=list)


@runtime_checkable
class Emitter(Protocol):
    """Protocol for publish-time emitters.

    Emitters are pure projections: ``emit`` reads the catalog on disk and
    stages derived files, never mutating catalog state. Implementations must
    wipe and rebuild their staging directory on every call so the output can
    never drift from the catalog by accumulation.
    """

    name: str

    def emit(
        self,
        catalog_root: Path,
        *,
        public_base: str,
        options: dict[str, str] | None = None,
    ) -> EmitResult:
        """Stage the derived serialization for the catalog at ``catalog_root``.

        Args:
            catalog_root: Directory containing ``catalog.json``.
            public_base: The https base URL the catalog is published under;
                staged metadata references data files at this base.
            options: Emitter-specific settings (all optional).

        Returns:
            An EmitResult mapping staged files to their remote object keys.
        """
        ...


def get_emitter(name: str) -> Emitter:
    """Get an emitter by name.

    Discovers emitters through two mechanisms:
    1. Built-in "iceberg-static" (requires the [iceberg] extra)
    2. External plugins registered via the "portolan.emitters" entry point

    Args:
        name: Emitter name.

    Returns:
        Emitter instance.

    Raises:
        ValueError: If the emitter is unknown or its dependencies are missing.
            The message lists available emitters or the install hint.
    """
    if name == "iceberg-static":
        try:
            from portolan_cli.emitters.iceberg_static.emitter import IcebergStaticEmitter
        except ImportError as e:
            raise ValueError(
                "The 'iceberg-static' emitter requires the iceberg extra. "
                "Install it with: pip install portolan-cli[iceberg]"
            ) from e
        logger.debug("Creating IcebergStaticEmitter instance")
        return IcebergStaticEmitter()

    eps = entry_points(group="portolan.emitters")
    for ep in eps:
        if ep.name == name:
            return _load_plugin_emitter(ep, name)

    available = ["iceberg-static", *(ep.name for ep in eps)]
    raise ValueError(f"Unknown emitter: {name}. Available: {', '.join(available)}")


def _load_plugin_emitter(ep: EntryPoint, name: str) -> Emitter:
    """Load and validate a plugin emitter from an entry point."""
    try:
        emitter_class = ep.load()
    except Exception as e:
        raise ValueError(f"Failed to load emitter '{name}': {e}") from e
    try:
        emitter = emitter_class()
    except Exception as e:
        raise ValueError(f"Failed to instantiate emitter '{name}': {e}") from e
    if not isinstance(emitter, Emitter):
        raise ValueError(f"Emitter '{name}' does not implement the Emitter protocol")
    return emitter
