"""Emitter registry: same optionality contract as backends.get_backend."""

from __future__ import annotations

import pytest

from portolan_cli.emitters import get_emitter

pytestmark = pytest.mark.unit


def test_unknown_emitter_lists_available() -> None:
    with pytest.raises(ValueError, match="iceberg-static"):
        get_emitter("nope")


def test_iceberg_static_emitter_resolves() -> None:
    pytest.importorskip("pyiceberg")
    emitter = get_emitter("iceberg-static")
    assert emitter.name == "iceberg-static"


def test_missing_extra_raises_install_hint(monkeypatch: pytest.MonkeyPatch) -> None:
    import builtins

    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name.startswith("portolan_cli.emitters.iceberg_static"):
            raise ImportError("No module named 'pyiceberg'")
        return real_import(name, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(builtins, "__import__", fake_import)
    with pytest.raises(ValueError, match=r"portolan-cli\[iceberg\]"):
        get_emitter("iceberg-static")
