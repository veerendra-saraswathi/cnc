"""STORAGE_BACKEND selects duckdb by default. Postgres connects on first use."""

from pathlib import Path

import pytest

from src.data_services.repositories import get_repositories, open_snapshot, storage_backend


def test_default_backend_is_duckdb(monkeypatch):
    monkeypatch.delenv("STORAGE_BACKEND", raising=False)
    assert storage_backend() == "duckdb"
    repos = get_repositories()
    assert repos.backend == "duckdb"


def test_postgres_backend_does_not_connect_on_construction(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "postgres")
    repos = get_repositories()
    assert repos.backend == "postgres"
    assert repos._engine is None


def test_postgres_snapshot_does_not_connect_on_construction(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "postgres")
    store = open_snapshot(Path("/tmp/cnc-snapshot-unused.db"))
    assert type(store).__name__ == "PostgresSnapshot"
    assert store._engine is None


def test_unknown_backend_raises(monkeypatch):
    monkeypatch.setenv("STORAGE_BACKEND", "mysql")
    with pytest.raises(RuntimeError):
        storage_backend()
