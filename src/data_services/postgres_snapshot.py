"""Operator snapshot documents in public.operator_snapshot.

The app role cannot create tables. connect() applies the DDL with the admin
engine, grants the app role, then returns the app engine for reads and writes.
"""

from __future__ import annotations

import json
import re

from sqlalchemy import text

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_DDL = """
CREATE TABLE IF NOT EXISTS operator_snapshot (
    kind        text NOT NULL,
    id          text NOT NULL,
    document    jsonb NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (kind, id)
)
"""


def _app_role() -> str:
    from src.data_services.settings import setting

    role = setting("POSTGRES_APP_USER", "cnc_app")
    if not _IDENT.match(role):
        raise RuntimeError(f"unsafe SQL identifier: {role!r}")
    return role


def connect_snapshot():
    """Ensure the table exists and return the application engine."""
    from src.data_services.postgres_backend import admin_engine, app_engine

    role = _app_role()
    with admin_engine().begin() as conn:
        conn.execute(text(_DDL))
        conn.execute(text(
            f"GRANT SELECT, INSERT, UPDATE, DELETE ON operator_snapshot TO {role}"
        ))
    engine = app_engine()
    with engine.connect() as conn:
        conn.execute(text("SELECT 1 FROM operator_snapshot LIMIT 1"))
    return engine


def save_document(engine, kind: str, object_id: str, document: dict) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO operator_snapshot (kind, id, document, updated_at)
                VALUES (:kind, :id, CAST(:document AS jsonb), now())
                ON CONFLICT (kind, id) DO UPDATE
                SET document = EXCLUDED.document, updated_at = now()
                """
            ),
            {
                "kind": kind,
                "id": object_id,
                "document": json.dumps(document, default=str),
            },
        )


def load_documents(engine, kind: str, limit: int | None) -> list:
    sql = """
        SELECT document FROM operator_snapshot
        WHERE kind = :kind
        ORDER BY updated_at DESC
    """
    params: dict = {"kind": kind}
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).all()
    documents = []
    for row in rows:
        value = row[0]
        if isinstance(value, str):
            value = json.loads(value)
        documents.append(value)
    return documents
