"""Transactional outbox for command publish.

Revision ID: 0002_outbox
Revises: 0001_kernel_tables
Create Date: 2026-10-12

One row is committed with the command and its event. The relay fills
published_at after JetStream accepts the message. The app role may insert
and update the row, and may not delete it.
"""

from __future__ import annotations

import re

from alembic import op

revision = "0002_outbox"
down_revision = "0001_kernel_tables"
branch_labels = None
depends_on = None

_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _app_role() -> str:
    from src.data_services.settings import setting

    role = setting("POSTGRES_APP_USER", "cnc_app")
    if not _IDENT.match(role):
        raise RuntimeError(f"unsafe SQL identifier: {role!r}")
    return role


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS outbox (
            id            uuid PRIMARY KEY,
            subject       text NOT NULL,
            payload       jsonb NOT NULL,
            created_at    timestamptz NOT NULL,
            published_at  timestamptz,
            attempts      integer NOT NULL DEFAULT 0
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS outbox_unpublished_idx
        ON outbox (created_at)
        WHERE published_at IS NULL
        """
    )
    role = _app_role()
    op.execute(f"GRANT SELECT, INSERT, UPDATE ON outbox TO {role}")
    op.execute(f"REVOKE DELETE ON outbox FROM {role}")
    op.execute("REVOKE DELETE ON outbox FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS outbox")
