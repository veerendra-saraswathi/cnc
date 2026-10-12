"""Operator snapshot documents and command status.

Revision ID: 0003_snapshot_and_command_status
Revises: 0002_outbox
Create Date: 2026-10-13

operator_snapshot holds the operator JSON the SQLite file used to hold.
commands.status and commands.result are updated after dispatch so a restart
can show the last known phase. The app role may change both.
"""

from __future__ import annotations

import re

from alembic import op

revision = "0003_snapshot_and_command_status"
down_revision = "0002_outbox"
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
        "ALTER TABLE commands ADD COLUMN IF NOT EXISTS status text NOT NULL DEFAULT 'pending'"
    )
    op.execute("ALTER TABLE commands ADD COLUMN IF NOT EXISTS result jsonb")
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS operator_snapshot (
            kind        text NOT NULL,
            id          text NOT NULL,
            document    jsonb NOT NULL,
            updated_at  timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (kind, id)
        )
        """
    )
    role = _app_role()
    op.execute(f"GRANT SELECT, INSERT, UPDATE, DELETE ON operator_snapshot TO {role}")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS operator_snapshot")
    op.execute("ALTER TABLE commands DROP COLUMN IF EXISTS result")
    op.execute("ALTER TABLE commands DROP COLUMN IF EXISTS status")
