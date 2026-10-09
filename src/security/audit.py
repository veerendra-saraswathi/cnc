"""Year-1 security: a role on the session, a tape of commands. Not an enclave."""

from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from uuid import uuid4

from pydantic import BaseModel, Field


class Role(str, Enum):
    OBSERVER = "observer"
    OPERATOR = "operator"
    SUPERVISOR = "supervisor"


class Session(BaseModel):
    session_id: str = Field(default_factory=lambda: str(uuid4()))
    issuer: str
    role: Role
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class AuditEntry(BaseModel):
    at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    session_id: str
    issuer: str
    role: Role
    asset_id: str
    verb: str
    accepted: bool
    reason: str = ""


class AuditLog:
    def __init__(self) -> None:
        self.entries: list[AuditEntry] = []

    def authorise(self, session: Session, asset_id: str, verb: str) -> AuditEntry:
        accepted = session.role is not Role.OBSERVER
        entry = AuditEntry(
            session_id=session.session_id,
            issuer=session.issuer,
            role=session.role,
            asset_id=asset_id,
            verb=verb,
            accepted=accepted,
            reason="" if accepted else "observer cannot issue a task",
        )
        self.entries.append(entry)
        if not accepted:
            raise PermissionError(entry.reason)
        return entry

    def issued_by(self, session: Session) -> str:
        return f"{session.role.value}:{session.issuer}"
