"""Dashboard command door. Uses the app Command, records the security tape."""

from __future__ import annotations

from src.security import AuditLog, Role, Session

audit_log = AuditLog()
operator = Session(issuer="op-1", role=Role.OPERATOR)


def stamp(asset_id: str, verb: str) -> str:
    audit_log.authorise(operator, asset_id, verb)
    return audit_log.issued_by(operator)
