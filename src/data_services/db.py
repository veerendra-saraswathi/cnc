"""
CNC Data & Services Layer – Persistent store using DuckDB
Stores: Assets, Telemetry, Commands, Missions, Events, Detections, Alerts
"""

import os
from pathlib import Path
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any
import json
import duckdb
from loguru import logger

# CNC_DUCKDB_PATH isolates a second process from the demo server's file lock.
# Unset, the path stays data/cnc.duckdb relative to the working directory.
DB_PATH = Path(os.environ.get("CNC_DUCKDB_PATH") or "data/cnc.duckdb")


def _json_object(value):
    if value is None or value == "":
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
        except json.JSONDecodeError:
            return None
        if isinstance(loaded, dict):
            return loaded
    return None


def _aware(value):
    from src.data_services.outbox_record import aware_utc
    return aware_utc(value)


def _command_row(row) -> dict:
    return {
        "id": row[0],
        "asset_id": row[1],
        "command_type": row[2],
        "parameters": _json_object(row[3]) or {},
        "issued_by": row[4] or "operator",
        "issued_at": _aware(row[5]),
        "status": row[6] or "pending",
        "result": _json_object(row[7]),
    }


def _event_row(row) -> dict:
    data = _json_object(row[6]) or {}
    actor = str(data.get("operator_id") or data.get("actor") or "system")
    return {
        "event_id": row[0],
        "timestamp": _aware(row[1]),
        "event_type": row[2],
        "asset_id": row[3] or "",
        "message": row[4],
        "severity": row[5] or "info",
        "data": data,
        "actor": actor,
    }


class CNCDatabase:
    def __init__(self, db_path: Path = DB_PATH):
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(self.db_path))
        self._create_tables()
        logger.info(f"CNC Database ready → {self.db_path}")

    def _create_tables(self):
        # Sequences must be created BEFORE tables that use them
        self.con.execute("CREATE SEQUENCE IF NOT EXISTS telemetry_seq START 1;")

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                asset_id        VARCHAR PRIMARY KEY,
                name            VARCHAR,
                asset_type      VARCHAR,
                capabilities    VARCHAR,
                max_speed       DOUBLE,
                created_at      TIMESTAMP
            );
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS telemetry (
                id              BIGINT PRIMARY KEY DEFAULT nextval('telemetry_seq'),
                asset_id        VARCHAR,
                timestamp       TIMESTAMP,
                lat             DOUBLE,
                lon             DOUBLE,
                alt             DOUBLE,
                heading         DOUBLE,
                speed           DOUBLE,
                battery_percent DOUBLE,
                status          VARCHAR,
                mode            VARCHAR
            );
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS commands (
                command_id      VARCHAR PRIMARY KEY,
                asset_id        VARCHAR,
                command_type    VARCHAR,
                parameters      VARCHAR,
                issued_by       VARCHAR,
                issued_at       TIMESTAMP
            );
        """)
        self.con.execute(
            "ALTER TABLE commands ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'pending'"
        )
        self.con.execute("ALTER TABLE commands ADD COLUMN IF NOT EXISTS result VARCHAR")

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS missions (
                mission_id      VARCHAR PRIMARY KEY,
                name            VARCHAR,
                status          VARCHAR,
                detection_lat   DOUBLE,
                detection_lon   DOUBLE,
                assigned_uavs   VARCHAR,
                assigned_ugv    VARCHAR,
                created_at      TIMESTAMP,
                completed_at    TIMESTAMP
            );
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id        VARCHAR PRIMARY KEY,
                timestamp       TIMESTAMP,
                event_type      VARCHAR,
                asset_id        VARCHAR,
                message         VARCHAR,
                severity        VARCHAR,
                data            VARCHAR
            );
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS detections (
                detection_id    VARCHAR PRIMARY KEY,
                timestamp       TIMESTAMP,
                asset_id        VARCHAR,
                lat             DOUBLE,
                lon             DOUBLE,
                label           VARCHAR,
                confidence      DOUBLE,
                mission_id      VARCHAR
            );
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                track_id        VARCHAR PRIMARY KEY,
                updated_at      TIMESTAMP,
                asset_id        VARCHAR,
                lat             DOUBLE,
                lon             DOUBLE,
                label           VARCHAR,
                status          VARCHAR,
                confidence      DOUBLE,
                payload         VARCHAR
            );
        """)

        self.con.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                alert_id        VARCHAR PRIMARY KEY,
                timestamp       TIMESTAMP,
                asset_id        VARCHAR,
                level           VARCHAR,
                message         VARCHAR,
                payload         VARCHAR
            );
        """)

        # JSON text is DuckDB's payload column. Postgres stores the same document as jsonb.
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS outbox (
                id              VARCHAR PRIMARY KEY,
                subject         VARCHAR NOT NULL,
                payload         VARCHAR NOT NULL,
                created_at      TIMESTAMP NOT NULL,
                published_at    TIMESTAMP,
                attempts        INTEGER NOT NULL DEFAULT 0
            );
        """)

    # ------------------------------------------------------------------
    # Assets
    # ------------------------------------------------------------------
    def upsert_asset(self, asset_id: str, name: str, asset_type: str,
                     capabilities: list = None, max_speed: float = None):
        self.con.execute("""
            INSERT OR REPLACE INTO assets
            (asset_id, name, asset_type, capabilities, max_speed, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            asset_id, name, asset_type,
            json.dumps(capabilities or []),
            max_speed,
            datetime.now(timezone.utc)
        ])

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------
    def insert_telemetry(self, asset_id: str, timestamp: datetime,
                         lat: float, lon: float, alt: float, heading: float,
                         speed: float, battery: float, status: str, mode: str):
        self.con.execute("""
            INSERT INTO telemetry
            (asset_id, timestamp, lat, lon, alt, heading, speed, battery_percent, status, mode)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [asset_id, timestamp, lat, lon, alt, heading, speed, battery, status, mode])

    # ------------------------------------------------------------------
    # Commands
    # ------------------------------------------------------------------
    def insert_command(self, command_id: str, asset_id: str, command_type: str,
                       parameters: dict, issued_by: str = "operator",
                       issued_at: datetime = None):
        self.con.execute("""
            INSERT OR IGNORE INTO commands
            (command_id, asset_id, command_type, parameters, issued_by, issued_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            command_id, asset_id, command_type,
            json.dumps(parameters or {}),
            issued_by, issued_at or datetime.now(timezone.utc)
        ])

    # ------------------------------------------------------------------
    # Missions
    # ------------------------------------------------------------------
    def upsert_mission(self, mission_id: str, name: str, status: str,
                       detection_lat: float = None, detection_lon: float = None,
                       assigned_uavs: list = None, assigned_ugv: str = None,
                       completed_at: datetime = None):
        self.con.execute("""
            INSERT OR REPLACE INTO missions
            (mission_id, name, status, detection_lat, detection_lon,
             assigned_uavs, assigned_ugv, created_at, completed_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            mission_id, name, status,
            detection_lat, detection_lon,
            json.dumps(assigned_uavs or []),
            assigned_ugv,
            datetime.now(timezone.utc),
            completed_at
        ])

    # ------------------------------------------------------------------
    # Events
    # ------------------------------------------------------------------
    def insert_event(self, event_id: str, timestamp: datetime, event_type: str,
                     message: str, asset_id: str = None,
                     severity: str = "info", data: dict = None):
        self.con.execute("""
            INSERT OR IGNORE INTO events
            (event_id, timestamp, event_type, asset_id, message, severity, data)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            event_id, timestamp, event_type, asset_id,
            message, severity, json.dumps(data or {})
        ])

    # ------------------------------------------------------------------
    # Detections
    # ------------------------------------------------------------------
    def insert_detection(self, detection_id: str, timestamp: datetime,
                         asset_id: str, lat: float, lon: float,
                         label: str = "unknown", confidence: float = 0.0,
                         mission_id: str = None):
        self.con.execute("""
            INSERT OR IGNORE INTO detections
            (detection_id, timestamp, asset_id, lat, lon, label, confidence, mission_id)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [detection_id, timestamp, asset_id, lat, lon, label, confidence, mission_id])

    # ------------------------------------------------------------------
    # Tracks and alerts
    # ------------------------------------------------------------------
    def upsert_track(self, track_id: str, updated_at: datetime, asset_id: str = None,
                     lat: float = None, lon: float = None, label: str = "unknown",
                     status: str = "", confidence: float = None, payload: dict = None):
        self.con.execute("""
            INSERT OR REPLACE INTO tracks
            (track_id, updated_at, asset_id, lat, lon, label, status, confidence, payload)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            track_id, updated_at, asset_id, lat, lon, label, status, confidence,
            json.dumps(payload or {}, default=str),
        ])

    def insert_alert(self, alert_id: str, timestamp: datetime, asset_id: str = None,
                     level: str = "info", message: str = "", payload: dict = None):
        self.con.execute("""
            INSERT OR REPLACE INTO alerts
            (alert_id, timestamp, asset_id, level, message, payload)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            alert_id, timestamp, asset_id, level, message,
            json.dumps(payload or {}, default=str),
        ])

    # ------------------------------------------------------------------
    # Query helpers
    # ------------------------------------------------------------------
    def get_recent_events(self, limit: int = 30) -> List[Dict]:
        rows = self.con.execute("""
            SELECT event_id, timestamp, event_type, asset_id, message, severity
            FROM events
            ORDER BY timestamp DESC
            LIMIT ?
        """, [limit]).fetchall()
        return [
            {
                "event_id": r[0], "timestamp": r[1], "event_type": r[2],
                "asset_id": r[3], "message": r[4], "severity": r[5]
            }
            for r in rows
        ]

    def get_mission(self, mission_id: str) -> Optional[Dict]:
        row = self.con.execute("""
            SELECT mission_id, name, status, detection_lat, detection_lon,
                   assigned_uavs, assigned_ugv, created_at, completed_at
            FROM missions WHERE mission_id = ?
        """, [mission_id]).fetchone()
        if not row:
            return None
        return {
            "mission_id": row[0], "name": row[1], "status": row[2],
            "detection_lat": row[3], "detection_lon": row[4],
            "assigned_uavs": json.loads(row[5] or "[]"),
            "assigned_ugv": row[6],
            "created_at": row[7], "completed_at": row[8]
        }

    def commit_issued(self, row: dict) -> None:
        """Command, event, and outbox. A failure rolls all three back."""
        payload = json.dumps(row["payload"], default=str)
        parameters = json.dumps(row["parameters"] or {})
        data = json.dumps(row["data"] or {})
        self.con.begin()
        try:
            self.con.execute(
                """
                INSERT INTO commands
                (command_id, asset_id, command_type, parameters, issued_by, issued_at, status, result)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["command_id"], row["asset_id"], row["command_type"],
                    parameters, row["issued_by"], row["issued_at"],
                    row.get("status") or "pending",
                    json.dumps(row["result"]) if row.get("result") is not None else None,
                ],
            )
            self.con.execute(
                """
                INSERT INTO events
                (event_id, timestamp, event_type, asset_id, message, severity, data)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    row["event_id"], row["event_time"], row["event_type"],
                    row["event_asset_id"], row["message"], row["severity"], data,
                ],
            )
            self.con.execute(
                """
                INSERT INTO outbox
                (id, subject, payload, created_at, published_at, attempts)
                VALUES (?, ?, ?, ?, NULL, 0)
                """,
                [row["outbox_id"], row["subject"], payload, row["created_at"]],
            )
            self.con.commit()
        except Exception:
            self.con.rollback()
            raise

    def list_unpublished(self) -> list[tuple]:
        return self.con.execute(
            """
            SELECT id, subject, payload, created_at, published_at, attempts
            FROM outbox
            WHERE published_at IS NULL
            ORDER BY created_at, id
            """
        ).fetchall()

    def mark_published(self, outbox_id: str, when: datetime) -> None:
        self.con.execute(
            """
            UPDATE outbox SET published_at = ?
            WHERE id = ? AND published_at IS NULL
            """,
            [when, outbox_id],
        )

    def note_attempt(self, outbox_id: str) -> None:
        self.con.execute(
            """
            UPDATE outbox SET attempts = attempts + 1
            WHERE id = ? AND published_at IS NULL
            """,
            [outbox_id],
        )

    def mark_published_command(self, command_id: str, when: datetime) -> None:
        self.con.execute(
            """
            UPDATE outbox SET published_at = ?
            WHERE published_at IS NULL
              AND json_extract_string(payload, '$.id') = ?
            """,
            [when, command_id],
        )

    def command_published(self, command_id: str) -> bool:
        row = self.con.execute(
            """
            SELECT published_at FROM outbox
            WHERE json_extract_string(payload, '$.id') = ?
            ORDER BY created_at DESC
            LIMIT 1
            """,
            [command_id],
        ).fetchone()
        return row is not None and row[0] is not None

    def update_command_result(self, command_id: str, status: str, result) -> None:
        self.con.execute(
            """
            UPDATE commands SET status = ?, result = ?
            WHERE command_id = ?
            """,
            [status, json.dumps(result) if result is not None else None, command_id],
        )

    def command_event_recorded(self, command_id: str) -> bool:
        row = self.con.execute(
            """
            SELECT 1 FROM events
            WHERE json_extract_string(data, '$.command_id') = ?
            LIMIT 1
            """,
            [command_id],
        ).fetchone()
        return row is not None

    def list_recent_commands(self, limit: int = 15) -> list[dict]:
        limit = max(1, min(int(limit), 50))
        rows = self.con.execute(
            """
            SELECT command_id, asset_id, command_type, parameters, issued_by,
                   issued_at, status, result
            FROM commands
            ORDER BY issued_at DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [_command_row(row) for row in rows]

    def list_recent_events(self, limit: int = 500) -> list[dict]:
        limit = max(1, min(int(limit), 500))
        rows = self.con.execute(
            """
            SELECT event_id, timestamp, event_type, asset_id, message, severity, data
            FROM events
            ORDER BY timestamp DESC
            LIMIT ?
            """,
            [limit],
        ).fetchall()
        return [_event_row(row) for row in rows]

    def close(self):
        self.con.close()


# Global instance
db = CNCDatabase()
