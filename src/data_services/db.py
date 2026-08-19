"""
CNC Data & Services Layer – Persistent store using DuckDB
Stores: Assets, Telemetry, Commands, Missions, Events, Detections, Alerts
"""

from pathlib import Path
from datetime import datetime
from typing import Optional, List, Dict, Any
import json
import duckdb
from loguru import logger

DB_PATH = Path("data/cnc.duckdb")


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
            datetime.utcnow()
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
                       parameters: dict, issued_by: str = "operator"):
        self.con.execute("""
            INSERT OR IGNORE INTO commands
            (command_id, asset_id, command_type, parameters, issued_by, issued_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [
            command_id, asset_id, command_type,
            json.dumps(parameters or {}),
            issued_by, datetime.utcnow()
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
            datetime.utcnow(),
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

    def close(self):
        self.con.close()


# Global instance
db = CNCDatabase()
