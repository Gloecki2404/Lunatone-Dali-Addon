"""SQLite storage for analyses: device history, bus statistics, event log, monitor."""

from __future__ import annotations

import json
import logging
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

_LOGGER = logging.getLogger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS device_state (
    ts REAL NOT NULL,
    device_id INTEGER NOT NULL,
    is_on INTEGER,
    level REAL,
    kelvin REAL,
    available INTEGER,
    lamp_failure INTEGER,
    gear_failure INTEGER
);
CREATE INDEX IF NOT EXISTS idx_device_state ON device_state(device_id, ts);

CREATE TABLE IF NOT EXISTS bus_minute (
    minute INTEGER NOT NULL,
    line INTEGER NOT NULL,
    frames INTEGER DEFAULT 0,
    forward INTEGER DEFAULT 0,
    backward INTEGER DEFAULT 0,
    events INTEGER DEFAULT 0,
    queries INTEGER DEFAULT 0,
    no_answer INTEGER DEFAULT 0,
    external INTEGER DEFAULT 0,
    bits INTEGER DEFAULT 0,
    PRIMARY KEY (minute, line)
);

CREATE TABLE IF NOT EXISTS address_quality (
    day INTEGER NOT NULL,
    line INTEGER NOT NULL,
    target TEXT NOT NULL,
    queries INTEGER DEFAULT 0,
    answered INTEGER DEFAULT 0,
    PRIMARY KEY (day, line, target)
);

CREATE TABLE IF NOT EXISTS event_log (
    ts REAL NOT NULL,
    level TEXT NOT NULL,
    category TEXT NOT NULL,
    device_id INTEGER,
    message TEXT NOT NULL,
    data TEXT
);
CREATE INDEX IF NOT EXISTS idx_event_log ON event_log(ts);

CREATE TABLE IF NOT EXISTS measurements (
    ts REAL NOT NULL,
    device_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    data TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_measurements ON measurements(device_id, source, ts);

CREATE TABLE IF NOT EXISTS sensor_values (
    ts REAL NOT NULL,
    sensor_id INTEGER NOT NULL,
    value REAL
);
CREATE INDEX IF NOT EXISTS idx_sensor_values ON sensor_values(sensor_id, ts);

CREATE TABLE IF NOT EXISTS monitor (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    line INTEGER,
    bits INTEGER,
    data TEXT,
    external INTEGER,
    kind TEXT,
    target TEXT,
    text TEXT
);

CREATE TABLE IF NOT EXISTS inputs (
    key TEXT PRIMARY KEY,
    line INTEGER,
    short_address INTEGER,
    instance_type INTEGER,
    instance_number INTEGER,
    kind TEXT,
    name TEXT,
    first_seen REAL,
    last_seen REAL,
    last_event TEXT,
    count INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS kv (
    key TEXT PRIMARY KEY,
    value TEXT
);
"""


class History:
    """Thread-safe (single connection + lock) SQLite wrapper."""

    def __init__(self, path: Path, history_days: int, monitor_buffer: int) -> None:
        self.path = path
        self.history_days = history_days
        self.monitor_buffer = monitor_buffer
        self._lock = threading.Lock()
        path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(str(path), check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=NORMAL")
        self._db.executescript(SCHEMA)
        self._db.commit()

    # ---------------------------------------------------------------- basics
    def execute(self, sql: str, params: tuple | list = ()) -> None:
        with self._lock:
            self._db.execute(sql, params)
            self._db.commit()

    def executemany(self, sql: str, rows: list[tuple]) -> None:
        if not rows:
            return
        with self._lock:
            self._db.executemany(sql, rows)
            self._db.commit()

    def query(self, sql: str, params: tuple | list = ()) -> list[dict[str, Any]]:
        with self._lock:
            cur = self._db.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]

    # -------------------------------------------------------------- key/value
    def kv_get(self, key: str, default: Any = None) -> Any:
        rows = self.query("SELECT value FROM kv WHERE key=?", (key,))
        if not rows:
            return default
        try:
            return json.loads(rows[0]["value"])
        except ValueError:
            return default

    def kv_set(self, key: str, value: Any) -> None:
        self.execute(
            "INSERT INTO kv(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )

    # ------------------------------------------------------------ event log
    def log_event(
        self,
        level: str,
        category: str,
        message: str,
        device_id: int | None = None,
        data: Any = None,
    ) -> dict[str, Any]:
        row = {
            "ts": time.time(),
            "level": level,
            "category": category,
            "device_id": device_id,
            "message": message,
            "data": data,
        }
        self.execute(
            "INSERT INTO event_log(ts, level, category, device_id, message, data) VALUES(?,?,?,?,?,?)",
            (row["ts"], level, category, device_id, message, json.dumps(data) if data is not None else None),
        )
        return row

    def events(self, since: float, limit: int = 500, category: str | None = None) -> list[dict[str, Any]]:
        sql = "SELECT * FROM event_log WHERE ts >= ?"
        params: list[Any] = [since]
        if category:
            sql += " AND category = ?"
            params.append(category)
        sql += " ORDER BY ts DESC LIMIT ?"
        params.append(limit)
        rows = self.query(sql, params)
        for r in rows:
            if r.get("data"):
                try:
                    r["data"] = json.loads(r["data"])
                except ValueError:
                    pass
        return rows

    # ---------------------------------------------------------- maintenance
    def cleanup(self) -> None:
        cutoff = time.time() - self.history_days * 86400
        with self._lock:
            self._db.execute("DELETE FROM device_state WHERE ts < ?", (cutoff,))
            self._db.execute("DELETE FROM bus_minute WHERE minute < ?", (int(cutoff // 60),))
            self._db.execute("DELETE FROM address_quality WHERE day < ?", (int(cutoff // 86400),))
            self._db.execute("DELETE FROM event_log WHERE ts < ?", (cutoff,))
            self._db.execute("DELETE FROM measurements WHERE ts < ?", (cutoff,))
            self._db.execute("DELETE FROM sensor_values WHERE ts < ?", (cutoff,))
            row = self._db.execute("SELECT MAX(id) FROM monitor").fetchone()
            if row and row[0]:
                self._db.execute("DELETE FROM monitor WHERE id <= ?", (row[0] - self.monitor_buffer,))
            self._db.commit()

    def size(self) -> int:
        try:
            return self.path.stat().st_size
        except OSError:
            return 0

    def close(self) -> None:
        with self._lock:
            self._db.close()
