"""Analyses computed from the history database."""

from __future__ import annotations

import json
import time
from typing import Any

from history import History

DALI_BAUD = 1200  # bit/s


def bus_series(history: History, hours: float, line: int | None = None) -> dict[str, Any]:
    since_min = int((time.time() - hours * 3600) // 60)
    bucket = 1 if hours <= 6 else 5 if hours <= 48 else 60 if hours <= 24 * 14 else 240
    sql = """SELECT (minute / ?) * ? AS b, SUM(frames) frames, SUM(forward) forward, SUM(backward) backward,
                    SUM(events) events, SUM(queries) queries, SUM(no_answer) no_answer,
                    SUM(external) external, SUM(bits) bits
             FROM bus_minute WHERE minute >= ?"""
    params: list[Any] = [bucket, bucket, since_min]
    if line is not None:
        sql += " AND line = ?"
        params.append(line)
    sql += " GROUP BY b ORDER BY b"
    rows = history.query(sql, params)
    for r in rows:
        r["t"] = r.pop("b") * 60
        r["load_pct"] = round(100 * (r["bits"] or 0) / (DALI_BAUD * 60 * bucket), 2)
    totals = {
        k: sum(r[k] or 0 for r in rows)
        for k in ("frames", "forward", "backward", "events", "queries", "no_answer", "external")
    }
    return {"bucket_minutes": bucket, "rows": rows, "totals": totals}


def quality(history: History, days: int, devices: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    since = int(time.time() // 86400) - days + 1
    rows = history.query(
        """SELECT line, target, SUM(queries) queries, SUM(answered) answered FROM address_quality
           WHERE day >= ? GROUP BY line, target ORDER BY line, target""",
        (since,),
    )
    by_addr = {(d.get("line", 0), f"A{d.get('address')}"): d for d in devices.values()}
    for r in rows:
        dev = by_addr.get((r["line"], r["target"]))
        r["device_id"] = dev.get("id") if dev else None
        r["name"] = dev.get("name") if dev else None
        r["ratio"] = round(100 * r["answered"] / r["queries"], 1) if r["queries"] else None
    return rows


def device_summary(history: History, hours: float, devices: dict[int, dict[str, Any]]) -> list[dict[str, Any]]:
    """On-time, switch cycles, average level and downtime per device."""
    now = time.time()
    start = now - hours * 3600
    out = []
    for dev_id, dev in devices.items():
        prev = history.query(
            "SELECT * FROM device_state WHERE device_id=? AND ts<? ORDER BY ts DESC LIMIT 1", (dev_id, start)
        )
        rows = history.query(
            "SELECT * FROM device_state WHERE device_id=? AND ts>=? ORDER BY ts", (dev_id, start)
        )
        seq = ([{**prev[0], "ts": start}] if prev else []) + rows
        on_s = unavailable_s = level_int = 0.0
        switches = failures = 0
        last = None
        for row in seq:
            if last is not None:
                dt = row["ts"] - last["ts"]
                if last["is_on"]:
                    on_s += dt
                    level_int += dt * (last["level"] or 0)
                if not last["available"]:
                    unavailable_s += dt
                if row["is_on"] and not last["is_on"]:
                    switches += 1
                if (row["lamp_failure"] and not last["lamp_failure"]) or (row["gear_failure"] and not last["gear_failure"]):
                    failures += 1
            last = row
        if last is not None:
            dt = now - last["ts"]
            if last["is_on"]:
                on_s += dt
                level_int += dt * (last["level"] or 0)
            if not last["available"]:
                unavailable_s += dt
        tracked = now - seq[0]["ts"] if seq else 0
        out.append(
            {
                "device_id": dev_id,
                "name": dev.get("name"),
                "address": dev.get("address"),
                "line": dev.get("line"),
                "tracked_s": round(tracked),
                "on_s": round(on_s),
                "on_pct": round(100 * on_s / tracked, 1) if tracked else None,
                "avg_level": round(level_int / on_s, 1) if on_s else None,
                "switches": switches,
                "unavailable_s": round(unavailable_s),
                "failures": failures,
            }
        )
    return out


def device_series(history: History, dev_id: int, hours: float) -> list[dict[str, Any]]:
    start = time.time() - hours * 3600
    prev = history.query(
        "SELECT * FROM device_state WHERE device_id=? AND ts<? ORDER BY ts DESC LIMIT 1", (dev_id, start)
    )
    rows = history.query("SELECT * FROM device_state WHERE device_id=? AND ts>=? ORDER BY ts", (dev_id, start))
    if prev:
        rows.insert(0, {**prev[0], "ts": start})
    return rows


def measurements(history: History, dev_id: int, source: str, hours: float) -> list[dict[str, Any]]:
    start = time.time() - hours * 3600
    rows = history.query(
        "SELECT ts, data FROM measurements WHERE device_id=? AND source=? AND ts>=? ORDER BY ts",
        (dev_id, source, start),
    )
    return [{"ts": r["ts"], **json.loads(r["data"])} for r in rows]


def sensor_series(history: History, sensor_id: int, hours: float) -> list[dict[str, Any]]:
    start = time.time() - hours * 3600
    return history.query(
        "SELECT ts, value FROM sensor_values WHERE sensor_id=? AND ts>=? ORDER BY ts", (sensor_id, start)
    )


def health(history: History, devices: dict[int, dict[str, Any]], quality_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Actionable health report for all DALI devices."""
    qmap = {r["device_id"]: r for r in quality_rows if r.get("device_id") is not None}
    items = []
    for dev_id, dev in devices.items():
        status = dev.get("status") or {}
        issues: list[dict[str, str]] = []
        if not dev.get("available", True):
            issues.append({"level": "error", "code": "unavailable"})
        for key, code, level in (
            ("lampFailure", "lamp_failure", "error"),
            ("controlGearFailure", "gear_failure", "error"),
            ("limitError", "limit_error", "info"),
            ("powerCycleSeen", "power_cycle", "info"),
            ("isUnaddressed", "unaddressed", "warning"),
            ("resetState", "reset_state", "info"),
        ):
            if status.get(key):
                issues.append({"level": level, "code": code})
        q = qmap.get(dev_id)
        if q and q.get("ratio") is not None and q["ratio"] < 98 and dev.get("available", True):
            issues.append({"level": "warning", "code": "bad_communication", "detail": f"{q['ratio']} %"})
        last_diag = history.query(
            "SELECT data FROM measurements WHERE device_id=? AND source='diagnostics' ORDER BY ts DESC LIMIT 1",
            (dev_id,),
        )
        if last_diag:
            diag = json.loads(last_diag[0]["data"])
            for key, value in diag.items():
                if isinstance(value, bool) and value:
                    issues.append({"level": "warning", "code": "diag", "detail": key})
            life = diag.get("ratedMedianUsefulLifeOfLuminaireHours")
            on_s = diag.get("lightSourceOnTimeSeconds")
            if life and on_s:
                used = 100 * (on_s / 3600) / life
                if used > 80:
                    issues.append({"level": "warning", "code": "end_of_life", "detail": f"{used:.0f} %"})
        score = 100
        for issue in issues:
            score -= {"error": 40, "warning": 15, "info": 3}[issue["level"]]
        items.append(
            {
                "device_id": dev_id,
                "name": dev.get("name"),
                "address": dev.get("address"),
                "line": dev.get("line"),
                "issues": issues,
                "score": max(0, score),
                "answer_ratio": q.get("ratio") if q else None,
            }
        )
    items.sort(key=lambda x: x["score"])
    ok = sum(1 for i in items if not any(x["level"] != "info" for x in i["issues"]))
    return {"devices": items, "ok": ok, "total": len(items)}
