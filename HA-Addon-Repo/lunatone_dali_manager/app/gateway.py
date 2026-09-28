"""Connection to the DALI-2 IoT gateway: websocket, state cache, history tracking."""

from __future__ import annotations

import asyncio
from collections import deque
import json
import logging
import time
from typing import Any, Awaitable, Callable

import aiohttp

from dali_decode import DaliDecoder, decode_event
from history import History

_LOGGER = logging.getLogger(__name__)

TIMEOUT = aiohttp.ClientTimeout(total=20)
POLL_INTERVAL = 60
EXTRAS_INTERVAL = 900
DT_ENERGY = 51
DT_DIAG = 52


def deep_merge(base: dict[str, Any], update: dict[str, Any]) -> dict[str, Any]:
    for key, value in update.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            deep_merge(base[key], value)
        else:
            base[key] = value
    return base


def _normalize(dev: dict[str, Any]) -> dict[str, Any]:
    status = dev.get("status")
    if isinstance(status, dict) and "shortAddress" in status:
        status.setdefault("isUnaddressed", status.pop("shortAddress"))
    return dev


def _status(features: dict[str, Any], key: str) -> Any:
    value = features.get(key)
    return value.get("status") if isinstance(value, dict) else None


class BusStats:
    """Per-minute bus statistics and per-address answer quality."""

    def __init__(self) -> None:
        self.minutes: dict[tuple[int, int], dict[str, int]] = {}
        self.quality: dict[tuple[int, int, str], list[int]] = {}
        self._pending: dict[int, tuple[str, float]] = {}
        self.recent_rate: deque[tuple[float, int]] = deque(maxlen=600)

    def _bucket(self, line: int, ts: float) -> dict[str, int]:
        key = (int(ts // 60), line)
        if key not in self.minutes:
            self.minutes[key] = dict.fromkeys(
                ("frames", "forward", "backward", "events", "queries", "no_answer", "external", "bits"), 0
            )
        return self.minutes[key]

    def _quality(self, line: int, ts: float, target: str) -> list[int]:
        key = (int(ts // 86400), line, target)
        return self.quality.setdefault(key, [0, 0])

    def add(self, decoded: dict[str, Any]) -> None:
        ts = decoded.get("timestamp") or time.time()
        line = decoded.get("line", 0)
        b = self._bucket(line, ts)
        b["frames"] += 1
        b["bits"] += int(decoded.get("bits") or 0) + 3  # start + stop bits approx.
        if decoded.get("external"):
            b["external"] += 1
        kind = decoded.get("kind")
        if kind == "backward":
            b["backward"] += 1
            pending = self._pending.pop(line, None)
            if pending:
                self._quality(line, pending[1], pending[0])[1] += 1
            return
        if kind == "event":
            b["events"] += 1
            return
        b["forward"] += 1
        # a new forward frame while a query is pending → no answer
        pending = self._pending.pop(line, None)
        if pending:
            self._bucket(line, pending[1])["no_answer"] += 1
        if kind == "query" or decoded.get("query"):
            b["queries"] += 1
            target = decoded.get("target") or "?"
            self._quality(line, ts, target)[0] += 1
            self._pending[line] = (target, ts)

    def flush(self, history: History) -> None:
        now_min = int(time.time() // 60)
        rows = []
        for (minute, line), c in list(self.minutes.items()):
            rows.append(
                (minute, line, c["frames"], c["forward"], c["backward"], c["events"], c["queries"],
                 c["no_answer"], c["external"], c["bits"])
            )
            if minute < now_min:
                del self.minutes[(minute, line)]
        history.executemany(
            """INSERT INTO bus_minute(minute,line,frames,forward,backward,events,queries,no_answer,external,bits)
               VALUES(?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(minute,line) DO UPDATE SET frames=excluded.frames, forward=excluded.forward,
               backward=excluded.backward, events=excluded.events, queries=excluded.queries,
               no_answer=excluded.no_answer, external=excluded.external, bits=excluded.bits""",
            rows,
        )
        qrows = [(d, l, t, q[0], q[1]) for (d, l, t), q in self.quality.items()]
        history.executemany(
            """INSERT INTO address_quality(day,line,target,queries,answered) VALUES(?,?,?,?,?)
               ON CONFLICT(day,line,target) DO UPDATE SET queries=queries+excluded.queries,
               answered=answered+excluded.answered""",
            qrows,
        )
        self.quality.clear()


class Gateway:
    """Keeps a live mirror of the gateway and feeds history + browser clients."""

    def __init__(
        self,
        session: aiohttp.ClientSession,
        history: History,
        broadcast: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self.session = session
        self.history = history
        self.broadcast = broadcast
        self.host: str | None = None
        self.connected = False
        self.last_error: str | None = None
        self.info: dict[str, Any] = {}
        self.devices: dict[int, dict[str, Any]] = {}
        self.zones: dict[int, dict[str, Any]] = {}
        self.sensors: dict[int, dict[str, Any]] = {}
        self.decoder = DaliDecoder()
        self.stats = BusStats()
        self.monitor: deque[dict[str, Any]] = deque(maxlen=5000)
        self._monitor_rows: list[tuple] = []
        self._last_state: dict[int, tuple] = {}
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        self._unsupported: set[tuple[int, str]] = set()
        self.on_connected: Callable[[str, dict[str, Any]], Awaitable[None]] | None = None

    @property
    def base_url(self) -> str:
        return f"http://{self.host}"

    # ------------------------------------------------------------ lifecycle
    async def start(self, host: str) -> None:
        await self.stop()
        self.host = host
        self._stop = asyncio.Event()
        self.devices, self.zones, self.sensors, self.info = {}, {}, {}, {}
        self._last_state = {}
        loops = (self._ws_loop, self._poll_loop, self._extras_loop, self._flush_loop, self._cleanup_loop)
        self._tasks = [asyncio.create_task(fn()) for fn in loops]
        _LOGGER.info("Using gateway %s", host)

    async def stop(self) -> None:
        self._stop.set()
        for task in self._tasks:
            task.cancel()
        for task in self._tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):  # noqa: BLE001
                pass
        self._tasks = []
        self.connected = False

    # ------------------------------------------------------------------ REST
    async def request(self, method: str, path: str, data: Any = None) -> Any:
        async with self.session.request(method, f"{self.base_url}{path}", json=data, timeout=TIMEOUT) as r:
            text = await r.text()
            if r.status >= 400:
                raise RuntimeError(f"{method} {path}: HTTP {r.status} {text[:200]}")
            return json.loads(text) if text else None

    async def snapshot(self) -> None:
        info = await self.request("GET", "/info")
        devices = (await self.request("GET", "/devices") or {}).get("devices", [])
        zones = (await self.request("GET", "/zones") or {}).get("zones", [])
        sensors = (await self.request("GET", "/sensors") or {}).get("sensors", [])
        self.info = info or {}
        self.devices = {d["id"]: _normalize(d) for d in devices}
        self.zones = {z["id"]: z for z in zones}
        self.sensors = {s["id"]: s for s in sensors}
        for dev_id in list(self.devices):
            self._track_device(dev_id)
        for sid, sensor in self.sensors.items():
            self._track_sensor(sid, sensor)

    def state(self) -> dict[str, Any]:
        return {
            "host": self.host,
            "connected": self.connected,
            "last_error": self.last_error,
            "info": self.info,
            "devices": list(self.devices.values()),
            "zones": list(self.zones.values()),
            "sensors": list(self.sensors.values()),
        }

    # ------------------------------------------------------------- tracking
    def _track_device(self, dev_id: int) -> None:
        dev = self.devices.get(dev_id) or {}
        feats = dev.get("features") or {}
        status = dev.get("status") or {}
        level = _status(feats, "dimmable")
        switch = _status(feats, "switchable")
        is_on = bool(switch) if switch is not None else (level or 0) > 0
        kelvin = _status(feats, "colorKelvin")
        state = (
            int(is_on),
            round(level, 1) if isinstance(level, (int, float)) else None,
            kelvin,
            int(bool(dev.get("available", True))),
            int(bool(status.get("lampFailure"))),
            int(bool(status.get("controlGearFailure"))),
        )
        prev = self._last_state.get(dev_id)
        if prev == state:
            return
        self._last_state[dev_id] = state
        self.history.execute(
            "INSERT INTO device_state(ts,device_id,is_on,level,kelvin,available,lamp_failure,gear_failure) VALUES(?,?,?,?,?,?,?,?)",
            (time.time(), dev_id, *state),
        )
        if prev is None:
            return
        name = dev.get("name") or f"#{dev_id}"
        checks = (
            (3, "availability", "Gerät nicht mehr erreichbar", "Gerät wieder erreichbar", True),
            (4, "lamp_failure", "Lampenfehler erkannt", "Lampenfehler behoben", False),
            (5, "gear_failure", "Betriebsgerätefehler erkannt", "Betriebsgerätefehler behoben", False),
        )
        for idx, cat, bad, good, bad_is_zero in checks:
            if prev[idx] == state[idx]:
                continue
            is_bad = (state[idx] == 0) if bad_is_zero else (state[idx] == 1)
            row = self.history.log_event(
                "error" if is_bad else "info", cat, f"{name}: {bad if is_bad else good}", dev_id
            )
            asyncio.get_running_loop().create_task(self.broadcast({"type": "addon_event", "data": row}))

    def _track_sensor(self, sid: int, sensor: dict[str, Any]) -> None:
        value = sensor.get("value")
        if isinstance(value, (int, float, bool)):
            self.history.execute(
                "INSERT INTO sensor_values(ts,sensor_id,value) VALUES(?,?,?)", (time.time(), sid, float(value))
            )

    # ------------------------------------------------------------ websocket
    async def _ws_loop(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                await self.snapshot()
                async with self.session.ws_connect(f"ws://{self.host}/", heartbeat=30) as ws:
                    self.connected = True
                    self.last_error = None
                    backoff = 1.0
                    _LOGGER.info("Connected to gateway websocket %s", self.host)
                    await self.broadcast({"type": "addon_status", "data": self.status()})
                    await self.broadcast({"type": "snapshot", "data": self.state()})
                    if self.on_connected:
                        await self.on_connected(self.host, self.info)
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            try:
                                data = json.loads(msg.data)
                            except ValueError:
                                continue
                            await self._on_message(data)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            break
            except asyncio.CancelledError:
                raise
            except Exception as err:  # noqa: BLE001
                self.last_error = str(err) or (
                    "Zeitüberschreitung (Gateway antwortet nicht)" if isinstance(err, asyncio.TimeoutError) else err.__class__.__name__
                )
                _LOGGER.warning("Gateway connection problem: %s", self.last_error)
            if self.connected:
                self.connected = False
                row = self.history.log_event("warning", "gateway", "Verbindung zum Gateway unterbrochen")
                await self.broadcast({"type": "addon_event", "data": row})
            await self.broadcast({"type": "addon_status", "data": self.status()})
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=backoff)
            except asyncio.TimeoutError:
                pass
            backoff = min(backoff * 2, 30)

    async def _on_message(self, msg: dict[str, Any]) -> None:
        mtype = msg.get("type")
        payload = msg.get("data")
        if mtype == "daliMonitor" and isinstance(payload, dict):
            decoded = self.decoder.decode(payload)
            self.stats.add(decoded)
            self.monitor.append(decoded)
            self._monitor_rows.append(
                (decoded.get("timestamp") or time.time(), decoded["line"], decoded["bits"],
                 json.dumps(decoded["data"]), int(decoded["external"]), decoded["kind"],
                 decoded.get("target", ""), decoded["text"])
            )
            if decoded["kind"] == "event":
                self._learn_input(decoded)
            await self.broadcast({"type": "monitor", "data": decoded})
            return
        if isinstance(payload, dict):
            if mtype == "devices":
                for dev in payload.get("devices", []):
                    did = dev.get("id")
                    if did in self.devices:
                        deep_merge(self.devices[did], _normalize(dev))
                        self._track_device(did)
                    else:
                        asyncio.get_running_loop().create_task(self._resnapshot())
            elif mtype == "zones":
                for zone in payload.get("zones", []):
                    zid = zone.get("id")
                    if zid in self.zones:
                        deep_merge(self.zones[zid], zone)
                    else:
                        asyncio.get_running_loop().create_task(self._resnapshot())
            elif mtype == "sensors":
                for sensor in payload.get("sensors", []):
                    sid = sensor.get("id")
                    if sid in self.sensors:
                        deep_merge(self.sensors[sid], sensor)
                        self._track_sensor(sid, self.sensors[sid])
                    else:
                        asyncio.get_running_loop().create_task(self._resnapshot())
            elif mtype == "info":
                self.info = payload
        await self.broadcast(msg)

    async def _resnapshot(self) -> None:
        await asyncio.sleep(1.5)
        try:
            await self.snapshot()
            await self.broadcast({"type": "snapshot", "data": self.state()})
        except Exception as err:  # noqa: BLE001
            _LOGGER.debug("Resnapshot failed: %s", err)

    # --------------------------------------------------------------- inputs
    def _learn_input(self, decoded: dict[str, Any]) -> None:
        evt = decoded.get("event") or {}
        key = evt.get("source_key")
        if not key:
            return
        now = time.time()
        existing = self.history.query("SELECT key FROM inputs WHERE key=?", (key,))
        if not existing:
            self.history.execute(
                """INSERT INTO inputs(key,line,short_address,instance_type,instance_number,kind,name,first_seen,last_seen,last_event,count)
                   VALUES(?,?,?,?,?,?,?,?,?,?,1)""",
                (key, evt.get("line"), evt.get("short_address"), evt.get("instance_type"),
                 evt.get("instance_number"), evt.get("kind"), "", now, now, evt.get("event_type")),
            )
            row = self.history.log_event("info", "input", f"Neuer DALI-Eingang erkannt: {key} ({evt.get('kind')})", data=evt)
            asyncio.get_running_loop().create_task(self.broadcast({"type": "addon_event", "data": row}))
        else:
            self.history.execute(
                "UPDATE inputs SET last_seen=?, last_event=?, count=count+1 WHERE key=?",
                (now, evt.get("event_type"), key),
            )

    # ------------------------------------------------------------ periodic
    async def _poll_loop(self) -> None:
        while True:
            await asyncio.sleep(POLL_INTERVAL)
            if not self.connected:
                continue
            try:
                await self.snapshot()
            except Exception as err:  # noqa: BLE001
                _LOGGER.debug("Poll failed: %s", err)

    async def _extras_loop(self) -> None:
        await asyncio.sleep(20)
        while True:
            if self.connected:
                await self.sample_extras()
            await asyncio.sleep(EXTRAS_INTERVAL)

    async def sample_extras(self) -> dict[int, dict[str, Any]]:
        result: dict[int, dict[str, Any]] = {}
        for dev_id, dev in list(self.devices.items()):
            types = dev.get("daliTypes") or []
            if not dev.get("available", True):
                continue
            for dt, source, path in ((DT_ENERGY, "energy", "energyReporting"), (DT_DIAG, "diagnostics", "diagnosticsMaintenance")):
                if dt not in types or (dev_id, source) in self._unsupported:
                    continue
                try:
                    async with self.session.get(f"{self.base_url}/device/{dev_id}/{path}", timeout=TIMEOUT) as r:
                        if r.status == 501:
                            self._unsupported.add((dev_id, source))
                            continue
                        if r.status >= 400:
                            continue
                        data = await r.json(content_type=None)
                except Exception:  # noqa: BLE001
                    continue
                result.setdefault(dev_id, {})[source] = data
                self.history.execute(
                    "INSERT INTO measurements(ts,device_id,source,data) VALUES(?,?,?,?)",
                    (time.time(), dev_id, source, json.dumps(data)),
                )
        return result

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(10)
            try:
                rows, self._monitor_rows = self._monitor_rows, []
                self.history.executemany(
                    "INSERT INTO monitor(ts,line,bits,data,external,kind,target,text) VALUES(?,?,?,?,?,?,?,?)", rows
                )
                self.stats.flush(self.history)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Flushing statistics failed")

    async def _cleanup_loop(self) -> None:
        while True:
            try:
                await asyncio.to_thread(self.history.cleanup)
            except Exception:  # noqa: BLE001
                _LOGGER.exception("Cleanup failed")
            await asyncio.sleep(3600)

    def status(self) -> dict[str, Any]:
        return {"host": self.host, "connected": self.connected, "last_error": self.last_error}
