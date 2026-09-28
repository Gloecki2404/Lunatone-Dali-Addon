"""Lunatone DALI-2 IoT Manager – Home Assistant add-on backend."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import os
from pathlib import Path
import time
from typing import Any

import aiohttp
from aiohttp import web

import analytics
import discovery
from gateway import Gateway
from history import History

VERSION = "1.0.0"
DATA_DIR = Path(os.environ.get("DATA_DIR", "/data"))
WWW_DIR = Path(os.environ.get("WWW_DIR", Path(__file__).resolve().parent.parent / "www"))
PORT = int(os.environ.get("PORT", "8099"))
INGRESS_IP = "172.30.32.2"
DEV_MODE = not os.environ.get("SUPERVISOR_TOKEN")

_LOGGER = logging.getLogger("lunatone")

# gateway paths that are never proxied without the explicit expert header
DANGEROUS = ("/reset", "/firmware/upload")


def load_options() -> dict[str, Any]:
    opts: dict[str, Any] = {"gateway_host": "", "history_days": 30, "monitor_buffer": 100000, "log_level": "info"}
    path = DATA_DIR / "options.json"
    if path.exists():
        try:
            opts.update(json.loads(path.read_text()))
        except ValueError:
            pass
    if os.environ.get("GATEWAY_HOST"):
        opts["gateway_host"] = os.environ["GATEWAY_HOST"]
    return opts


class App:
    def __init__(self) -> None:
        self.options = load_options()
        logging.basicConfig(
            level=getattr(logging, str(self.options.get("log_level", "info")).upper(), logging.INFO),
            format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        )
        self.history = History(
            DATA_DIR / "lunatone.db", int(self.options["history_days"]), int(self.options["monitor_buffer"])
        )
        self.clients: set[web.WebSocketResponse] = set()
        self.session: aiohttp.ClientSession | None = None
        self.gateway: Gateway | None = None
        self.ha: dict[str, Any] = {}
        self._announced: str | None = self.history.kv_get("announced_host")

    # ------------------------------------------------------------ lifecycle
    async def startup(self, app: web.Application) -> None:
        self.session = aiohttp.ClientSession()
        self.gateway = Gateway(self.session, self.history, self.broadcast)
        self.gateway.on_connected = self._on_gateway_connected
        self.ha = await discovery.ha_config(self.session)
        host = self.options.get("gateway_host") or self.history.kv_get("gateway_host")
        if not host:
            asyncio.create_task(self._auto_discover())
        else:
            await self.gateway.start(host)

    async def cleanup(self, app: web.Application) -> None:
        if self.gateway:
            await self.gateway.stop()
        for ws in list(self.clients):
            await ws.close()
        if self.session:
            await self.session.close()
        self.history.close()

    async def _auto_discover(self) -> None:
        assert self.session and self.gateway
        _LOGGER.info("No gateway configured – searching the local network...")
        found = await discovery.scan_network(self.session)
        if found:
            host = found[0]["host"]
            _LOGGER.info("Found gateway %s at %s", found[0].get("name"), host)
            self.history.kv_set("gateway_host", host)
            await self.gateway.start(host)
        else:
            _LOGGER.warning("No gateway found. Please set 'gateway_host' in the add-on configuration or in the UI.")
            await self.broadcast({"type": "addon_status", "data": self.status()})

    async def _on_gateway_connected(self, host: str, info: dict[str, Any]) -> None:
        if self._announced != host and self.session:
            if await discovery.announce(self.session, host):
                self._announced = host
                self.history.kv_set("announced_host", host)

    def status(self) -> dict[str, Any]:
        gw = self.gateway.status() if self.gateway else {}
        return {
            "version": VERSION,
            "dev_mode": DEV_MODE,
            "gateway": gw,
            "ha": self.ha,
            "db_size": self.history.size(),
            "history_days": self.history.history_days,
            "monitor_buffer": self.history.monitor_buffer,
            "configured_host": self.options.get("gateway_host") or None,
        }

    # ------------------------------------------------------------ websocket
    async def broadcast(self, msg: dict[str, Any]) -> None:
        if not self.clients:
            return
        data = json.dumps(msg, default=str)
        for ws in list(self.clients):
            if ws.closed:
                self.clients.discard(ws)
                continue
            try:
                await ws.send_str(data)
            except (ConnectionResetError, RuntimeError):
                self.clients.discard(ws)

    async def ws_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=30)
        await ws.prepare(request)
        self.clients.add(ws)
        try:
            await ws.send_str(json.dumps({"type": "addon_status", "data": self.status()}, default=str))
            if self.gateway and self.gateway.connected:
                await ws.send_str(json.dumps({"type": "snapshot", "data": self.gateway.state()}, default=str))
            async for _msg in ws:
                pass
        finally:
            self.clients.discard(ws)
        return ws

    # ----------------------------------------------------------------- proxy
    async def proxy(self, request: web.Request) -> web.StreamResponse:
        assert self.session and self.gateway
        if not self.gateway.host:
            return web.json_response({"detail": "No gateway configured"}, status=503)
        path = "/" + request.match_info["path"]
        if path.startswith(DANGEROUS) and request.headers.get("X-Expert-Confirm") != "yes":
            return web.json_response({"detail": "Expert confirmation required"}, status=403)
        url = f"{self.gateway.base_url}{path}"
        body = await request.read() if request.can_read_body else None
        headers = {}
        if request.content_type and body:
            headers["Content-Type"] = request.headers.get("Content-Type", request.content_type)
        # short connect timeout → fast, clear error while the gateway is offline
        timeout = aiohttp.ClientTimeout(total=600 if "upload" in path else 60, sock_connect=6)
        try:
            async with self.session.request(
                request.method, url, params=request.query, data=body, headers=headers, timeout=timeout
            ) as resp:
                out_headers = {
                    k: v
                    for k, v in resp.headers.items()
                    if k.lower() in ("content-type", "content-disposition")
                }
                payload = await resp.read()
                return web.Response(status=resp.status, body=payload, headers=out_headers)
        except asyncio.TimeoutError:
            return web.json_response(
                {"detail": f"Gateway {self.gateway.host} antwortet nicht (Zeitüberschreitung)"}, status=504
            )
        except aiohttp.ClientError as err:
            return web.json_response({"detail": f"Gateway not reachable: {err}"}, status=502)

    # ------------------------------------------------------------------- api
    async def api_status(self, request: web.Request) -> web.Response:
        if not self.ha and self.session:
            self.ha = await discovery.ha_config(self.session)
        return web.json_response(self.status())

    async def api_state(self, request: web.Request) -> web.Response:
        assert self.gateway
        return web.json_response(self.gateway.state())

    async def api_set_gateway(self, request: web.Request) -> web.Response:
        assert self.gateway and self.session
        data = await request.json()
        host = str(data.get("host", "")).strip()
        if not host:
            return web.json_response({"detail": "host missing"}, status=400)
        try:
            async with self.session.get(f"http://{host}/info", timeout=aiohttp.ClientTimeout(total=5)) as r:
                info = await r.json(content_type=None)
            if "uid" not in info:
                raise ValueError("not a DALI-2 IoT")
        except Exception as err:  # noqa: BLE001
            return web.json_response({"detail": f"Kein DALI-2 IoT Gateway unter {host}: {err}"}, status=400)
        self.history.kv_set("gateway_host", host)
        await self.gateway.start(host)
        return web.json_response({"ok": True, "info": info})

    async def api_discover(self, request: web.Request) -> web.Response:
        assert self.session
        return web.json_response(await discovery.scan_network(self.session))

    async def api_announce(self, request: web.Request) -> web.Response:
        assert self.session and self.gateway
        ok = bool(self.gateway.host) and await discovery.announce(self.session, self.gateway.host)
        return web.json_response({"ok": ok, "supervisor": not DEV_MODE})

    async def api_monitor(self, request: web.Request) -> web.Response:
        assert self.gateway
        limit = min(int(request.query.get("limit", 1000)), 5000)
        before = request.query.get("before")
        if before:
            rows = self.history.query(
                "SELECT * FROM monitor WHERE id < ? ORDER BY id DESC LIMIT ?", (int(before), limit)
            )
            for r in rows:
                r["data"] = json.loads(r["data"] or "[]")
                r["timestamp"] = r.pop("ts")
            return web.json_response(list(reversed(rows)))
        return web.json_response(list(self.gateway.monitor)[-limit:])

    async def api_monitor_csv(self, request: web.Request) -> web.StreamResponse:
        hours = float(request.query.get("hours", 1))
        rows = self.history.query(
            "SELECT ts,line,bits,data,external,kind,target,text FROM monitor WHERE ts >= ? ORDER BY id",
            (time.time() - hours * 3600,),
        )
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter=";")
        writer.writerow(["time", "line", "bits", "data", "external", "kind", "target", "text"])
        for r in rows:
            writer.writerow([
                time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["ts"])) + f".{int((r['ts'] % 1) * 1000):03d}",
                r["line"], r["bits"], " ".join(f"{b:02X}" for b in json.loads(r["data"] or "[]")),
                r["external"], r["kind"], r["target"], r["text"],
            ])
        return web.Response(
            text=buf.getvalue(),
            content_type="text/csv",
            headers={"Content-Disposition": f'attachment; filename="dali-monitor-{int(time.time())}.csv"'},
        )

    async def api_events(self, request: web.Request) -> web.Response:
        hours = float(request.query.get("hours", 168))
        return web.json_response(
            self.history.events(time.time() - hours * 3600, int(request.query.get("limit", 500)), request.query.get("category"))
        )

    async def api_bus(self, request: web.Request) -> web.Response:
        line = request.query.get("line")
        return web.json_response(
            analytics.bus_series(self.history, float(request.query.get("hours", 24)), int(line) if line else None)
        )

    async def api_quality(self, request: web.Request) -> web.Response:
        assert self.gateway
        return web.json_response(analytics.quality(self.history, int(request.query.get("days", 7)), self.gateway.devices))

    async def api_devices_summary(self, request: web.Request) -> web.Response:
        assert self.gateway
        result = await asyncio.to_thread(
            analytics.device_summary, self.history, float(request.query.get("hours", 168)), self.gateway.devices
        )
        return web.json_response(result)

    async def api_device_series(self, request: web.Request) -> web.Response:
        return web.json_response(
            analytics.device_series(self.history, int(request.match_info["id"]), float(request.query.get("hours", 24)))
        )

    async def api_measurements(self, request: web.Request) -> web.Response:
        return web.json_response(
            analytics.measurements(
                self.history, int(request.match_info["id"]), request.query.get("source", "energy"),
                float(request.query.get("hours", 168)),
            )
        )

    async def api_sample_extras(self, request: web.Request) -> web.Response:
        assert self.gateway
        return web.json_response(await self.gateway.sample_extras())

    async def api_sensor_series(self, request: web.Request) -> web.Response:
        return web.json_response(
            analytics.sensor_series(self.history, int(request.match_info["id"]), float(request.query.get("hours", 24)))
        )

    async def api_health(self, request: web.Request) -> web.Response:
        assert self.gateway
        q = analytics.quality(self.history, 1, self.gateway.devices)
        return web.json_response(analytics.health(self.history, self.gateway.devices, q))

    async def api_inputs(self, request: web.Request) -> web.Response:
        return web.json_response(self.history.query("SELECT * FROM inputs ORDER BY line, short_address, key"))

    async def api_input_update(self, request: web.Request) -> web.Response:
        data = await request.json()
        self.history.execute("UPDATE inputs SET name=? WHERE key=?", (str(data.get("name", "")), request.match_info["key"]))
        return web.json_response({"ok": True})

    async def api_input_delete(self, request: web.Request) -> web.Response:
        self.history.execute("DELETE FROM inputs WHERE key=?", (request.match_info["key"],))
        return web.json_response({"ok": True})

    async def api_clear_history(self, request: web.Request) -> web.Response:
        what = request.query.get("what", "all")
        tables = {
            "monitor": ["monitor"],
            "bus": ["bus_minute", "address_quality"],
            "events": ["event_log"],
            "all": ["monitor", "bus_minute", "address_quality", "event_log", "device_state", "measurements", "sensor_values"],
        }.get(what, [])
        for t in tables:
            self.history.execute(f"DELETE FROM {t}")
        self.history.execute("VACUUM")
        return web.json_response({"ok": True, "cleared": tables})

    # ------------------------------------------------------------ frontend
    async def index(self, request: web.Request) -> web.StreamResponse:
        index = WWW_DIR / "index.html"
        if not index.exists():
            return web.Response(text="Frontend not built", status=500)
        return web.FileResponse(index, headers={"Cache-Control": "no-cache"})


@web.middleware
async def ingress_only(request: web.Request, handler):
    if not DEV_MODE and request.remote not in (INGRESS_IP, "127.0.0.1"):
        return web.Response(status=403, text="Access only via Home Assistant ingress")
    return await handler(request)


@web.middleware
async def errors(request: web.Request, handler):
    try:
        return await handler(request)
    except web.HTTPException:
        raise
    except Exception as err:  # noqa: BLE001
        _LOGGER.exception("Request %s failed", request.path)
        return web.json_response({"detail": str(err)}, status=500)


def build_app() -> web.Application:
    core = App()
    app = web.Application(middlewares=[ingress_only, errors], client_max_size=64 * 1024 * 1024)
    app.on_startup.append(core.startup)
    app.on_cleanup.append(core.cleanup)
    r = app.router
    r.add_get("/api/ws", core.ws_handler)
    r.add_get("/api/status", core.api_status)
    r.add_get("/api/state", core.api_state)
    r.add_post("/api/gateway", core.api_set_gateway)
    r.add_post("/api/discover", core.api_discover)
    r.add_post("/api/announce", core.api_announce)
    r.add_get("/api/monitor", core.api_monitor)
    r.add_get("/api/monitor.csv", core.api_monitor_csv)
    r.add_get("/api/events", core.api_events)
    r.add_get("/api/analytics/bus", core.api_bus)
    r.add_get("/api/analytics/quality", core.api_quality)
    r.add_get("/api/analytics/devices", core.api_devices_summary)
    r.add_get("/api/analytics/device/{id}", core.api_device_series)
    r.add_get("/api/analytics/measurements/{id}", core.api_measurements)
    r.add_post("/api/analytics/sample", core.api_sample_extras)
    r.add_get("/api/analytics/sensor/{id}", core.api_sensor_series)
    r.add_get("/api/analytics/health", core.api_health)
    r.add_get("/api/inputs", core.api_inputs)
    r.add_put("/api/inputs/{key}", core.api_input_update)
    r.add_delete("/api/inputs/{key}", core.api_input_delete)
    r.add_delete("/api/history", core.api_clear_history)
    r.add_route("*", "/api/gw/{path:.*}", core.proxy)
    r.add_get("/", core.index)
    if (WWW_DIR / "assets").exists():
        r.add_static("/assets", WWW_DIR / "assets", append_version=False)
    for extra in ("favicon.svg", "logo.svg"):
        if (WWW_DIR / extra).exists():
            r.add_get(f"/{extra}", lambda req, e=extra: web.FileResponse(WWW_DIR / e))
    return app


if __name__ == "__main__":
    web.run_app(build_app(), host="0.0.0.0", port=PORT, access_log=None)
