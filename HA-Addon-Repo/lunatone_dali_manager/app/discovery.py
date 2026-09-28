"""Supervisor / Home Assistant helpers and network discovery of gateways."""

from __future__ import annotations

import asyncio
import ipaddress
import logging
import os
from typing import Any

import aiohttp

_LOGGER = logging.getLogger(__name__)

SUPERVISOR = "http://supervisor"
TOKEN = os.environ.get("SUPERVISOR_TOKEN")


def _headers() -> dict[str, str]:
    return {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}


async def supervisor_get(session: aiohttp.ClientSession, path: str) -> Any:
    if not TOKEN:
        return None
    try:
        async with session.get(f"{SUPERVISOR}{path}", headers=_headers(), timeout=aiohttp.ClientTimeout(total=10)) as r:
            if r.status != 200:
                return None
            data = await r.json(content_type=None)
            return data.get("data", data) if isinstance(data, dict) else data
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("Supervisor request %s failed: %s", path, err)
        return None


async def ha_config(session: aiohttp.ClientSession) -> dict[str, Any]:
    """Home Assistant core config (language, location, time zone)."""
    if not TOKEN:
        return {}
    try:
        async with session.get(
            f"{SUPERVISOR}/core/api/config", headers=_headers(), timeout=aiohttp.ClientTimeout(total=10)
        ) as r:
            if r.status == 200:
                data = await r.json(content_type=None)
                return {
                    k: data.get(k)
                    for k in ("language", "latitude", "longitude", "time_zone", "location_name", "version", "country")
                }
    except Exception as err:  # noqa: BLE001
        _LOGGER.debug("HA config request failed: %s", err)
    return {}


async def announce(session: aiohttp.ClientSession, host: str) -> bool:
    """Tell Home Assistant about the gateway → integration shows up as discovered."""
    if not TOKEN:
        return False
    try:
        async with session.post(
            f"{SUPERVISOR}/discovery",
            headers=_headers(),
            json={"service": "lunatone_dali", "config": {"host": host}},
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            ok = r.status == 200
            _LOGGER.info("Discovery announcement for %s: %s", host, "ok" if ok else r.status)
            return ok
    except Exception as err:  # noqa: BLE001
        _LOGGER.warning("Discovery announcement failed: %s", err)
        return False


async def _local_networks(session: aiohttp.ClientSession) -> list[ipaddress.IPv4Network]:
    nets: list[ipaddress.IPv4Network] = []
    info = await supervisor_get(session, "/network/info")
    if isinstance(info, dict):
        for iface in info.get("interfaces", []):
            ipv4 = iface.get("ipv4") or {}
            for addr in ipv4.get("address") or []:
                try:
                    iface_net = ipaddress.ip_interface(addr)
                except ValueError:
                    continue
                if iface_net.ip.is_loopback or iface_net.ip.is_link_local:
                    continue
                prefix = max(iface_net.network.prefixlen, 24)
                nets.append(ipaddress.ip_network(f"{iface_net.ip}/{prefix}", strict=False))
    extra = os.environ.get("SCAN_NETWORKS")
    if extra:
        for net in extra.split(","):
            try:
                nets.append(ipaddress.ip_network(net.strip(), strict=False))
            except ValueError:
                pass
    return nets


async def scan_network(session: aiohttp.ClientSession) -> list[dict[str, Any]]:
    """Probe local /24 networks for DALI-2 IoT gateways."""
    nets = await _local_networks(session)
    hosts = {str(h) for net in nets for h in net.hosts()}
    sem = asyncio.Semaphore(64)
    found: list[dict[str, Any]] = []

    async def check(host: str) -> None:
        async with sem:
            try:
                async with session.get(f"http://{host}/info", timeout=aiohttp.ClientTimeout(total=2)) as r:
                    if r.status != 200:
                        return
                    info = await r.json(content_type=None)
            except Exception:  # noqa: BLE001
                return
            if isinstance(info, dict) and "uid" in info and "descriptor" in info:
                found.append({"host": host, "name": info.get("name"), "version": info.get("version"), "uid": info.get("uid")})

    await asyncio.gather(*(check(h) for h in hosts))
    return sorted(found, key=lambda x: x["host"])
