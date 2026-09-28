"""DALI frame decoder (IEC 62386-102 / -103 / -301 / -303 / -304).

Decodes the raw frames the Lunatone DALI-2 IoT gateway publishes on its
websocket as ``daliMonitor`` messages into human readable descriptions and
structured event information (push buttons, occupancy sensors, ...).

This module has no Home Assistant dependencies so that it can be shared with
the add-on (a copy lives in ``lunatone_dali_manager/app/dali_decode.py``).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

# --------------------------------------------------------------------------- #
# IEC 62386-102 – control gear commands (16 bit forward frames)
# --------------------------------------------------------------------------- #
GEAR_COMMANDS: dict[int, str] = {
    0: "OFF",
    1: "UP",
    2: "DOWN",
    3: "STEP UP",
    4: "STEP DOWN",
    5: "RECALL MAX LEVEL",
    6: "RECALL MIN LEVEL",
    7: "STEP DOWN AND OFF",
    8: "ON AND STEP UP",
    9: "ENABLE DAPC SEQUENCE",
    10: "GO TO LAST ACTIVE LEVEL",
    11: "CONTINUOUS UP",
    12: "CONTINUOUS DOWN",
    32: "RESET",
    33: "STORE ACTUAL LEVEL IN DTR0",
    34: "SAVE PERSISTENT VARIABLES",
    35: "SET OPERATING MODE (DTR0)",
    36: "RESET MEMORY BANK (DTR0)",
    37: "IDENTIFY DEVICE",
    42: "SET MAX LEVEL (DTR0)",
    43: "SET MIN LEVEL (DTR0)",
    44: "SET SYSTEM FAILURE LEVEL (DTR0)",
    45: "SET POWER ON LEVEL (DTR0)",
    46: "SET FADE TIME (DTR0)",
    47: "SET FADE RATE (DTR0)",
    48: "SET EXTENDED FADE TIME (DTR0)",
    128: "SET SHORT ADDRESS (DTR0)",
    129: "ENABLE WRITE MEMORY",
    144: "QUERY STATUS",
    145: "QUERY CONTROL GEAR PRESENT",
    146: "QUERY LAMP FAILURE",
    147: "QUERY LAMP POWER ON",
    148: "QUERY LIMIT ERROR",
    149: "QUERY RESET STATE",
    150: "QUERY MISSING SHORT ADDRESS",
    151: "QUERY VERSION NUMBER",
    152: "QUERY CONTENT DTR0",
    153: "QUERY DEVICE TYPE",
    154: "QUERY PHYSICAL MINIMUM",
    155: "QUERY POWER FAILURE",
    156: "QUERY CONTENT DTR1",
    157: "QUERY CONTENT DTR2",
    158: "QUERY OPERATING MODE",
    159: "QUERY LIGHT SOURCE TYPE",
    160: "QUERY ACTUAL LEVEL",
    161: "QUERY MAX LEVEL",
    162: "QUERY MIN LEVEL",
    163: "QUERY POWER ON LEVEL",
    164: "QUERY SYSTEM FAILURE LEVEL",
    165: "QUERY FADE TIME/FADE RATE",
    166: "QUERY MANUFACTURER SPECIFIC MODE",
    167: "QUERY NEXT DEVICE TYPE",
    168: "QUERY EXTENDED FADE TIME",
    170: "QUERY CONTROL GEAR FAILURE",
    192: "QUERY GROUPS 0-7",
    193: "QUERY GROUPS 8-15",
    194: "QUERY RANDOM ADDRESS (H)",
    195: "QUERY RANDOM ADDRESS (M)",
    196: "QUERY RANDOM ADDRESS (L)",
    197: "READ MEMORY LOCATION (DTR1, DTR0)",
}
for _n in range(16):
    GEAR_COMMANDS[16 + _n] = f"GO TO SCENE {_n}"
    GEAR_COMMANDS[64 + _n] = f"SET SCENE {_n} (DTR0)"
    GEAR_COMMANDS[80 + _n] = f"REMOVE FROM SCENE {_n}"
    GEAR_COMMANDS[96 + _n] = f"ADD TO GROUP {_n}"
    GEAR_COMMANDS[112 + _n] = f"REMOVE FROM GROUP {_n}"
    GEAR_COMMANDS[176 + _n] = f"QUERY SCENE LEVEL {_n}"

# Application extended commands (224..255) depend on the device type that was
# enabled with ENABLE DEVICE TYPE directly before.
EXTENDED_COMMANDS: dict[int, dict[int, str]] = {
    6: {  # IEC 62386-207 LED modules
        224: "REFERENCE SYSTEM POWER",
        225: "SELECT DIMMING CURVE (DTR0)",
        226: "SET FAST FADE TIME (DTR0)",
        237: "QUERY CONTROL GEAR TYPE",
        238: "QUERY DIMMING CURVE",
        240: "QUERY FEATURES",
        241: "QUERY FAILURE STATUS",
        242: "QUERY SHORT CIRCUIT",
        243: "QUERY OPEN CIRCUIT",
        244: "QUERY LOAD DECREASE",
        245: "QUERY LOAD INCREASE",
        246: "QUERY CURRENT PROTECTOR ACTIVE",
        247: "QUERY THERMAL SHUTDOWN",
        248: "QUERY THERMAL OVERLOAD",
        249: "QUERY REFERENCE RUNNING",
        250: "QUERY REFERENCE MEASUREMENT FAILED",
        251: "QUERY CURRENT PROTECTOR ENABLED",
        252: "QUERY OPERATING MODE",
        253: "QUERY FAST FADE TIME",
        254: "QUERY MIN FAST FADE TIME",
        255: "QUERY EXTENDED VERSION NUMBER",
    },
    8: {  # IEC 62386-209 colour control
        224: "SET TEMPORARY X-COORDINATE (DTR1:DTR0)",
        225: "SET TEMPORARY Y-COORDINATE (DTR1:DTR0)",
        226: "ACTIVATE",
        227: "X-COORDINATE STEP UP",
        228: "X-COORDINATE STEP DOWN",
        229: "Y-COORDINATE STEP UP",
        230: "Y-COORDINATE STEP DOWN",
        231: "SET TEMPORARY COLOUR TEMPERATURE (DTR1:DTR0)",
        232: "COLOUR TEMPERATURE STEP COOLER",
        233: "COLOUR TEMPERATURE STEP WARMER",
        234: "SET TEMPORARY PRIMARY N DIMLEVEL",
        235: "SET TEMPORARY RGB DIMLEVEL",
        236: "SET TEMPORARY WAF DIMLEVEL",
        237: "SET TEMPORARY RGBWAF CONTROL",
        238: "COPY REPORT TO TEMPORARY",
        240: "STORE TY PRIMARY N",
        241: "STORE XY-COORDINATE PRIMARY N",
        242: "STORE COLOUR TEMPERATURE LIMIT",
        243: "STORE GEAR FEATURES/STATUS",
        245: "ASSIGN COLOUR TO LINKED CHANNEL",
        246: "START AUTO CALIBRATION",
        247: "QUERY GEAR FEATURES/STATUS",
        248: "QUERY COLOUR STATUS",
        249: "QUERY COLOUR TYPE FEATURES",
        250: "QUERY COLOUR VALUE (DTR0)",
        251: "QUERY RGBWAF CONTROL",
        252: "QUERY ASSIGNED COLOUR",
        255: "QUERY EXTENDED VERSION NUMBER",
    },
}

SPECIAL_COMMANDS_16: dict[int, str] = {
    0xA1: "TERMINATE",
    0xA3: "DTR0",
    0xA5: "INITIALISE",
    0xA7: "RANDOMISE",
    0xA9: "COMPARE",
    0xAB: "WITHDRAW",
    0xAD: "PING",
    0xB1: "SEARCHADDRH",
    0xB3: "SEARCHADDRM",
    0xB5: "SEARCHADDRL",
    0xB7: "PROGRAM SHORT ADDRESS",
    0xB9: "VERIFY SHORT ADDRESS",
    0xBB: "QUERY SHORT ADDRESS",
    0xC1: "ENABLE DEVICE TYPE",
    0xC3: "DTR1",
    0xC5: "DTR2",
    0xC7: "WRITE MEMORY LOCATION",
    0xC9: "WRITE MEMORY LOCATION - NO REPLY",
}
SPECIAL_QUERIES_16 = {0xA9, 0xB9, 0xBB, 0xC7}

# --------------------------------------------------------------------------- #
# IEC 62386-103 – control device commands (24 bit forward frames)
# --------------------------------------------------------------------------- #
DEVICE_COMMANDS: dict[int, str] = {
    0x00: "IDENTIFY DEVICE",
    0x01: "RESET POWER CYCLE SEEN",
    0x10: "RESET",
    0x11: "RESET MEMORY BANK (DTR0)",
    0x14: "SET SHORT ADDRESS (DTR0)",
    0x15: "ENABLE WRITE MEMORY",
    0x16: "ENABLE APPLICATION CONTROLLER",
    0x17: "DISABLE APPLICATION CONTROLLER",
    0x18: "SET OPERATING MODE (DTR0)",
    0x19: "ADD TO DEVICE GROUPS 0-15",
    0x1A: "ADD TO DEVICE GROUPS 16-31",
    0x1B: "REMOVE FROM DEVICE GROUPS 0-15",
    0x1C: "REMOVE FROM DEVICE GROUPS 16-31",
    0x1D: "START QUIESCENT MODE",
    0x1E: "STOP QUIESCENT MODE",
    0x1F: "ENABLE POWER CYCLE NOTIFICATION",
    0x20: "DISABLE POWER CYCLE NOTIFICATION",
    0x21: "SAVE PERSISTENT VARIABLES",
    0x30: "QUERY DEVICE STATUS",
    0x31: "QUERY APPLICATION CONTROLLER ERROR",
    0x32: "QUERY INPUT DEVICE ERROR",
    0x33: "QUERY MISSING SHORT ADDRESS",
    0x34: "QUERY VERSION NUMBER",
    0x35: "QUERY NUMBER OF INSTANCES",
    0x36: "QUERY CONTENT DTR0",
    0x37: "QUERY CONTENT DTR1",
    0x38: "QUERY CONTENT DTR2",
    0x39: "QUERY RANDOM ADDRESS (H)",
    0x3A: "QUERY RANDOM ADDRESS (M)",
    0x3B: "QUERY RANDOM ADDRESS (L)",
    0x3C: "READ MEMORY LOCATION",
    0x3D: "QUERY APPLICATION CONTROL ENABLED",
    0x3E: "QUERY OPERATING MODE",
    0x3F: "QUERY MANUFACTURER SPECIFIC MODE",
    0x40: "QUERY QUIESCENT MODE",
    0x41: "QUERY DEVICE GROUPS 0-7",
    0x42: "QUERY DEVICE GROUPS 8-15",
    0x43: "QUERY DEVICE GROUPS 16-23",
    0x44: "QUERY DEVICE GROUPS 24-31",
    0x45: "QUERY POWER CYCLE NOTIFICATION",
    0x46: "QUERY DEVICE CAPABILITIES",
    0x47: "QUERY EXTENDED VERSION NUMBER",
    0x48: "QUERY RESET STATE",
    0x49: "QUERY APPLICATION CONTROLLER ALWAYS ACTIVE",
    0x61: "ENABLE INSTANCE",
    0x62: "DISABLE INSTANCE",
    0x63: "SET PRIMARY INSTANCE GROUP (DTR0)",
    0x64: "SET INSTANCE GROUP 1 (DTR0)",
    0x65: "SET INSTANCE GROUP 2 (DTR0)",
    0x66: "SET EVENT SCHEME (DTR0)",
    0x67: "SET EVENT FILTER (DTR2:DTR1:DTR0)",
    0x68: "SET EVENT PRIORITY (DTR0)",
    0x80: "QUERY INSTANCE TYPE",
    0x81: "QUERY RESOLUTION",
    0x82: "QUERY INSTANCE ERROR",
    0x83: "QUERY INSTANCE STATUS",
    0x84: "QUERY EVENT PRIORITY",
    0x86: "QUERY INSTANCE ENABLED",
    0x88: "QUERY PRIMARY INSTANCE GROUP",
    0x89: "QUERY INSTANCE GROUP 1",
    0x8A: "QUERY INSTANCE GROUP 2",
    0x8B: "QUERY EVENT SCHEME",
    0x8C: "QUERY INPUT VALUE",
    0x8D: "QUERY INPUT VALUE LATCH",
    0x8E: "QUERY FEATURE TYPE",
    0x8F: "QUERY NEXT FEATURE TYPE",
    0x90: "QUERY EVENT FILTER 0-7",
    0x91: "QUERY EVENT FILTER 8-15",
    0x92: "QUERY EVENT FILTER 16-23",
}

SPECIAL_COMMANDS_24: dict[int, str] = {
    0x00: "TERMINATE",
    0x01: "INITIALISE",
    0x02: "RANDOMISE",
    0x03: "COMPARE",
    0x04: "WITHDRAW",
    0x05: "SEARCHADDRH",
    0x06: "SEARCHADDRM",
    0x07: "SEARCHADDRL",
    0x08: "PROGRAM SHORT ADDRESS",
    0x09: "VERIFY SHORT ADDRESS",
    0x0A: "QUERY SHORT ADDRESS",
    0x20: "WRITE MEMORY LOCATION",
    0x21: "WRITE MEMORY LOCATION - NO REPLY",
    0x30: "DTR0",
    0x31: "DTR1",
    0x32: "DTR2",
    0x33: "SEND TESTFRAME",
}

# --------------------------------------------------------------------------- #
# Instance types (IEC 62386-3xx) and their event information
# --------------------------------------------------------------------------- #
INSTANCE_TYPES: dict[int, str] = {
    0: "generic",
    1: "push_button",
    2: "absolute_input",
    3: "occupancy_sensor",
    4: "light_sensor",
    5: "colour_sensor",
    6: "general_purpose_sensor",
}

# IEC 62386-301 push button events → HA friendly event names
BUTTON_EVENTS: dict[int, str] = {
    0b0000000000: "released",
    0b0000000001: "pressed",
    0b0000000010: "short_press",
    0b0000000101: "double_press",
    0b0000001001: "long_press_start",
    0b0000001011: "long_press_repeat",
    0b0000001100: "long_press_stop",
    0b0000001110: "button_free",
    0b0000001111: "button_stuck",
}
BUTTON_EVENT_TYPES: list[str] = list(BUTTON_EVENTS.values())

OCCUPANCY_EVENT_TYPES: list[str] = ["occupied", "vacant"]


@dataclass
class DaliEvent:
    """A decoded IEC 62386-103 event frame."""

    scheme: str  # instance | device | device_instance | device_group | instance_group | power_cycle
    line: int
    info: int
    short_address: int | None = None
    instance_type: int | None = None
    instance_number: int | None = None
    device_group: int | None = None
    instance_group: int | None = None
    event_type: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def source_key(self) -> str | None:
        """Stable key identifying the sending input instance."""
        if self.short_address is None:
            return None
        inst = (
            f"i{self.instance_number}"
            if self.instance_number is not None
            else f"t{self.instance_type}"
        )
        return f"{self.line}_{self.short_address}_{inst}"

    @property
    def kind(self) -> str:
        """Instance kind (push_button, occupancy_sensor, ...)."""
        if self.instance_type is None:
            return "unknown"
        return INSTANCE_TYPES.get(self.instance_type, f"type_{self.instance_type}")

    def as_dict(self) -> dict[str, Any]:
        return {
            "scheme": self.scheme,
            "line": self.line,
            "info": self.info,
            "short_address": self.short_address,
            "instance_type": self.instance_type,
            "instance_number": self.instance_number,
            "device_group": self.device_group,
            "instance_group": self.instance_group,
            "event_type": self.event_type,
            "kind": self.kind,
            "source_key": self.source_key,
            "attributes": self.attributes,
        }


def _classify_event(evt: DaliEvent) -> None:
    """Fill event_type/attributes from the 10 bit event information."""
    info = evt.info
    if evt.instance_type == 1:
        evt.event_type = BUTTON_EVENTS.get(info, f"unknown_{info}")
    elif evt.instance_type == 3:
        # IEC 62386-303: bit0 movement, bit1 occupied
        movement = bool(info & 0x01)
        occupied = bool(info & 0x02)
        evt.attributes = {"movement": movement, "occupied": occupied}
        evt.event_type = "occupied" if occupied else "vacant"
    elif evt.instance_type == 4:
        evt.event_type = "illuminance"
        evt.attributes = {"raw": info}
    elif evt.instance_type == 2:
        evt.event_type = "position"
        evt.attributes = {"raw": info}
    else:
        evt.event_type = "event"
        evt.attributes = {"raw": info}


def decode_event(line: int, data: list[int]) -> DaliEvent | None:
    """Decode a 24 bit frame as event, or return None if it is a command."""
    if len(data) != 3:
        return None
    frame = (data[0] << 16) | (data[1] << 8) | data[2]
    if frame & 0x010000:  # bit 16 set → command frame, not an event
        return None
    info = frame & 0x3FF
    b23 = bool(frame & 0x800000)
    b22 = bool(frame & 0x400000)
    b15 = bool(frame & 0x008000)
    field_5 = (frame >> 10) & 0x1F  # bits 14..10
    # Power cycle notification: 1111 1110 1110 0000 (0xFE, 0xE0|..)
    if data[0] == 0xFE:
        evt = DaliEvent("power_cycle", line, info)
        evt.event_type = "power_cycle"
        evt.attributes = {"raw": data}
        return evt
    if not b23:
        short = (frame >> 17) & 0x3F
        if not b15:
            evt = DaliEvent("instance", line, info, short_address=short, instance_type=field_5)
        else:
            evt = DaliEvent("device", line, info, short_address=short, instance_number=field_5)
    elif not b22:
        group = (frame >> 17) & 0x1F
        evt = DaliEvent("device_group", line, info, device_group=group, instance_type=field_5)
    else:
        group = (frame >> 17) & 0x1F
        evt = DaliEvent("instance_group", line, info, instance_group=group, instance_type=field_5)
    _classify_event(evt)
    return evt


def _address16(a: int) -> tuple[str, str]:
    """Return (kind, label) of a 16 bit address byte."""
    if a <= 0x7F:
        return "short", f"A{a >> 1}"
    if a <= 0x9F:
        return "group", f"G{(a >> 1) & 0x0F}"
    if a in (0xFE, 0xFF):
        return "broadcast", "BC"
    if a in (0xFC, 0xFD):
        return "broadcast_unaddressed", "BC-unaddr"
    return "special", ""


def _address24(a: int) -> str:
    if a in (0xFE, 0xFF):
        return "BC"
    if a in (0xFC, 0xFD):
        return "BC-unaddr"
    if not a & 0x80:
        return f"D{(a >> 1) & 0x3F}"
    if (a & 0xC0) == 0x80:
        return f"DG{(a >> 1) & 0x1F}"
    return f"0x{a:02X}"


def _instance24(i: int) -> str:
    if i == 0xFE:
        return "device"
    if i == 0xFF:
        return "all instances"
    if i <= 0x1F:
        return f"instance {i}"
    if (i & 0xE0) == 0x80:
        return f"instance type {i & 0x1F}"
    if (i & 0xE0) == 0xA0:
        return f"instance group {i & 0x1F}"
    if (i & 0xE0) == 0xC0:
        return f"feature on instance {i & 0x1F}"
    if (i & 0xE0) == 0x60:
        return f"feature instance type {i & 0x1F}"
    return f"0x{i:02X}"


def level_to_percent(level: int) -> float:
    """Linear DALI arc level to percent (as the Lunatone gateway does)."""
    return round(level * 100 / 254, 1)


class DaliDecoder:
    """Stateful decoder – pairs answers with queries, tracks ENABLE DEVICE TYPE."""

    def __init__(self) -> None:
        self._last_query: dict[int, str] = {}
        self._enabled_dt: dict[int, int | None] = {}

    def decode(self, msg: dict[str, Any]) -> dict[str, Any]:
        """Decode the ``data`` part of a ``daliMonitor`` websocket message."""
        line = int(msg.get("line", 0))
        bits = int(msg.get("bits", 0))
        data = list(msg.get("data") or [])
        out: dict[str, Any] = {
            "line": line,
            "bits": bits,
            "data": data,
            "hex": " ".join(f"{b:02X}" for b in data),
            "external": bool(msg.get("externalSource")),
            "query": bool(msg.get("query")),
            "timestamp": msg.get("timestamp"),
            "kind": "unknown",
            "target": "",
            "text": "",
        }
        try:
            if bits == 8 and data:
                out["kind"] = "backward"
                q = self._last_query.pop(line, None)
                val = data[0]
                out["text"] = f"Answer {val} (0x{val:02X}, 0b{val:08b})"
                if q:
                    out["text"] += f" ← {q}"
                    out["answer_to"] = q
            elif bits == 16 and len(data) == 2:
                self._decode16(line, data, out)
            elif bits == 24 and len(data) == 3:
                self._decode24(line, data, out)
            elif bits in (25, 32):
                out["kind"] = "event"
                out["text"] = "Frame " + out["hex"]
            else:
                out["text"] = f"{bits} bit frame {out['hex']}"
        except Exception as err:  # noqa: BLE001 – decoder must never crash
            out["text"] = f"Decode error: {err}"
        return out

    # ----------------------------------------------------------------- 16 bit
    def _decode16(self, line: int, data: list[int], out: dict[str, Any]) -> None:
        a, c = data
        kind, label = _address16(a)
        out["target"] = label
        if kind == "special" or (a & 0x01 and 0xA0 <= a <= 0xCB):
            out["kind"] = "special"
            name = SPECIAL_COMMANDS_16.get(a, f"SPECIAL 0x{a:02X}")
            if a == 0xC1:
                self._enabled_dt[line] = c
                name = f"ENABLE DEVICE TYPE {c}"
            elif a in (0xA3, 0xC3, 0xC5):
                name = f"{name} = {c}"
            else:
                name = f"{name} {c}"
            out["text"] = name
            if a in SPECIAL_QUERIES_16:
                self._last_query[line] = name
            return
        if not a & 0x01:  # DAPC
            out["kind"] = "dapc"
            if c == 0xFF:
                out["text"] = f"{label}: DAPC MASK (stop fading)"
            else:
                out["text"] = f"{label}: DAPC {c} ({level_to_percent(c)} %)"
            self._enabled_dt.pop(line, None)
            return
        out["kind"] = "command"
        dt = self._enabled_dt.pop(line, None)
        if c >= 224:
            name = EXTENDED_COMMANDS.get(dt or -1, {}).get(c, f"EXTENDED COMMAND {c} (DT{dt})")
        else:
            name = GEAR_COMMANDS.get(c, f"COMMAND {c}")
        if name.startswith("QUERY"):
            out["kind"] = "query"
            self._last_query[line] = f"{label}: {name}"
        out["text"] = f"{label}: {name}"

    # ----------------------------------------------------------------- 24 bit
    def _decode24(self, line: int, data: list[int], out: dict[str, Any]) -> None:
        evt = decode_event(line, data)
        if evt is not None:
            out["kind"] = "event"
            out["event"] = evt.as_dict()
            src = (
                f"D{evt.short_address}"
                if evt.short_address is not None
                else f"DG{evt.device_group}"
                if evt.device_group is not None
                else f"IG{evt.instance_group}"
                if evt.instance_group is not None
                else ""
            )
            inst = (
                f" inst#{evt.instance_number}"
                if evt.instance_number is not None
                else f" {evt.kind}"
            )
            out["target"] = src
            out["text"] = f"EVENT {src}{inst}: {evt.event_type} (info {evt.info})"
            return
        a, i, c = data
        if a == 0xC1:
            out["kind"] = "special"
            name = SPECIAL_COMMANDS_24.get(i, f"SPECIAL 0x{i:02X}")
            out["text"] = f"{name} {c}"
            if i in (0x03, 0x09, 0x0A):
                self._last_query[line] = name
            return
        label = _address24(a)
        out["target"] = label
        name = DEVICE_COMMANDS.get(c, f"COMMAND 0x{c:02X}")
        out["kind"] = "query" if name.startswith("QUERY") else "command"
        text = f"{label} [{_instance24(i)}]: {name}"
        if out["kind"] == "query":
            self._last_query[line] = text
        out["text"] = text
