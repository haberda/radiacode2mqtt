#!/usr/bin/env python3
"""
radiacode2mqtt.py (publisher-only) with robust BLE recovery (NO addrType patch).

Why this version:
- MQTT connects first so status is always visible even if BLE connect hangs.
- Optional BLE scan preflight to avoid connect attempts when device isn't advertising.
- Hard connect timeout so bluepy/radiacode connect cannot stall forever.
- Recovery:
  1) Detect stale realtime stream (no RealTimeData for watchdog_s)
  2) Close/disconnect best-effort
  3) Kill bluepy-helper processes (best-effort)
  4) Reconnect with exponential backoff
  5) If recovery fails repeatedly, exit(1) so HA restarts the add-on

Topics (base = <topic_prefix>/<device_id>):
- <base>/state            (NOT retained)
- <base>/availability     ("online"/"offline"; retained)
- <base>/status           (JSON status/errors; NOT retained)
- <base>/heartbeat        (ts only; NOT retained)
- <base>/raw_fields       (debug snapshot; retained; only when debug enabled)
- <base>/spectrum         (optional; retain configurable)
"""

from __future__ import annotations

import json
import logging
import signal
import subprocess
import sys
import time
import traceback
from dataclasses import dataclass
from typing import Any, Optional, Tuple

import paho.mqtt.client as mqtt
from radiacode import RadiaCode, RealTimeData, RareData


# ---------------------------
# Units / scaling
# ---------------------------

PREFIX_FACTOR = {
    "whole": 1.0,
    "deci": 10.0,
    "centi": 100.0,
    "milli": 1_000.0,
    "micro": 1_000_000.0,
    "nano": 1_000_000_000.0,
}
PREFIX_SYMBOL = {
    "whole": "",
    "deci": "d",
    "centi": "c",
    "milli": "m",
    "micro": "µ",
    "nano": "n",
}


# ---------------------------
# Config
# ---------------------------

@dataclass
class MqttConfig:
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    topic_prefix: str
    discovery_prefix: str
    discovery: bool


def load_options() -> dict[str, Any]:
    with open("/data/options.json", "r", encoding="utf-8") as f:
        return json.load(f)


def parse_mqtt_cfg(opts: dict[str, Any]) -> MqttConfig:
    m = opts.get("mqtt", {}) or {}
    return MqttConfig(
        host=m.get("host", "core-mosquitto"),
        port=int(m.get("port", 1883)),
        username=(m.get("username") or None),
        password=(m.get("password") or None),
        topic_prefix=m.get("topic_prefix", "radiacode"),
        discovery_prefix=m.get("discovery_prefix", "homeassistant"),
        discovery=bool(m.get("discovery", True)),
    )


def get_system_and_prefix(opts: dict[str, Any]) -> tuple[str, str]:
    d = opts.get("dose") or opts.get("dose_rate") or {}
    system = d.get("system", "Sv")
    prefix = d.get("prefix", "micro")
    if system not in ("Sv", "R"):
        system = "Sv"
    if prefix not in PREFIX_FACTOR:
        prefix = "micro"
    return system, prefix


def get_rate_unit_and_factor(opts: dict[str, Any]) -> Tuple[str, float]:
    system, prefix = get_system_and_prefix(opts)
    factor = float(PREFIX_FACTOR[prefix])
    unit = f"{PREFIX_SYMBOL[prefix]}{system}/h"
    return unit, factor


def get_dose_unit_and_factor(opts: dict[str, Any]) -> Tuple[str, float]:
    system, prefix = get_system_and_prefix(opts)
    factor = float(PREFIX_FACTOR[prefix])
    unit = f"{PREFIX_SYMBOL[prefix]}{system}"
    return unit, factor


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def get_ble_connect_timeout(opts: dict[str, Any]) -> int:
    try:
        return int(opts.get("ble_connect_timeout_s", 20))
    except Exception:
        return 20


def get_ble_scan_enabled(opts: dict[str, Any]) -> bool:
    return bool(opts.get("ble_scan_enabled", True))


def get_ble_scan_seconds(opts: dict[str, Any]) -> float:
    try:
        return float(opts.get("ble_scan_seconds", 5))
    except Exception:
        return 5.0


# ---------------------------
# Logging
# ---------------------------

def setup_logging(debug: bool) -> logging.Logger:
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        stream=sys.stdout,
    )
    log = logging.getLogger("radiacode2mqtt")
    log.setLevel(level)
    return log


# ---------------------------
# MQTT helpers
# ---------------------------

def mqtt_make_client(cfg: MqttConfig, client_id: str) -> mqtt.Client:
    client = mqtt.Client(client_id=client_id, callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    if cfg.username and cfg.password:
        client.username_pw_set(cfg.username, cfg.password)
    return client


def mqtt_connect_and_loop(client: mqtt.Client, cfg: MqttConfig, log: logging.Logger, will_topic: str) -> dict[str, Any]:
    mqtt_state: dict[str, Any] = {"connected": False, "last_connect_ts": None, "last_disconnect_ts": None}

    client.will_set(will_topic, payload="offline", qos=0, retain=True)

    def on_connect(_client, _userdata, _flags, reason_code, _props=None):
        mqtt_state["connected"] = True
        mqtt_state["last_connect_ts"] = int(time.time())
        log.info("MQTT connected to %s:%s (reason_code=%s)", cfg.host, cfg.port, reason_code)

    def on_disconnect(_client, _userdata, reason_code, _props=None):
        mqtt_state["connected"] = False
        mqtt_state["last_disconnect_ts"] = int(time.time())
        if reason_code == 0:
            log.info("MQTT disconnected cleanly")
        else:
            log.warning("MQTT disconnected (reason_code=%s)", reason_code)

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    log.info("Connecting to MQTT broker %s:%s", cfg.host, cfg.port)
    client.connect(cfg.host, cfg.port, keepalive=60)
    client.loop_start()
    return mqtt_state


# ---------------------------
# HA Discovery
# ---------------------------

def publish_discovery(client: mqtt.Client, cfg: MqttConfig, opts: dict[str, Any], device_id: str, log: logging.Logger) -> None:
    base = f"{cfg.topic_prefix}/{device_id}"
    state_topic = f"{base}/state"
    avail_topic = f"{base}/availability"

    rate_unit, _ = get_rate_unit_and_factor(opts)
    dose_unit, _ = get_dose_unit_and_factor(opts)

    device_block = {
        "identifiers": [device_id],
        "name": "Radiacode",
        "manufacturer": "Radiacode",
    }

    def pub_sensor(
        object_id: str,
        name: str,
        value_template: str,
        unit: Optional[str] = None,
        device_class: Optional[str] = None,
    ) -> None:
        payload: dict[str, Any] = {
            "name": name,
            "unique_id": f"{device_id}_{object_id}",
            "state_topic": state_topic,
            "value_template": value_template,
            "availability_topic": avail_topic,
            "device": device_block,
        }
        if unit is not None:
            payload["unit_of_measurement"] = unit
        if device_class is not None:
            payload["device_class"] = device_class

        topic = f"{cfg.discovery_prefix}/sensor/{device_id}/{object_id}/config"
        client.publish(topic, json_dumps(payload), retain=True)

    pub_sensor("dose_rate", "Radiacode Dose Rate", "{{ value_json.dose_rate }}", rate_unit)
    pub_sensor("cps", "Radiacode CPS", "{{ value_json.cps }}", "cps")
    pub_sensor("count_rate_err", "Radiacode CPS Error", "{{ value_json.count_rate_err }}", "%")
    pub_sensor("dose_rate_err", "Radiacode Dose Rate Error", "{{ value_json.dose_rate_err }}", "%")
    pub_sensor("flags", "Radiacode Flags", "{{ value_json.flags }}")
    pub_sensor("real_time_flags", "Radiacode Real-Time Flags", "{{ value_json.real_time_flags }}")

    pub_sensor("temperature_c", "Radiacode Temperature", "{{ value_json.temperature_c }}", "°C", device_class="temperature")
    pub_sensor("battery_pct", "Radiacode Battery", "{{ value_json.battery_pct }}", "%", device_class="battery")
    pub_sensor("spectrum_duration_s", "Radiacode Spectrum Duration", "{{ value_json.spectrum_duration_s }}", "s", device_class="duration")
    pub_sensor("dose_total", "Radiacode Total Dose", "{{ value_json.dose_total }}", dose_unit)

    pub_sensor("last_seen_age_s", "Radiacode Last Seen Age", "{{ value_json.last_seen_age_s }}", "s", device_class="duration")
    pub_sensor("device_status", "Radiacode Device Status", "{{ value_json.device_status }}")
    pub_sensor("mqtt_connected", "Radiacode MQTT Connected", "{{ value_json.mqtt_connected }}")

    log.info("Published MQTT Discovery entities (device_id=%s)", device_id)


# ---------------------------
# Radiacode helpers
# ---------------------------

def safe_close_device(dev: Any, log: logging.Logger) -> None:
    for name in ("close", "disconnect", "stop"):
        fn = getattr(dev, name, None)
        if callable(fn):
            try:
                fn()
                log.debug("Called device.%s()", name)
            except Exception as e:
                log.debug("device.%s() failed: %s", name, e)


def kill_bluepy_helper(log: logging.Logger) -> None:
    """Best-effort kill of bluepy-helper processes in the container."""
    try:
        res = subprocess.run(["pkill", "-f", "bluepy-helper"], capture_output=True, text=True)
        if res.returncode == 0:
            log.warning("Killed bluepy-helper via pkill -f bluepy-helper")
    except Exception:
        pass

    try:
        res = subprocess.run(["killall", "bluepy-helper"], capture_output=True, text=True)
        if res.returncode == 0:
            log.warning("Killed bluepy-helper via killall bluepy-helper")
    except Exception:
        pass


class _ConnectTimeout(Exception):
    pass


def _alarm_handler(_signum, _frame):
    raise _ConnectTimeout("connect timeout")


def compute_device_id_from_opts(opts: dict[str, Any]) -> tuple[str, str]:
    mac = (opts.get("radiacode_mac") or "").strip()
    if mac:
        return mac.lower().replace(":", ""), "ble"
    return "radiacode_usb", "usb"


def ble_scan_for_mac(target_mac: str, scan_s: float, log: logging.Logger) -> bool:
    """Return True if target_mac is seen during scan window."""
    try:
        from bluepy.btle import Scanner  # type: ignore
        target = target_mac.strip().lower()
        log.debug("BLE scan preflight: scanning %.1fs for %s", scan_s, target)
        devs = Scanner().scan(scan_s)
        for d in devs:
            if d.addr.lower() == target:
                log.info("BLE scan preflight: saw target %s (rssi=%s)", d.addr, d.rssi)
                return True
        log.warning("BLE scan preflight: did NOT see target %s", target)
        return False
    except Exception as e:
        log.warning("BLE scan preflight failed: %s", e, exc_info=True)
        return False


def make_device(opts: dict[str, Any], log: logging.Logger) -> tuple[RadiaCode, str, str]:
    mac = (opts.get("radiacode_mac") or "").strip()

    if mac:
        timeout_s = get_ble_connect_timeout(opts)
        log.info("Connecting to Radiacode via BLE (mac=%s timeout=%ss)", mac, timeout_s)

        old_handler = signal.signal(signal.SIGALRM, _alarm_handler)
        signal.alarm(timeout_s)
        try:
            dev = RadiaCode(bluetooth_mac=mac)
        finally:
            signal.alarm(0)
            signal.signal(signal.SIGALRM, old_handler)

        return dev, mac.lower().replace(":", ""), "ble"

    log.info("Connecting to Radiacode via USB (auto-detect)")
    dev = RadiaCode()
    return dev, "radiacode_usb", "usb"


def pick_first(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


def get_latest_records(device: RadiaCode) -> tuple[Optional[RealTimeData], Optional[RareData], dict[str, int]]:
    buf = device.data_buf()
    rt: Optional[RealTimeData] = None
    rare: Optional[RareData] = None
    types: dict[str, int] = {}
    for rec in buf:
        t = type(rec).__name__
        types[t] = types.get(t, 0) + 1
        if isinstance(rec, RealTimeData):
            rt = rec
        elif isinstance(rec, RareData):
            rare = rec
    return rt, rare, types


# ---------------------------
# Main loop with HARD BLE recovery
# ---------------------------

def main() -> None:
    opts = load_options()
    debug = bool(opts.get("debug", False))
    log = setup_logging(debug)
    cfg = parse_mqtt_cfg(opts)

    poll_s = int(opts.get("poll_interval_s", 5))
    first_data_timeout_s = float(opts.get("first_data_timeout_s", 60))
    watchdog_s = float(opts.get("watchdog_s", 30))
    status_publish_every_s = float(opts.get("status_publish_every_s", 30))

    spectrum_cfg = (opts.get("spectrum") or {})
    spectrum_enabled = bool(spectrum_cfg.get("enabled", False))
    spectrum_interval_s = int(spectrum_cfg.get("interval_s", 120))
    spectrum_retain = bool(spectrum_cfg.get("retain", False))

    # BLE recovery tuning
    ble_max_recoveries_before_exit = int(opts.get("ble_max_recoveries_before_exit", 8))
    ble_backoff_s = float(opts.get("ble_backoff_s", 2.0))
    ble_backoff_max_s = float(opts.get("ble_backoff_max_s", 60.0))

    ble_scan_enabled = get_ble_scan_enabled(opts)
    ble_scan_s = get_ble_scan_seconds(opts)
    ble_connect_timeout_s = get_ble_connect_timeout(opts)

    system, prefix = get_system_and_prefix(opts)
    rate_unit, _ = get_rate_unit_and_factor(opts)
    dose_unit, _ = get_dose_unit_and_factor(opts)

    log.info("Starting radiacode2mqtt (publisher-only)")
    log.info("Config: poll_interval_s=%s first_data_timeout_s=%s watchdog_s=%s debug=%s", poll_s, first_data_timeout_s, watchdog_s, debug)
    log.info("Dose config: system=%s prefix=%s (rate_unit=%s dose_unit=%s)", system, prefix, rate_unit, dose_unit)
    log.info("Spectrum: enabled=%s interval_s=%s retain=%s", spectrum_enabled, spectrum_interval_s, spectrum_retain)
    log.info("MQTT: host=%s port=%s topic_prefix=%s discovery_prefix=%s discovery=%s",
             cfg.host, cfg.port, cfg.topic_prefix, cfg.discovery_prefix, cfg.discovery)
    log.info("BLE: scan_enabled=%s scan_seconds=%s connect_timeout_s=%s",
             ble_scan_enabled, ble_scan_s, ble_connect_timeout_s)
    log.info("BLE recovery: max_recoveries_before_exit=%s backoff_s=%s backoff_max_s=%s",
             ble_max_recoveries_before_exit, ble_backoff_s, ble_backoff_max_s)

    # Compute device_id without connecting so MQTT is always available
    device_id, mode_guess = compute_device_id_from_opts(opts)

    base = f"{cfg.topic_prefix}/{device_id}"
    state_topic = f"{base}/state"
    status_topic = f"{base}/status"
    avail_topic = f"{base}/availability"
    heartbeat_topic = f"{base}/heartbeat"
    raw_fields_topic = f"{base}/raw_fields"
    spectrum_topic = f"{base}/spectrum"

    # MQTT connect first
    mqttc = mqtt_make_client(cfg, client_id=f"radiacode2mqtt-{device_id}")
    mqtt_state = mqtt_connect_and_loop(mqttc, cfg, log, will_topic=avail_topic)

    if cfg.discovery:
        try:
            publish_discovery(mqttc, cfg, opts, device_id, log)
        except Exception as e:
            log.error("Failed to publish MQTT discovery: %s", e, exc_info=True)

    mqttc.publish(avail_topic, "online", retain=True)
    mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "started", "mode_guess": mode_guess}), retain=False)

    # Connect device with retry/backoff
    device: Optional[RadiaCode] = None
    device_mode: str = mode_guess

    connect_attempt = 0
    connect_backoff = ble_backoff_s

    while device is None:
        connect_attempt += 1
        mac = (opts.get("radiacode_mac") or "").strip()

        if mode_guess == "ble" and ble_scan_enabled and mac:
            seen = ble_scan_for_mac(mac, ble_scan_s, log)
            mqttc.publish(status_topic, json_dumps({
                "ts": int(time.time()),
                "status": "ble_scan",
                "attempt": connect_attempt,
                "target_mac": mac.lower(),
                "seen": seen,
            }), retain=False)
            if not seen:
                time.sleep(connect_backoff)
                connect_backoff = min(ble_backoff_max_s, connect_backoff * 2.0)
                continue

        try:
            mqttc.publish(status_topic, json_dumps({
                "ts": int(time.time()),
                "status": "connecting_device",
                "attempt": connect_attempt,
                "mode_guess": mode_guess,
            }), retain=False)

            device, _device_id2, device_mode = make_device(opts, log)
            mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "device_connected", "mode": device_mode}), retain=False)
            break

        except _ConnectTimeout as e:
            log.error("Device connect timed out: %s", e)
            mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "device_connect_timeout", "error": str(e)}), retain=False)
            kill_bluepy_helper(log)

        except Exception as e:
            log.error("Device connect failed: %s", e, exc_info=True)
            mqttc.publish(status_topic, json_dumps({
                "ts": int(time.time()),
                "status": "device_connect_failed",
                "error": str(e),
                "traceback_tail": traceback.format_exc().splitlines()[-10:],
            }), retain=False)
            kill_bluepy_helper(log)

        if mode_guess == "ble":
            time.sleep(connect_backoff)
            connect_backoff = min(ble_backoff_max_s, connect_backoff * 2.0)
            if connect_attempt >= ble_max_recoveries_before_exit:
                mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "exiting_for_restart", "attempts": connect_attempt}), retain=False)
                time.sleep(1.0)
                raise SystemExit(1)
        else:
            time.sleep(3)

    assert device is not None

    # Tracking
    first_data_deadline = time.time() + first_data_timeout_s
    last_seen_ts: Optional[float] = None
    last_status_publish_ts: float = 0.0
    logged_waiting = False
    device_status = "waiting"
    last_error: Optional[str] = None

    cached_temperature_c: Optional[float] = None
    cached_battery_pct: Optional[float] = None
    cached_spectrum_duration_s: Optional[int] = None
    cached_dose_total: Optional[float] = None
    cached_rare_seen_ts: Optional[float] = None

    next_spectrum_at = time.time() + 3.0

    ble_recoveries = 0
    ble_next_allowed_reconnect_ts: float = 0.0
    current_backoff_s = ble_backoff_s

    log.info("Publishing topics base=%s (device_mode=%s)", base, device_mode)

    while True:
        now = time.time()
        mqttc.publish(heartbeat_topic, json_dumps({"ts": int(now)}), retain=False)

        # BLE watchdog recovery
        if device_mode == "ble" and last_seen_ts is not None and (now - last_seen_ts) >= watchdog_s:
            stale_for = now - last_seen_ts

            if now < ble_next_allowed_reconnect_ts:
                time.sleep(min(poll_s, max(0.1, ble_next_allowed_reconnect_ts - now)))
                continue

            ble_recoveries += 1
            log.warning("BLE stale for %.1fs -> recovery attempt %s/%s", stale_for, ble_recoveries, ble_max_recoveries_before_exit)
            mqttc.publish(status_topic, json_dumps({
                "ts": int(now),
                "status": "ble_stale",
                "stale_for_s": stale_for,
                "recovery_attempt": ble_recoveries,
                "backoff_s": current_backoff_s,
            }), retain=False)

            try:
                safe_close_device(device, log)
            except Exception:
                pass

            kill_bluepy_helper(log)
            time.sleep(1.5)

            try:
                device, _did, device_mode = make_device(opts, log)
                last_seen_ts = None
                logged_waiting = False
                device_status = "waiting"
                last_error = None

                current_backoff_s = ble_backoff_s
                ble_next_allowed_reconnect_ts = time.time() + 0.5

                log.warning("BLE recovery succeeded")
                mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "ble_recovered", "attempt": ble_recoveries}), retain=False)

            except Exception as e:
                last_error = str(e)
                device_status = "error"
                log.error("BLE recovery failed: %s", e, exc_info=True)
                mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "ble_recovery_failed", "error": last_error}), retain=False)

                current_backoff_s = min(ble_backoff_max_s, current_backoff_s * 2.0)
                ble_next_allowed_reconnect_ts = time.time() + current_backoff_s

                if ble_recoveries >= ble_max_recoveries_before_exit:
                    mqttc.publish(status_topic, json_dumps({"ts": int(time.time()), "status": "exiting_for_restart", "recovery_attempts": ble_recoveries}), retain=False)
                    time.sleep(1.0)
                    raise SystemExit(1)

            time.sleep(0.5)
            continue

        # Spectrum
        if spectrum_enabled and now >= next_spectrum_at:
            try:
                spec = device.spectrum()
                payload = {
                    "ts": int(time.time()),
                    "duration_s": getattr(spec, "duration", None).total_seconds() if getattr(spec, "duration", None) else None,
                    "a0": getattr(spec, "a0", None),
                    "a1": getattr(spec, "a1", None),
                    "a2": getattr(spec, "a2", None),
                    "counts": getattr(spec, "counts", None),
                }
                mqttc.publish(spectrum_topic, json_dumps(payload), qos=0, retain=spectrum_retain)
            except Exception as e:
                log.debug("Spectrum read failed (ignored): %s", e)
            next_spectrum_at = now + max(5, spectrum_interval_s)

        try:
            rt, rare, type_hist = get_latest_records(device)

            if rt is None:
                if last_seen_ts is None:
                    device_status = "waiting" if now < first_data_deadline else "stale"
                else:
                    device_status = "stale" if (now - last_seen_ts) >= watchdog_s else "waiting"

                if not logged_waiting:
                    log.warning("No RealTimeData (status=%s) buf_types=%s", device_status, type_hist)
                    logged_waiting = True

                if (now - last_status_publish_ts) >= status_publish_every_s:
                    mqttc.publish(status_topic, json_dumps({
                        "ts": int(now),
                        "status": "waiting_for_realtime_data" if now < first_data_deadline else "realtime_data_timeout",
                        "device_status": device_status,
                        "buf_types": type_hist,
                    }), retain=False)
                    last_status_publish_ts = now

                last_seen_age_s = None if last_seen_ts is None else max(0.0, now - last_seen_ts)
                mqttc.publish(state_topic, json_dumps({
                    "ts": int(now),
                    "device_status": device_status,
                    "last_seen_ts": None if last_seen_ts is None else int(last_seen_ts),
                    "last_seen_age_s": last_seen_age_s,
                    "mqtt_connected": bool(mqtt_state.get("connected", False)),
                    "device_mode": device_mode,
                    "last_error": last_error,
                    "temperature_c": cached_temperature_c,
                    "battery_pct": cached_battery_pct,
                    "spectrum_duration_s": cached_spectrum_duration_s,
                    "dose_total": cached_dose_total,
                    "rare_last_seen_age_s": None if cached_rare_seen_ts is None else max(0.0, now - cached_rare_seen_ts),
                }), qos=0, retain=False)

                time.sleep(poll_s)
                continue

            # Realtime OK
            last_seen_ts = now
            if logged_waiting:
                log.info("Realtime stream resumed")
                logged_waiting = False
            device_status = "ok"
            last_error = None

            if rare is not None:
                try:
                    cached_temperature_c = getattr(rare, "temperature", cached_temperature_c)
                    cached_battery_pct = getattr(rare, "charge_level", cached_battery_pct)
                    cached_spectrum_duration_s = getattr(rare, "duration", cached_spectrum_duration_s)
                    raw_dose_total = getattr(rare, "dose", None)
                    if raw_dose_total is not None:
                        _, dose_factor = get_dose_unit_and_factor(opts)
                        cached_dose_total = raw_dose_total * dose_factor
                    cached_rare_seen_ts = now
                except Exception as e:
                    log.debug("RareData cache failed: %s", e)

            cps = pick_first(getattr(rt, "cps", None), getattr(rt, "count_rate", None))
            count_rate_err = getattr(rt, "count_rate_err", None)
            raw_dose_rate = getattr(rt, "dose_rate", None)
            dose_rate_err = getattr(rt, "dose_rate_err", None)
            flags = getattr(rt, "flags", None)
            real_time_flags = getattr(rt, "real_time_flags", None)

            _, rate_factor = get_rate_unit_and_factor(opts)
            dose_rate = (raw_dose_rate * rate_factor) if raw_dose_rate is not None else None

            mqttc.publish(state_topic, json_dumps({
                "ts": int(now),
                "device_status": device_status,
                "last_seen_ts": int(last_seen_ts),
                "last_seen_age_s": 0.0,
                "mqtt_connected": bool(mqtt_state.get("connected", False)),
                "device_mode": device_mode,

                "dose_rate": dose_rate,
                "cps": cps,
                "count_rate_err": count_rate_err,
                "dose_rate_err": dose_rate_err,
                "flags": flags,
                "real_time_flags": real_time_flags,

                "temperature_c": cached_temperature_c,
                "battery_pct": cached_battery_pct,
                "spectrum_duration_s": cached_spectrum_duration_s,
                "dose_total": cached_dose_total,
                "dose_total_unit": get_dose_unit_and_factor(opts)[0],
                "rare_last_seen_age_s": None if cached_rare_seen_ts is None else max(0.0, now - cached_rare_seen_ts),

                "raw": {"dose_rate": raw_dose_rate, "count_rate": cps},
            }), qos=0, retain=False)

            if debug:
                mqttc.publish(raw_fields_topic, json_dumps({
                    "ts": int(now),
                    "buf_types": type_hist,
                    "realtime_fields": {
                        "count_rate": cps,
                        "count_rate_err": count_rate_err,
                        "dose_rate": raw_dose_rate,
                        "dose_rate_err": dose_rate_err,
                        "flags": flags,
                        "real_time_flags": real_time_flags,
                    },
                    "rare_fields": None if rare is None else {
                        "duration": getattr(rare, "duration", None),
                        "dose": getattr(rare, "dose", None),
                        "temperature": getattr(rare, "temperature", None),
                        "charge_level": getattr(rare, "charge_level", None),
                        "flags": getattr(rare, "flags", None),
                    },
                }), retain=True)

            if (now - last_status_publish_ts) >= status_publish_every_s:
                log.info("OK: mode=%s cps=%s dose_rate=%s %s", device_mode, cps, dose_rate, get_rate_unit_and_factor(opts)[0])
                mqttc.publish(status_topic, "ok", retain=False)
                last_status_publish_ts = now

        except SystemExit:
            raise
        except Exception as e:
            device_status = "error"
            last_error = str(e)
            log.error("Unhandled error in main loop: %s", e, exc_info=True)
            mqttc.publish(status_topic, json_dumps({
                "ts": int(time.time()),
                "status": "error",
                "error": str(e),
                "traceback_tail": traceback.format_exc().splitlines()[-12:],
            }), retain=False)
            time.sleep(2)

        time.sleep(poll_s)


if __name__ == "__main__":
    main()
