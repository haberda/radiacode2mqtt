from __future__ import annotations

import json
import logging
import signal
import asyncio
import multiprocessing
import re
import threading
import sys
import time
from dataclasses import dataclass, field
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
    # Buffered protocol values are R (dose) and R/h (rate).
    # Match the device convention: 100 R = 1 Sv; see UNIT_NOTES.md.
    factor = float(PREFIX_FACTOR[prefix]) * (0.01 if system == "Sv" else 1.0)
    unit = f"{PREFIX_SYMBOL[prefix]}{system}/h"
    return unit, factor


def get_dose_unit_and_factor(opts: dict[str, Any]) -> Tuple[str, float]:
    system, prefix = get_system_and_prefix(opts)
    # Buffered protocol values are R (dose) and R/h (rate).
    # Match the device convention: 100 R = 1 Sv; see UNIT_NOTES.md.
    factor = float(PREFIX_FACTOR[prefix]) * (0.01 if system == "Sv" else 1.0)
    unit = f"{PREFIX_SYMBOL[prefix]}{system}"
    return unit, factor


def json_dumps(obj: Any) -> str:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


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

class MqttBridge:
    """Paho owns network I/O; the application owns device health."""

    def __init__(self, cfg, opts, device_id, log):
        self.cfg, self.opts, self.device_id, self.log = cfg, opts, device_id, log
        self.base = f"{cfg.topic_prefix}/{device_id}"
        self.connected = threading.Event()
        self.refresh = threading.Event()
        self.client = mqtt.Client(client_id=f"radiacode2mqtt-{device_id}",
                                  callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        if cfg.username:
            self.client.username_pw_set(cfg.username, cfg.password)
        self.client.will_set(f"{self.base}/availability", "offline", qos=1, retain=True)
        self.client.reconnect_delay_set(min_delay=1, max_delay=60)
        self.client.max_queued_messages_set(32)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.enable_logger(log)

    def on_connect(self, client, userdata, flags, reason_code, properties):
        if reason_code.is_failure:
            self.connected.clear()
            self.log.error("MQTT connection rejected: %s", reason_code)
            return
        self.connected.set()
        self.refresh.set()
        client.subscribe(f"{self.cfg.discovery_prefix}/status", qos=1)
        self.log.info("MQTT connected")

    def on_disconnect(self, client, userdata, flags, reason_code, properties):
        self.connected.clear()
        self.log.info("MQTT disconnected: %s", reason_code)

    def on_message(self, client, userdata, message):
        if message.topic == f"{self.cfg.discovery_prefix}/status" and message.payload == b"online":
            self.refresh.set()

    def start(self):
        self.client.connect_async(self.cfg.host, self.cfg.port, keepalive=30)
        self.client.loop_start()

    def publish(self, suffix, payload, *, retain=False, qos=0):
        if not self.connected.is_set():
            return None  # Do not replay stale measurements after a broker outage.
        if not isinstance(payload, (str, bytes, bytearray)):
            payload = json_dumps(payload)
        info = self.client.publish(f"{self.base}/{suffix}", payload, qos=qos, retain=retain)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            self.log.warning("MQTT publish failed for %s: %s", suffix, info.rc)
        return info

    def sync(self, healthy):
        if self.connected.is_set() and self.refresh.is_set():
            self.refresh.clear()
            if self.cfg.discovery:
                publish_discovery(self.client, self.cfg, self.opts, self.device_id, self.log)
            self.publish("availability", "online", retain=True, qos=1)
            self.health(healthy)

    def health(self, healthy):
        self.publish("device_availability", "online" if healthy else "offline", retain=True, qos=1)

    def close(self):
        try:
            self.health(False)
            info = self.publish("availability", "offline", retain=True, qos=1)
            if info is not None:
                info.wait_for_publish(timeout=2)
        except (RuntimeError, ValueError) as error:
            self.log.debug("Offline delivery failed: %s", error)
        finally:
            self.client.disconnect()
            self.client.loop_stop()


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
            "availability": [{"topic": avail_topic}],
            "availability_mode": "all",
            "device": device_block,
        }
        diagnostics = {"flags", "real_time_flags", "last_seen_age_s", "device_status", "mqtt_connected", "count_rate_err", "dose_rate_err"}
        if object_id in diagnostics:
            payload["entity_category"] = "diagnostic"
        else:
            payload["availability"].append({"topic": f"{base}/device_availability"})
        if object_id == "dose_total":
            payload["state_class"] = "total_increasing"
        if unit is not None and object_id not in {"dose_total", "dose_duration_s", "spectrum_duration_s", "last_seen_age_s"}:
            payload["state_class"] = "measurement"
        if object_id not in diagnostics:
            payload["expire_after"] = int(opts.get("watchdog_s", 30)) + int(opts.get("poll_interval_s", 5))
        if unit is not None:
            payload["unit_of_measurement"] = unit
        if device_class is not None:
            payload["device_class"] = device_class

        topic = f"{cfg.discovery_prefix}/sensor/{device_id}/{object_id}/config"
        client.publish(topic, json_dumps(payload), qos=1, retain=True)

    pub_sensor("dose_rate", "Radiacode Dose Rate", "{{ value_json.dose_rate }}", rate_unit)
    pub_sensor("cps", "Radiacode CPS", "{{ value_json.cps }}", "cps")
    pub_sensor("count_rate_err", "Radiacode CPS Error", "{{ value_json.count_rate_err }}", "%")
    pub_sensor("dose_rate_err", "Radiacode Dose Rate Error", "{{ value_json.dose_rate_err }}", "%")
    pub_sensor("flags", "Radiacode Flags", "{{ value_json.flags }}")
    pub_sensor("real_time_flags", "Radiacode Real-Time Flags", "{{ value_json.real_time_flags }}")

    pub_sensor("temperature_c", "Radiacode Temperature", "{{ value_json.temperature_c }}", "°C", device_class="temperature")
    pub_sensor("battery_pct", "Radiacode Battery", "{{ value_json.battery_pct }}", "%", device_class="battery")
    pub_sensor("spectrum_duration_s", "Radiacode Spectrum Duration", "{{ value_json.spectrum_duration_s }}", "s", device_class="duration")
    pub_sensor("dose_duration_s", "Radiacode Dose Duration", "{{ value_json.dose_duration_s }}", "s", device_class="duration")
    pub_sensor("dose_total", "Radiacode Total Dose", "{{ value_json.dose_total }}", dose_unit)

    pub_sensor("last_seen_age_s", "Radiacode Last Seen Age", "{{ value_json.last_seen_age_s }}", "s", device_class="duration")
    pub_sensor("device_status", "Radiacode Device Status", "{{ value_json.device_status }}")
    pub_sensor("mqtt_connected", "Radiacode MQTT Connected", "{{ value_json.mqtt_connected }}")

    spectrum = opts.get("spectrum") or {}
    camera_topic = f"{cfg.discovery_prefix}/camera/{device_id}/spectrum/config"
    if spectrum.get("enabled", False) and spectrum.get("image_enabled", True):
        camera = {
            "name": "Radiacode Spectrum",
            "unique_id": f"{device_id}_spectrum_camera",
            "topic": f"{base}/spectrum/image",
            "encoding": "",
            "availability": [{"topic": avail_topic}, {"topic": f"{base}/device_availability"}],
            "availability_mode": "all",
            "device": device_block,
        }
        client.publish(camera_topic, json_dumps(camera), qos=1, retain=True)
    else:
        # Remove only this add-on's camera when its feature is switched off.
        client.publish(camera_topic, "", qos=1, retain=True)
        client.publish(f"{base}/spectrum/image", b"", qos=1, retain=True)

    log.info("Published MQTT Discovery entities (device_id=%s)", device_id)


# ---------------------------
# Radiacode helpers
# ---------------------------

def compute_device_id_from_opts(opts):
    mac = (opts.get("radiacode_mac") or "").strip()
    device_id = (opts.get("device_id") or "").strip()
    if device_id and not re.fullmatch(r"[A-Za-z0-9_-]+", device_id):
        raise ValueError("device_id must contain only letters, digits, underscores or hyphens")
    # Empty preserves existing Home Assistant unique IDs. Set device_id to a
    # serial-based name when adding another instance; see DOCS.md.
    return device_id or (mac.lower().replace(":", "") if mac else "radiacode_usb"), "ble" if mac else "usb"


def validate_options(opts):
    poll = float(opts.get("poll_interval_s", 5))
    watchdog = float(opts.get("watchdog_s", 30))
    if poll <= 0 or watchdog <= poll:
        raise ValueError("watchdog_s must be greater than poll_interval_s > 0")
    for key, default in (("first_data_timeout_s", 60), ("ble_connect_timeout_s", 20),
                         ("operation_timeout_s", 30), ("status_publish_every_s", 30),
                         ("max_recoveries", 8)):
        if float(opts.get(key, default)) <= 0:
            raise ValueError(f"{key} must be positive")
    if (opts.get("spectrum") or {}).get("image_scale", "log") not in {"linear", "log"}:
        raise ValueError("spectrum.image_scale must be linear or log")
    compute_device_id_from_opts(opts)
    cfg = parse_mqtt_cfg(opts)
    if not cfg.host or not 1 <= cfg.port <= 65535:
        raise ValueError("Invalid MQTT host or port")
    for prefix in (cfg.topic_prefix, cfg.discovery_prefix):
        if not prefix or any(char in prefix for char in ("#", "+", "\x00")):
            raise ValueError("MQTT prefixes must be nonempty and contain no wildcards")


def device_process(pipe, opts):
    """Isolate native USB and BLE calls so *all* operations can be bounded."""
    device = None
    try:
        mac = (opts.get("radiacode_mac") or "").strip()
        if mac and opts.get("ble_scan_enabled", True):
            from bleak import BleakScanner
            try:
                asyncio.run(BleakScanner.find_device_by_address(
                    mac, timeout=float(opts.get("ble_scan_seconds", 5))))
            except Exception:
                pass  # Optional preflight is advisory; always attempt connection.
        device = RadiaCode(bluetooth_mac=mac or None,
                           serial_number=opts.get("radiacode_serial") or None)
        pipe.send((True, device.serial_number()))
        while True:
            command = pipe.recv()
            if command == "close":
                break
            try:
                result = device.data_buf() if command == "records" else device.spectrum()
                pipe.send((True, result))
            except Exception as error:
                pipe.send((False, f"{type(error).__name__}: {error}"))
    except Exception as error:
        try:
            pipe.send((False, f"{type(error).__name__}: {error}"))
        except (BrokenPipeError, EOFError, OSError):
            pass
    finally:
        if device is not None:
            try:
                device.close()
            except Exception:
                pass
        pipe.close()


class DeviceWorker:
    def __init__(self, opts, stop):
        self.opts, self.stop = opts, stop
        context = multiprocessing.get_context("spawn")
        self.pipe, child = context.Pipe()
        self.process = context.Process(target=device_process, args=(child, opts), daemon=True)
        self.process.start()
        child.close()

    def receive(self, timeout):
        deadline = time.monotonic() + timeout
        while not self.stop.is_set():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError("Radiacode operation timed out")
            if self.pipe.poll(min(0.2, remaining)):
                ok, result = self.pipe.recv()
                if not ok:
                    raise ConnectionError(result)
                return result
        raise InterruptedError("Stopping")

    def connect(self):
        scan = (float(self.opts.get("ble_scan_seconds", 5))
                if self.opts.get("radiacode_mac") and self.opts.get("ble_scan_enabled", True) else 0)
        return self.receive(float(self.opts.get("ble_connect_timeout_s", 20)) + scan)

    def request(self, command):
        self.pipe.send(command)
        return self.receive(float(self.opts.get("operation_timeout_s", 30)))

    def close(self):
        try:
            if self.process.is_alive():
                try:
                    self.pipe.send("close")
                    self.process.join(timeout=2)
                except (BrokenPipeError, EOFError, OSError):
                    pass
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(timeout=2)
            if self.process.is_alive():
                self.process.kill()
                self.process.join(timeout=2)
        finally:
            self.pipe.close()


@dataclass
class Recovery:
    first_timeout: float
    watchdog: float
    max_attempts: int = 8
    connected_at: float = 0
    last_seen: Optional[float] = None
    healthy_since: Optional[float] = None
    attempts: int = 0

    def connected(self, now):
        self.connected_at = now
        self.last_seen = None
        self.healthy_since = None

    def observe(self, now):
        self.last_seen = now
        if self.healthy_since is None:
            self.healthy_since = now
        if now - self.healthy_since >= self.watchdog:
            self.attempts = 0

    def expired(self, now):
        return (now - self.connected_at >= self.first_timeout if self.last_seen is None
                else now - self.last_seen >= self.watchdog)

    def failed(self):
        self.healthy_since = None
        self.last_seen = None
        self.attempts += 1
        if self.attempts >= self.max_attempts:
            raise RuntimeError("Device recovery attempts exhausted")
        return min(60, 2 ** self.attempts)


@dataclass
class Measurements:
    opts: dict
    values: dict = field(default_factory=dict)
    raw: dict = field(default_factory=dict)
    rare_seen: Optional[float] = None

    def update(self, records, now):
        realtime = False
        types = {}
        for record in records:
            name = type(record).__name__
            types[name] = types.get(name, 0) + 1
            if isinstance(record, RareData):
                self.rare_seen = now
                self.raw["dose"] = record.dose
                self.values.update(temperature_c=record.temperature, battery_pct=record.charge_level,
                                   dose_duration_s=record.duration,
                                   dose_total=record.dose * get_dose_unit_and_factor(self.opts)[1])
            elif isinstance(record, RealTimeData):
                realtime = True
                self.raw.update(dose_rate=record.dose_rate, count_rate=record.count_rate)
                self.values.update(dose_rate=record.dose_rate * get_rate_unit_and_factor(self.opts)[1],
                                   cps=record.count_rate, count_rate_err=record.count_rate_err,
                                   dose_rate_err=record.dose_rate_err, flags=record.flags,
                                   real_time_flags=record.real_time_flags,
                                   measurement_time=record.dt.isoformat())
        return realtime, types

    def spectrum(self, spec, timestamp):
        duration = spec.duration.total_seconds()
        self.values["spectrum_duration_s"] = duration
        return dict(ts=timestamp, duration_s=duration, a0=spec.a0, a1=spec.a1, a2=spec.a2,
                    counts=spec.counts, channels=len(spec.counts))

    def payload(self, now, timestamp, status, recovery, mode, error=None):
        fields = {name: self.values.get(name) for name in (
            "dose_rate", "cps", "count_rate_err", "dose_rate_err", "flags", "real_time_flags",
            "temperature_c", "battery_pct", "spectrum_duration_s", "dose_duration_s", "dose_total")}
        fields.update(ts=timestamp, device_status=status, device_mode=mode, last_error=error,
                      last_seen_age_s=None if recovery.last_seen is None else max(0, now - recovery.last_seen),
                      rare_last_seen_age_s=None if self.rare_seen is None else max(0, now - self.rare_seen),
                      mqtt_connected=True, dose_rate_unit=get_rate_unit_and_factor(self.opts)[0],
                      dose_total_unit=get_dose_unit_and_factor(self.opts)[0],
                      measurement_time=self.values.get("measurement_time"),
                      raw=dict(self.raw, dose_rate_unit="R/h", dose_unit="R"))
        return fields


def run(opts, stop, log, bridge=None, worker_factory=DeviceWorker):
    validate_options(opts)
    device_id, mode = compute_device_id_from_opts(opts)
    bridge = bridge or MqttBridge(parse_mqtt_cfg(opts), opts, device_id, log)
    recovery = Recovery(float(opts.get("first_data_timeout_s", 60)),
                        float(opts.get("watchdog_s", 30)), int(opts.get("max_recoveries", 8)))
    measurements = Measurements(opts)
    poll = float(opts.get("poll_interval_s", 5))
    spectrum = opts.get("spectrum") or {}
    worker = None
    next_poll = next_spectrum = next_status = next_connect = 0.0
    status, error = "connecting", None
    bridge.start()
    try:
        while not stop.is_set():
            now = time.monotonic()
            healthy = status == "ok" and not recovery.expired(now)
            bridge.sync(healthy)
            if now >= next_status:
                bridge.publish("heartbeat", {"ts": int(time.time())})
                bridge.publish("status", {"ts": int(time.time()), "status": status, "error": error})
                bridge.publish("state", measurements.payload(now, int(time.time()), status, recovery, mode, error))
                log.info("Device status=%s mode=%s attempts=%s", status, mode, recovery.attempts)
                next_status = now + float(opts.get("status_publish_every_s", 30))
            if now < next_connect or (worker is not None and now < next_poll):
                stop.wait(0.2)
                continue
            try:
                if worker is None:
                    status = "connecting"
                    bridge.health(False)
                    worker = worker_factory(opts, stop)
                    serial = worker.connect()
                    log.info("Connected to Radiacode %s via %s", serial, mode)
                    recovery.connected(time.monotonic())
                    measurements = Measurements(opts)
                    status, error = "waiting", None
                    next_spectrum = time.monotonic() + 3
                records = worker.request("records")
                now = time.monotonic()
                seen, types = measurements.update(records, now)
                if seen:
                    recovery.observe(now)
                    status, error = "ok", None
                elif recovery.expired(now):
                    raise TimeoutError("No fresh realtime data within the configured deadline")
                bridge.health(status == "ok")
                if spectrum.get("enabled", False) and now >= next_spectrum:
                    try:
                        spec = worker.request("spectrum")
                        timestamp = int(time.time())
                        bridge.publish("spectrum", measurements.spectrum(spec, timestamp),
                                       retain=bool(spectrum.get("retain", False)))
                        if spectrum.get("image_enabled", True):
                            try:
                                from spectrum_plot import render_spectrum
                                png = render_spectrum(spec, timestamp, scale=spectrum.get("image_scale", "log"))
                                bridge.publish("spectrum/image", png, retain=True)
                            except Exception:
                                # Plotting must not interrupt device polling or raw spectrum publication.
                                log.exception("Spectrum image rendering failed")
                    except (TimeoutError, ConnectionError, EOFError, OSError):
                        raise  # Transport faults require a new worker.
                    next_spectrum = time.monotonic() + max(5, int(spectrum.get("interval_s", 120)))
                now = time.monotonic()
                bridge.publish("state", measurements.payload(now, int(time.time()), status, recovery, mode))
                if opts.get("debug", False):
                    bridge.publish("raw_fields", {"ts": int(time.time()), "buf_types": types,
                                                   "raw": measurements.raw}, retain=False)
                next_poll = now + poll
            except InterruptedError:
                break
            except Exception as exc:
                error, status = str(exc), "recovering"
                log.warning("Device operation failed: %s", error)
                bridge.health(False)
                bridge.publish("status", {"ts": int(time.time()), "status": status, "error": error})
                if worker is not None:
                    worker.close()
                    worker = None
                next_connect = time.monotonic() + recovery.failed()
    finally:
        if worker is not None:
            worker.close()
        bridge.close()


def main():
    opts = load_options()
    log = setup_logging(bool(opts.get("debug", False)))
    stop = threading.Event()
    def shutdown(signum, frame):
        stop.set()
    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    run(opts, stop, log)


if __name__ == "__main__":
    main()
