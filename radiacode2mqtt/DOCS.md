# radiacode2mqtt add-on documentation

This document covers configuration, MQTT payload formats, permissions, and troubleshooting.

---

## How it works

The add-on:
1. Connects to the Radiacode via **USB** (default) or **Bluetooth LE** (when `radiacode_mac` is set).
2. Polls realtime data from the device periodically and publishes a JSON message to MQTT.
3. Optionally publishes:
   - “rare” device info (temperature, charge, total dose, etc.) when available
   - spectrum payloads at a slower interval
4. Optionally publishes MQTT Discovery config topics so Home Assistant creates sensors automatically.

Only **one device** is supported at a time.

---

## Add-on configuration

### Full example

```yaml
radiacode_mac: ""          # empty => USB; set BLE MAC address for BLE mode
radiacode_serial: ""       # optional; generally not needed for single-device setups
poll_interval_s: 5
first_data_timeout_s: 60

# Logging / health
debug: false
watchdog_s: 30
status_publish_every_s: 30

dose:
  system: Sv               # Sv or R
  prefix: micro            # whole|deci|centi|milli|micro|nano

spectrum:
  enabled: true
  interval_s: 120
  retain: false

mqtt:
  host: core-mosquitto
  port: 1883
  username: ""
  password: ""
  topic_prefix: radiacode
  discovery_prefix: homeassistant
  discovery: true
