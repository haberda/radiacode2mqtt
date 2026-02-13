# radiacode2mqtt (Home Assistant Add-on)

Publish Radiacode readings (USB or Bluetooth LE) to an MQTT broker and optionally create Home Assistant sensors via MQTT Discovery.

This add-on is intended as a lightweight “publisher-only” bridge:
- Radiacode (USB or BLE) → `radiacode/<device_id>/state` payloads
- Optional Spectrum payload → `radiacode/<device_id>/spectrum`
- Optional Home Assistant MQTT Discovery sensors

**Note the bluetooth interface in the upstream radiacode python package is not very reliable. I suggest USB connection.**

## Features

- **USB and BLE support** (single device at a time)
- **MQTT Discovery** (Home Assistant auto-creates sensors)
- **Dose configuration**:
  - System: `Sv` or `R`
  - Prefix: `whole`, `deci`, `centi`, `milli`, `micro`, `nano`
- **Realtime metrics**:
  - count rate (CPS)
  - dose rate
  - associated percent errors (if provided by device)
  - flags
- **Device metrics (when available)**:
  - temperature (°C)
  - battery (%)
  - total dose (integrated)
  - spectrum accumulation duration (s)
- **Optional spectrum publishing** (1024 channels) with configurable interval and retain
- **Health/status topics** for diagnosing connection issues

## Quick start

1. Install the add-on from your repository (or add it via your add-on store source).
2. Configure MQTT broker settings.
3. Choose connection method:
   - **USB**: leave `radiacode_mac` empty, plug device into the HA host
   - **BLE**: set `radiacode_mac` to the device MAC address

4. Start the add-on.
5. If MQTT Discovery is enabled, new sensors should appear in Home Assistant automatically.

## Configuration (high level)

- `radiacode_mac`: BLE MAC address (empty = USB)
- `poll_interval_s`: realtime polling interval
- `dose.system`: `Sv` or `R`
- `dose.prefix`: `whole|deci|centi|milli|micro|nano`
- `spectrum.enabled`: publish spectrum payloads
- `mqtt.*`: broker connection and topic settings

See `docs.md` for the complete schema and examples.

## MQTT topics

Assuming:
- `mqtt.topic_prefix = radiacode`
- `device_id` is `radiacode_usb` for USB, or the BLE MAC without colons for BLE.

Then the add-on publishes:

- `radiacode/<device_id>/availability` (retained): `online` / `offline`
- `radiacode/<device_id>/state` (not retained): current readings (JSON)
- `radiacode/<device_id>/status` (not retained): status / errors (JSON or simple string)
- `radiacode/<device_id>/heartbeat` (not retained): `{ "ts": <unix> }`
- `radiacode/<device_id>/spectrum` (optional; retain configurable): latest spectrum payload
- `radiacode/<device_id>/raw_fields` (debug only; retained): raw buffer snapshot

## Important BLE notes

BLE access in Home Assistant OS is sensitive to container permissions and host Bluetooth state.

If you see errors like:
- `Permission Denied` from bluepy management commands
- connect attempts that never succeed / time out
- device not found while it is clearly advertising

Then review the “BLE permissions” and “Troubleshooting” sections in `docs.md`.

## Support / troubleshooting

Please include:
- Add-on logs (from start through failure)
- Your add-on configuration (redact passwords)
- MQTT broker logs (if relevant)
- Whether the Radiacode is connected to a phone at the same time (often prevents BLE connection)

See `docs.md` for a checklist and common fixes.

## License

MIT (or your preferred license)
