# radiacode2mqtt

Home Assistant add-on publishing Radiacode USB or Bluetooth LE measurements to
MQTT, with automatic sensor discovery, spectrum data, and connection recovery.
Supports amd64 and aarch64.

## Installation

1. Open Home Assistant **Settings → Apps → App store** (or **Add-ons → Add-on store**).
2. Open the menu → **Repositories**, and add `https://github.com/haberda/radiacode2mqtt`.
3. Install **radiacode2mqtt**, configure your MQTT broker, and start it.

Leave `radiacode_mac` empty for USB, or set it for Bluetooth. New installations
should set `device_id` to a stable serial-based identifier. Existing installations
can leave it empty to preserve their Home Assistant entity IDs.

## Version 0.2.0 upgrade

Dose output in Sv is corrected to 1/100 of the previous value. Review existing
thresholds and history. Bluetooth now uses Bleak throughout; USB and BLE share
bounded recovery. Compilers, bluepy, and the unused web interface are removed.

See [configuration, migration, and troubleshooting](radiacode2mqtt/DOCS.md) and the linked unit
research notes for conversion evidence and hardware verification limits.

## License

MIT.
