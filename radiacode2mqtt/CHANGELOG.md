# Changelog

## 0.2.0

- Correct Sv dose conversion (100× lower than 0.1.x); preserve raw R-based values and document evidence.
- Fix Paho v2 callbacks, broker retry, discovery refresh, and bridge/device availability.
- Unify bounded USB/BLE recovery and graceful shutdown using an isolated device worker.
- Replace bluepy scanning with advisory Bleak scanning.
- Preserve rare-only records; distinguish dose and spectrum duration.
- Add stable configurable device identity, USB serial selection, and discovery statistics.
- Pin runtime dependencies and remove unused web assets and build/runtime packages.
- Remove raw Bluetooth capabilities and host networking; retain USB and host D-Bus access.
- Add regression tests and migration documentation.

## 0.1.1

- Use Python 3.11 slim Bookworm and build Python dependencies in a separate stage to exclude build tools from the runtime image.
- Include process utilities used by existing BLE recovery.
- Forward container shutdown signals to the Python process.
- Support amd64 and aarch64; remove deprecated armv7 support.
- Add Home Assistant repository metadata and installation instructions.

## 0.1.0

- Initial Radiacode USB/BLE to MQTT add-on.
