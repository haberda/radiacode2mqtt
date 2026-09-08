# Changelog

## 0.1.1

- Use Python 3.11 slim Bookworm and build Python dependencies in a separate stage to exclude build tools from the runtime image.
- Include process utilities used by existing BLE recovery.
- Forward container shutdown signals to the Python process.
- Support amd64 and aarch64; remove deprecated armv7 support.
- Add Home Assistant repository metadata and installation instructions.

## 0.1.0

- Initial Radiacode USB/BLE to MQTT add-on.
