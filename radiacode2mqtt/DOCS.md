# Radiacode MQTT configuration

## Connection

Leave `radiacode_mac` empty for USB. Optionally set `radiacode_serial` to the
USB serial (for example `RC-103-123456`) to select a particular instrument.
For Bluetooth, set `radiacode_mac`; the serial selector applies only to USB.
Bluetooth uses Bleak and the Home Assistant host's BlueZ service over D-Bus.
No Bluetooth daemon runs inside the add-on. Disconnect phone apps if they
prevent the instrument from accepting another connection.

`ble_scan_enabled` and `ble_scan_seconds` control an optional advisory Bleak
scan. A missing advertisement or scan error does not prevent connection.
`ble_connect_timeout_s` bounds connection initialization for either transport;
the optional scan has an additional `ble_scan_seconds` allowance.
`operation_timeout_s` bounds each buffer or spectrum operation (default 30 s).
A separate worker process allows a hung native operation to be terminated.

## Identity and upgrades

`device_id` is a stable MQTT/HA identifier using letters, digits, underscores
and hyphens. For new installations, set it to your instrument's serial-based
name, e.g. `RC-103-123456`, and keep it unchanged when switching USB/BLE.
`radiacode_serial` selects hardware; `device_id` identifies entities.

An empty `device_id` preserves the legacy USB ID `radiacode_usb`, or the BLE
MAC without colons. Existing users should leave it empty to preserve entity
IDs. Multiple instances on one broker must use distinct IDs, even if their
MQTT topic prefixes differ.

Changing `device_id` creates new discovery entities and a new MQTT client ID.
To migrate deliberately, stop the old instance, remove its retained discovery
messages (`<discovery_prefix>/sensor/<old_id>/+/config`) using your broker's
MQTT tooling, remove obsolete HA entities, configure the new ID, and restart.
Update dashboards and automations to the new entities. The add-on does not
silently delete old retained topics. Likewise, disabling discovery does not
remove existing retained discovery messages.

## Dose units

`dose.system` selects `Sv` or `R`; `dose.prefix` selects `whole`, `deci`,
`centi`, `milli`, `micro`, or `nano`. Buffered rate is interpreted as R/h and
accumulated dose as R, with the device convention 100 R = 1 Sv. For example,
a raw rate of 0.00001 becomes 10 µR/h or 0.1 µSv/h. The device's display and
alarm settings are not changed. See [unit evidence and verification limits](UNIT_NOTES.md).

**Version 0.2.0 corrects Sv values by a factor of 100 compared with 0.1.x.**
Old HA history is not rescaled. Review thresholds, automations, and statistics
when upgrading. Accumulated dose can reset on the instrument; HA discovery
uses `total_increasing` to account for resets. Dose accumulation duration and
spectrum duration are now separate fields.

## Polling and recovery

- `poll_interval_s`: delay between completed polls, default 5 s.
- `watchdog_s`: maximum gap without realtime data, default 30 s; must exceed
  the polling interval.
- `first_data_timeout_s`: grace period for the first realtime record after
  each connection, default 60 s.
- `max_recoveries`: consecutive failed connection/recovery cycles before
  exiting with an error, default 8. Recovery attempts reset after a healthy
  stream lasts at least `watchdog_s`.
- `status_publish_every_s`: status log and heartbeat interval, default 30 s.

USB and BLE reconnect on transport errors or stale readings using exponential
backoff capped at 60 s. Deadlines use a monotonic clock. Device calls are
bounded separately, so detecting a silent device may take an operation timeout
plus a poll interval beyond the data deadline. Shutdown allows a short graceful
worker close, then terminates it if necessary. An exhausted recovery budget
exits nonzero for Supervisor to handle; inspect Supervisor logs if it does not
restart the add-on.

## MQTT and availability

Configure `mqtt.host`, `port`, `username`, `password`, `topic_prefix`,
`discovery_prefix`, and `discovery`. Broker connection attempts retry with
backoff. Discovery and availability are republished after reconnect and on
Home Assistant's birth message. Telemetry is not queued while disconnected.
The application currently uses plain MQTT TCP; use a trusted local broker.

Topics below are relative to `<topic_prefix>/<device_id>`:

| Topic | Payload / retention |
| --- | --- |
| `state` | JSON measurements, units, raw values, timestamps and health; not retained |
| `availability` | Bridge `online`/`offline`; retained, QoS 1, offline last will |
| `device_availability` | Instrument data health; retained, QoS 1 |
| `status` | Consistent JSON status and error; not retained |
| `heartbeat` | JSON Unix timestamp; not retained |
| `spectrum` | Duration, calibration `a0/a1/a2`, counts, channel count; retention configurable |
| `raw_fields` | Debug records when `debug` is true; not retained |

Measurement sensors require both bridge and instrument availability. Diagnostic
sensors depend only on the bridge. Numeric measurement sensors expire without
updates. Missing fields are JSON null; cached rare readings remain available
with `rare_last_seen_age_s`. `measurement_time` is the instrument record's
timestamp; `ts` is the publication timestamp. Raw dose fields explicitly carry
R and R/h unit labels.

`spectrum.enabled`, `interval_s` (default 120), and `retain` control spectrum
publication. Spectrum reads follow realtime reads. With spectra disabled,
`spectrum_duration_s` remains null. A zero-duration spectrum is valid.

## Troubleshooting

Enable `debug` temporarily and inspect status/error messages. For USB, check
cable, serial selection, and Supervisor device access. For BLE, check the host
adapter, D-Bus/BlueZ access and competing connections. The add-on no longer
uses bluepy or raw-HCI scanning and does not request NET_ADMIN/NET_RAW or host
networking. Test BLE on your HA host after upgrading; container tests do not
establish that the host's D-Bus policy permits a physical connection.

If MQTT reports rejected authentication, check credentials; it is not a device
connection error. If readings differ from the instrument display, record raw
rate/dose, selected output units, device display units and firmware version.

## Development checks

Build with `podman build -t radiacode2mqtt radiacode2mqtt` (or Docker).
Run `python -m unittest discover -s tests -v` with the pinned runtime dependencies
installed. The tests simulate faults and do not connect to hardware.

## Spectrum camera (0.3.0)

With MQTT discovery and spectra enabled, a **Radiacode Spectrum** camera appears
on the same Home Assistant device as the sensors. Add that camera to a Picture
Entity card on your dashboard. Use the entity selector to find its actual ID;
Home Assistant may adjust the generated name if there is a conflict.

```yaml
spectrum:
  enabled: true
  interval_s: 120
  retain: false
  image_enabled: true
  image_scale: log
```

`image_enabled` defaults to true; set it false to disable rendering and remove
the camera discovery entry. `image_scale` is `log` (default) or `linear`.
Plots show counts per channel against calibrated energy in keV. Invalid or
non-increasing calibration falls back to channel numbers. Zero bins are masked
on log plots; empty spectra show a waiting message. The PNG includes acquisition
duration and a UTC capture timestamp so an older frame is identifiable.

The image is a 1200×600 PNG sent as **raw binary bytes**, not JSON or Base64, on
`<topic_prefix>/<device_id>/spectrum/image`. The latest image is always retained
by the broker, independently of `spectrum.retain`, which controls the numeric
JSON spectrum. This lets the camera receive the latest frame when Home Assistant
restarts. The camera requires both bridge and instrument availability; its last
frame may predate reconnection until the next spectrum update.

No HTTP server, exposed port, or ingress configuration is needed. Image updates
use the existing spectrum interval. Plot failures are logged without interrupting
measurement polling or JSON spectrum publication. Disabling images or spectra
with discovery enabled removes this camera's retained configuration and image.

Matplotlib adds approximately 132 MB to the amd64 container (about 272 MB total).
It is imported only when rendering is enabled, and each figure is released after
encoding. Turning images off saves runtime rendering work, but does not remove
the installed plotting libraries.

For manual configuration when discovery is disabled:

```yaml
mqtt:
  camera:
    - name: Radiacode Spectrum
      topic: radiacode/radiacode_usb/spectrum/image
      encoding: ""
      availability:
        - topic: radiacode/radiacode_usb/availability
        - topic: radiacode/radiacode_usb/device_availability
      availability_mode: all
```

Replace the topic prefix and device ID to match your configuration. See the
[Home Assistant MQTT camera documentation](https://www.home-assistant.io/integrations/camera.mqtt/).
