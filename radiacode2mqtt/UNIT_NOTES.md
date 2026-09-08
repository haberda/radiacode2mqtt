# Buffered dose units

This add-on interprets `RealTimeData.dose_rate` as R/h and `RareData.dose`
as R. It converts at the MQTT boundary, preserving the raw protocol values.
It follows the device's display convention of 100 R = 1 Sv; this is not a
universal physical conversion between exposure and dose equivalent.

| Output | Multiply raw value by |
| --- | --- |
| R or R/h | 1 |
| µR or µR/h | 1,000,000 |
| Sv or Sv/h | 0.01 |
| µSv or µSv/h | 10,000 |

The same SI prefix multiplier applies to other prefixes.

## Evidence and limits

- [Upstream exporter](https://github.com/cdump/radiacode/blob/2b217916f49f5eedddf8f0d9116fcfd3c6b8832e/src/radiacode/examples/radiacode-exporter.py)
  explicitly multiplies buffered dose rate by 10,000 to produce µSv/h.
- [Buffer decoder](https://github.com/cdump/radiacode/blob/2b217916f49f5eedddf8f0d9116fcfd3c6b8832e/src/radiacode/decoders/databuf.py)
  passes both dose floats through without unit conversion.
- [Device alarm implementation](https://github.com/cdump/radiacode/blob/2b217916f49f5eedddf8f0d9116fcfd3c6b8832e/src/radiacode/radiacode.py)
  uses a factor of 100 for the device's R/Sv display convention. Alarm
  registers have their own scaling and must not be confused with buffer floats.
- [Independent C++ example](https://github.com/mkgeiger/RadiaCode/blob/6db23d5db4acafd554eb4b0dc8c9cfddd192661e/README.md)
  uses the same 10,000 factor for buffered dose rate.

The rate conversion is directly supported by the exporter. Applying the same
base unit to accumulated dose is an inference from the shared protocol and
unit convention, not a published vendor protocol guarantee. Upstream's
measurement guide remains cautious about protocol units. No physical device
was available during implementation to compare the raw accumulated dose and
display. Validate both rate and accumulated dose against your device, ideally
with its display set to R and then Sv. Do not reset its accumulated dose for
this comparison. Record firmware version, raw values, and displayed values.

Before version 0.2.0, Sv output was numerically identical to R output and thus
100 times the corrected value under this convention. Existing Home Assistant
history is not rewritten by upgrading. Review old statistics and thresholds.
