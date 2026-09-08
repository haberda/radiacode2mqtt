import logging
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'radiacode2mqtt' / 'app'))
import radiacode2mqtt as app


class UnitTests(unittest.TestCase):
    def test_protocol_roentgen_conversion(self):
        for system, expected in [('R', 10.0), ('Sv', 0.1)]:
            opts = {'dose': {'system': system, 'prefix': 'micro'}}
            rate_unit, rate_factor = app.get_rate_unit_and_factor(opts)
            dose_unit, dose_factor = app.get_dose_unit_and_factor(opts)
            self.assertAlmostEqual(0.00001 * rate_factor, expected)
            self.assertAlmostEqual(0.00001 * dose_factor, expected)
            self.assertEqual(rate_unit, f'µ{system}/h')
            self.assertEqual(dose_unit, f'µ{system}')

    def test_all_prefixes(self):
        for prefix, factor in app.PREFIX_FACTOR.items():
            for system, scale in [('R', 1), ('Sv', 0.01)]:
                opts = {'dose': {'system': system, 'prefix': prefix}}
                self.assertEqual(app.get_dose_unit_and_factor(opts)[1], factor * scale)



from datetime import datetime, timedelta
from unittest.mock import Mock, patch
from paho.mqtt.reasoncodes import ReasonCode
from paho.mqtt.packettypes import PacketTypes
from radiacode import RealTimeData, RareData, Spectrum
import threading


def realtime():
    return RealTimeData(datetime.now(), 5, 1, 0.00001, 2, 0, 0)


class MqttTests(unittest.TestCase):
    def setUp(self):
        self.cfg = app.MqttConfig('broker', 1883, None, None, 'radiacode', 'homeassistant', True)
        self.bridge = app.MqttBridge(self.cfg, {}, 'test', logging.getLogger())
        self.bridge.client = Mock()

    def test_rejected_connection_and_v2_disconnect(self):
        self.bridge.on_connect(self.bridge.client, None, None,
                               ReasonCode(PacketTypes.CONNACK, 'Not authorized'), None)
        self.assertFalse(self.bridge.connected.is_set())
        self.bridge.on_connect(self.bridge.client, None, None,
                               ReasonCode(PacketTypes.CONNACK, 'Success'), None)
        self.assertTrue(self.bridge.connected.is_set())
        self.bridge.on_disconnect(self.bridge.client, None, None,
                                  ReasonCode(PacketTypes.DISCONNECT, 'Normal disconnection'), None)
        self.assertFalse(self.bridge.connected.is_set())

    def test_reconnect_republishes_discovery_and_health(self):
        self.bridge.client.publish.return_value.rc = 0
        for _ in range(2):
            self.bridge.on_connect(self.bridge.client, None, None,
                                   ReasonCode(PacketTypes.CONNACK, 'Success'), None)
            self.bridge.sync(True)
        publishes = self.bridge.client.publish.call_args_list
        self.assertEqual(sum(c.args[0].endswith('/dose_rate/config') for c in publishes), 2)
        self.assertEqual(sum(c.args[:2] == ('radiacode/test/availability', 'online') for c in publishes), 2)

    def test_disconnected_measurements_are_dropped(self):
        self.bridge.publish('state', {'cps': 5})
        self.bridge.client.publish.assert_not_called()

    def test_birth_triggers_refresh(self):
        message = Mock(topic='homeassistant/status', payload=b'online')
        self.bridge.on_message(None, None, message)
        self.assertTrue(self.bridge.refresh.is_set())


class RecoveryTests(unittest.TestCase):
    def test_first_read_timeout_and_reset(self):
        recovery = app.Recovery(60, 30)
        recovery.connected(100)
        self.assertFalse(recovery.expired(159))
        self.assertTrue(recovery.expired(160))
        recovery.connected(200)
        self.assertFalse(recovery.expired(201))

    def test_backoff_and_healthy_reset(self):
        recovery = app.Recovery(60, 30, 3)
        self.assertEqual(recovery.failed(), 2)
        self.assertEqual(recovery.failed(), 4)
        recovery.connected(10)
        recovery.observe(10)
        self.assertEqual(recovery.attempts, 2)
        recovery.observe(40)
        self.assertEqual(recovery.attempts, 0)
        self.assertEqual(recovery.failed(), 2)
        recovery.failed()
        with self.assertRaises(RuntimeError):
            recovery.failed()

    def test_poll_validation(self):
        with self.assertRaises(ValueError):
            app.validate_options({'poll_interval_s': 30, 'watchdog_s': 30})

    def test_legacy_identity_and_custom_identity(self):
        self.assertEqual(app.compute_device_id_from_opts({})[0], 'radiacode_usb')
        self.assertEqual(app.compute_device_id_from_opts({'device_id': 'RC-103-123'})[0], 'RC-103-123')
        with self.assertRaises(ValueError):
            app.compute_device_id_from_opts({'device_id': 'bad/topic'})


class MeasurementTests(unittest.TestCase):
    def test_rare_only_buffer_is_preserved(self):
        data = app.Measurements({})
        rare = RareData(datetime.now(), 120, 0.00001, 25, 80, 0)
        seen, _ = data.update([rare], 10)
        self.assertFalse(seen)
        self.assertEqual(data.values['temperature_c'], 25)
        self.assertAlmostEqual(data.values['dose_total'], 0.1)
        self.assertEqual(data.values['dose_duration_s'], 120)
        self.assertNotIn('spectrum_duration_s', data.values)
        spec = Spectrum(timedelta(seconds=0), 0, 1, 0, [1, 2])
        self.assertEqual(data.spectrum(spec, 0)['duration_s'], 0)
        self.assertEqual(data.values['spectrum_duration_s'], 0)

    def test_payload_preserves_raw_units_and_missing_fields(self):
        data = app.Measurements({})
        data.update([realtime()], 10)
        recovery = app.Recovery(60, 30)
        recovery.observe(10)
        payload = data.payload(12, 1000, 'ok', recovery, 'usb')
        self.assertAlmostEqual(payload['dose_rate'], 0.1)
        self.assertEqual(payload['raw']['dose_rate'], 0.00001)
        self.assertEqual(payload['raw']['dose_rate_unit'], 'R/h')
        self.assertIsNone(payload['battery_pct'])
        self.assertEqual(payload['last_seen_age_s'], 2)


class LoopTests(unittest.TestCase):
    def test_usb_and_ble_read_failure_reconnect_then_shutdown(self):
        for mac in ('', 'AA:BB:CC:DD:EE:FF'):
            clock = [0.0]
            stop = threading.Event()
            bridge = Mock()
            first, second = Mock(), Mock()
            first.request.side_effect = OSError('unplugged')
            def read(command):
                stop.set()
                return [realtime()]
            second.request.side_effect = read
            factory = Mock(side_effect=[first, second])
            def wait(seconds):
                clock[0] += seconds
                return stop.is_set()
            with patch.object(app.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(stop, 'wait', side_effect=wait):
                app.run({'radiacode_mac': mac}, stop, logging.getLogger(), bridge, factory)
            self.assertEqual(factory.call_count, 2)
            first.close.assert_called_once()
            second.close.assert_called_once()
            bridge.close.assert_called_once()

    def test_silent_device_eventually_exhausts_recovery(self):
        clock = [0.0]
        stop = threading.Event()
        bridge = Mock()
        worker = Mock()
        worker.request.return_value = []
        def wait(seconds):
            clock[0] += seconds
            if clock[0] > 100:
                self.fail('Recovery did not terminate')
            return False
        with patch.object(app.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(stop, 'wait', side_effect=wait):
            with self.assertRaisesRegex(RuntimeError, 'exhausted'):
                app.run({'first_data_timeout_s': 1, 'max_recoveries': 2}, stop, logging.getLogger(), bridge, Mock(return_value=worker))
        self.assertEqual(worker.close.call_count, 2)
        bridge.close.assert_called_once()

import time


def stalled_device(pipe, opts):
    time.sleep(60)


class WorkerTests(unittest.TestCase):
    def test_hung_native_operation_is_terminated(self):
        with patch.object(app, 'device_process', stalled_device):
            worker = app.DeviceWorker({}, threading.Event())
        try:
            with self.assertRaises(TimeoutError):
                worker.receive(0.05)
        finally:
            worker.close()
        self.assertFalse(worker.process.is_alive())

    def test_stop_interrupts_wait(self):
        stop = threading.Event()
        with patch.object(app, 'device_process', stalled_device):
            worker = app.DeviceWorker({}, stop)
        try:
            stop.set()
            with self.assertRaises(InterruptedError):
                worker.receive(30)
        finally:
            worker.close()
        self.assertFalse(worker.process.is_alive())


class DiscoveryTests(unittest.TestCase):
    def test_measurement_and_diagnostic_availability(self):
        import json
        client = Mock()
        cfg = app.MqttConfig('broker', 1883, None, None, 'radiacode', 'homeassistant', True)
        app.publish_discovery(client, cfg, {}, 'device', logging.getLogger())
        payloads = {call.args[0].split('/')[-2]: json.loads(call.args[1])
                    for call in client.publish.call_args_list if "/sensor/" in call.args[0]}
        self.assertEqual(len(payloads['dose_rate']['availability']), 2)
        self.assertEqual(len(payloads['device_status']['availability']), 1)
        self.assertEqual(payloads['dose_rate']['state_class'], 'measurement')
        self.assertEqual(payloads['dose_total']['state_class'], 'total_increasing')
        self.assertEqual(payloads['flags']['entity_category'], 'diagnostic')
        self.assertIn('dose_duration_s', payloads)


if __name__ == '__main__':
    unittest.main()
