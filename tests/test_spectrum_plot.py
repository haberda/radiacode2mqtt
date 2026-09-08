import json
import logging
import sys
import unittest
from datetime import timedelta
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'radiacode2mqtt' / 'app'))
from PIL import Image
from radiacode import Spectrum
import radiacode2mqtt as app
from spectrum_plot import render_spectrum, spectrum_axis


class SpectrumPlotTests(unittest.TestCase):
    def spec(self, counts, **kwargs):
        return Spectrum(timedelta(seconds=120), kwargs.get('a0', 0), kwargs.get('a1', 3),
                        kwargs.get('a2', 0.001), counts)

    def test_png_and_edge_cases(self):
        for counts in ([], [0], [0] * 1024, [0, 1, 50, 2, 0]):
            for scale in ('log', 'linear'):
                png = render_spectrum(self.spec(counts), 1700000000, scale=scale)
                with Image.open(BytesIO(png)) as image:
                    self.assertEqual(image.format, 'PNG')
                    self.assertEqual(image.size, (1200, 600))
                    image.verify()

    def test_calibration_and_fallback(self):
        x, label = spectrum_axis(self.spec([1, 2, 3]))
        self.assertAlmostEqual(x[2], 6.004)
        self.assertEqual(label, 'Energy (keV)')
        for coefficients in ({'a1': -3}, {'a0': float('nan')}, {'a1': None}):
            x, label = spectrum_axis(self.spec([1, 2, 3], **coefficients))
            self.assertEqual(list(x), [0, 1, 2])
            self.assertIn('Channel', label)

    def test_invalid_counts(self):
        for counts in ([-1], [float('nan')], [float('inf')]):
            with self.assertRaises(ValueError):
                render_spectrum(self.spec(counts), 1700000000)

    def test_binary_mqtt_payload(self):
        cfg = app.MqttConfig('broker', 1883, None, None, 'radiacode', 'homeassistant', True)
        bridge = app.MqttBridge(cfg, {}, 'test', logging.getLogger())
        bridge.client = Mock()
        bridge.client.publish.return_value.rc = 0
        bridge.connected.set()
        png = render_spectrum(self.spec([1, 2, 3]), 1700000000)
        bridge.publish('spectrum/image', png, retain=True)
        bridge.client.publish.assert_called_once_with('radiacode/test/spectrum/image', png, qos=0, retain=True)

    def test_camera_discovery_and_removal(self):
        client = Mock()
        cfg = app.MqttConfig('broker', 1883, None, None, 'radiacode', 'homeassistant', True)
        for enabled in (True, False):
            client.reset_mock()
            app.publish_discovery(client, cfg, {'spectrum': {'enabled': True, 'image_enabled': enabled}}, 'test', logging.getLogger())
            camera = next(c for c in client.publish.call_args_list if '/camera/' in c.args[0])
            self.assertEqual(camera.args[0], 'homeassistant/camera/test/spectrum/config')
            if enabled:
                payload = json.loads(camera.args[1])
                self.assertEqual(payload['encoding'], '')
                self.assertEqual(payload['topic'], 'radiacode/test/spectrum/image')
                self.assertEqual(payload['device']['identifiers'], ['test'])
            else:
                self.assertEqual(camera.args[1], '')

    def test_polling_publishes_image_and_survives_renderer_failure(self):
        import threading
        from datetime import datetime
        from unittest.mock import patch
        from radiacode import RealTimeData
        for fail in (False, True):
            clock = [0.0]
            stop = threading.Event()
            worker, bridge = Mock(), Mock()
            def request(command):
                if command == 'spectrum':
                    stop.set()
                    return self.spec([1, 2, 3])
                return [RealTimeData(datetime.now(), 5, 1, 0.00001, 2, 0, 0)]
            worker.request.side_effect = request
            factory = Mock(return_value=worker)
            def wait(seconds):
                clock[0] += seconds
                if clock[0] > 30:
                    self.fail('Spectrum was never requested')
                return stop.is_set()
            with patch.object(app.time, 'monotonic', side_effect=lambda: clock[0]), patch.object(stop, 'wait', side_effect=wait), patch('spectrum_plot.render_spectrum', side_effect=ValueError('bad plot') if fail else None, return_value=b'png'):
                app.run({'spectrum': {'enabled': True}}, stop, Mock(), bridge, factory)
            topics = [c.args[0] for c in bridge.publish.call_args_list]
            self.assertIn('spectrum', topics)
            self.assertEqual('spectrum/image' in topics, not fail)
            self.assertEqual(factory.call_count, 1)
            worker.close.assert_called_once()
