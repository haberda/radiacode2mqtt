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


if __name__ == '__main__':
    unittest.main()
