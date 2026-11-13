import unittest
from datetime import datetime, timedelta, timezone

from ingestion.events import simulate
from streaming.rules import evaluate, validation_error, output_id
import json


class StreamingRulesTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 11, 3, 15, tzinfo=timezone.utc)

    def risk(self, **overrides):
        args = dict(status='in_transit', promised=self.now+timedelta(hours=12),
                    last_movement=self.now, latitude=6.2442, longitude=-75.5812,
                    speed=65, gps_time=self.now, dest_lat=4.711, dest_lon=-74.0721,
                    route_speed=65, now=self.now)
        args.update(overrides)
        return evaluate(**args)

    def test_normal_and_overdue(self):
        self.assertEqual(self.risk()[:2], (False, 'low'))
        self.assertEqual(self.risk(promised=self.now-timedelta(minutes=1))[:2], (True, 'high'))

    def test_delivered_resolves_alert(self):
        self.assertEqual(self.risk(status='delivered', promised=self.now-timedelta(hours=1))[:2], (False, 'low'))

    def test_stale_and_missing_gps(self):
        self.assertIn('desactualizado', self.risk(gps_time=self.now-timedelta(minutes=20))[2])
        self.assertEqual(self.risk(latitude=None)[:2], (True, 'medium'))

    def test_eta_and_stationary(self):
        self.assertEqual(self.risk(promised=self.now+timedelta(minutes=1))[1], 'high')
        self.assertIn('detenido', self.risk(speed=0, last_movement=self.now-timedelta(minutes=20))[2])

    def test_clock_reassessment_without_new_event(self):
        self.assertFalse(self.risk()[0])
        self.assertTrue(self.risk(now=self.now+timedelta(hours=13))[0])

    def test_validation_checks_json_key_and_type(self):
        event = next(simulate())[0]
        payload = json.dumps(event)
        self.assertEqual(validation_error(payload, 'vehicle_gps', event['vehicle_id']), '')
        self.assertTrue(validation_error(payload, 'vehicle_gps', 'wrong'))
        self.assertTrue(validation_error('{', 'vehicle_gps', 'wrong'))
        self.assertTrue(validation_error(payload, 'shipment_events', event['vehicle_id']))

    def test_output_identity_stable_across_retry(self):
        args = ('GUIA-1', 'e1', 'gps1', 'high', 'vencido')
        self.assertEqual(output_id(*args), output_id(*args))
        self.assertNotEqual(output_id(*args), output_id('GUIA-1', 'e1', 'gps1', 'low', ''))

    def test_accepts_real_source_without_scenario(self):
        event = next(simulate())[0]
        event['source'] = 'gps_gateway'
        del event['scenario']
        self.assertEqual(validation_error(json.dumps(event), 'vehicle_gps', event['vehicle_id']), '')

    def test_nan_coordinates_rejected(self):
        event = next(simulate())[0]
        event['latitude'] = float('nan')
        self.assertTrue(validation_error(json.dumps(event), 'vehicle_gps', event['vehicle_id']))

    def test_promise_fixed_for_guide(self):
        events = [e for e, _ in simulate(1, steps=12) if e['event_type'] == 'shipment_event']
        self.assertEqual(len({e['promised_delivery'] for e in events}), 1)
