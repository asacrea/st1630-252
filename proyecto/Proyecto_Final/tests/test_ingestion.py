import json
import tempfile
import unittest
from pathlib import Path

from jsonschema import ValidationError
from ingestion.events import simulate, validate, routing
from ingestion.outbox import Outbox


class IngestionTests(unittest.TestCase):
    def test_contract_and_keys(self):
        events = [e for e, _ in simulate(3, steps=12)]
        self.assertEqual(len(events), 72)
        self.assertEqual(len({e['event_id'] for e in events}), 72)
        for event in events:
            validate(event)
            topic, key = routing(event)
            self.assertEqual(key, event.get('shipment_id', event['vehicle_id']))
        shipments = [e for e in events if e['event_type'] == 'shipment_event']
        self.assertEqual(shipments[-1]['status'], 'delivered')

    def test_invalid_event(self):
        event = next(simulate())[0]
        event['latitude'] = 100
        with self.assertRaises(ValidationError):
            validate(event)
        event['latitude'] = 5
        event['event_time'] = 'ayer'
        with self.assertRaises(ValidationError):
            validate(event)

    def test_delay_stops_vehicle(self):
        events = [e for e, _ in simulate(1, 'delay', 12)]
        self.assertEqual(events[-2]['speed_kmh'], 0)
        self.assertNotEqual(events[-1]['status'], 'delivered')

    def test_failure_restart_and_replay_preserves_identity_and_order(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
            path = str(Path(directory) / 'outbox.sqlite3')
            events = [e for e, _ in simulate(1, steps=2)]
            box = Outbox(path)
            for event in events:
                box.enqueue(event)
            def unavailable(*args):
                raise ConnectionError('Falla simulada de Kafka')
            self.assertFalse(box.drain(unavailable, attempts=2, backoff=0))
            self.assertEqual(box.count(), 4)
            box.close()
            box = Outbox(path)
            received = []
            self.assertTrue(box.drain(lambda t, k, p: received.append(json.loads(p))))
            self.assertEqual(received, events)
            self.assertEqual(box.count(), 0)
            box.close()

    def test_uncertain_ack_keeps_same_id_for_deduplication(self):
        with tempfile.TemporaryDirectory(dir=Path(__file__).parent) as directory:
            box = Outbox(str(Path(directory) / 'outbox.sqlite3'))
            event = next(simulate())[0]
            box.enqueue(event)
            box.enqueue(event)
            self.assertEqual(box.count(), 1)
            received = []
            def lost_ack(t, k, p):
                received.append(json.loads(p)['event_id'])
                raise TimeoutError('Se perdió la confirmación')
            self.assertFalse(box.drain(lost_ack, attempts=1))
            box.drain(lambda t, k, p: received.append(json.loads(p)['event_id']))
            self.assertEqual(received, [event['event_id'], event['event_id']])
            box.close()


if __name__ == '__main__':
    unittest.main()

