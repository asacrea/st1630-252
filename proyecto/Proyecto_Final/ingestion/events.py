import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker

SCHEMA = json.loads((Path(__file__).parent.parent / 'schemas/event.schema.json').read_text(encoding='utf-8'))
VALIDATOR = Draft202012Validator(SCHEMA, format_checker=FormatChecker())
TOPICS = ('shipment_events', 'vehicle_gps', 'external_conditions', 'operational_updates')


def validate(event):
    VALIDATOR.validate(event)


def routing(event):
    validate(event)
    if event['event_type'] == 'shipment_event':
        return 'shipment_events', event['shipment_id']
    return 'vehicle_gps', event['vehicle_id']


def stamp(value):
    return value.isoformat().replace('+00:00', 'Z')


def simulate(shipments=3, scenario='normal', steps=12):
    """Ronda por segundo; estados y GPS correlacionados por vehículo y ruta."""
    run_id = uuid4().hex[:8]
    stages = ('received', 'departed_center', 'in_transit', 'arrived_center', 'out_for_delivery', 'delivered')
    started_at = datetime.now(timezone.utc)
    for step in range(steps):
        now = datetime.now(timezone.utc)
        progress = step / max(steps - 1, 1)
        for index in range(shipments):
            delayed = scenario == 'delay' and index == 0
            fraction = min(progress, .3) if delayed else progress
            base = dict(schema_version='1.0', event_id=str(uuid4()),
                        event_time=stamp(now), source='python_simulator',
                        vehicle_id=f'VEH-{run_id}-{index:03}', route_id='MED-BOG',
                        scenario='delay' if delayed else 'normal')
            yield dict(base, event_type='vehicle_gps', latitude=6.2442 + (4.7110-6.2442)*fraction,
                       longitude=-75.5812 + (-74.0721+75.5812)*fraction,
                       speed_kmh=0 if delayed and progress > .3 else 65), step
            status = stages[min(int(fraction * (len(stages)-1)), len(stages)-1)]
            yield dict(base, event_id=str(uuid4()), event_type='shipment_event',
                       shipment_id=f'GUIA-{run_id}-{index:04}', status=status,
                       origin_city='Medellín', destination_city='Bogotá',
                       center_id='CEN-MED' if fraction < .5 else 'CEN-BOG',
                       promised_delivery=stamp(started_at + timedelta(minutes=-10 if delayed else 120))), step
