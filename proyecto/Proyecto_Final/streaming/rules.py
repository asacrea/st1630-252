import hashlib
import json
import math
from datetime import datetime, timezone
from uuid import UUID


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value.replace('Z', '+00:00'))
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def evaluate(status, promised, last_movement, latitude, longitude, speed,
             gps_time, dest_lat, dest_lon, route_speed, now, stale_minutes=15):
    """Reglas operativas sobre datos unidos por Spark; unidades km, km/h y minutos."""
    now = utc(now)
    inactive = max(0, (now - utc(last_movement)).total_seconds() / 60)
    if status == 'delivered':
        return False, 'low', '', 0.0, 0.0, inactive
    if latitude is None or longitude is None or gps_time is None:
        distance, eta = None, None
        reason, risk = 'Sin GPS disponible', 'medium'
    elif (now - utc(gps_time)).total_seconds() > stale_minutes * 60:
        distance, eta = None, None
        reason, risk = 'GPS desactualizado', 'medium'
    elif dest_lat is None or dest_lon is None or route_speed is None:
        distance, eta = None, None
        reason, risk = 'Ruta sin configuración de destino', 'medium'
    else:
        a, b = math.radians(latitude), math.radians(dest_lat)
        dlat, dlon = b-a, math.radians(dest_lon-longitude)
        hav = math.sin(dlat/2)**2 + math.cos(a)*math.cos(b)*math.sin(dlon/2)**2
        distance = 6371 * 2 * math.asin(min(1, math.sqrt(hav)))
        # Cota aproximada, no ETA vial: velocidad esperada de ruta, no GPS instantáneo.
        eta = distance / route_speed * 60
        reason, risk = '', 'low'
        if (utc(promised) - now).total_seconds() / 60 < eta:
            reason, risk = 'Tiempo disponible inferior al viaje estimado', 'high'
        elif speed is not None and speed < 1 and inactive >= stale_minutes:
            reason, risk = 'Vehículo detenido y envío sin movimientos recientes', 'high'
    if utc(promised) < now:
        reason, risk = 'Promesa de entrega vencida', 'high'
    elif not reason and inactive >= stale_minutes:
        reason, risk = 'Envío sin movimientos recientes', 'medium'
    return bool(reason), risk, reason, distance, eta, inactive


def validation_error(payload, topic, key):
    from ingestion.events import validate
    try:
        event = json.loads(payload)
        validate(event)
        for name in ('latitude', 'longitude', 'speed_kmh'):
            if name in event and not math.isfinite(event[name]):
                return f'Campo {name} no es finito'
        for name in ('source', 'vehicle_id', 'route_id', 'shipment_id', 'center_id'):
            if name in event and not event[name].strip():
                return f'Campo {name} vacío después de normalizar'
        expected = 'vehicle_gps' if topic == 'vehicle_gps' else 'shipment_event'
        if event['event_type'] != expected:
            return 'Tipo de evento incompatible con tópico'
        expected_key = event['vehicle_id'] if topic == 'vehicle_gps' else event['shipment_id']
        if key != expected_key:
            return 'Clave Kafka incompatible con identificador'
        return ''
    except Exception as error:
        return str(error)[:1500]


def output_id(shipment_id, shipment_event_id, gps_event_id, risk, reason):
    content = json.dumps([shipment_id, shipment_event_id, gps_event_id, risk, reason], separators=(',', ':'))
    return str(UUID(hashlib.sha256(content.encode()).hexdigest()[:32]))
