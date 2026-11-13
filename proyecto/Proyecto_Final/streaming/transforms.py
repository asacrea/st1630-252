from pyspark.sql import functions as F, types as T, Window
from .rules import evaluate, validation_error, output_id

EVENT_SCHEMA = T.StructType([
    T.StructField(name, T.DoubleType() if name in ('latitude', 'longitude', 'speed_kmh') else T.StringType())
    for name in ('schema_version', 'event_id', 'event_type', 'event_time', 'source', 'vehicle_id',
                 'route_id', 'scenario', 'shipment_id', 'status', 'origin_city', 'destination_city',
                 'center_id', 'promised_delivery', 'latitude', 'longitude', 'speed_kmh')])


def parse(batch):
    checked = batch.withColumn('validation_error', F.udf(validation_error, 'string')('raw_json', 'topic', 'key'))
    parsed = checked.withColumn('event', F.from_json('raw_json', EVENT_SCHEMA)).select('*', 'event.*').drop('event')
    parsed = parsed.withColumn('event_timestamp', F.to_timestamp('event_time'))
    parsed = parsed.withColumn('validation_error', F.when(
        F.col('event_timestamp') > F.col('kafka_timestamp') + F.expr('INTERVAL 5 MINUTES'),
        F.lit('Fecha de evento más de cinco minutos en el futuro')).otherwise(F.col('validation_error')))
    for name in ('vehicle_id', 'shipment_id', 'center_id', 'route_id', 'origin_city', 'destination_city', 'source'):
        parsed = parsed.withColumn(name, F.trim(F.col(name)))
    parsed = parsed.withColumn('event_id', F.lower('event_id')).withColumn('route_id', F.upper('route_id'))
    return parsed


def latest(events, identity):
    window = Window.partitionBy(identity).orderBy(F.col('event_timestamp').desc(), F.col('event_id').desc())
    return events.withColumn('_row', F.row_number().over(window)).filter('_row = 1').drop('_row')


RISK_SCHEMA = 'active_alert boolean, risk_level string, possible_delay_reason string, distance_km double, estimated_minutes double, inactive_minutes double'


def assess(shipments, gps, routes, now, stale_minutes=15):
    # Estado actual: join por vehículo Y ruta. GPS tardío no reemplaza uno más reciente.
    s = latest(shipments, 'shipment_id').alias('s')
    g = latest(gps, 'vehicle_id').alias('g')
    joined = s.join(g, (F.col('s.vehicle_id') == F.col('g.vehicle_id')) &
                      (F.col('s.route_id') == F.col('g.route_id')), 'left').select(
        's.*', F.col('g.event_id').alias('gps_event_id'), F.col('g.event_timestamp').alias('gps_time'),
        'g.latitude', 'g.longitude', 'g.speed_kmh')
    joined = joined.join(F.broadcast(routes), 'route_id', 'left')
    risk = F.udf(evaluate, RISK_SCHEMA)
    joined = joined.withColumn('risk', risk('status', F.to_timestamp('promised_delivery'),
        'event_timestamp', 'latitude', 'longitude', 'speed_kmh', 'gps_time',
        'destination_latitude', 'destination_longitude', 'expected_speed_kmh',
        F.lit(now).cast('timestamp'), F.lit(stale_minutes)))
    joined = joined.select('*', 'risk.*').drop('risk')
    return joined.withColumn('output_event_id', F.udf(output_id, 'string')(
        'shipment_id', 'event_id', 'gps_event_id', 'risk_level', 'possible_delay_reason'))
