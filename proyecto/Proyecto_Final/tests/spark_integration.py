"""Prueba de Spark y Delta real con fixtures; ejecutar con Dockerfile.spark."""
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker
from pyspark.sql import SparkSession
from streaming.job import process, kafka_rows
from streaming.lakehouse import LakehousePaths

spark = (SparkSession.builder.appName('Streaming-integration-test')
    .config('spark.sql.session.timeZone', 'UTC')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.databricks.delta.snapshotPartitions', '2')
    .config('spark.sql.shuffle.partitions', '2').getOrCreate())
spark.sparkContext.setLogLevel('WARN')

try:
    with tempfile.TemporaryDirectory() as directory:
        now = datetime(2026, 11, 3, 14, tzinfo=timezone.utc)
        paths = LakehousePaths(directory)
        project_root = Path(__file__).parent.parent
        routes_path = str(project_root / 'config' / 'routes.jsonl')

        def shipment_event(shipment_id, vehicle_id, event_time, status, promised_delivery,
                           center_id='C1'):
            return dict(schema_version='1.0', event_id=str(uuid4()), event_time=event_time,
                        source='python_simulator', scenario='normal', vehicle_id=vehicle_id,
                        route_id='MED-BOG', event_type='shipment_event', shipment_id=shipment_id,
                        status=status, origin_city='Medellín', destination_city='Bogotá',
                        center_id=center_id, promised_delivery=promised_delivery)

        shipment = shipment_event('S1', 'V1', '2026-11-03T14:00:00Z', 'in_transit',
                                  '2026-11-03T20:00:00Z')
        gps = dict(schema_version='1.0', event_id=str(uuid4()),
                   event_time='2026-11-03T14:00:00Z', source='python_simulator',
                   scenario='normal', vehicle_id='V1', route_id='MED-BOG',
                   event_type='vehicle_gps', latitude=6.2442, longitude=-75.5812,
                   speed_kmh=65)
        on_time_received = shipment_event('S2', 'V2', '2026-11-03T10:00:00Z', 'received',
                                          '2026-11-03T12:00:00Z', 'C2')
        on_time_delivered = shipment_event('S2', 'V2', '2026-11-03T11:00:00Z', 'delivered',
                                           '2026-11-03T12:00:00Z', 'C2')
        late_received = shipment_event('S3', 'V3', '2026-11-03T10:00:00Z', 'received',
                                       '2026-11-03T12:00:00Z', 'C3')
        late_delivered = shipment_event('S3', 'V3', '2026-11-03T13:00:00Z', 'delivered',
                                        '2026-11-03T12:00:00Z', 'C3')
        schema = ('key string, raw_json string, topic string, partition int, '
                  'offset long, kafka_timestamp timestamp, clock_tick boolean')

        def row(event, offset):
            topic = 'vehicle_gps' if event['event_type'] == 'vehicle_gps' else 'shipment_events'
            return (event.get('shipment_id', event['vehicle_id']), json.dumps(event),
                    topic, 0, offset, now, False)

        # UUID duplicado en offsets diferentes y JSON inválido.
        records = [
            row(shipment, 0), row(gps, 0), row(shipment, 1),
            ('INVALID', '{', 'shipment_events', 0, 2, now, False),
            row(on_time_received, 3), row(on_time_delivered, 4),
            row(late_received, 5), row(late_delivered, 6),
        ]
        batch = spark.createDataFrame(records, schema)
        process(spark, batch, 0, directory, '', routes_path, publish=False)
        process(spark, batch, 0, directory, '', routes_path, publish=False)
        assert spark.read.format('delta').load(paths.bronze_events).count() == 8
        assert spark.read.format('delta').load(paths.silver_events).count() == 6
        assert spark.read.format('delta').load(paths.silver_rejected_events).count() == 1
        current = spark.read.format('delta').load(paths.silver_current_shipments)
        assert current.count() == 3
        current_s1 = current.filter("shipment_id = 'S1'").first()
        assert current_s1['risk_level'] == 'low', current_s1.asDict()
        validator = Draft202012Validator(
            json.loads((project_root / 'schemas' / 'operational.schema.json').read_text()),
            format_checker=FormatChecker())
        operational_value = kafka_rows(
            current.filter("shipment_id = 'S1'"), now).first()['value']
        validator.validate(json.loads(operational_value))

        fact_path = paths.gold_table('fact_desempeno_envio')
        fact = spark.read.format('delta').load(fact_path)
        assert fact.count() == 3
        assert fact.filter("shipment_id = 'S2'").first()['is_on_time'] is True
        late_fact = fact.filter("shipment_id = 'S3'").first()
        assert late_fact['is_late'] is True
        assert late_fact['delay_minutes'] == 60.0
        assert late_fact['known_center_dwell_minutes'] == 180.0
        indicators = spark.read.format('delta').load(
            paths.gold_table('indicadores_cumplimiento_diario')).first()
        assert indicators['total_shipments'] == 3
        assert indicators['delivered_shipments'] == 2
        assert indicators['on_time_shipments'] == 1
        assert indicators['late_shipments'] == 1
        assert indicators['on_time_delivery_rate'] == 50.0

        for dimension in ('dim_fecha', 'dim_ruta', 'dim_ciudad', 'dim_centro_logistico',
                          'dim_vehiculo', 'dim_estado_envio'):
            assert spark.read.format('delta').load(paths.gold_table(dimension)).count() > 0

        # Evento tardío no debe hacer retroceder el estado.
        late = dict(shipment, event_id=str(uuid4()), status='received',
                    event_time='2026-11-03T11:00:00Z')
        process(spark, spark.createDataFrame([row(late, 7)], schema), 1, directory, '',
                routes_path, publish=False)
        assert spark.read.format('delta').load(paths.silver_current_shipments).filter(
            "shipment_id = 'S1'").first()['status'] == 'in_transit'

        # Reloj técnico sin eventos nuevos: la promesa vencida debe generar riesgo alto.
        tick = spark.createDataFrame(
            [(None, None, None, None, None, now+timedelta(hours=10), True)], schema)
        process(spark, tick, 2, directory, '', routes_path, publish=False)
        assert spark.read.format('delta').load(paths.silver_current_shipments).filter(
            "shipment_id = 'S1'").first()['risk_level'] == 'high'
        refreshed_fact = spark.read.format('delta').load(fact_path)
        assert refreshed_fact.count() == 3
        assert refreshed_fact.filter("shipment_id = 'S1'").first()['is_late'] is True
        refreshed_indicators = spark.read.format('delta').load(
            paths.gold_table('indicadores_cumplimiento_diario')).first()
        assert refreshed_indicators['late_shipments'] == 2
        assert refreshed_indicators['high_risk_shipments'] == 1
        print('OK: Bronze/Silver/Gold, estrella, indicadores, MERGE idempotente, '
              'cuarentena, evento tardío y reloj')
finally:
    spark.stop()
