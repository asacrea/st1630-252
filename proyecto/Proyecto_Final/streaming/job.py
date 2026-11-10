"""Kafka -> Bronze -> Silver -> operational state -> dimensional Gold."""
import argparse
import os

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F, Window

from streaming.delta_io import merge_insert, merge_upsert
from streaming.gold import update_gold
from streaming.lakehouse import LakehousePaths
from streaming.transforms import parse, assess


def process(spark, batch, batch_id, root, brokers, routes_path, publish=True):
    paths = LakehousePaths(root)
    actual = batch.filter('NOT clock_tick').drop('clock_tick').cache()
    try:
        # Bronze conserva el registro Kafka original y sus metadatos, incluso si es inválido.
        raw = actual.dropDuplicates(['topic', 'partition', 'offset'])
        merge_insert(spark, raw, paths.bronze_events,
                     'target.topic=source.topic AND target.partition=source.partition '
                     'AND target.offset=source.offset')
        parsed = parse(raw).cache()
        try:
            rejected = parsed.filter("validation_error != ''").select(*raw.columns, 'validation_error')
            merge_insert(spark, rejected, paths.silver_rejected_events,
                         'target.topic=source.topic AND target.partition=source.partition '
                         'AND target.offset=source.offset')
            valid = parsed.filter("validation_error = ''").drop('validation_error')
            # Deduplicación global persistida: también cubre reenvíos fuera del watermark.
            first_seen = Window.partitionBy('event_id').orderBy(
                'kafka_timestamp', 'topic', 'partition', 'offset')
            unique = (valid.withColumn('_first', F.row_number().over(first_seen))
                      .filter('_first = 1').drop('_first'))
            merge_insert(spark, unique, paths.silver_events,
                         'target.event_id=source.event_id')
        finally:
            parsed.unpersist()
        if not DeltaTable.isDeltaTable(spark, paths.silver_events):
            return
        events = spark.read.format('delta').load(paths.silver_events)
        shipments = events.filter("event_type = 'shipment_event'").drop(
            'latitude', 'longitude', 'speed_kmh')
        gps = events.filter("event_type = 'vehicle_gps'")
        route_schema = ('route_id string, destination_latitude double, '
                        'destination_longitude double, expected_speed_kmh double')
        routes = spark.read.schema(route_schema).json(routes_path)
        invalid_routes = routes.filter(
            'route_id IS NULL OR destination_latitude IS NULL OR '
            'destination_longitude IS NULL OR expected_speed_kmh IS NULL OR '
            'abs(destination_latitude) > 90 OR abs(destination_longitude) > 180 OR '
            'expected_speed_kmh <= 0')
        duplicated_routes = routes.groupBy('route_id').count().filter('count > 1')
        if not invalid_routes.isEmpty() or not duplicated_routes.isEmpty():
            raise ValueError('Catálogo de rutas inválido o con identificadores duplicados')
        # Instante estable del microbatch para reintentos y evaluación durante silencio de Kafka.
        now = batch.agg(F.max('kafka_timestamp')).first()[0]
        decisions = assess(shipments, gps, routes, now).cache()
        try:
            current_path = paths.silver_current_shipments
            changes = decisions
            if DeltaTable.isDeltaTable(spark, current_path):
                previous = spark.read.format('delta').load(current_path).select(
                    'shipment_id', 'output_event_id')
                changes = decisions.join(previous, ['shipment_id', 'output_event_id'], 'left_anti')
            if not changes.isEmpty():
                # Publicar primero: si cae antes del MERGE, el reintento repite el mismo UUID.
                if publish:
                    (kafka_rows(changes, now).write.format('kafka')
                     .option('kafka.bootstrap.servers', brokers)
                     .option('topic', 'operational_updates')
                     .option('kafka.acks', 'all').save())
            merge_upsert(spark, decisions, current_path,
                         'target.shipment_id=source.shipment_id')
            update_gold(spark, shipments, decisions, now, paths)
            print(f'Batch {batch_id}: Silver y Gold actualizados', flush=True)
        finally:
            decisions.unpersist()
    finally:
        actual.unpersist()


def kafka_rows(changes, now):
    return changes.select(F.col('shipment_id').alias('key'), F.to_json(F.struct(
        F.lit('1.0').alias('schema_version'), F.col('output_event_id').alias('event_id'),
        F.lit('operational_update').alias('event_type'),
        F.date_format(F.lit(now), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias('event_time'),
        'shipment_id', 'vehicle_id', 'route_id', 'status', 'promised_delivery',
        'active_alert', 'risk_level', 'possible_delay_reason', 'distance_km', 'estimated_minutes',
        'inactive_minutes', F.col('event_id').alias('shipment_event_id'), 'gps_event_id'
    )).alias('value'))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--data', default=os.getenv('SPARK_DATA_DIR', '/data/streaming'))
    parser.add_argument('--routes', default='/app/config/routes.jsonl')
    parser.add_argument('--trigger', default='10 seconds')
    args = parser.parse_args()
    spark = (SparkSession.builder.appName('SID-Streaming').config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.databricks.delta.snapshotPartitions', '2')
        .config('spark.sql.shuffle.partitions', '4').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    brokers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:29092')
    paths = LakehousePaths(args.data)
    kafka = (spark.readStream.format('kafka').option('kafka.bootstrap.servers', brokers)
        .option('subscribe', 'shipment_events,vehicle_gps').option('startingOffsets', 'earliest')
        .option('failOnDataLoss', 'true').option('maxOffsetsPerTrigger', 10000).load()
        .selectExpr('CAST(key AS STRING) key', 'CAST(value AS STRING) raw_json', 'topic',
                    'partition', 'offset', 'timestamp AS kafka_timestamp', 'false AS clock_tick'))
    # Reloj técnico: obliga a reevaluar riesgo incluso si no hay movimientos nuevos.
    # No crea eventos de negocio y nunca se almacena en las tablas de eventos.
    clock = spark.readStream.format('rate').option('rowsPerSecond', 1).load().selectExpr(
        'CAST(NULL AS STRING) key', 'CAST(NULL AS STRING) raw_json', 'CAST(NULL AS STRING) topic',
        'CAST(NULL AS INT) partition', 'CAST(NULL AS LONG) offset',
        'timestamp AS kafka_timestamp', 'true AS clock_tick')
    query = (kafka.unionByName(clock).writeStream.queryName('sid_streaming').option(
        'checkpointLocation', paths.checkpoint).trigger(processingTime=args.trigger)
        .foreachBatch(lambda batch, batch_id: process(
            spark, batch, batch_id, args.data, brokers, args.routes)).start())
    query.awaitTermination()


if __name__ == '__main__':
    main()
