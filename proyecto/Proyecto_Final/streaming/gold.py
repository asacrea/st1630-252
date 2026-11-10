"""Dimensional Gold model and delivery-compliance indicators."""
from pyspark.sql import Window, functions as F

from streaming.delta_io import merge_insert, merge_upsert, replace_table
from streaming.lakehouse import FACT_TABLE, KPI_TABLE

SHIPMENT_STATUSES = (
    ("received", "Recibido", 1, False),
    ("departed_center", "Salió del centro", 2, False),
    ("in_transit", "En tránsito", 3, False),
    ("arrived_center", "Llegó al centro", 4, False),
    ("out_for_delivery", "En reparto", 5, False),
    ("delivered", "Entregado", 6, True),
)


def _key(prefix, column):
    normalized = F.coalesce(F.trim(column.cast("string")), F.lit("__unknown__"))
    return F.sha2(F.concat_ws(":", F.lit(prefix), normalized), 256)


def _latest(frame, identity):
    window = Window.partitionBy(identity).orderBy(
        F.col("event_timestamp").desc(), F.col("event_id").desc())
    return frame.withColumn("_row_number", F.row_number().over(window)).filter(
        "_row_number = 1").drop("_row_number")


def build_dimensions(spark, shipment_events):
    """Create Type-1 dimensions from normalized Silver shipment events."""
    latest_routes = _latest(shipment_events, "route_id")
    dim_route = latest_routes.select(
        _key("route", F.col("route_id")).alias("route_key"),
        "route_id",
        _key("city", F.col("origin_city")).alias("origin_city_key"),
        _key("city", F.col("destination_city")).alias("destination_city_key"),
        "origin_city",
        "destination_city",
    )

    cities = (shipment_events.select(F.col("origin_city").alias("city_name"))
              .union(shipment_events.select(F.col("destination_city").alias("city_name")))
              .filter("city_name IS NOT NULL AND city_name <> ''").distinct())
    dim_city = cities.select(
        _key("city", F.col("city_name")).alias("city_key"), "city_name")

    latest_centers = _latest(shipment_events.filter("center_id IS NOT NULL"), "center_id")
    center_city = F.when(
        F.col("status").isin("received", "departed_center"), F.col("origin_city")
    ).otherwise(F.col("destination_city"))
    dim_center = latest_centers.select(
        _key("center", F.col("center_id")).alias("center_key"),
        "center_id",
        center_city.alias("city_name"),
        _key("city", center_city).alias("city_key"),
    )

    latest_vehicles = _latest(shipment_events, "vehicle_id")
    dim_vehicle = latest_vehicles.select(
        _key("vehicle", F.col("vehicle_id")).alias("vehicle_key"),
        "vehicle_id",
        _key("route", F.col("route_id")).alias("current_route_key"),
    )

    dim_status = spark.createDataFrame(
        SHIPMENT_STATUSES,
        "status_code string, status_name string, status_order int, is_terminal boolean",
    ).select(
        _key("status", F.col("status_code")).alias("status_key"), "*"
    )

    dates = (shipment_events.select(F.to_date("event_timestamp").alias("calendar_date"))
             .union(shipment_events.select(
                 F.to_date(F.to_timestamp("promised_delivery")).alias("calendar_date")))
             .filter("calendar_date IS NOT NULL").distinct())
    dim_date = dates.select(
        F.date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
        "calendar_date",
        F.year("calendar_date").alias("year"),
        F.quarter("calendar_date").alias("quarter"),
        F.month("calendar_date").alias("month"),
        F.dayofmonth("calendar_date").alias("day"),
        F.dayofweek("calendar_date").alias("day_of_week"),
        F.weekofyear("calendar_date").alias("week_of_year"),
    )

    return {
        "dim_fecha": dim_date,
        "dim_ruta": dim_route,
        "dim_ciudad": dim_city,
        "dim_centro_logistico": dim_center,
        "dim_vehiculo": dim_vehicle,
        "dim_estado_envio": dim_status,
    }


def build_shipment_fact(shipment_events, current_shipments, evaluation_time):
    """Build one fact row per shipment from its complete Silver event history."""
    event_order = Window.partitionBy("shipment_id").orderBy("event_timestamp", "event_id")
    ordered = (shipment_events
               .withColumn("_previous_event_time", F.lag("event_timestamp").over(event_order))
               .withColumn("_previous_center_id", F.lag("center_id").over(event_order)))
    known_dwell = F.when(
        (F.col("center_id") == F.col("_previous_center_id")) &
        F.col("_previous_event_time").isNotNull(),
        (F.col("event_timestamp").cast("long") -
         F.col("_previous_event_time").cast("long")) / F.lit(60.0),
    ).otherwise(F.lit(0.0))

    history = ordered.groupBy("shipment_id").agg(
        F.min("event_timestamp").alias("first_event_time"),
        F.max("event_timestamp").alias("last_event_time"),
        F.max(F.to_timestamp("promised_delivery")).alias("promised_delivery"),
        F.min(F.when(F.col("status") == "delivered", F.col("event_timestamp"))).alias(
            "delivered_at"),
        F.countDistinct("event_id").cast("long").alias("movement_count"),
        F.sum(known_dwell).alias("known_center_dwell_minutes"),
    )

    current = current_shipments.select(
        "shipment_id", "vehicle_id", "route_id", "center_id", "origin_city",
        "destination_city", "status", "risk_level", "active_alert",
        "possible_delay_reason", "distance_km", "estimated_minutes", "inactive_minutes",
        F.col("event_id").alias("source_shipment_event_id"),
        F.col("gps_event_id").alias("source_gps_event_id"),
        F.col("output_event_id").alias("source_operational_event_id"),
    )
    fact = history.join(current, "shipment_id", "inner")
    evaluation = F.lit(evaluation_time).cast("timestamp")
    delivered = F.col("delivered_at").isNotNull()
    late = ((delivered & (F.col("delivered_at") > F.col("promised_delivery"))) |
            (~delivered & (evaluation > F.col("promised_delivery"))))
    effective_end = F.when(delivered, F.col("delivered_at")).otherwise(evaluation)

    return fact.select(
        _key("shipment", F.col("shipment_id")).alias("shipment_key"),
        "shipment_id",
        F.date_format("promised_delivery", "yyyyMMdd").cast("int").alias(
            "promised_date_key"),
        _key("route", F.col("route_id")).alias("route_key"),
        _key("city", F.col("origin_city")).alias("origin_city_key"),
        _key("city", F.col("destination_city")).alias("destination_city_key"),
        _key("center", F.col("center_id")).alias("center_key"),
        _key("vehicle", F.col("vehicle_id")).alias("vehicle_key"),
        _key("status", F.col("status")).alias("status_key"),
        "first_event_time", "last_event_time", "promised_delivery", "delivered_at",
        ((F.col("promised_delivery").cast("long") - F.col("first_event_time").cast("long")) /
         F.lit(60.0)).alias("promised_delivery_minutes"),
        F.when(delivered,
               (F.col("delivered_at").cast("long") - F.col("first_event_time").cast("long")) /
               F.lit(60.0)).alias("actual_delivery_minutes"),
        F.greatest(
            (effective_end.cast("long") - F.col("promised_delivery").cast("long")) /
            F.lit(60.0), F.lit(0.0)).alias("delay_minutes"),
        "known_center_dwell_minutes", "movement_count",
        delivered.alias("is_delivered"),
        late.alias("is_late"),
        F.when(delivered, ~late).cast("boolean").alias("is_on_time"),
        "risk_level",
        F.when(F.col("risk_level") == "high", 3)
        .when(F.col("risk_level") == "medium", 2).otherwise(1).alias("risk_score"),
        "active_alert", "possible_delay_reason", "distance_km", "estimated_minutes",
        "inactive_minutes", "source_shipment_event_id", "source_gps_event_id",
        "source_operational_event_id", evaluation.alias("evaluated_at"),
        F.current_timestamp().alias("gold_updated_at"),
    )


def build_compliance_indicators(fact):
    """Aggregate dashboard indicators by promised day and route."""
    grouped = fact.groupBy("promised_date_key", "route_key").agg(
        F.countDistinct("shipment_key").cast("long").alias("total_shipments"),
        F.sum(F.col("is_delivered").cast("long")).alias("delivered_shipments"),
        F.sum(F.coalesce(F.col("is_on_time").cast("long"), F.lit(0))).alias(
            "on_time_shipments"),
        F.sum(F.col("is_late").cast("long")).alias("late_shipments"),
        F.sum((F.col("risk_level") == "high").cast("long")).alias("high_risk_shipments"),
        F.avg(F.when(F.col("is_delivered"), F.col("delay_minutes"))).alias(
            "average_delivery_delay_minutes"),
        F.max("known_center_dwell_minutes").alias("maximum_known_center_dwell_minutes"),
        F.max("evaluated_at").alias("evaluated_at"),
    )
    return grouped.withColumn(
        "on_time_delivery_rate",
        F.when(F.col("delivered_shipments") > 0,
               F.round(F.col("on_time_shipments") * 100.0 /
                       F.col("delivered_shipments"), 2)),
    )


def update_gold(spark, shipment_events, current_shipments, evaluation_time, paths):
    """Idempotently materialize all Gold dimensions, facts, and indicators."""
    if shipment_events.isEmpty() or current_shipments.isEmpty():
        return

    dimensions = build_dimensions(spark, shipment_events)
    merge_insert(spark, dimensions["dim_fecha"], paths.gold_table("dim_fecha"),
                 "target.date_key = source.date_key")
    for table_name, business_key in (
        ("dim_ruta", "route_key"),
        ("dim_ciudad", "city_key"),
        ("dim_centro_logistico", "center_key"),
        ("dim_vehiculo", "vehicle_key"),
        ("dim_estado_envio", "status_key"),
    ):
        merge_upsert(spark, dimensions[table_name], paths.gold_table(table_name),
                     f"target.{business_key} = source.{business_key}")

    incoming_fact = build_shipment_fact(shipment_events, current_shipments, evaluation_time)
    fact_path = paths.gold_table(FACT_TABLE)
    merge_upsert(spark, incoming_fact, fact_path,
                 "target.shipment_key = source.shipment_key")
    complete_fact = spark.read.format("delta").load(fact_path)
    replace_table(build_compliance_indicators(complete_fact), paths.gold_table(KPI_TABLE))
