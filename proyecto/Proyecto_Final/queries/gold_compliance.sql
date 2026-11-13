-- Las vistas se registran desde streaming/gold_queries.py sobre las tablas Delta.

-- name: overall_compliance
SELECT
  COUNT(*) AS total_shipments,
  SUM(CAST(is_delivered AS BIGINT)) AS delivered_shipments,
  SUM(COALESCE(CAST(is_on_time AS BIGINT), 0)) AS on_time_shipments,
  SUM(CAST(is_late AS BIGINT)) AS late_shipments,
  SUM(CAST(risk_level = 'high' AS BIGINT)) AS high_risk_shipments,
  ROUND(
    100.0 * SUM(COALESCE(CAST(is_on_time AS BIGINT), 0)) /
    NULLIF(SUM(CAST(is_delivered AS BIGINT)), 0),
    2
  ) AS on_time_delivery_rate
FROM fact_desempeno_envio;

-- name: compliance_by_route
SELECT
  route.route_id,
  route.origin_city,
  route.destination_city,
  COUNT(*) AS total_shipments,
  SUM(CAST(fact.is_delivered AS BIGINT)) AS delivered_shipments,
  SUM(CAST(fact.is_late AS BIGINT)) AS late_shipments,
  ROUND(
    100.0 * SUM(COALESCE(CAST(fact.is_on_time AS BIGINT), 0)) /
    NULLIF(SUM(CAST(fact.is_delivered AS BIGINT)), 0),
    2
  ) AS on_time_delivery_rate,
  ROUND(AVG(CASE WHEN fact.is_delivered THEN fact.delay_minutes END), 2)
    AS average_delivery_delay_minutes
FROM fact_desempeno_envio fact
JOIN dim_ruta route ON route.route_key = fact.route_key
GROUP BY route.route_id, route.origin_city, route.destination_city
ORDER BY late_shipments DESC, route.route_id;

-- name: late_shipments_by_destination
SELECT
  city.city_name AS destination_city,
  COUNT(*) AS total_shipments,
  SUM(CAST(fact.is_late AS BIGINT)) AS late_shipments,
  ROUND(100.0 * SUM(CAST(fact.is_late AS BIGINT)) / COUNT(*), 2) AS late_rate
FROM fact_desempeno_envio fact
JOIN dim_ciudad city ON city.city_key = fact.destination_city_key
GROUP BY city.city_name
ORDER BY late_shipments DESC, destination_city;

-- name: high_risk_backlog
SELECT
  fact.shipment_id,
  route.route_id,
  status.status_name,
  fact.promised_delivery,
  fact.delay_minutes,
  fact.inactive_minutes,
  fact.possible_delay_reason,
  fact.source_shipment_event_id,
  fact.source_gps_event_id,
  fact.source_operational_event_id
FROM fact_desempeno_envio fact
JOIN dim_ruta route ON route.route_key = fact.route_key
JOIN dim_estado_envio status ON status.status_key = fact.status_key
WHERE fact.risk_level = 'high' AND NOT fact.is_delivered
ORDER BY fact.delay_minutes DESC, fact.shipment_id;

-- name: center_dwell
SELECT
  center.center_id,
  center.city_name,
  COUNT(*) AS current_shipments,
  ROUND(AVG(fact.known_center_dwell_minutes), 2) AS average_known_dwell_minutes,
  ROUND(MAX(fact.known_center_dwell_minutes), 2) AS maximum_known_dwell_minutes
FROM fact_desempeno_envio fact
JOIN dim_centro_logistico center ON center.center_key = fact.center_key
GROUP BY center.center_id, center.city_name
ORDER BY maximum_known_dwell_minutes DESC, center.center_id;

-- name: daily_route_indicators
SELECT
  date.calendar_date,
  route.route_id,
  indicator.total_shipments,
  indicator.delivered_shipments,
  indicator.on_time_shipments,
  indicator.late_shipments,
  indicator.high_risk_shipments,
  indicator.on_time_delivery_rate,
  indicator.average_delivery_delay_minutes,
  indicator.maximum_known_center_dwell_minutes,
  indicator.evaluated_at
FROM indicadores_cumplimiento_diario indicator
JOIN dim_fecha date ON date.date_key = indicator.promised_date_key
JOIN dim_ruta route ON route.route_key = indicator.route_key
ORDER BY date.calendar_date DESC, route.route_id;
