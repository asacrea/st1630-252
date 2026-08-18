#!/usr/bin/env python3
"""04_athena_benchmark.py — Lab 1b (ST1630-2026-2, S5-S6)

Ejecuta las queries de la Parte 5 del lab contra Athena usando boto3,
mide tiempo de ejecución y bytes escaneados, y escribe los resultados
en ../benchmark_resultados.md.

A diferencia de los scripts 00-03, este NO corre en el clúster EMR --
corre en tu máquina local (o en un notebook con boto3 disponible),
porque solo necesita hablar con la API de Athena, no con Spark.

Uso:
    python3 04_athena_benchmark.py

Dependencias: boto3 (pip install boto3), y credenciales de AWS Academy
ya configuradas (ver Lab 1a, Parte 1.2).

Qué puedes delegar aquí: el boilerplate de boto3 (polling de
get_query_execution, manejo de estados). Qué debes decidir tú: cómo
interpretar el ratio de bytes escaneados en benchmark_resultados.md
(pregunta 5 de pipeline_analysis.md) -- este script solo mide, no
interpreta.
"""

import time
from pathlib import Path

import boto3

# ─────────────────────────────────────────────────────────────
# EDITAR ANTES DE EJECUTAR
# ─────────────────────────────────────────────────────────────
REGION = "us-east-1"                     # EDITAR si tu región es otra
BUCKET = "st1630-tu-usuario"              # EDITAR: el mismo bucket del Lab 1a
ATHENA_DATABASE = "default"               # EDITAR si registraste las tablas en otra base
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"
CSV_10K_LOCATION = f"s3://{BUCKET}/benchmark/csv_10k/"  # ver Parte 5.2 del README
# ─────────────────────────────────────────────────────────────

athena = boto3.client("athena", region_name=REGION)

RESULTADOS_PATH = Path(__file__).resolve().parent.parent / "benchmark_resultados.md"


def ejecutar_query(sql: str, nombre: str) -> dict:
    """Lanza una query en Athena, espera a que termine, y devuelve
    tiempo de ejecución + bytes escaneados. Bloqueante (hace polling
    simple) -- para el volumen de este lab, la espera es de segundos."""
    print(f"\nEjecutando '{nombre}'...")
    inicio = time.time()

    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_id = resp["QueryExecutionId"]

    while True:
        estado_resp = athena.get_query_execution(QueryExecutionId=query_id)
        estado = estado_resp["QueryExecution"]["Status"]["State"]
        if estado in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    duracion_wall = time.time() - inicio

    if estado != "SUCCEEDED":
        razon = estado_resp["QueryExecution"]["Status"].get("StateChangeReason", "sin detalle")
        raise RuntimeError(f"Query '{nombre}' terminó en estado {estado}: {razon}")

    stats = estado_resp["QueryExecution"]["Statistics"]
    bytes_escaneados = stats.get("DataScannedInBytes", 0)
    tiempo_motor_ms = stats.get("EngineExecutionTimeInMillis", 0)

    print(f"  Tiempo motor Athena: {tiempo_motor_ms} ms  |  Tiempo total (wall): {duracion_wall:.2f} s")
    print(f"  Bytes escaneados: {bytes_escaneados:,} ({bytes_escaneados / (1024**2):.2f} MB)")

    return {
        "nombre": nombre,
        "query_id": query_id,
        "tiempo_motor_ms": tiempo_motor_ms,
        "tiempo_wall_s": round(duracion_wall, 2),
        "bytes_escaneados": bytes_escaneados,
    }


def main() -> None:
    resultados = []

    # ═══════════════════════════════════════════════════════════
    # TODO 5.1 · Query de negocio: Top 5 regiones por ventas del
    # último trimestre, sobre Gold en Parquet (con Z-ordering aplicado)
    # ═══════════════════════════════════════════════════════════
    # TODO: escribe el SQL (dialecto Presto/Athena) que responda:
    # "las 5 regiones con más ventas totales en los últimos 3 meses",
    # usando la tabla `gold_ventas_region_fecha` que registraste en
    # Glue Catalog (03_gold.py, Parte 4.5). Pistas de funciones útiles
    # de Presto: date_add('month', -3, current_date), GROUP BY, ORDER
    # BY ... DESC, LIMIT.
    query_negocio = """
        -- TODO: tu query aquí
    """
    resultados.append(ejecutar_query(query_negocio, "5.1 Top 5 regiones (Gold Parquet, Z-ordered)"))

    # ═══════════════════════════════════════════════════════════
    # 5.2 · Benchmark: misma query sobre Parquet (Gold) vs. CSV sin
    # particionar. La query de Parquet ya se ejecutó arriba -- aquí
    # se repite sobre la tabla externa CSV para comparar.
    #
    # Prerequisito (ver Parte 5.2 del README): debes haber creado la
    # tabla externa `benchmark_csv_10k` sobre una muestra de 10.000
    # filas de tu Silver, exportada a CSV sin particionar en
    # CSV_10K_LOCATION. El DDL de ejemplo:
    #
    #   CREATE EXTERNAL TABLE IF NOT EXISTS benchmark_csv_10k (
    #       pedido_id string, fecha date, region string, canal string,
    #       categoria string, producto string, cantidad int,
    #       precio_unit double, total_silver double
    #   )
    #   ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    #   LOCATION 's3://<tu-bucket>/benchmark/csv_10k/'
    #   TBLPROPERTIES ('skip.header.line.count'='1')
    # ═══════════════════════════════════════════════════════════
    # TODO: escribe la MISMA pregunta de negocio que en 5.1 (top 5
    # regiones por ventas), pero contra `benchmark_csv_10k`. Ojo: esa
    # tabla no tiene la columna `ventas_totales` ya agregada como Gold
    # -- tienes `total_silver` fila por fila, así que necesitas
    # SUM(total_silver) en vez de SUM(ventas_totales).
    query_csv = """
        -- TODO: tu query aquí
    """
    resultados.append(ejecutar_query(query_csv, "5.2 Misma query (CSV sin particionar)"))

    bytes_parquet = resultados[0]["bytes_escaneados"]
    bytes_csv = resultados[1]["bytes_escaneados"]
    ratio = bytes_csv / bytes_parquet if bytes_parquet > 0 else float("inf")

    print(f"\n=== Ratio de bytes escaneados: CSV / Parquet = {ratio:.2f}x ===")

    # ── Escribir benchmark_resultados.md ──────────────────────
    contenido = f"""# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`

## Resultados crudos

| Query | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---|---|---|
"""
    for r in resultados:
        contenido += f"| {r['nombre']} | {r['tiempo_motor_ms']} | {r['tiempo_wall_s']} | {r['bytes_escaneados']:,} |\n"

    contenido += f"""
## Ratio de bytes escaneados

**CSV / Parquet = {ratio:.2f}x**

> Completa la Pregunta 5 de `pipeline_analysis.md` con este número:
> ¿coincide con el orden de magnitud teórico (~9x) visto en el slide
> de S4? Si no coincide, ¿a qué se lo atribuyes -- tamaño de la
> muestra, efecto del Z-ordering, selectividad de la query?
"""

    RESULTADOS_PATH.write_text(contenido, encoding="utf-8")
    print(f"\nResultados guardados en: {RESULTADOS_PATH}")


if __name__ == "__main__":
    main()

# ### Este script no usa el clúster EMR (solo habla con Athena vía
# ### boto3), pero si ya terminaste TODO el lab, apágalo:
# ###   aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1
