#!/usr/bin/env python3
"""04_athena_benchmark.py — Lab 1b (ST1630-2026-2, S5-S6)

Ejecuta las consultas de la Parte 5 contra Athena usando boto3, mide
tiempo de ejecución y bytes escaneados, y escribe los resultados en
../benchmark_resultados.md.

Este script no se ejecuta en el clúster EMR. Se ejecuta localmente o
en un notebook con boto3 y credenciales temporales de AWS Academy.

Uso:
    python3 04_athena_benchmark.py

Dependencias:
    pip install boto3
"""

import time
from pathlib import Path

import boto3


# ─────────────────────────────────────────────────────────────
# Configuración del laboratorio
# ─────────────────────────────────────────────────────────────
REGION = "us-east-1"  # Región donde existen Athena, Glue y el bucket.
BUCKET = "st1630-ssalazarh3-2026"
ATHENA_DATABASE = "default"  # Base donde Gold fue registrado en Glue.

# Athena siempre necesita una ubicación S3 donde escribir el resultado
# tabular y los metadatos de cada ejecución, aunque el cliente solo use
# las estadísticas devueltas por la API.
ATHENA_OUTPUT = f"s3://{BUCKET}/athena-results/"

# Esta ruta debe contener previamente la muestra exportada por
# 03_gold.py. Crear una tabla externa no crea ni copia los datos.
CSV_10K_LOCATION = f"s3://{BUCKET}/benchmark/csv_10k/"

INTERVALO_POLLING_SEGUNDOS = 1
TIMEOUT_QUERY_SEGUNDOS = 300

# boto3 busca las credenciales en la cadena estándar de AWS (variables
# de entorno, ~/.aws/credentials, perfil, rol, etc.). En AWS Academy
# deben estar vigentes Access Key, Secret Key y Session Token.
athena = boto3.client("athena", region_name=REGION)

# Si el archivo está en `scripts/`, el resultado queda en la raíz del
# repositorio, tal como lo solicita el laboratorio.
RESULTADOS_PATH = (
    Path(__file__).resolve().parent.parent / "benchmark_resultados.md"
)


def ejecutar_query(sql: str, nombre: str) -> dict:
    """Ejecuta una consulta en Athena y devuelve sus estadísticas.

    La función espera mediante polling hasta que Athena reporte un
    estado terminal. También aplica un timeout para evitar una espera
    infinita si la consulta queda bloqueada.
    """

    print(f"\nEjecutando '{nombre}'...")
    inicio = time.perf_counter()

    # Athena es asíncrono: esta llamada solo encola la consulta y
    # devuelve un identificador; no espera a que el SQL termine.
    respuesta = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )

    query_id = respuesta["QueryExecutionId"]

    # Polling: se consulta periódicamente el estado hasta llegar a uno
    # terminal. El intervalo evita saturar la API con solicitudes.
    while True:
        estado_respuesta = athena.get_query_execution(
            QueryExecutionId=query_id
        )
        estado = estado_respuesta["QueryExecution"]["Status"]["State"]

        if estado in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break

        duracion_actual = time.perf_counter() - inicio
        if duracion_actual > TIMEOUT_QUERY_SEGUNDOS:
            athena.stop_query_execution(QueryExecutionId=query_id)
            raise TimeoutError(
                f"La query '{nombre}' superó el límite de "
                f"{TIMEOUT_QUERY_SEGUNDOS} segundos y fue cancelada."
            )

        time.sleep(INTERVALO_POLLING_SEGUNDOS)

    # Wall time incluye cola, planificación, ejecución y latencia de las
    # llamadas API. No es lo mismo que EngineExecutionTimeInMillis.
    duracion_wall = time.perf_counter() - inicio

    if estado != "SUCCEEDED":
        razon = (
            estado_respuesta["QueryExecution"]["Status"]
            .get("StateChangeReason", "sin detalle")
        )
        raise RuntimeError(
            f"Query '{nombre}' terminó en estado {estado}: {razon}"
        )

    # DataScannedInBytes es la métrica central del benchmark: Athena
    # cobra y trabaja principalmente según los bytes leídos, no solo
    # según el número de filas devueltas.
    estadisticas = estado_respuesta["QueryExecution"].get(
        "Statistics", {}
    )
    bytes_escaneados = estadisticas.get("DataScannedInBytes", 0)
    tiempo_motor_ms = estadisticas.get("EngineExecutionTimeInMillis", 0)

    print(
        f"  Tiempo motor Athena: {tiempo_motor_ms} ms | "
        f"Tiempo total: {duracion_wall:.2f} s"
    )
    print(
        f"  Bytes escaneados: {bytes_escaneados:,} "
        f"({bytes_escaneados / (1024 ** 2):.2f} MB)"
    )

    return {
        "nombre": nombre,
        "query_id": query_id,
        "tiempo_motor_ms": tiempo_motor_ms,
        "tiempo_wall_s": round(duracion_wall, 2),
        "bytes_escaneados": bytes_escaneados,
    }


def asegurar_tabla_csv() -> None:
    """Crea la tabla externa CSV si aún no existe en Glue/Athena.

    La muestra de 10.000 registros debe haberse exportado previamente
    desde Silver hacia CSV_10K_LOCATION, con encabezado y exactamente
    las columnas declaradas aquí.
    """

    # IF NOT EXISTS hace el registro idempotente. El DDL describe cómo
    # interpretar los bytes ubicados en S3, pero no verifica que ya
    # exista un part-*.csv; esa exportación pertenece a 03_gold.py.
    ddl_csv = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS benchmark_csv_10k (
        pedido_id string,
        fecha date,
        region string,
        canal string,
        categoria string,
        producto string,
        cantidad int,
        precio_unit double,
        total_silver double
    )
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
    LOCATION '{CSV_10K_LOCATION}'
    TBLPROPERTIES ('skip.header.line.count'='1')
    """

    ejecutar_query(ddl_csv, "Preparación tabla benchmark_csv_10k")


def construir_markdown(resultados: list[dict], ratio: float) -> str:
    """Construye el contenido de benchmark_resultados.md."""

    contenido = """# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`

## Resultados crudos

| Query | Query ID | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---|---:|---:|---:|
"""

    # Se incluye el QueryExecutionId para que cada cifra pueda auditarse
    # posteriormente en la consola o la API de Athena.
    for resultado in resultados:
        contenido += (
            f"| {resultado['nombre']} | `{resultado['query_id']}` | "
            f"{resultado['tiempo_motor_ms']} | "
            f"{resultado['tiempo_wall_s']:.2f} | "
            f"{resultado['bytes_escaneados']:,} |\n"
        )

    contenido += f"""

## Ratio de bytes escaneados

**CSV / Parquet = {ratio:.2f}x**

> Usa este resultado para completar la Pregunta 5 de
> `pipeline_analysis.md`: ¿coincide con el orden de magnitud teórico
> aproximado de 9x visto en el slide de S4? Si no coincide, analiza el
> tamaño de la muestra, el efecto del formato columnar, la selectividad
> de la consulta y el Z-ordering.
"""

    return contenido


def main() -> None:
    resultados = []

    # La tabla CSV se registra automáticamente si todavía no existe.
    # Si la ruta está vacía, el DDL puede tener éxito pero la consulta no
    # tendrá filas; por eso 03_gold.py debe ejecutarse primero.
    asegurar_tabla_csv()

    # 5.1 · Top 5 regiones por ventas durante los últimos tres meses,
    # consultando la tabla Gold almacenada en Delta/Parquet y ordenada
    # físicamente por región y fecha. Gold ya está agregado por día y
    # región, así que Athena lee menos filas y solo tres columnas útiles.
    query_negocio = """
    SELECT
        region,
        SUM(ventas_totales) AS total_ventas
    FROM gold_ventas_region_fecha
    WHERE fecha >= date_add('month', -3, current_date)
    GROUP BY region
    ORDER BY total_ventas DESC
    LIMIT 5
    """

    resultados.append(
        ejecutar_query(
            query_negocio,
            "5.1 Top 5 regiones (Gold Parquet, Z-ordered)",
        )
    )

    # 5.2 · Misma pregunta de negocio sobre la muestra CSV sin
    # particionar. En CSV se suma total_silver fila por fila porque la
    # tabla no contiene la agregación ventas_totales de Gold. CSV es
    # orientado a filas y no puede omitir físicamente las columnas no
    # seleccionadas como lo hace Parquet.
    query_csv = """
    SELECT
        region,
        SUM(total_silver) AS total_ventas
    FROM benchmark_csv_10k
    WHERE fecha >= date_add('month', -3, current_date)
    GROUP BY region
    ORDER BY total_ventas DESC
    LIMIT 5
    """

    resultados.append(
        ejecutar_query(
            query_csv,
            "5.2 Misma query (CSV sin particionar)",
        )
    )

    bytes_parquet = resultados[0]["bytes_escaneados"]
    bytes_csv = resultados[1]["bytes_escaneados"]
    # El ratio > 1 indica que CSV leyó más bytes. No se interpreta aquí
    # porque sus causas (compresión, preagregación, data skipping,
    # tamaño de muestra) se discuten en pipeline_analysis.md.
    ratio = (
        bytes_csv / bytes_parquet
        if bytes_parquet > 0
        else float("inf")
    )

    print(
        "\n=== Ratio de bytes escaneados: "
        f"CSV / Parquet = {ratio:.2f}x ==="
    )

    contenido = construir_markdown(resultados, ratio)
    RESULTADOS_PATH.write_text(contenido, encoding="utf-8")

    print(f"\nResultados guardados en: {RESULTADOS_PATH}")


if __name__ == "__main__":
    main()

# Este script no usa el clúster EMR. Si ya terminaste todo el lab,
# apágalo para evitar consumir el tiempo disponible de AWS Academy:
# aws emr terminate-clusters --cluster-ids <tu-cluster-id> --region us-east-1