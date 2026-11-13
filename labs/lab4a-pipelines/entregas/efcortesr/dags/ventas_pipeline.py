"""DAG diario de ventas Bronze → Silver → Gold para el Lab 4a."""

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ─────────────────────────────────────────────────────────────
# Rutas dentro del contenedor (dadas por el docker-compose.yml de este
# lab -- no las edites, ya están montadas correctamente)
# ─────────────────────────────────────────────────────────────
RAW_CSV = "/opt/airflow/data/raw/ventas_colombia_lab4a.csv"
BRONZE_DIR = "/opt/airflow/data/output/bronze/ventas_partitioned"
SILVER_DIR = "/opt/airflow/data/output/silver/ventas"
GOLD_REGION_DIR = "/opt/airflow/data/output/gold/ventas_por_region"
GOLD_TOP_PRODUCTOS_DIR = "/opt/airflow/data/output/gold/top_productos"

logger = logging.getLogger(__name__)


def _spark_session(app_name: str):
    """Dado -- crea la SparkSession con Delta Lake configurado. Mismo
    patrón que viste en ../../lab1b-batch/scripts/*.py, pero en
    local[2] en vez de contra un clúster EMR: acá Airflow corre en un
    solo contenedor, sin un clúster de Spark separado detrás."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")  # dataset chico + un solo contenedor, no 32 como en EMR
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


# ═══════════════════════════════════════════════════════════════
# Task 1 · ingest_bronze
# ═══════════════════════════════════════════════════════════════
def ingest_bronze(ds: str, **context):
    """Simula la llegada diaria de datos: filtra el CSV histórico por
    la fecha de ejecución del DAG (`ds`, ej. "2026-01-03") y la escribe
    como la partición Bronze de ESE día.

    Dado: la lectura del CSV, el conteo, y el manejo del caso "no hay
    datos para esta fecha". Tu trabajo: la escritura idempotente.
    """
    from pyspark.sql import functions as F

    spark = _spark_session("lab4a-ingest-bronze")
    try:
        df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(RAW_CSV)
        df_dia = df_raw.filter(F.col("fecha") == ds)

        n_filas = df_dia.count()
        if n_filas == 0:
            # No hay dato para esta fecha -- no es un error, es un día
            # sin ventas (o una fecha fuera del rango del dataset, p.
            # ej. si programas un backfill más allá de 2026-01-10). El
            # DAG debe seguir corriendo, no fallar.
            logger.info("No hay filas para fecha=%s -- nada que ingestar hoy.", ds)
            context["ti"].xcom_push(key="n_filas_bronze", value=0)
            return

        destino = f"{BRONZE_DIR}/fecha={ds}"

        # La ruta incluye ds: overwrite reemplaza únicamente ese día.
        df_dia.write.mode("overwrite").parquet(destino)

        logger.info("Bronze escrito en %s (%s filas)", destino, f"{n_filas:,}")
        context["ti"].xcom_push(key="n_filas_bronze", value=n_filas)
    finally:
        spark.stop()


# ═══════════════════════════════════════════════════════════════
# Task 2 · validate_bronze -- data quality checks
# ═══════════════════════════════════════════════════════════════
# Cada check vive en su propia función (no todo suelto dentro de
# validate_bronze) a propósito: así dags/tests/test_ventas_pipeline.py
# puede importarlas y probarlas una por una, sin tener que levantar un
# DAG completo de Airflow para verificar tu lógica.

COLUMNAS_CRITICAS = ["pedido_id", "producto", "total"]
UMBRAL_NULOS = 0.05  # 5%
CANALES_ESPERADOS_MAX = 4


def check_nulos_criticos(df):
    """Falla cuando alguna columna crítica supera el 5 % de nulos."""
    from pyspark.sql import functions as F

    total_filas = df.count()
    if total_filas == 0:
        return

    for columna in COLUMNAS_CRITICAS:
        n_nulos = df.filter(F.col(columna).isNull()).count()
        porcentaje_nulos = n_nulos / total_filas
        if porcentaje_nulos > UMBRAL_NULOS:
            raise ValueError(
                f"La columna {columna} tiene {porcentaje_nulos:.2%} de nulos "
                f"({n_nulos}/{total_filas}); supera el umbral de {UMBRAL_NULOS:.0%}."
            )


def check_rango_monto(df):
    """Falla si existe al menos una venta con monto no positivo."""
    from pyspark.sql import functions as F

    n_invalidos = df.filter(F.col("total") <= 0).count()
    if n_invalidos:
        raise ValueError(f"Se encontraron {n_invalidos} ventas con total <= 0.")


def check_cardinalidad_canal(df):
    """Dado -- este check es un AVISO (logging.warning), no lanza
    excepción: que 'canal' tenga más de CANALES_ESPERADOS_MAX valores
    distintos no necesariamente significa datos corruptos (puede ser
    un canal nuevo legítimo del negocio), así que no debe tumbar el
    pipeline por sí solo. Contraste a propósito con los dos checks de
    arriba, que sí fallan duro -- en dag_design.md tienes que justificar
    por qué este caso es distinto."""
    n_canales = df.select("canal").distinct().count()
    if n_canales > CANALES_ESPERADOS_MAX:
        logger.warning(
            "canal tiene %s valores distintos (se esperaban %s) -- revisa "
            "si apareció un canal nuevo o es un dato corrupto.",
            n_canales, CANALES_ESPERADOS_MAX,
        )


def validate_bronze(ds: str, **context):
    """Dado -- orquesta los 3 checks de arriba sobre la partición
    Bronze de ds. Si ingest_bronze no escribió nada hoy, no hay nada
    que validar. Si cualquiera de los dos checks "duros" lanza
    ValueError, esta task falla, Airflow la marca en rojo, y
    transform_silver/build_gold NO se ejecutan hoy."""
    n_filas_bronze = context["ti"].xcom_pull(task_ids="ingest_bronze", key="n_filas_bronze")
    if n_filas_bronze == 0:
        logger.info("Sin datos Bronze para fecha=%s -- se omite la validación.", ds)
        return

    spark = _spark_session("lab4a-validate-bronze")
    try:
        df = spark.read.parquet(f"{BRONZE_DIR}/fecha={ds}")

        check_nulos_criticos(df)
        logger.info("Check nulos críticos: OK")

        check_rango_monto(df)
        logger.info("Check rango de monto: OK")

        check_cardinalidad_canal(df)
        logger.info("Check cardinalidad de canal: completado (ver warning arriba, si lo hubo)")
    finally:
        spark.stop()


# ═══════════════════════════════════════════════════════════════
# Task 3 · transform_silver
# ═══════════════════════════════════════════════════════════════
def transform_silver(ds: str, **context):
    """Toma la partición Bronze de ds, aplica las transformaciones de
    tipos/negocio, y la integra a Silver -- esa integración es la
    idempotencia real de esta semana: si vuelves a correr este task
    para el mismo ds (reintento, o backfill repetido), Silver NO debe
    crecer ni duplicarse."""
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    n_filas_bronze = context["ti"].xcom_pull(task_ids="ingest_bronze", key="n_filas_bronze")
    if n_filas_bronze == 0:
        logger.info("Sin datos Bronze para fecha=%s -- se omite transform_silver.", ds)
        return

    spark = _spark_session("lab4a-transform-silver")
    try:
        df_bronze = spark.read.parquet(f"{BRONZE_DIR}/fecha={ds}")

        # Tipos explícitos y exclusión de devoluciones antes del MERGE.
        df_transformado = (
            df_bronze
            .withColumn("fecha", F.to_date(F.col("fecha")))
            .withColumn("total", F.col("total").cast("double"))
            .filter(F.col("devuelto") == False)
            .withColumn("anio_mes", F.date_format(F.col("fecha"), "yyyy-MM"))
        )

        n_tras_filtro = df_transformado.count()
        logger.info(
            "transform_silver %s: %s filas Bronze -> %s filas tras excluir devueltos",
            ds, f"{n_filas_bronze:,}", f"{n_tras_filtro:,}",
        )

        # pedido_id es la clave de negocio; reintentar actualiza la fila existente.
        if DeltaTable.isDeltaTable(spark, SILVER_DIR):
            silver_table = DeltaTable.forPath(spark, SILVER_DIR)
            (
                silver_table.alias("target")
                .merge(
                    df_transformado.alias("source"),
                    "target.pedido_id = source.pedido_id",
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            logger.info("Silver no existe todavía -- primera escritura (sin MERGE).")
            df_transformado.write.format("delta").mode("overwrite").save(SILVER_DIR)

        df_silver_actual = spark.read.format("delta").load(SILVER_DIR)
        logger.info("Silver tiene ahora %s filas en total.", f"{df_silver_actual.count():,}")
    finally:
        spark.stop()


# ═══════════════════════════════════════════════════════════════
# Task 4 · build_gold
# ═══════════════════════════════════════════════════════════════
def build_gold(**context):
    """Lee TODO Silver (no solo el día de hoy -- Gold es una foto
    acumulada de todo el histórico) y recalcula las 2 tablas de Gold
    desde cero. Como Gold se sobreescribe completo en cada corrida, es
    idempotente "gratis": el resultado depende solo del estado actual
    de Silver, sin importar cuántas veces corras este task."""
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable

    spark = _spark_session("lab4a-build-gold")
    try:
        if not DeltaTable.isDeltaTable(spark, SILVER_DIR):
            logger.info("Sin datos Silver previos: no hay tablas Gold para actualizar.")
            return

        df_silver = spark.read.format("delta").load(SILVER_DIR)

        # Gold se recalcula completo a partir de Silver.
        df_ventas_por_region = df_silver.groupBy("region", "anio_mes").agg(
            F.sum("total").alias("total_ventas"),
            F.count("*").alias("n_transacciones"),
        )

        df_top_productos = (
            df_silver.groupBy("producto")
            .agg(F.sum("total").alias("total_ventas"))
            .orderBy(F.desc("total_ventas"))
            .limit(20)
        )

        df_ventas_por_region.write.format("delta").mode("overwrite").save(GOLD_REGION_DIR)
        df_top_productos.write.format("delta").mode("overwrite").save(GOLD_TOP_PRODUCTOS_DIR)

        n_regiones = df_ventas_por_region.select("region").distinct().count()
        total_recaudado = df_silver.agg(F.sum("total")).first()[0] or 0
        top1 = df_top_productos.orderBy(F.desc("total_ventas")).first()

        logger.info(
            "Gold actualizado: %s regiones, total recaudado=$%s, producto #1=%s",
            n_regiones, f"{total_recaudado:,.0f}", top1["producto"] if top1 else "N/A",
        )
    finally:
        spark.stop()


# ═══════════════════════════════════════════════════════════════
# Definición del DAG (dado)
# ═══════════════════════════════════════════════════════════════
dag = DAG(
    dag_id="ventas_colombia_pipeline",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    # catchup=False: al activar el DAG por primera vez, Airflow NO
    # re-ejecuta automáticamente todas las fechas pasadas desde
    # start_date -- solo corre hacia adelante desde que lo activaste.
    # Justifica en dag_design.md qué pasaría distinto con catchup=True.
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "owner": "st1630",
    },
    tags=["st1630", "lab4a", "ventas"],
)

ingest_bronze_task = PythonOperator(
    task_id="ingest_bronze",
    python_callable=ingest_bronze,
    dag=dag,
)

validate_bronze_task = PythonOperator(
    task_id="validate_bronze",
    python_callable=validate_bronze,
    dag=dag,
)

transform_silver_task = PythonOperator(
    task_id="transform_silver",
    python_callable=transform_silver,
    dag=dag,
)

build_gold_task = PythonOperator(
    task_id="build_gold",
    python_callable=build_gold,
    dag=dag,
)

ingest_bronze_task >> validate_bronze_task >> transform_silver_task >> build_gold_task
