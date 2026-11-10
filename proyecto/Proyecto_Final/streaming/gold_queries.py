"""Run the reviewed Gold compliance queries against Delta tables."""
import argparse
import re
from pathlib import Path

from streaming.lakehouse import DIMENSION_TABLES, FACT_TABLE, KPI_TABLE, LakehousePaths


DEFAULT_QUERY_FILE = Path(__file__).parent.parent / "queries" / "gold_compliance.sql"


def load_queries(path):
    """Load SQL statements delimited by ``-- name: query_name`` markers."""
    parts = re.split(r"^-- name:\s*([a-z][a-z0-9_]*)\s*$",
                     Path(path).read_text(encoding="utf-8"), flags=re.MULTILINE)
    queries = {}
    for index in range(1, len(parts), 2):
        statement = parts[index + 1].strip().removesuffix(";").strip()
        if statement:
            queries[parts[index]] = statement
    if not queries:
        raise ValueError(f"No named SQL queries found in {path}")
    return queries


def register_gold_views(spark, paths):
    """Expose path-based Delta tables as session-scoped SQL views."""
    for table_name in (*DIMENSION_TABLES, FACT_TABLE, KPI_TABLE):
        (spark.read.format("delta").load(paths.gold_table(table_name))
         .createOrReplaceTempView(table_name))


def main():
    from pyspark.sql import SparkSession

    parser = argparse.ArgumentParser(description="Consultas de cumplimiento de la capa Gold")
    parser.add_argument("--data", default="/data/streaming")
    parser.add_argument("--query-file", default=str(DEFAULT_QUERY_FILE))
    parser.add_argument("--query", default="all",
                        help="Nombre marcado en el SQL o 'all'")
    parser.add_argument("--limit", type=int, default=100)
    args = parser.parse_args()
    if args.limit < 1:
        parser.error("--limit debe ser mayor que cero")

    queries = load_queries(args.query_file)
    if args.query != "all" and args.query not in queries:
        parser.error(f"Consulta desconocida. Disponibles: {', '.join(queries)}")

    spark = (SparkSession.builder.appName("SID-Gold-Queries")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
             .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
             .getOrCreate())
    try:
        register_gold_views(spark, LakehousePaths(args.data))
        selected = queries.items() if args.query == "all" else [(args.query, queries[args.query])]
        for query_name, statement in selected:
            print(f"\n=== {query_name} ===", flush=True)
            spark.sql(statement).show(args.limit, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
