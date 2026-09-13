#!/usr/bin/env python3
"""Verify that Spark on EMR can read the Lab 1a datalake in S3."""

import sys
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def benchmark(df, label):
    start = time.time()
    result = (
        df.filter((F.col("region") == "Bogotá") & (F.col("categoria") == "Electrónica"))
        .groupBy("canal")
        .agg(F.sum("total").alias("total_vendido"))
        .orderBy("canal")
        .collect()
    )
    elapsed = time.time() - start
    return elapsed, result


def main():
    if len(sys.argv) != 2:
        raise SystemExit("Usage: verificar_spark_s3.py <bucket-name>")

    bucket = sys.argv[1]
    parquet_path = f"s3://{bucket}/bronze/ventas/prueba_parquet.parquet"
    csv_path = f"s3://{bucket}/bronze/ventas/prueba_csv.csv"
    output_path = f"s3://{bucket}/gold/verificacion_lab1a"

    spark = SparkSession.builder.appName("ST1630-Lab1a-S3-Verification").getOrCreate()

    df_parquet = spark.read.parquet(parquet_path)
    df_csv = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

    parquet_count = df_parquet.count()
    csv_count = df_csv.count()
    parquet_elapsed, parquet_result = benchmark(df_parquet, "parquet")
    csv_elapsed, csv_result = benchmark(df_csv, "csv")

    rows = [
        "ST1630 Lab 1a Spark verification",
        f"bucket={bucket}",
        f"parquet_path={parquet_path}",
        f"csv_path={csv_path}",
        f"parquet_count={parquet_count}",
        f"csv_count={csv_count}",
        f"parquet_seconds={parquet_elapsed:.4f}",
        f"csv_seconds={csv_elapsed:.4f}",
        f"csv_vs_parquet_ratio={csv_elapsed / parquet_elapsed:.2f}",
        f"parquet_result={[(row['canal'], row['total_vendido']) for row in parquet_result]}",
        f"csv_result={[(row['canal'], row['total_vendido']) for row in csv_result]}",
    ]

    spark.sparkContext.parallelize(rows, 1).saveAsTextFile(output_path)
    print("\n".join(rows))
    spark.stop()


if __name__ == "__main__":
    main()
