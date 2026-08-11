#!/usr/bin/env python3
"""Genera los dos archivos de prueba del Lab 1a (ST1630-2026-2, S4).

Uso:
    python3 generar_datos.py

Dependencias: pandas, pyarrow (pip install pandas pyarrow)

Produce, en la misma carpeta de este script:
    - prueba_parquet.parquet  (formato columnar, compresión snappy)
    - prueba_csv.csv          (mismos datos, formato de fila)

Ambos archivos tienen las mismas 10.000 filas de ventas sintéticas
colombianas -- existen en dos formatos a propósito, para el benchmark
Parquet vs. CSV de la Parte 5 del lab (ver ../README.md).
"""

import random
from pathlib import Path

import pandas as pd

SEED = 1630
N_FILAS = 10_000
OUT_DIR = Path(__file__).resolve().parent

random.seed(SEED)

# Distribución de región calcada de la que se discutió en clase (S4):
# el datalake de un e-commerce colombiano concentra tráfico en Bogotá,
# con una cola larga de ciudades intermedias.
REGIONES = ["Bogotá", "Medellín", "Cali", "Barranquilla", "Otro"]
PESOS_REGION = [0.40, 0.20, 0.15, 0.10, 0.15]

# category -> (productos, rango de precio unitario en COP)
CATALOGO = {
    "Electrónica": (
        ["Audífonos", "Cargador", "Mouse", "Teclado", "Parlante Bluetooth", "Monitor"],
        (30_000, 800_000),
    ),
    "Hogar": (
        ["Licuadora", "Cafetera", "Aspiradora", "Lámpara", "Ventilador"],
        (40_000, 350_000),
    ),
    "Ropa": (
        ["Camiseta", "Pantalón", "Chaqueta", "Zapatos", "Gorra"],
        (25_000, 180_000),
    ),
    "Deportes": (
        ["Balón", "Bicicleta", "Mancuernas", "Tenis running", "Maleta deportiva"],
        (20_000, 500_000),
    ),
    "Alimentos": (
        ["Café molido", "Panela", "Chocolate", "Arroz", "Aceite"],
        (5_000, 60_000),
    ),
}
CATEGORIAS = list(CATALOGO.keys())

CANALES = ["online", "tienda"]
PESOS_CANAL = [0.60, 0.40]

FECHA_INICIO = pd.Timestamp("2025-08-01")
FECHA_FIN = pd.Timestamp("2026-07-31")
RANGO_DIAS = (FECHA_FIN - FECHA_INICIO).days

PROB_DEVOLUCION = 0.06


def generar_fila(order_id: int) -> dict:
    categoria = random.choice(CATEGORIAS)
    productos, (precio_min, precio_max) = CATALOGO[categoria]
    producto = random.choice(productos)

    fecha = FECHA_INICIO + pd.Timedelta(days=random.randint(0, RANGO_DIAS))
    region = random.choices(REGIONES, weights=PESOS_REGION, k=1)[0]
    canal = random.choices(CANALES, weights=PESOS_CANAL, k=1)[0]

    # cantidad sesgada hacia compras pequeñas (1-2 unidades), con cola hasta 5
    cantidad = random.choices([1, 2, 3, 4, 5], weights=[0.45, 0.25, 0.15, 0.10, 0.05], k=1)[0]
    precio_unit = round(random.uniform(precio_min, precio_max), -2)  # redondeado a la centena
    total = cantidad * precio_unit
    devuelto = random.random() < PROB_DEVOLUCION

    return {
        "order_id": f"ORD-{order_id:06d}",
        "fecha": fecha.strftime("%Y-%m-%d"),
        "region": region,
        "producto": producto,
        "categoria": categoria,
        "cantidad": cantidad,
        "precio_unit": precio_unit,
        "total": total,
        "canal": canal,
        "devuelto": devuelto,
    }


def main() -> None:
    filas = [generar_fila(i + 1) for i in range(N_FILAS)]
    df = pd.DataFrame(filas)

    ruta_parquet = OUT_DIR / "prueba_parquet.parquet"
    ruta_csv = OUT_DIR / "prueba_csv.csv"

    df.to_parquet(ruta_parquet, engine="pyarrow", compression="snappy", index=False)
    df.to_csv(ruta_csv, index=False)

    tam_parquet = ruta_parquet.stat().st_size
    tam_csv = ruta_csv.stat().st_size
    ratio = tam_csv / tam_parquet

    print(f"Filas generadas: {len(df):,}")
    print(f"Regiones: {dict(df['region'].value_counts())}")
    print()
    print(f"{'Archivo':<20}{'Tamaño':>12}")
    print(f"{'-'*32}")
    print(f"{ruta_parquet.name:<20}{tam_parquet / 1024:>9.1f} KB")
    print(f"{ruta_csv.name:<20}{tam_csv / 1024:>9.1f} KB")
    print()
    print(f"Ratio de compresión CSV / Parquet: {ratio:.2f}x")
    print("(esta es la misma comparación que vas a repetir con Spark en la Parte 5 del lab)")


if __name__ == "__main__":
    main()
