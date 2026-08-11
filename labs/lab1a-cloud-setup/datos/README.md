# Datos de prueba — Lab 1a

**Curso:** ST1630-2026-2 · **Semana:** S4-S5 · **Última edición:** 2026-07-30

## Archivos

Ejecuta `generar_datos.py` (ver ese archivo) para generar, en esta misma
carpeta:

| Archivo | Formato | Filas | Uso |
|---|---|---|---|
| `prueba_parquet.parquet` | Parquet, compresión snappy | 10.000 | Sube a `Bronze/` en tu bucket S3; es el archivo que lee el notebook de verificación |
| `prueba_csv.csv` | CSV | 10.000 | Mismos datos, para el benchmark Parquet vs. CSV de la Parte 5 del lab |

Ambos archivos contienen exactamente las mismas 10.000 filas de ventas
sintéticas colombianas — existen en dos formatos a propósito, para que
puedas medir en Spark la diferencia de tamaño en disco y de tiempo de
consulta entre un formato columnar y uno de fila (ver `../README.md`,
Parte 5).

## Schema

| Columna | Tipo | Descripción |
|---|---|---|
| `order_id` | string | Identificador único de la orden (`ORD-000001`, ...) |
| `fecha` | string (`YYYY-MM-DD`) | Fecha de la venta, entre 2025-08-01 y 2026-07-31 |
| `region` | string | Ciudad del comprador: `Bogotá`, `Medellín`, `Cali`, `Barranquilla`, `Otro` |
| `producto` | string | Nombre del producto vendido |
| `categoria` | string | `Electrónica`, `Hogar`, `Ropa`, `Deportes`, `Alimentos` |
| `cantidad` | int | Unidades compradas (1-5) |
| `precio_unit` | float | Precio unitario en COP |
| `total` | float | `cantidad * precio_unit` |
| `canal` | string | `online` o `tienda` |
| `devuelto` | bool | `true` si la orden fue devuelta (~6% de las filas) |

## Cómo subirlos a S3

Estos archivos van a la capa **Bronze** de tu datalake (datos crudos,
tal como se generaron — sin transformar). El script `setup_s3.sh`
(ver `../scripts/`) ya incluye el comando de subida, pero si necesitas
hacerlo a mano:

```bash
aws s3 cp prueba_parquet.parquet s3://<tu-bucket>/bronze/ventas/prueba_parquet.parquet
aws s3 cp prueba_csv.csv s3://<tu-bucket>/bronze/ventas/prueba_csv.csv
```

Verifica la subida con:

```bash
aws s3 ls s3://<tu-bucket>/bronze/ventas/ --human-readable
```
