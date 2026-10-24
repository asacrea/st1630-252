# Entrega Lab 4a · Orquestación de Pipeline End-to-End con Apache Airflow

**Curso:** ST1630 Sistemas Intensivos en Datos — Universidad EAFIT  
**Semana:** S11  
**Integrantes:**
- Mateo Sanz Medina (`msanzm@eafit.edu.co`)
- Samuel Arango Echeverri (`sarangoe3@eafit.edu.co`)
- Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

---

## 📁 Estructura del Entregable

```
labs/lab4a-pipelines/entregas/msanzmSamuA/
├── dags/
│   └── ventas_pipeline.py          # DAG completo con los 4 bloques # TODO resueltos
├── dag_design.md                   # Documento de diseño arquitectural y análisis técnico
├── bitacora_delegacion.md          # Bitácora de delegación y cumplimiento de política de IA
├── README.md                       # Documentación y guía de verificación
└── capturas/
    ├── grid_view_backfill.png      # Grid/Tree View con las 7 corridas de backfill en verde (100% SUCCESS)
    ├── graph_view.png              # Graph View del DAG con dependencias lineales y panel de inspección
    └── log_validate_bronze.png     # Log detallado de la task validate_bronze superando todos los checks
```

---

## 🚀 Resumen de Implementación y Solución de los TODOs

El DAG `ventas_colombia_pipeline` orquesta el flujo de datos transaccionales del Lakehouse en arquitectura Medallion (Bronze ➔ Silver ➔ Gold):

1. **TODO 1 (`ingest_bronze`):**  
   Ingesta particionada por fecha (`fecha={ds}`) escribiendo en formato Parquet con `mode("overwrite")` para garantizar idempotencia en la capa Bronze.

2. **TODO 2 & 3 (`validate_bronze`):**  
   Checks de calidad de datos que detienen el pipeline ante datos corruptos:
   - `check_nulos_criticos`: evalúa `pedido_id`, `producto`, `total` con umbral del 5% (`UMBRAL_NULOS = 0.05`).
   - `check_rango_monto`: tolerancia cero a transacciones con `total <= 0` o nulo.
   - `check_cardinalidad_canal`: emite un `logging.warning` informativo sin romper el pipeline ante nuevos canales.

3. **TODO 2 & 3 (`transform_silver`):**  
   - Casteo de tipos (`fecha` a `DateType`, `total` a `DoubleType`).
   - Filtrado de transacciones canceladas/devueltas (`devuelto == False`).
   - Creación de columna derivada `anio_mes` (`yyyy-MM`).
   - **MERGE idempotente en Delta Lake:** mediante `DeltaTable.merge()` por la clave natural `pedido_id`, con `whenMatchedUpdateAll()` y `whenNotMatchedInsertAll()`.

4. **TODO 4 (`build_gold`):**  
   Recálculo completo en cada corrida (`mode("overwrite")`) de dos tablas agregadas en Delta Lake:
   - `ventas_por_region`: agregación por `region` y `anio_mes` con `total_ventas` y `n_transacciones`.
   - `top_productos`: agregación por `producto` con `total_ventas` ordenado descendente (Top 20).

---

## 🧪 Pruebas Unitarias y de Integración

Los checks de calidad fueron validados objetivamente mediante la suite de tests unitarios:
```bash
pytest dags/tests/test_ventas_pipeline.py -v
```
**Resultado:** 3 de 3 tests pasando al 100%:
- `test_validate_no_nulls_passes`: PASSED
- `test_validate_nulls_fails`: PASSED
- `test_validate_negative_totals_fails`: PASSED

---

## 📊 Verificación de Idempotencia y Resultados Numéricos

- **Rango evaluado:** 7 fechas de backfill (`2026-01-01` a `2026-01-07`).
- **Filas en Bronze:** 2,957 filas.
- **Filas en Silver:** 2,820 filas (137 filas devueltas filtradas).
- **Prueba de reejecución de `2026-01-03`:**
  - Filas en Bronze para `2026-01-03`: exactamente 529 filas (sobrescritura limpia).
  - Filas en Silver tras reejecución: **2,820 filas exactas ($N = N'$)**.
  - Delta commit metrics: `numTargetRowsMatchedUpdated = 498`, `numTargetRowsInserted = 0`. Cero duplicados.
