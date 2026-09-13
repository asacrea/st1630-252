# Resumen limpio de profiling - Lab 1b

Fuente: `datos/ventas_colombia_raw.csv`, generado con la misma semilla del laboratorio. Usa esto como evidencia auxiliar y contrasta con `00_profiling_output.txt`.

- Filas totales: 101,500

- Duplicados exactos: 1,500 (1.48%)

- Total nulo: 2,571

- Total <= 0 o nulo: 3,959 (3.90%)

- Precio_unit negativos: 641

- Cantidad <= 0: 622

- vendedor_id entero: 69,592

- vendedor_id prefijado VEN-: 28,056

- vendedor_id mixto: 3,852

- Emails nulos: 144

- Emails invalidos no nulos: 1,175


## Formatos de fecha


| patron | count | ejemplos |
| --- | --- | --- |
| dd/MM/yyyy o MM/dd/yyyy (ambiguo) | 40,644 | 09/06/2025, 03/19/2025, 18/07/2025 |
| yyyy/MM/dd | 20,293 | 2025/01/23, 2025/07/16, 2026/03/21 |
| yyyy-MM-dd | 20,284 | 2025-01-10, 2026-05-23, 2026-04-27 |
| dd-MM-yyyy | 20,279 | 01-06-2025, 21-06-2026, 08-06-2026 |


## Region - 35 variantes


| region | count |
| --- | --- |
| BOGOTÁ | 5,017 |
| Bogota  | 4,956 |
| bogota | 4,894 |
| BTA | 4,803 |
| Bta | 4,796 |
| BOGOTA | 4,759 |
|  Bogotá | 4,701 |
| Bogotá | 4,677 |
| Medellín | 3,487 |
| MEDELLÍN | 3,444 |
| medellin | 3,425 |
| Medellin  | 3,392 |
| MDE | 3,332 |
| medellín | 3,316 |
| CALI | 2,598 |
| Cali | 2,579 |
|  Cali | 2,570 |
| CLO | 2,550 |
| cali | 2,487 |
| cali  | 2,473 |
| BARRANQUILLA | 2,042 |
| Bquilla | 2,018 |
| Barranquilla | 2,015 |
| BAQ | 2,015 |
| barranquilla | 1,912 |
| BGA | 1,869 |
| Bucaramanga | 1,842 |
| Buca | 1,837 |
| bucaramanga | 1,830 |
| BUCARAMANGA | 1,734 |
| Desconocido | 1,665 |
| otro | 1,634 |
| N/A | 1,632 |
| NA | 1,615 |
| OTRO | 1,584 |


## Posibles variantes de Bogota para revisar


| region | count |
| --- | --- |
| BOGOTÁ | 5,017 |
| Bogota  | 4,956 |
| bogota | 4,894 |
| BTA | 4,803 |
| Bta | 4,796 |
| BOGOTA | 4,759 |
|  Bogotá | 4,701 |
| Bogotá | 4,677 |


## Canal - 20 variantes


| canal | count |
| --- | --- |
| App Móvil | 7,198 |
| móvil | 7,158 |
| app movil | 7,121 |
| APP MOVIL | 7,090 |
| APP_MOVIL | 7,004 |
| online | 6,112 |
| pagina_web | 6,111 |
| WEB | 6,083 |
| sitio_web | 6,082 |
| Web | 6,036 |
| TIENDA FISICA | 5,118 |
| Tienda Física | 5,105 |
| tienda | 5,065 |
| TIENDA | 4,977 |
| físico | 4,893 |
| call_center | 2,181 |
| llamada | 2,076 |
| TELEFONO | 2,054 |
| tel | 2,020 |
| Teléfono | 2,016 |


## Posibles variantes de app_movil para revisar


| canal | count |
| --- | --- |
| App Móvil | 7,198 |
| móvil | 7,158 |
| app movil | 7,121 |
| APP MOVIL | 7,090 |
| APP_MOVIL | 7,004 |

