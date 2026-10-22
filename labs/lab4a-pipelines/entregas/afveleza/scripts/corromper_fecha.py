"""Parte 4 del Lab 4a: corrompe A PROPOSITO una fecha del CSV en un
porcentaje de sus filas. Dos modos, uno por cada check duro:

  nulo      -> deja vacia la columna `total`   (check_nulos_criticos)
  negativo  -> cambia `total` por su negativo  (check_rango_monto)

Uso (desde labs/lab4a-pipelines/datos):
    python corromper_fecha.py                          # 2026-01-08, 10%, nulo
    python corromper_fecha.py 2026-01-08 0.10 nulo
    python corromper_fecha.py 2026-01-09 0.02 negativo

Para deshacerlo: python gen_dataset.py  (regenera el CSV limpio).
"""
import csv
import sys

ARCHIVO = "ventas_colombia_lab4a.csv"
fecha = sys.argv[1] if len(sys.argv) > 1 else "2026-01-08"
fraccion = float(sys.argv[2]) if len(sys.argv) > 2 else 0.10
modo = sys.argv[3] if len(sys.argv) > 3 else "nulo"
if modo not in ("nulo", "negativo"):
    sys.exit(f"modo desconocido: {modo} (usa 'nulo' o 'negativo')")

with open(ARCHIVO, newline="", encoding="utf-8") as f:
    lector = csv.DictReader(f)
    columnas = lector.fieldnames
    filas = list(lector)

del_dia = [fila for fila in filas if fila["fecha"] == fecha]
if not del_dia:
    sys.exit(f"no hay filas con fecha={fecha}")
a_corromper = max(1, round(len(del_dia) * fraccion))
for fila in del_dia[:a_corromper]:
    if modo == "nulo":
        fila["total"] = ""
    else:
        fila["total"] = str(-abs(float(fila["total"])))

with open(ARCHIVO, "w", newline="", encoding="utf-8") as f:
    escritor = csv.DictWriter(f, fieldnames=columnas)
    escritor.writeheader()
    escritor.writerows(filas)

accion = "vaciada" if modo == "nulo" else "vuelta negativa"
print(f"{fecha}: {accion} la columna 'total' en {a_corromper} de {len(del_dia)} filas "
      f"({a_corromper / len(del_dia):.1%}).")
