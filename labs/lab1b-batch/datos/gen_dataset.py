#!/usr/bin/env python3
"""Genera ventas_colombia_raw.csv — Lab 1b (ST1630-2026-2, S5-S6).

Ejecuta esto UNA VEZ, al principio de la Parte 1, para generar tu copia
local del dataset -- el archivo no viene en el repo (`.gitignore`
excluye datasets pesados, igual que en el Lab 1a). La semilla es fija
(`SEED = 1630`), así que tu archivo va a ser byte-idéntico al de
cualquier otro estudiante de la cohorte y al que usó el profesor para
escribir `data_quality_report.md` -- no lo edites ni cambies la
semilla, o tus números de profiling dejarán de coincidir con la
rúbrica.

Uso:
    python3 gen_dataset.py

Dependencias: pandas, numpy (pip install pandas numpy)

Produce, en esta misma carpeta:
    - ventas_colombia_raw.csv

Y al final imprime un resumen de profiling (los mismos números que
debería reproducir scripts/00_profiling.py sobre este archivo) -- es
solo un auto-chequeo de que la generación funcionó, **no** es tu
`data_profiling.md`. Ese lo escribes tú, ejecutando
`scripts/00_profiling.py` de verdad (ver ../README.md, Parte 1).
"""

import numpy as np
import pandas as pd
from pathlib import Path

SEED = 1630
N_BASE = 100_000       # filas únicas antes de duplicar
N_DUPLICADAS = 1_500    # filas que se van a copiar exactamente
N_TOTAL = N_BASE + N_DUPLICADAS  # 101.500

OUT_DIR = Path(__file__).resolve().parent
OUT_PATH = OUT_DIR / "ventas_colombia_raw.csv"

rng = np.random.default_rng(SEED)

# ─────────────────────────────────────────────────────────────
# Catálogo de productos (mismo espíritu que Lab 1a, para continuidad
# narrativa entre labs -- un datalake de un e-commerce colombiano).
# ─────────────────────────────────────────────────────────────
CATALOGO = {
    "Electrónica": (["Audífonos", "Cargador", "Mouse", "Teclado", "Parlante Bluetooth", "Monitor"], (30_000, 800_000)),
    "Hogar": (["Licuadora", "Cafetera", "Aspiradora", "Lámpara", "Ventilador"], (40_000, 350_000)),
    "Ropa": (["Camiseta", "Pantalón", "Chaqueta", "Zapatos", "Gorra"], (25_000, 180_000)),
    "Deportes": (["Balón", "Bicicleta", "Mancuernas", "Tenis running", "Maleta deportiva"], (20_000, 500_000)),
    "Alimentos": (["Café molido", "Panela", "Chocolate", "Arroz", "Aceite"], (5_000, 60_000)),
}
CATEGORIAS = list(CATALOGO.keys())

# ─────────────────────────────────────────────────────────────
# Las 35 variantes de región (6 canónicas) y las 20 de canal
# (4 canónicos) -- este es el inventario "fuente de la verdad" que
# también debe cubrir por completo el MAPA_REGION / MAPA_CANAL de
# scripts/02_silver.py. Si agregas una variante aquí, agrégala
# también allá, o la verificación de "6 valores distintos" del lab
# fallará a propósito (es la Pregunta 3 de pipeline_analysis).
# ─────────────────────────────────────────────────────────────
VARIANTES_REGION = {
    "BOGOTÁ": ["Bogotá", "bogota", "BOGOTÁ", "Bogota ", " Bogotá", "BOGOTA", "Bta", "BTA"],
    "MEDELLÍN": ["Medellín", "medellin", "MEDELLÍN", "Medellin ", "MDE", "medellín"],
    "CALI": ["Cali", "CALI", "cali", "cali ", " Cali", "CLO"],
    "BARRANQUILLA": ["Barranquilla", "BARRANQUILLA", "barranquilla", "Bquilla", "BAQ"],
    "BUCARAMANGA": ["Bucaramanga", "BUCARAMANGA", "bucaramanga", "BGA", "Buca"],
    "OTRO": ["OTRO", "otro", "N/A", "NA", "Desconocido"],
}
assert sum(len(v) for v in VARIANTES_REGION.values()) == 35

VARIANTES_CANAL = {
    "app_movil": ["App Móvil", "APP_MOVIL", "app movil", "móvil", "APP MOVIL"],
    "web": ["WEB", "Web", "sitio_web", "online", "pagina_web"],
    "tienda_fisica": ["Tienda Física", "TIENDA", "tienda", "físico", "TIENDA FISICA"],
    "telefono": ["Teléfono", "TELEFONO", "call_center", "tel", "llamada"],
}
assert sum(len(v) for v in VARIANTES_CANAL.values()) == 20

REGIONES_CANONICAS = list(VARIANTES_REGION.keys())
PESOS_REGION = [0.38, 0.20, 0.15, 0.10, 0.09, 0.08]  # Bogotá concentra tráfico, como en Lab 1a

CANALES_CANONICOS = list(VARIANTES_CANAL.keys())
PESOS_CANAL = [0.35, 0.30, 0.25, 0.10]

METODOS_PAGO = ["tarjeta_credito", "tarjeta_debito", "efectivo", "PSE", "nequi"]
DOMINIOS_EMAIL = ["gmail.com", "hotmail.com", "yahoo.com", "outlook.com"]

FECHA_INICIO = pd.Timestamp("2025-01-01")
FECHA_FIN = pd.Timestamp("2026-06-30")
RANGO_DIAS = (FECHA_FIN - FECHA_INICIO).days
FORMATOS_FECHA = ["%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%m/%d/%Y", "%Y/%m/%d"]

NOMBRES = ["juan", "camila", "andres", "laura", "santiago", "valentina", "sara", "mateo", "gabriela", "david"]
APELLIDOS = ["gomez", "rodriguez", "perez", "martinez", "lopez", "garcia", "ramirez", "torres", "diaz", "castro"]


def construir_base(n: int) -> pd.DataFrame:
    categoria_idx = rng.integers(0, len(CATEGORIAS), n)
    categorias = np.array(CATEGORIAS)[categoria_idx]

    productos = np.empty(n, dtype=object)
    precio_unit = np.empty(n, dtype=float)
    for i, cat in enumerate(categorias):
        prods, (pmin, pmax) = CATALOGO[cat]
        productos[i] = prods[rng.integers(0, len(prods))]
        precio_unit[i] = round(rng.uniform(pmin, pmax), -2)

    cantidad = rng.choice([1, 2, 3, 4, 5], size=n, p=[0.45, 0.25, 0.15, 0.10, 0.05])
    total = cantidad * precio_unit

    region_idx = rng.choice(len(REGIONES_CANONICAS), size=n, p=PESOS_REGION)
    region_canon = np.array(REGIONES_CANONICAS)[region_idx]

    canal_idx = rng.choice(len(CANALES_CANONICOS), size=n, p=PESOS_CANAL)
    canal_canon = np.array(CANALES_CANONICOS)[canal_idx]

    dias = rng.integers(0, RANGO_DIAS, n)
    fechas = FECHA_INICIO + pd.to_timedelta(dias, unit="D")

    vendedor_num = rng.integers(1000, 9999, n)

    nombre_idx = rng.integers(0, len(NOMBRES), n)
    apellido_idx = rng.integers(0, len(APELLIDOS), n)
    emails = [f"{NOMBRES[ni]}.{APELLIDOS[ai]}{rng.integers(1, 999)}@{DOMINIOS_EMAIL[rng.integers(0, len(DOMINIOS_EMAIL))]}"
              for ni, ai in zip(nombre_idx, apellido_idx)]

    metodo_pago = np.array(METODOS_PAGO)[rng.integers(0, len(METODOS_PAGO), n)]
    devuelto = rng.random(n) < 0.08
    calificacion = rng.choice([1, 2, 3, 4, 5], size=n, p=[0.05, 0.05, 0.15, 0.35, 0.40])

    return pd.DataFrame({
        "pedido_id": [f"PED-{i:06d}" for i in range(1, n + 1)],
        "fecha": fechas,
        "region_canon": region_canon,
        "canal_canon": canal_canon,
        "categoria": categorias,
        "producto": productos,
        "cantidad": cantidad.astype(float),
        "precio_unit": precio_unit,
        "total": total,
        "vendedor_num": vendedor_num,
        "email_cliente": emails,
        "metodo_pago": metodo_pago,
        "devuelto": devuelto,
        "calificacion": calificacion,
    })


def elegir_indices(n_pool: int, k: int, excluir: set) -> np.ndarray:
    """Elige k índices en [0, n_pool) que no estén en `excluir`."""
    disponibles = np.setdiff1d(np.arange(n_pool), np.array(sorted(excluir)), assume_unique=False)
    elegidos = rng.choice(disponibles, size=k, replace=False)
    excluir.update(elegidos.tolist())
    return elegidos


def aplicar_variantes_texto(serie_canon: pd.Series, variantes: dict) -> pd.Series:
    salida = serie_canon.copy().astype(object)
    for canon, formas in variantes.items():
        mask = (serie_canon == canon).to_numpy()
        idx = np.where(mask)[0]
        elegidas = rng.integers(0, len(formas), len(idx))
        salida.iloc[idx] = [formas[j] for j in elegidas]
    return salida


def main() -> None:
    df = construir_base(N_BASE)
    usados_total = set()  # índices ya usados para alguna corrupción de 'total' (mutuamente excluyentes)

    # ── pedido_id: 204 nulos ──────────────────────────────────
    idx_pedido_null = elegir_indices(N_BASE, 204, set())
    df.loc[idx_pedido_null, "pedido_id"] = None

    # ── fecha: 5 formatos distintos, asignados a TODAS las filas ──
    formato_idx = rng.integers(0, len(FORMATOS_FECHA), N_BASE)
    df["fecha"] = [f.strftime(FORMATOS_FECHA[fi]) for f, fi in zip(df["fecha"], formato_idx)]

    # ── región / canal: reemplazar el valor canónico por una variante ──
    df["region"] = aplicar_variantes_texto(df["region_canon"], VARIANTES_REGION)
    df["canal"] = aplicar_variantes_texto(df["canal_canon"], VARIANTES_CANAL)
    df = df.drop(columns=["region_canon", "canal_canon"])

    # ── cantidad: 610 filas <= 0 ──────────────────────────────
    idx_cantidad_mala = elegir_indices(N_BASE, 610, set())
    df.loc[idx_cantidad_mala, "cantidad"] = rng.integers(-5, 1, len(idx_cantidad_mala))

    # ── precio_unit: 633 filas negativas ──────────────────────
    idx_precio_malo = elegir_indices(N_BASE, 633, set())
    df.loc[idx_precio_malo, "precio_unit"] = -df.loc[idx_precio_malo, "precio_unit"]

    # ── total: 3 corrupciones DISJUNTAS entre sí ──────────────
    idx_total_null = elegir_indices(N_BASE, 2_539, usados_total)
    df.loc[idx_total_null, "total"] = np.nan

    idx_total_negzero = elegir_indices(N_BASE, 1_365, usados_total)
    df.loc[idx_total_negzero, "total"] = -rng.uniform(1_000, 50_000, len(idx_total_negzero))
    # una fracción de estas se deja en 0 en vez de negativa
    idx_cero = idx_total_negzero[: len(idx_total_negzero) // 3]
    df.loc[idx_cero, "total"] = 0.0

    idx_total_escala = elegir_indices(N_BASE, 800, usados_total)
    df.loc[idx_total_escala, "total"] = df.loc[idx_total_escala, "total"] * 1000

    # el resto de filas ya tiene total = cantidad * precio_unit correcto
    # (calculado en construir_base), salvo que cantidad/precio_unit hayan
    # sido corrompidos por separado -- eso es intencional: son problemas
    # independientes que a veces coinciden en la misma fila, como en un
    # dataset real.

    # ── vendedor_id: 69.608 "enteros" / 31.892 "strings" (sobre el TOTAL
    #    final de 101.500 -- se corrige la proporción después de duplicar) ─
    es_digito = rng.random(N_BASE) < 0.6858
    vendedor_id = np.empty(N_BASE, dtype=object)
    idx_digito = np.where(es_digito)[0]
    idx_no_digito = np.where(~es_digito)[0]
    vendedor_id[idx_digito] = [str(v) for v in df.loc[idx_digito, "vendedor_num"]]
    # de los "no dígito": ~88% con prefijo VEN-, ~12% en otro formato mixto
    es_ven = rng.random(len(idx_no_digito)) < 0.88
    for pos, i in enumerate(idx_no_digito):
        v = df.loc[i, "vendedor_num"]
        vendedor_id[i] = f"VEN-{v}" if es_ven[pos] else f"v{v}"
    df["vendedor_id"] = vendedor_id
    df = df.drop(columns=["vendedor_num"])

    # ── email: 143 nulos + 1.159 con formato inválido ─────────
    usados_email = set()
    idx_email_null = elegir_indices(N_BASE, 143, usados_email)
    df.loc[idx_email_null, "email_cliente"] = None

    idx_email_malo = elegir_indices(N_BASE, 1_159, usados_email)
    patrones_malos = [
        lambda e: e.replace("@", ""),                 # sin @
        lambda e: e.split("@")[0] + "@",               # sin dominio
        lambda e: e.split("@")[0] + "@dominio",         # sin TLD
        lambda e: e.replace("@", " en "),               # formato roto
    ]
    for i in idx_email_malo:
        patron = patrones_malos[rng.integers(0, len(patrones_malos))]
        df.loc[i, "email_cliente"] = patron(df.loc[i, "email_cliente"])

    # ── duplicados exactos: 1.500 filas que copian una fila existente ──
    idx_fuente = rng.choice(N_BASE, size=N_DUPLICADAS, replace=False)
    duplicadas = df.iloc[idx_fuente].copy()
    df_final = pd.concat([df, duplicadas], ignore_index=True)

    # orden final aleatorio (no afecta ningún conteo, solo realismo)
    df_final = df_final.sample(frac=1.0, random_state=SEED).reset_index(drop=True)

    df_final.to_csv(OUT_PATH, index=False)

    # ── resumen de profiling para auditar el dataset generado ──
    print(f"Archivo generado: {OUT_PATH}")
    print(f"Filas totales: {len(df_final):,}")
    print(f"Tamaño: {OUT_PATH.stat().st_size / (1024*1024):.1f} MB")
    print()
    print(f"Duplicados exactos (todas las columnas): {df_final.duplicated().sum():,}")
    print(f"pedido_id nulos: {df_final['pedido_id'].isna().sum():,}")
    print(f"total nulos: {df_final['total'].isna().sum():,}")
    print(f"total <= 0 (incluye nulos): {((df_final['total'] <= 0) | df_final['total'].isna()).sum():,}")
    print(f"precio_unit negativos: {(df_final['precio_unit'] < 0).sum():,}")
    print(f"cantidad <= 0: {(df_final['cantidad'] <= 0).sum():,}")
    print(f"región -- valores únicos distintos: {df_final['region'].nunique()}")
    print(f"canal -- valores únicos distintos: {df_final['canal'].nunique()}")
    digitos = df_final['vendedor_id'].astype(str).str.isdigit()
    prefijo = df_final['vendedor_id'].astype(str).str.startswith('VEN-')
    print(f"vendedor_id -- tipo entero (isdigit): {digitos.sum():,}")
    print(f"vendedor_id -- tipo prefijado (VEN-): {prefijo.sum():,}")
    print(f"vendedor_id -- tipo mixto (otro): {(~digitos & ~prefijo).sum():,}")
    print(f"email nulos: {df_final['email_cliente'].isna().sum():,}")
    sin_arroba = ~df_final['email_cliente'].astype(str).str.contains("@", na=False)
    print(f"email sin '@' (subconjunto de inválidos): {(sin_arroba & df_final['email_cliente'].notna()).sum():,}")


if __name__ == "__main__":
    main()
