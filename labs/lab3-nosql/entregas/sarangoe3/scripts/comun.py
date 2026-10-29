"""comun.py — Lab 3 (ST1630-2026-2)

Utilidades compartidas por todos los scripts del lab (dadas, no las
modifiques): lectura de tu dataset, conexiones a los 4 motores, medición
de tiempos y los parámetros de prueba de las consultas.
"""

import argparse
import json
import os
import random
import statistics
import time
from pathlib import Path

def _carpeta_datos() -> Path:
    """La carpeta datos/ del lab, sin importar si corres los scripts desde
    scripts/ o desde tu copia en entregas/<tu-usuario>/scripts/."""
    for carpeta in Path(__file__).resolve().parents:
        if (carpeta / "datos" / "gen_dataset.py").exists():
            return carpeta / "datos"
    raise SystemExit("No encuentro labs/lab3-nosql/datos/: corre los scripts dentro del repo del curso")


DATOS = _carpeta_datos()


# ── Argumentos y datos ───────────────────────────────────────────────────────

def usuario_desde_args(descripcion: str) -> str:
    parser = argparse.ArgumentParser(description=descripcion)
    parser.add_argument("--usuario", required=True, help="tu usuario EAFIT, el mismo de gen_dataset.py")
    return parser.parse_args().usuario.strip().lower()


def cargar_datos(usuario: str) -> dict:
    carpeta = DATOS / usuario
    if not carpeta.exists():
        raise SystemExit(f"No existe {carpeta}. Corre primero: python datos/gen_dataset.py --usuario {usuario}")
    return {nombre: json.loads((carpeta / f"{nombre}.json").read_text(encoding="utf-8"))
            for nombre in ("clientes", "productos", "pedidos", "eventos_stream")}


def parametros_prueba(datos: dict) -> dict:
    """Valores con los que se prueban las consultas. Se eligen de forma
    determinista a partir de tu dataset: siempre los mismos para ti."""
    rng = random.Random(len(datos["pedidos"]) * 7 + len(datos["clientes"]))
    pedidos = datos["pedidos"]
    clientes_con_compras = sorted({p["cliente_id"] for p in pedidos})
    pares = sorted({(p["region"], p["fecha"][:10]) for p in pedidos})
    return {
        "pedidos": rng.sample([p["pedido_id"] for p in pedidos], 5),
        "region_dia": rng.sample(pares, 3),
        "clientes_historial": rng.sample(clientes_con_compras, 3),
        "clientes_recomendacion": rng.sample(clientes_con_compras, 3),
    }


# ── Conexiones ───────────────────────────────────────────────────────────────

def mongo_db():
    from pymongo import MongoClient

    return MongoClient(os.getenv("MONGO_URI", "mongodb://localhost:27017"))["lab3"]


def cassandra_session():
    """Sesión sobre el keyspace lab3 (factor de replicación 1: un solo nodo)."""
    from cassandra.cluster import Cluster

    cluster = Cluster([os.getenv("CASSANDRA_HOST", "localhost")])
    session = cluster.connect()
    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS lab3 "
        "WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.set_keyspace("lab3")
    return session


def redis_client(db: int = 0):
    """db=0 para las Partes 1-2; db=1 (aparte) para el stream de la Parte 3."""
    import redis

    return redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), db=db, decode_responses=True)


def neo4j_driver():
    from neo4j import GraphDatabase

    return GraphDatabase.driver(
        os.getenv("NEO4J_URI", "bolt://localhost:7687"),
        auth=("neo4j", os.getenv("NEO4J_PASSWORD", "lab3-st1630")),
    )


# ── Medición ─────────────────────────────────────────────────────────────────

def medir(nombre: str, funcion, *args, repeticiones: int = 20):
    """Ejecuta funcion(*args) varias veces y reporta la mediana en ms.
    Devuelve el resultado de la última ejecución."""
    tiempos = []
    for _ in range(repeticiones):
        inicio = time.perf_counter()
        resultado = funcion(*args)
        tiempos.append((time.perf_counter() - inicio) * 1000)
    print(f"  ⏱  {nombre}: mediana {statistics.median(tiempos):.2f} ms ({repeticiones} repeticiones)")
    return resultado


def cronometro(nombre: str):
    """Para tiempos de carga: with cronometro("carga MongoDB"): ..."""
    class _C:
        def __enter__(self):
            self.inicio = time.perf_counter()

        def __exit__(self, *exc):
            print(f"  ⏱  {nombre}: {time.perf_counter() - self.inicio:.1f} s")
    return _C()


def pendiente(todo: str):
    raise NotImplementedError(f"Falta completar: {todo}")
