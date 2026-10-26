"""neo4j_lab.py — Lab 3 (ST1630-2026-2) · Parte 2: modelo de GRAFO

En un grafo, las relaciones son datos de primera clase: recorrerlas no
requiere JOIN ni índices extra, cada nodo sabe quiénes son sus vecinos.
Es el modelo natural cuando lo que importa son las conexiones
muchos-a-muchos (Kleppmann, cap. 2: "graph-like data models").

Modelo de este lab:
    (:Cliente {cliente_id, nombre, region})-[:COMPRO {unidades}]->(:Producto {producto_id, nombre, categoria})
Una sola relación COMPRO por par cliente-producto, con las unidades
acumuladas de todos sus pedidos.

Uso (dentro del contenedor app):
    python scripts/neo4j_lab.py --usuario <tu-usuario>
UI para explorar el grafo: http://localhost:7474 (neo4j / lab3-st1630)

Los bloques # TODO son tu trabajo. Respeta la firma y el formato de
retorno de la consulta: scripts/verificar.py la llama.
"""

from collections import defaultdict

from comun import cargar_datos, cronometro, medir, neo4j_driver, parametros_prueba, pendiente, usuario_desde_args

LOTE = 1000

# ── TODO 1 · Carga del grafo en Cypher ───────────────────────────────────────
# Cada consulta recibe un parámetro $filas (una lista de diccionarios) y
# debe crear los nodos/relaciones de todo el lote con UNWIND. Usa MERGE
# (no CREATE) para que volver a cargar no duplique nada.
#   $filas de productos: [{producto_id, nombre, categoria}, ...]
#   $filas de clientes:  [{cliente_id, nombre, region}, ...]
#   $filas de compras:   [{cliente_id, producto_id, unidades}, ...]
CYPHER_PRODUCTOS = """
UNWIND $filas AS f
MERGE (p:Producto {producto_id: f.producto_id})
SET p.nombre = f.nombre, p.categoria = f.categoria
"""
CYPHER_CLIENTES = """
UNWIND $filas AS f
MERGE (c:Cliente {cliente_id: f.cliente_id})
SET c.nombre = f.nombre, c.region = f.region
"""
CYPHER_COMPRAS = """
UNWIND $filas AS f
MATCH (c:Cliente {cliente_id: f.cliente_id})
MATCH (p:Producto {producto_id: f.producto_id})
MERGE (c)-[r:COMPRO]->(p)
SET r.unidades = f.unidades
"""

# ── TODO 2 · Q4 en Cypher: recomendaciones ───────────────────────────────────
# La MISMA definición que q4_recomendaciones_mongo:
#   productos comprados por OTROS clientes que compraron al menos un
#   producto en común con $cliente_id, que $cliente_id no ha comprado;
#   puntaje = número de esos clientes distintos que lo compraron.
# Debe devolver columnas producto_id y puntaje, puntaje desc y, en empate,
# producto_id asc, limitado a $n.
CYPHER_Q4 = """
MATCH (c:Cliente {cliente_id: $cliente_id})-[:COMPRO]->(:Producto)<-[:COMPRO]-(vecino:Cliente)
WHERE vecino <> c
WITH DISTINCT c, vecino
MATCH (vecino)-[:COMPRO]->(rec:Producto)
WHERE NOT (c)-[:COMPRO]->(rec)
RETURN rec.producto_id AS producto_id, count(DISTINCT vecino) AS puntaje
ORDER BY puntaje DESC, producto_id ASC
LIMIT $n
"""


# ── Dado ─────────────────────────────────────────────────────────────────────

def q4_recomendaciones_neo4j(driver, cliente_id: str, n: int = 5) -> list:
    if "TODO" in CYPHER_Q4:
        pendiente("TODO 2 · CYPHER_Q4")
    registros, _, _ = driver.execute_query(CYPHER_Q4, cliente_id=cliente_id, n=n, database_="neo4j")
    return [(r["producto_id"], r["puntaje"]) for r in registros]


def compras_agregadas(pedidos: list) -> list:
    unidades = defaultdict(int)
    for pedido in pedidos:
        for item in pedido["items"]:
            unidades[(pedido["cliente_id"], item["producto_id"])] += item["cantidad"]
    return [{"cliente_id": c, "producto_id": p, "unidades": u} for (c, p), u in unidades.items()]


def cargar(driver, datos: dict) -> None:
    driver.execute_query("MATCH (n) DETACH DELETE n", database_="neo4j")
    driver.execute_query("CREATE CONSTRAINT cliente_id IF NOT EXISTS FOR (c:Cliente) REQUIRE c.cliente_id IS UNIQUE")
    driver.execute_query("CREATE CONSTRAINT producto_id IF NOT EXISTS FOR (p:Producto) REQUIRE p.producto_id IS UNIQUE")

    lotes = (
        (CYPHER_PRODUCTOS, [{k: p[k] for k in ("producto_id", "nombre", "categoria")} for p in datos["productos"]]),
        (CYPHER_CLIENTES, [{k: c[k] for k in ("cliente_id", "nombre", "region")} for c in datos["clientes"]]),
        (CYPHER_COMPRAS, compras_agregadas(datos["pedidos"])),
    )
    with cronometro("carga del grafo en Neo4j"):
        for cypher, filas in lotes:
            if "TODO" in cypher:
                pendiente("TODO 1 · Cypher de carga")
            for i in range(0, len(filas), LOTE):
                driver.execute_query(cypher, filas=filas[i:i + LOTE], database_="neo4j")


if __name__ == "__main__":
    usuario = usuario_desde_args("Parte 2 · Neo4j")
    datos = cargar_datos(usuario)
    params = parametros_prueba(datos)
    driver = neo4j_driver()

    cargar(driver, datos)
    nodos = driver.execute_query("MATCH (n) RETURN count(n) AS n").records[0]["n"]
    relaciones = driver.execute_query("MATCH ()-[r:COMPRO]->() RETURN count(r) AS n").records[0]["n"]
    print(f"  nodos: {nodos:,} · relaciones COMPRO: {relaciones:,}")

    print("\nQ4 · recomendaciones (Neo4j)")
    for cliente_id in params["clientes_recomendacion"]:
        print(f"  {cliente_id}:", medir(f"Q4 Neo4j {cliente_id}", q4_recomendaciones_neo4j, driver, cliente_id))
    driver.close()
