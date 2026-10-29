"""verificar.py — Lab 3 (ST1630-2026-2) · Autoevaluación (dado, no lo modifiques)

Llama a tus funciones de consulta con los parámetros de prueba de TU
dataset y compara cada resultado contra la respuesta correcta, calculada
aquí directamente desde tus archivos JSON.

Antes de correrlo, carga los motores con cada script de la parte que
quieras verificar (mongo_lab.py, cassandra_lab.py, redis_lab.py,
neo4j_lab.py, stream_lab.py).

Uso (dentro del contenedor app):
    python scripts/verificar.py --usuario <tu-usuario>
    python scripts/verificar.py --usuario <tu-usuario> --parte mongo

Al final imprime una HUELLA: un código calculado a partir de tu usuario y
de los resultados de tus consultas. Pégalo en tu nosql_design.md junto con
la salida completa de este script.
"""

import argparse
import hashlib
import json
from collections import defaultdict

from comun import cargar_datos, parametros_prueba

PARTES = ["mongo", "cassandra", "redis", "neo4j", "stream"]


# ── Respuestas correctas, calculadas desde los JSON ──────────────────────────

class Verdad:
    def __init__(self, datos: dict):
        self.d = datos
        self.clientes = {c["cliente_id"]: c for c in datos["clientes"]}
        self.productos = {p["producto_id"]: p for p in datos["productos"]}
        self.pedidos = {p["pedido_id"]: p for p in datos["pedidos"]}
        self.compras = defaultdict(set)  # cliente -> productos comprados
        for p in datos["pedidos"]:
            for it in p["items"]:
                self.compras[p["cliente_id"]].add(it["producto_id"])

    def q1(self, pedido_id):
        p = self.pedidos[pedido_id]
        return {
            "pedido_id": p["pedido_id"], "fecha": p["fecha"],
            "cliente": {"cliente_id": p["cliente_id"], "nombre": self.clientes[p["cliente_id"]]["nombre"]},
            "items": [{"producto_id": it["producto_id"], "nombre": self.productos[it["producto_id"]]["nombre"],
                       "cantidad": it["cantidad"], "precio_unit": it["precio_unit"]} for it in p["items"]],
            "total": p["total"],
        }

    def q2a(self, region, dia):
        del_dia = [p for p in self.d["pedidos"] if p["region"] == region and p["fecha"][:10] == dia]
        recientes = sorted(del_dia, key=lambda p: (p["fecha"], p["pedido_id"]), reverse=True)[:5]
        return (len(del_dia), sum(p["total"] for p in del_dia), [p["pedido_id"] for p in recientes])

    def q2b(self, cliente_id):
        suyos = [p for p in self.d["pedidos"] if p["cliente_id"] == cliente_id]
        suyos.sort(key=lambda p: (p["fecha"], p["pedido_id"]), reverse=True)
        return [(p["pedido_id"], p["fecha"], p["total"]) for p in suyos[:10]]

    def q3(self, n=10):
        unidades = defaultdict(int)
        for p in self.d["pedidos"]:
            for it in p["items"]:
                unidades[it["producto_id"]] += it["cantidad"]
        return sorted(unidades.items(), key=lambda x: (-x[1], x[0]))[:n]

    def q4(self, cliente_id, n=5):
        propios = self.compras[cliente_id]
        vecinos = [c for c, prods in self.compras.items() if c != cliente_id and prods & propios]
        puntaje = defaultdict(int)
        for v in vecinos:
            for prod in self.compras[v] - propios:
                puntaje[prod] += 1
        return sorted(puntaje.items(), key=lambda x: (-x[1], x[0]))[:n]

    def stream(self):
        unidades = defaultdict(int)
        for e in {e["event_id"]: e for e in self.d["eventos_stream"]}.values():
            unidades[e["producto_id"]] += e["cantidad"]
        return dict(unidades)


# ── Normalización de lo que devuelven tus funciones ──────────────────────────

def norm_q1(r):
    return {
        "pedido_id": str(r["pedido_id"]), "fecha": str(r["fecha"]),
        "cliente": {"cliente_id": str(r["cliente"]["cliente_id"]), "nombre": str(r["cliente"]["nombre"])},
        "items": [{"producto_id": str(i["producto_id"]), "nombre": str(i["nombre"]),
                   "cantidad": int(i["cantidad"]), "precio_unit": int(i["precio_unit"])} for i in r["items"]],
        "total": int(r["total"]),
    }


def norm_pares(r):
    return [(str(a), int(b)) for a, b in r]


def norm_q2a(r):
    return (int(r[0]), int(r[1]), [str(x) for x in r[2]])


def norm_q2b(r):
    return [(str(a), str(b), int(c)) for a, b, c in r]


# ── Ejecución de las verificaciones ──────────────────────────────────────────

class Resultado:
    def __init__(self):
        self.ok = self.mal = self.pendientes = 0
        self.salidas = {}

    def verificar(self, nombre, llamada, esperado, normalizar):
        try:
            obtenido = normalizar(llamada())
        except NotImplementedError as e:
            self.pendientes += 1
            print(f"  … {nombre}: {e}")
            return
        except Exception as e:  # noqa: BLE001 -- mostrar cualquier error al estudiante
            self.mal += 1
            print(f"  ✗ {nombre}: error al ejecutar → {type(e).__name__}: {str(e)[:200]}")
            return
        self.salidas[nombre] = obtenido
        if obtenido == esperado:
            self.ok += 1
            print(f"  ✓ {nombre}")
        else:
            self.mal += 1
            print(f"  ✗ {nombre}\n      esperado: {str(esperado)[:300]}\n      obtenido: {str(obtenido)[:300]}")


def main():
    parser = argparse.ArgumentParser(description="Autoevaluación del Lab 3")
    parser.add_argument("--usuario", required=True)
    parser.add_argument("--parte", choices=PARTES + ["todo"], default="todo")
    args = parser.parse_args()
    usuario = args.usuario.strip().lower()
    partes = PARTES if args.parte == "todo" else [args.parte]

    datos = cargar_datos(usuario)
    verdad, params, res = Verdad(datos), parametros_prueba(datos), Resultado()

    if "mongo" in partes:
        import mongo_lab
        from comun import mongo_db
        db = mongo_db()
        print("\nMongoDB")
        for pid in params["pedidos"]:
            res.verificar(f"Q1 {pid}", lambda: mongo_lab.q1_pedido(db, pid), verdad.q1(pid), norm_q1)
        res.verificar("Q3 MongoDB", lambda: mongo_lab.q3_top_productos_mongo(db), verdad.q3(), norm_pares)
        for cid in params["clientes_recomendacion"]:
            res.verificar(f"Q4 MongoDB {cid}", lambda: mongo_lab.q4_recomendaciones_mongo(db, cid), verdad.q4(cid), norm_pares)

    if "cassandra" in partes:
        import cassandra_lab
        from comun import cassandra_session
        session = cassandra_session()
        print("\nCassandra")
        for region, dia in params["region_dia"]:
            res.verificar(f"Q2a {region} {dia}", lambda: cassandra_lab.q2a_ventas_region_dia(session, region, dia),
                          verdad.q2a(region, dia), norm_q2a)
        for cid in params["clientes_historial"]:
            res.verificar(f"Q2b {cid}", lambda: cassandra_lab.q2b_ultimos_pedidos_cliente(session, cid),
                          verdad.q2b(cid), norm_q2b)

    if "redis" in partes:
        import redis_lab
        from comun import redis_client
        r = redis_client()
        print("\nRedis")
        res.verificar("Q3 Redis", lambda: redis_lab.q3_top_productos_redis(r), verdad.q3(), norm_pares)

    if "neo4j" in partes:
        import neo4j_lab
        from comun import neo4j_driver
        driver = neo4j_driver()
        print("\nNeo4j")
        for cid in params["clientes_recomendacion"]:
            res.verificar(f"Q4 Neo4j {cid}", lambda: neo4j_lab.q4_recomendaciones_neo4j(driver, cid), verdad.q4(cid), norm_pares)
        driver.close()

    if "stream" in partes:
        import stream_lab
        from comun import cassandra_session, redis_client
        r1, session = redis_client(db=1), cassandra_session()
        esperado = verdad.stream()
        productos = [p["producto_id"] for p in datos["productos"]]
        print("\nStream (corre antes stream_lab.py)")

        def ranking_stream():
            ranking = r1.zrange(stream_lab.CLAVE_RANKING, 0, -1, withscores=True)
            if not ranking:
                raise NotImplementedError("el ranking del stream está vacío: corre scripts/stream_lab.py")
            return {k: int(v) for k, v in ranking}

        res.verificar("Stream Redis idempotente", ranking_stream, esperado, dict)
        res.verificar("Stream Cassandra idempotente",
                      lambda: {k: int(v) for k, v in stream_lab.unidades_por_producto_cassandra(session, productos).items() if v},
                      esperado, dict)

    total = res.ok + res.mal + res.pendientes
    huella = hashlib.sha256(
        (usuario + json.dumps(res.salidas, sort_keys=True, ensure_ascii=False, default=str)).encode()
    ).hexdigest()[:12]
    print(f"\n{res.ok}/{total} verificaciones correctas · {res.mal} con error · {res.pendientes} pendientes")
    print(f"Usuario: {usuario} · partes: {', '.join(partes)} · HUELLA: {huella}")


if __name__ == "__main__":
    main()
