"""redis_lab.py — Lab 3 (ST1630-2026-2) · Parte 2: modelo CLAVE-VALOR

Redis guarda todo en memoria y solo sabe buscar por clave: no hay
consultas ad hoc. A cambio ofrece estructuras de datos (hashes, sets,
sorted sets) con operaciones atómicas muy rápidas. El diseño consiste en
decidir qué claves y qué estructura sirven a cada consulta, y mantenerlas
actualizadas en el momento de escribir.

Uso (dentro del contenedor app):
    python scripts/redis_lab.py --usuario <tu-usuario>

Los bloques # TODO son tu trabajo. Respeta la firma y el formato de
retorno de las consultas: scripts/verificar.py las llama.
"""

from comun import cargar_datos, cronometro, medir, pendiente, redis_client, usuario_desde_args

CLAVE_RANKING = "ranking:unidades"


# ── TODO 1 · Catálogo de productos ───────────────────────────────────────────
# Guarda cada producto en un hash con clave "producto:<producto_id>" y
# campos nombre, categoria y precio. Usa un pipeline para no hacer un
# viaje de red por producto.
def cargar_productos(r, productos: list) -> None:
    pipe = r.pipeline(transaction=False)
    for p in productos:
        pipe.hset(
            f"producto:{p['producto_id']}",
            mapping={
                "nombre": p["nombre"],
                "categoria": p["categoria"],
                "precio": p["precio"],
            },
        )
    pipe.execute()


# ── TODO 2 · Ranking de unidades vendidas ────────────────────────────────────
# Mantén en el sorted set CLAVE_RANKING las unidades vendidas por
# producto (miembro = producto_id, puntaje = unidades). Recorre todos los
# ítems de todos los pedidos. Usa un pipeline.
def cargar_ranking(r, pedidos: list) -> None:
    pipe = r.pipeline(transaction=False)
    for pedido in pedidos:
        for item in pedido["items"]:
            pipe.zincrby(CLAVE_RANKING, int(item["cantidad"]), item["producto_id"])
    pipe.execute()


# ── TODO 3 · Q3 en Redis: top de productos ───────────────────────────────────
# Los n productos con más unidades vendidas, MISMO formato y MISMO orden
# que q3_top_productos_mongo: [(producto_id, unidades:int), ...] por
# unidades desc y, en empate, producto_id asc.
# Ojo: ZREVRANGE ordena los empates por miembro en orden INVERSO. Resuélvelo
# sin traerte todo el sorted set.
def q3_top_productos_redis(r, n: int = 10) -> list:
    preliminar = r.zrevrange(CLAVE_RANKING, 0, n - 1, withscores=True)
    if not preliminar:
        return []
    puntaje_corte = preliminar[-1][1]
    candidatos = r.zrevrangebyscore(CLAVE_RANKING, "+inf", puntaje_corte, withscores=True)
    ordenados = sorted(
        ((pid, int(score)) for pid, score in candidatos),
        key=lambda x: (-x[1], x[0]),
    )
    return ordenados[:n]


# ── Carga y ejecución (dado) ─────────────────────────────────────────────────

def cargar(r, datos: dict) -> None:
    r.flushdb()
    with cronometro("carga de productos y ranking en Redis"):
        cargar_productos(r, datos["productos"])
        cargar_ranking(r, datos["pedidos"])


if __name__ == "__main__":
    usuario = usuario_desde_args("Parte 2 · Redis")
    datos = cargar_datos(usuario)
    r = redis_client()

    cargar(r, datos)
    print(f"  claves: {r.dbsize():,} · productos en el ranking: {r.zcard(CLAVE_RANKING)}")

    print("\nQ3 · top 10 productos (Redis)")
    top = medir("Q3 Redis", q3_top_productos_redis, r, repeticiones=50)
    for producto_id, unidades in top:
        print(f"  {producto_id} {r.hget(f'producto:{producto_id}', 'nombre'):<20} {unidades}")
