"""mongo_lab.py — Lab 3 (ST1630-2026-2) · Parte 1: modelo de DOCUMENTO

En un modelo de documento, la unidad de almacenamiento es un "agregado":
todo lo que una consulta necesita junto, guardado junto. La pregunta de
diseño es qué embeber dentro del documento y qué dejar como referencia
(Kleppmann, cap. 2: "localidad" y el problema de las relaciones
muchos-a-muchos).

Uso (dentro del contenedor app):
    python scripts/mongo_lab.py --usuario <tu-usuario>

Los bloques # TODO son tu trabajo. Respeta la firma y el formato de
retorno de cada función de consulta: scripts/verificar.py las llama y
compara su resultado contra la respuesta correcta para TU dataset.
"""

from comun import cargar_datos, cronometro, medir, mongo_db, parametros_prueba, pendiente, usuario_desde_args


# ── TODO 1 · Diseño del documento ────────────────────────────────────────────
# Devuelve el documento que se guardará en la colección "pedidos" para un
# pedido. La consulta Q1 (abajo) debe poder responderse con UNA sola
# lectura por _id, sin $lookup ni consultas extra: el documento tiene que
# traer lo necesario del cliente y de cada producto.
#
# Decide (y justifica en nosql_design.md):
#   - qué datos del cliente embebes y cuáles no (¿qué pasa si el cliente
#     cambia de nombre? ¿necesitas su email aquí?)
#   - qué datos del producto copias en cada ítem (¿el precio del catálogo
#     o el precio_unit que se pagó?)
# Usa pedido_id como _id del documento.
def documento_pedido(pedido: dict, clientes: dict, productos: dict) -> dict:
    """clientes y productos son diccionarios {id: registro}."""
    cli = clientes[pedido["cliente_id"]]
    return {
        "_id": pedido["pedido_id"],
        "pedido_id": pedido["pedido_id"],
        "fecha": pedido["fecha"],
        "region": pedido["region"],
        "canal": pedido["canal"],
        "cliente": {
            "cliente_id": cli["cliente_id"],
            "nombre": cli["nombre"],
            "segmento": cli["segmento"],
        },
        "items": [
            {
                "producto_id": it["producto_id"],
                "nombre": productos[it["producto_id"]]["nombre"],
                "categoria": productos[it["producto_id"]]["categoria"],
                "cantidad": int(it["cantidad"]),
                "precio_unit": int(it["precio_unit"]),
            }
            for it in pedido["items"]
        ],
        "total": int(pedido["total"]),
    }


# ── TODO 2 · Índices ─────────────────────────────────────────────────────────
# Crea los índices que necesitan las consultas Q3 y Q4 de abajo (piensa en
# qué campos filtran o agrupan). El _id ya tiene índice.
def crear_indices(db) -> None:
    db.pedidos.create_index("cliente.cliente_id")
    db.pedidos.create_index("items.producto_id")


# ── TODO 3 · Q1: el pedido completo ──────────────────────────────────────────
# Devuelve el pedido con este formato exacto (lo compara verificar.py):
#   {"pedido_id": str, "fecha": "YYYY-MM-DDTHH:MM:SS",
#    "cliente": {"cliente_id": str, "nombre": str},
#    "items": [{"producto_id": str, "nombre": str, "cantidad": int, "precio_unit": int}, ...],
#    "total": int}
# Los ítems en el mismo orden en que vienen en el pedido. Una sola lectura.
def q1_pedido(db, pedido_id: str) -> dict:
    doc = db.pedidos.find_one({"_id": pedido_id})
    if not doc:
        return {}
    return {
        "pedido_id": doc["pedido_id"],
        "fecha": doc["fecha"],
        "cliente": {
            "cliente_id": doc["cliente"]["cliente_id"],
            "nombre": doc["cliente"]["nombre"],
        },
        "items": [
            {
                "producto_id": it["producto_id"],
                "nombre": it["nombre"],
                "cantidad": int(it["cantidad"]),
                "precio_unit": int(it["precio_unit"]),
            }
            for it in doc["items"]
        ],
        "total": int(doc["total"]),
    }


# ── TODO 4 · Q3 en MongoDB: top de productos ─────────────────────────────────
# Los n productos con más UNIDADES vendidas (suma de cantidad en todos los
# ítems de todos los pedidos), con un aggregation pipeline.
# Formato: [(producto_id, unidades), ...] ordenado por unidades desc y, en
# empate, producto_id asc. En la Parte 2 resuelves lo mismo en Redis y
# comparas.
def q3_top_productos_mongo(db, n: int = 10) -> list:
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.producto_id",
                "unidades": {"$sum": "$items.cantidad"},
            }
        },
        {"$sort": {"unidades": -1, "_id": 1}},
        {"$limit": n},
    ]
    return [(doc["_id"], int(doc["unidades"])) for doc in db.pedidos.aggregate(pipeline)]


# ── TODO 5 · Q4 en MongoDB: recomendaciones ──────────────────────────────────
# "Clientes que compraron lo mismo que tú también compraron...":
#   1. P = productos que ha comprado el cliente.
#   2. Vecinos = OTROS clientes que compraron al menos un producto de P.
#   3. Candidatos = productos comprados por los vecinos que NO están en P.
#   4. Puntaje de un candidato = número de vecinos DISTINTOS que lo compraron.
# Formato: [(producto_id, puntaje), ...] top n, puntaje desc y, en empate,
# producto_id asc.
# Puedes usar aggregation pipelines, varias consultas y lógica en Python:
# lo que importa es que notes cuánto trabajo cuesta una relación
# muchos-a-muchos en este modelo (la resuelves en Neo4j en la Parte 2).
def q4_recomendaciones_mongo(db, cliente_id: str, n: int = 5) -> list:
    propios = set(
        db.pedidos.distinct("items.producto_id", {"cliente.cliente_id": cliente_id})
    )
    if not propios:
        return []
    vecinos = [
        c
        for c in db.pedidos.distinct(
            "cliente.cliente_id",
            {
                "items.producto_id": {"$in": list(propios)},
                "cliente.cliente_id": {"$ne": cliente_id},
            },
        )
    ]
    if not vecinos:
        return []
    pipeline = [
        {"$match": {"cliente.cliente_id": {"$in": vecinos}}},
        {"$unwind": "$items"},
        {"$match": {"items.producto_id": {"$nin": list(propios)}}},
        {
            "$group": {
                "_id": "$items.producto_id",
                "vecinos_set": {"$addToSet": "$cliente.cliente_id"},
            }
        },
        {
            "$project": {
                "_id": 1,
                "puntaje": {"$size": "$vecinos_set"},
            }
        },
        {"$sort": {"puntaje": -1, "_id": 1}},
        {"$limit": n},
    ]
    return [(doc["_id"], int(doc["puntaje"])) for doc in db.pedidos.aggregate(pipeline)]


# ── Carga y ejecución (dado) ─────────────────────────────────────────────────

def cargar(db, datos: dict) -> None:
    clientes = {c["cliente_id"]: c for c in datos["clientes"]}
    productos = {p["producto_id"]: p for p in datos["productos"]}
    db.pedidos.drop()
    with cronometro(f"carga de {len(datos['pedidos']):,} pedidos en MongoDB"):
        db.pedidos.insert_many([documento_pedido(p, clientes, productos) for p in datos["pedidos"]])
        crear_indices(db)


if __name__ == "__main__":
    usuario = usuario_desde_args("Parte 1 · MongoDB")
    datos = cargar_datos(usuario)
    params = parametros_prueba(datos)
    db = mongo_db()

    cargar(db, datos)
    print(f"  documentos: {db.pedidos.count_documents({}):,} · índices: {sorted(db.pedidos.index_information())}")

    print("\nQ1 · pedido completo")
    print(" ", medir("Q1 MongoDB", q1_pedido, db, params["pedidos"][0]))
    print("\nQ3 · top 10 productos (MongoDB)")
    print(" ", medir("Q3 MongoDB", q3_top_productos_mongo, db, repeticiones=5))
    print("\nQ4 · recomendaciones (MongoDB)")
    for cliente_id in params["clientes_recomendacion"]:
        print(f"  {cliente_id}:", medir(f"Q4 MongoDB {cliente_id}", q4_recomendaciones_mongo, db, cliente_id, repeticiones=5))
