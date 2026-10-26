"""cassandra_lab.py — Lab 3 (ST1630-2026-2) · Parte 1: modelo COLUMNAR ANCHO

En Cassandra no se modela a partir de las entidades sino a partir de las
CONSULTAS: una tabla por consulta, y la llave primaria decide todo.
  - La partition key decide en qué nodo vive cada fila y qué filas se
    pueden leer juntas de forma eficiente (una consulta = una partición).
  - Las clustering columns deciden el orden de las filas DENTRO de la
    partición.
No hay JOIN, y filtrar por una columna que no está en la llave exige
ALLOW FILTERING (leer todo el clúster): es justo lo que hay que evitar.

Uso (dentro del contenedor app):
    python scripts/cassandra_lab.py --usuario <tu-usuario>

Los bloques # TODO son tu trabajo. Respeta la firma y el formato de
retorno de las consultas: scripts/verificar.py las llama.
"""

from datetime import date, datetime

from cassandra.concurrent import execute_concurrent_with_args

from comun import cargar_datos, cassandra_session, cronometro, medir, parametros_prueba, pendiente, usuario_desde_args


# ── TODO 1 · Tabla para Q2a ──────────────────────────────────────────────────
# Consulta a servir: "los pedidos de una REGIÓN en un DÍA, del más reciente
# al más antiguo". Con ella Q2a calcula cuántos pedidos y cuánto vendió esa
# región ese día, y cuáles fueron los 5 más recientes.
# Escribe el CREATE TABLE ventas_por_region_dia (...) con las columnas que
# necesites y, sobre todo, su PRIMARY KEY. Justifica en nosql_design.md la
# partition key y las clustering columns que elegiste.
CREATE_VENTAS_REGION_DIA = """
CREATE TABLE ventas_por_region_dia (
    region text,
    dia date,
    fecha timestamp,
    pedido_id text,
    cliente_id text,
    canal text,
    total bigint,
    PRIMARY KEY ((region, dia), fecha, pedido_id)
) WITH CLUSTERING ORDER BY (fecha DESC, pedido_id DESC)
"""

# ── TODO 2 · Tabla para Q2b ──────────────────────────────────────────────────
# Consulta a servir: "los 10 pedidos más recientes de un CLIENTE".
# Escribe el CREATE TABLE pedidos_por_cliente (...).
CREATE_PEDIDOS_CLIENTE = """
CREATE TABLE pedidos_por_cliente (
    cliente_id text,
    fecha timestamp,
    pedido_id text,
    region text,
    canal text,
    total bigint,
    PRIMARY KEY (cliente_id, fecha, pedido_id)
) WITH CLUSTERING ORDER BY (fecha DESC, pedido_id DESC)
"""

# ── TODO 3 · Inserción ───────────────────────────────────────────────────────
# Escribe el INSERT de cada tabla (con ? como marcadores) y la función que
# convierte un pedido en la tupla de valores de ese INSERT. Un mismo pedido
# se escribe en las DOS tablas: en Cassandra duplicar datos por consulta es
# lo normal (¿por qué? respóndelo en nosql_design.md).
# Pista: el driver acepta datetime para columnas timestamp y date para date.
INSERT_VENTAS_REGION_DIA = """
INSERT INTO ventas_por_region_dia (region, dia, fecha, pedido_id, cliente_id, canal, total)
VALUES (?, ?, ?, ?, ?, ?, ?)
"""
INSERT_PEDIDOS_CLIENTE = """
INSERT INTO pedidos_por_cliente (cliente_id, fecha, pedido_id, region, canal, total)
VALUES (?, ?, ?, ?, ?, ?)
"""


def valores_ventas_region_dia(pedido: dict) -> tuple:
    dt = datetime.fromisoformat(pedido["fecha"])
    return (
        pedido["region"],
        dt.date(),
        dt,
        pedido["pedido_id"],
        pedido["cliente_id"],
        pedido["canal"],
        int(pedido["total"]),
    )


def valores_pedidos_cliente(pedido: dict) -> tuple:
    dt = datetime.fromisoformat(pedido["fecha"])
    return (
        pedido["cliente_id"],
        dt,
        pedido["pedido_id"],
        pedido["region"],
        pedido["canal"],
        int(pedido["total"]),
    )


# ── TODO 4 · Q2a ─────────────────────────────────────────────────────────────
# Para una región y un día ("YYYY-MM-DD") devuelve:
#   (num_pedidos, total_vendido, [los 5 pedido_id más recientes])
# leyendo UNA sola partición, sin ALLOW FILTERING. Puedes usar las
# funciones de agregación de CQL o sumar en Python.
def q2a_ventas_region_dia(session, region: str, dia: str) -> tuple:
    filas = list(
        session.execute(
            "SELECT pedido_id, total FROM ventas_por_region_dia WHERE region = %s AND dia = %s",
            (region, date.fromisoformat(dia)),
        )
    )
    return (
        len(filas),
        int(sum(f.total for f in filas)),
        [f.pedido_id for f in filas[:5]],
    )


# ── TODO 5 · Q2b ─────────────────────────────────────────────────────────────
# Los 10 pedidos más recientes de un cliente, del más reciente al más antiguo:
#   [(pedido_id, "YYYY-MM-DDTHH:MM:SS", total), ...]
# El orden debe salir de la tabla (clustering), no de un sort en Python.
def q2b_ultimos_pedidos_cliente(session, cliente_id: str) -> list:
    filas = session.execute(
        "SELECT pedido_id, fecha, total FROM pedidos_por_cliente WHERE cliente_id = %s LIMIT 10",
        (cliente_id,),
    )
    return [
        (f.pedido_id, f.fecha.strftime("%Y-%m-%dT%H:%M:%S"), int(f.total))
        for f in filas
    ]


# ── Carga y ejecución (dado) ─────────────────────────────────────────────────

def cargar(session, datos: dict) -> None:
    for tabla in ("ventas_por_region_dia", "pedidos_por_cliente"):
        session.execute(f"DROP TABLE IF EXISTS {tabla}")
    for ddl in (CREATE_VENTAS_REGION_DIA, CREATE_PEDIDOS_CLIENTE):
        if "TODO" in ddl:
            pendiente("TODO 1/2 · CREATE TABLE")
        session.execute(ddl)

    pedidos = datos["pedidos"]
    with cronometro(f"carga de {len(pedidos):,} pedidos × 2 tablas en Cassandra"):
        for insert, valores in ((INSERT_VENTAS_REGION_DIA, valores_ventas_region_dia),
                                (INSERT_PEDIDOS_CLIENTE, valores_pedidos_cliente)):
            if "TODO" in insert:
                pendiente("TODO 3 · INSERT")
            preparado = session.prepare(insert)
            execute_concurrent_with_args(session, preparado, [valores(p) for p in pedidos], concurrency=64)


if __name__ == "__main__":
    usuario = usuario_desde_args("Parte 1 · Cassandra")
    datos = cargar_datos(usuario)
    params = parametros_prueba(datos)
    session = cassandra_session()

    cargar(session, datos)
    for tabla in ("ventas_por_region_dia", "pedidos_por_cliente"):
        print(f"  {tabla}: {session.execute(f'SELECT COUNT(*) FROM {tabla}').one()[0]:,} filas")

    print("\nQ2a · ventas de una región en un día")
    for region, dia in params["region_dia"]:
        print(f"  {region} {dia}:", medir(f"Q2a {region} {dia}", q2a_ventas_region_dia, session, region, dia))
    print("\nQ2b · últimos 10 pedidos de un cliente")
    for cliente_id in params["clientes_historial"]:
        resultado = medir(f"Q2b {cliente_id}", q2b_ultimos_pedidos_cliente, session, cliente_id)
        print(f"  {cliente_id}: {len(resultado)} pedidos, el más reciente {resultado[0] if resultado else '-'}")
