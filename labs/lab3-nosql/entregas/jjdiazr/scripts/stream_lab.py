"""stream_lab.py — Lab 3 (ST1630-2026-2) · Parte 3: INGESTA DE UN STREAM

Las ventas en vivo llegan como un stream con semántica at-least-once,
igual que en el Lab 2a con Kafka: el consumidor confirma su avance cada
cierto número de eventos y, si se cae antes de confirmar, al reiniciar
vuelve a recibir los eventos no confirmados. Además, la fuente ya trae
reentregas (el mismo event_id llega más de una vez, a veces tarde).

Tu trabajo: que los dos destinos (un ranking en vivo en Redis y una tabla
en Cassandra) cuenten cada venta UNA sola vez, aunque el evento llegue
varias veces. Es decir, que la escritura sea idempotente.

Uso (dentro del contenedor app, con Redis y Cassandra arriba):
    python scripts/stream_lab.py --usuario <tu-usuario>
"""

from comun import cargar_datos, cassandra_session, pendiente, redis_client, usuario_desde_args

CLAVE_INGENUO = "stream:ingenuo"
CLAVE_RANKING = "stream:ranking"
COMMIT_CADA = 500
CAIDA_EN = 3700  # posición del stream donde se cae el consumidor (dado)


# ── TODO 1 · Sink idempotente en Redis ───────────────────────────────────────
# Suma evento["cantidad"] al puntaje de evento["producto_id"] en el sorted
# set CLAVE_RANKING, pero SOLO la primera vez que llega ese event_id.
# Usa la base de Redis que recibes (db=1, solo para el stream).
# Decide cómo recordar qué eventos ya procesaste (y por cuánto tiempo:
# en producción esa memoria no puede crecer para siempre).
# En nosql_design.md: ¿tu solución sigue siendo correcta si dos
# consumidores procesan el mismo evento al mismo tiempo? ¿Por qué?
def sink_redis(r, evento: dict) -> None:
    if r.set(f"stream:seen:{evento['event_id']}", 1, nx=True, ex=86400):
        r.zincrby(CLAVE_RANKING, int(evento["cantidad"]), evento["producto_id"])


# ── TODO 2 · Tabla y sink idempotente en Cassandra ───────────────────────────
# Diseña la tabla ventas_stream para que escribir el mismo evento dos veces
# deje la tabla igual (en Cassandra un INSERT con la misma llave primaria
# sobrescribe la fila: úsalo a tu favor). Debe permitir, además, sumar las
# unidades de un producto leyendo una sola partición (TODO 3).
CREATE_VENTAS_STREAM = """
CREATE TABLE ventas_stream (
    producto_id text,
    event_id text,
    cantidad int,
    ts text,
    PRIMARY KEY (producto_id, event_id)
)
"""
INSERT_VENTAS_STREAM = """
INSERT INTO ventas_stream (producto_id, event_id, cantidad, ts)
VALUES (?, ?, ?, ?)
"""


def valores_evento(evento: dict) -> tuple:
    return (
        evento["producto_id"],
        evento["event_id"],
        int(evento["cantidad"]),
        evento["ts"],
    )


# ── TODO 3 · Lectura en Cassandra ────────────────────────────────────────────
# Unidades vendidas por producto en el stream: {producto_id: unidades}
# para todos los productos que aparecen en la tabla, consultando partición
# por partición (sin ALLOW FILTERING).
def unidades_por_producto_cassandra(session, productos: list) -> dict:
    """productos: lista de producto_id del catálogo."""
    stmt = session.prepare("SELECT cantidad FROM ventas_stream WHERE producto_id = ?")
    resultado = {}
    for pid in productos:
        filas = list(session.execute(stmt, (pid,)))
        if filas:
            resultado[pid] = int(sum(f.cantidad for f in filas))
    return resultado


# ── Dado: el consumidor at-least-once ────────────────────────────────────────

def consumir(eventos: list, procesar) -> int:
    """Procesa el stream confirmando cada COMMIT_CADA eventos. En CAIDA_EN
    simula una caída: al reiniciar retoma desde el último commit, así que
    los eventos entre ese commit y la caída se reciben de nuevo."""
    entregas, ultimo_commit, i, cayo = 0, 0, 0, False
    while i < len(eventos):
        if i == CAIDA_EN and not cayo:
            cayo = True
            print(f"  💥 caída en la posición {i}: se reentregan {i - ultimo_commit} eventos desde el último commit")
            i = ultimo_commit
        procesar(eventos[i])
        entregas += 1
        i += 1
        if i % COMMIT_CADA == 0:
            ultimo_commit = i
    return entregas


if __name__ == "__main__":
    usuario = usuario_desde_args("Parte 3 · Stream")
    datos = cargar_datos(usuario)
    eventos = datos["eventos_stream"]
    productos = [p["producto_id"] for p in datos["productos"]]
    unicos = {e["event_id"]: e for e in eventos}
    r, session = redis_client(db=1), cassandra_session()

    # Limpieza: el script se puede correr varias veces desde cero. El stream
    # usa su propia base de Redis (db=1), así que se vacía completa.
    r.flushdb()
    session.execute("DROP TABLE IF EXISTS ventas_stream")
    if "TODO" in CREATE_VENTAS_STREAM or "TODO" in INSERT_VENTAS_STREAM:
        pendiente("TODO 2 · CREATE/INSERT de ventas_stream")
    session.execute(CREATE_VENTAS_STREAM)
    insert = session.prepare(INSERT_VENTAS_STREAM)

    def procesar(evento):
        r.zincrby(CLAVE_INGENUO, evento["cantidad"], evento["producto_id"])  # sink ingenuo, para comparar
        sink_redis(r, evento)
        session.execute(insert, valores_evento(evento))

    entregas = consumir(eventos, procesar)
    reales = sum(e["cantidad"] for e in unicos.values())
    ingenuo = int(sum(score for _, score in r.zrange(CLAVE_INGENUO, 0, -1, withscores=True)))
    idempotente = int(sum(score for _, score in r.zrange(CLAVE_RANKING, 0, -1, withscores=True)))
    en_cassandra = sum(unidades_por_producto_cassandra(session, productos).values())

    print(f"\n  entregas procesadas: {entregas:,} · eventos únicos: {len(unicos):,}")
    print(f"  unidades reales vendidas: {reales:,}")
    print(f"  Redis ingenuo (ZINCRBY a todo): {ingenuo:,}  ({ingenuo - reales:+,} de más)")
    print(f"  Redis idempotente (TODO 1):    {idempotente:,}")
    print(f"  Cassandra (TODO 2-3):          {en_cassandra:,}")
