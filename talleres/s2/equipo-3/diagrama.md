# Diagrama de arquitectura — Caso 1: Fraude bancario

**Equipo:** Hellen Yanes Doria y Samuel Samper Cardona
**Patrón:** Híbrido (streaming Kappa-like para la decisión en tiempo real + Lakehouse batch para el ledger contable exacto)

```mermaid
flowchart LR
    subgraph GEN["1 · Generación"]
        A1["Core bancario<br/>(autorizador de tarjetas)"]
        A2["App / POS / ATM"]
    end

    subgraph ING["2 · Ingesta"]
        B1["Kafka<br/>topic: transacciones<br/>partición por cuenta"]
    end

    subgraph ALM["3 · Almacenamiento"]
        C1["Feature store<br/>(Redis) — estado caliente"]
        C2["S3 bronze<br/>(Delta/Iceberg) — crudo inmutable"]
    end

    subgraph TRA["4 · Transformación"]
        D1["Flink / Structured Streaming<br/>scoring de fraude en vivo"]
        D2["Spark batch nocturno<br/>dedup + reconciliación exacta"]
    end

    subgraph SER["5 · Servicio"]
        E1["API de autorización<br/>aprobar / bloquear / revisar<br/>(&lt; 300 ms)"]
        E2["Ledger contable (gold)<br/>+ reporte regulatorio"]
        E3["Cola de revisión manual<br/>(casos ambiguos)"]
    end

    A1 --> B1
    A2 --> B1
    B1 --> C1
    B1 --> C2
    C1 --> D1
    D1 --> E1
    D1 --> E3
    C2 --> D2
    D2 --> E2
    E2 -. reconciliación / reversos .-> E1

    style GEN fill:#eef2ff,stroke:#4338ca
    style ING fill:#fef3c7,stroke:#b45309
    style ALM fill:#dcfce7,stroke:#15803d
    style TRA fill:#fee2e2,stroke:#b91c1c
    style SER fill:#e0e7ff,stroke:#4338ca
```

## Lectura del diagrama

| Etapa | Tecnología | Por qué |
|---|---|---|
| 1 · Generación | Core bancario / autorizador de tarjetas, app, POS, ATM | Fuente del evento de transacción |
| 2 · Ingesta | Kafka, partición por cuenta/tarjeta | Sostiene 80.000 tx/s sin cuello de botella (throughput) |
| 3 · Almacenamiento | Redis (estado caliente) + S3 bronze en Delta/Iceberg | Redis resuelve features en microsegundos (latencia); Delta/Iceberg da ACID para el rastro contable (consistencia/auditoría) |
| 4 · Transformación | Flink (scoring en vivo) + Spark batch nocturno | Flink no espera ventanas largas (latencia); el batch certifica el número final (consistencia) |
| 5 · Servicio | API de autorización + ledger gold + cola de revisión manual | La API responde en el presupuesto de 300 ms; los casos ambiguos van a revisión humana en vez de sobre-diseñar |

**Corriente subterránea crítica:** seguridad y gobernanza — cada evento lleva un `trace_id` propagado hasta el ledger, para la auditoría regulatoria.

La flecha punteada (ledger → API) representa el proceso de **reconciliación y reversión**: cuando el batch nocturno certifica una discrepancia frente a lo decidido en caliente, se dispara un contracargo — ver el detalle en [`adr.md`](./adr.md).

El mapeo completo por ejes (latencia, throughput, consistencia, costo) está en [`arquitectura.md`](./arquitectura.md).
