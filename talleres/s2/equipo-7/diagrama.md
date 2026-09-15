# Diagrama de arquitectura — Equipo Andres Velez - Sebastian Salazar · Caso 1 (Fraude bancario)

Arquitectura **Lambda**: dos caminos (speed layer + batch layer) que
convergen en la capa de servicio.

```mermaid
flowchart TD
    subgraph GEN["🏦 1 · Generación"]
        direction LR
        A1["💳 Tarjetas NFC<br/>débito / crédito"]
        A2["📱 App móvil"]
        A3["🏧 Cajeros ATM"]
        A4["🏪 Puntos físicos POS"]
    end

    B1{{"📡 2 · Ingesta<br/><b>Kafka</b><br/>log distribuido de transacciones"}}

    A1 --> B1
    A2 --> B1
    A3 --> B1
    A4 --> B1

    B1 ==>|"streaming<br/>evento a evento"| C1
    B1 -.->|"recolección<br/>por lotes"| D1

    subgraph SPEED["⚡ SPEED LAYER — camino rápido"]
        direction TB
        C1["🧠 <b>Spark Streaming</b><br/>reglas de detección de fraude<br/>en tiempo real"]
        C2[("🗄️ <b>Redis</b><br/>NoSQL en memoria<br/>estado reciente por tarjeta/cliente")]
        C1 <-->|"consulta<br/>&lt; 1 ms"| C2
    end

    subgraph BATCH["🐢 BATCH LAYER — camino exacto"]
        direction TB
        D1[("🪣 <b>S3 / Object Storage</b><br/>lake — histórico del día")]
        D2["🧮 <b>Spark Batch</b><br/>recálculo exacto nocturno"]
        D1 --> D2
    end

    C1 ==>|"✅ decisión<br/>&lt; 300 ms"| E1
    D2 -.->|"💰 número exacto<br/>centavo a centavo"| E2

    subgraph SERV["📤 5 · Servicio"]
        direction LR
        E1["🔌 API baja latencia<br/>aprobar / bloquear<br/>POS · ATM · app"]
        E2["📊 Reporte de conciliación<br/>dashboard interno<br/>equipo contabilidad"]
    end

    classDef gen fill:#f5f0ff,stroke:#8b5cf6,stroke-width:1.5px,color:#3b0764
    classDef ingest fill:#fef9e7,stroke:#d4a017,stroke-width:2px,color:#4a3800
    classDef speed fill:#e6f4ff,stroke:#2563eb,stroke-width:2px,color:#1e3a8a
    classDef batch fill:#fff1e6,stroke:#ea580c,stroke-width:2px,color:#7c2d12
    classDef serv fill:#e9fbe9,stroke:#16a34a,stroke-width:2px,color:#14532d

    class A1,A2,A3,A4 gen
    class B1 ingest
    class C1,C2 speed
    class D1,D2 batch
    class E1,E2 serv

    style SPEED fill:#f0f8ff,stroke:#2563eb,stroke-width:2px
    style BATCH fill:#fff8f0,stroke:#ea580c,stroke-width:2px
    style GEN fill:#faf8ff,stroke:#8b5cf6,stroke-width:1px,stroke-dasharray: 3 3
    style SERV fill:#f2fbf2,stroke:#16a34a,stroke-width:1px,stroke-dasharray: 3 3
```

**Leyenda visual:**
- Flecha gruesa (`==>`) = camino **caliente**, tiempo real.
- Flecha punteada (`-.->`) = camino **frío**, procesamiento por lotes.
- Morado = Generación · Amarillo = Ingesta · Azul = Speed layer · Naranja = Batch layer · Verde = Servicio.

**Lectura del diagrama:**
- El *speed layer* (azul) resuelve la decisión de bloqueo/aprobación en
  tiempo real, consultando estado reciente en Redis.
- El *batch layer* (naranja) relee el lake al final del día y calcula
  el número de conciliación exacto, sin presión de tiempo.
- Ambos caminos comparten la misma fuente (Kafka) pero corren lógica
  separada — ese es el "precio de Lambda" discutido en el ADR.
