# Bitácora de delegación — Lab 2a

**Curso:** ST1630-2026-2 · **Semana:** S6-S7 · **Fecha:** 28/08/2026  
**Equipo:** Mateo Sanz Medina (`msanzm@eafit.edu.co`), Samuel Arango (`sarangoe3@eafit.edu.co`), Nathalia Cardoza (`nvcardozaa@eafit.edu.co`)

Este laboratorio sigue las políticas de uso de IA definidas en `docs/politica-ia.md`.

## Resumen de tareas y delegación

| Tarea | Integrante Responsable | ¿Delegado a agente? | Justificación / Herramienta |
|---|---|---|---|
| Setup de Infraestructura KRaft y Docker Compose | **Mateo** | No | Verificación del archivo `docker-compose.yml` monopunto en KRaft y levantamiento de contenedores. |
| Creación del Topic e Inspección Parte 0 | **Samuel** | No | Creación manual del topic `pedidos-ventas` (4 particiones, factor 1) e inspección en Kafka UI. |
| Implementación del Productor (`key=region`, `acks='all'`) | **Mateo** | Parcial | La decisión de diseño de usar `key=region` y `acks='all'` fue tomada por el estudiante. Se usó asistencia de IA para la sintaxis puntual de serializadores UTF-8/JSON en `kafka-python`. |
| Implementación del Consumidor (`at-least-once`) | **Nathalia** | Parcial | La decisión de diseño de `enable_auto_commit=False` y la coreografía commit-post-MERGE se asumió por los estudiantes. Se delegó el formateo de las columnas de trazabilidad `_kafka_*`. |
| Ejecución y evidencia de Prueba de Idempotencia | **Nathalia** | No | Ejecución manual del consumidor, detención con Ctrl+C antes del commit y conteo directo en Delta Lake demostrando $N = N'$. |
| Redacción de respuestas en `kafka_design.md` | **Todos** | No | Análisis técnico propio del balanceo por region, *hot partitions*, KRaft y escalabilidad 100×. |
