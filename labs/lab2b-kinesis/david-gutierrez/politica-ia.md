---
title: politica-ia.md

---

---
title: bitacora_delegacion.md
---

# Política de uso de IA — ST1630

## Principio rector
**Curso:** ST1630-2026-2 · **Semana:** S8-S9 · **Fecha:** 13/09/2026  
**Estudiante:** 
* Athina Alejandra Cappelleti García (aacappellg@eafit.edu.co)
* David Alejandro Gutiérrez Leal (dagutierrl@eafit.edu.co)
* Emmanuel Álvarez Castrillón (ealvarezc1@eafit.edu.co)
* Ginna Alejandra Valencia Macuace (gavalencim@eafit.edu.co)
* Mariamny Del Valle Ramírez Telles (mvramirezt@eafit.edu.co)

Usar agentes de IA (Claude Code, Copilot, ChatGPT, etc.) es **legítimo** en este curso. **No saber explicar el resultado, no lo es.** Evaluamos tu criterio como ingeniero, no si sabes escribir cada línea a mano.

> Regla mnemotécnica del taller de S2: *"la IA dibuja y pule; tú decides y firmas."*

## La bitácora de delegación

Cada entregable calificado (labs, talleres, proyecto final) debe incluir, al final del documento o del README de tu entrega, una tabla como esta:


## Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Configuración de compatibilidad con Java 17 | Sí | Al ejecutar el entorno local para seguir el tutorial del `README.md`, se presentó una incompatibilidad por tener Java 22 instalado en el sistema en lugar de Java 17 (requerido por Spark/Delta). Se delegó a la IA el troubleshooting de la versión del JDK y las banderas de entorno necesarias. |
| Elección del tamaño de ventana y Watermark | No | Decisión de diseño central de la Parte 2. Se definió manualmente una ventana de 5 minutos con un watermark de 10 minutos para equilibrar la tolerancia a datos tardíos y la liberación de estado en memoria. |
| Diseño de la llave del MERGE en el Sink | No | Decisión de arquitectura central de la Parte 3. Se definió manualmente la llave compuesta `(window_start, window_end, region)` para garantizar que las actualizaciones por micro-batch de agregados de una misma ventana sean idempotentes. |
| Verificación empírica de idempotencia | No | Se corrió manualmente el script del productor en múltiples iteraciones para comprobar en la tabla Delta que los agregados se actualizaban (UPDATE) sin duplicar filas. |
| Redacción de `streaming_design.md` | No | Entregable analítico escrito a mano a partir de las observaciones reales durante el despliegue y los requerimientos de la rúbrica. |