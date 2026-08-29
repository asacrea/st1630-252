------
title: politica-ia.md

---

# Política de uso de IA — ST1630

## Principio rector
**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 19/08/2026
**Estudiante:** 
* Athina Alejandra Cappelleti García (aacappellg@eafit.edu.co)
* David Alejandro Gutiérrez Leal (dagutierrl@eafit.edu.co)
* Emmanuel Álvarez Castrillón (ealvarezc1@eafit.edu.co)
* Ginna Alejandra Valencia Macuace (gavalencim@eafit.edu.co)
* Mariamny Del Valle Ramírez Telles (mvramirezt@eafit.edu.co)

Usar agentes de IA (Claude Code, Copilot, ChatGPT, etc.) es **legítimo**
en este curso. **No saber explicar el resultado, no lo es.** Evaluamos tu
criterio como ingeniero, no si sabes escribir cada línea a mano.

> Regla mnemotécnica del taller de S2: *"la IA dibuja y pule; tú decides
> y firmas."*

## La bitácora de delegación

Cada entregable calificado (labs, talleres, proyecto final) debe incluir,
al final del documento o del README de tu entrega, una tabla como esta:

```markdown
## Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Setup de credenciales AWS | Sí | Troubleshooting repetitivo, bajo valor de aprendizaje |
| Elección de partición del datalake | No | Decisión de diseño central del lab |
| Script de ingesta batch | Parcial | Boilerplate del agente + lógica de negocio propia |
```

**No hay penalización por delegar tareas de bajo valor pedagógico.**
**Sí hay penalización por:**
1. No declarar una delegación.
2. Delegar algo que la rúbrica específica de esa actividad marque
   explícitamente como "debe hacerse a mano".

## Qué se permite delegar por defecto (salvo que la rúbrica diga lo contrario)

- Generar diagramas (Mermaid, etc.) a partir de un diseño que TÚ ya definiste.
- Formatear o pulir la redacción de un documento.
- Resolver dudas puntuales de sintaxis o de una tecnología específica.
- Troubleshooting de instalación/configuración de entornos.
- Boilerplate repetitivo (imports, estructura básica de un script).

## Qué debe hacerse a mano por defecto (salvo que la rúbrica diga lo contrario)

- Extracción de requisitos y decisiones de arquitectura.
- Elección del patrón/tecnología y su justificación.
- Architecture Decision Records (ADR) completos.
- Revisión cruzada del trabajo de otro equipo.
- Cualquier pregunta de un parcial marcada como "sin agentes".



# Bitacora de Delegación
| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
|Consulta de estructura de algunas líneas de código |Sí |Se utilizó IA para averiguar cómo se debían completar algunas líneas de código que se nos solicitaban, como por ejemplo: cuáles eran las funciones a utilizar, qué hacían exactamente y qué impacto tenía en la ejecución de los archivos. |
|Explicación de conceptos |Sí |En algunas partes de los archivos a ejecutar se nos solitaban, por medio de comentarios, contestar algunas preguntas, principalmente seleccionar si la operación a hacer era un NARROW o WIDE y explicar el por qué. |
|Organización de las columnas en las tablas |Sí | A la hora de ejecución, se nos presentó un problema con respecto a eso porque se obtenían resultados raros acerca de los datos nulos (no se obtenía el resultado esperado). Se tuvo que consultar a la IA para ver cuál podría ser la causa y resultó que algunas columnas no estaban en el orden correcto. Por esta razón, en el mismo archivo de _"0.1_bronze.py"_, se tuvo que reacomodar el nombre de esas columnas y volver a ejecutar el archivo para solucionar el problema.|
|Permisos de Athena |Sí | Encontramos un inconveniente en el último paso, ya que parece que no se tenía los suficientes permisos para el uso de Athena, por ende, se dificultaba realizar las consultas. Se tuvo que optar por otra alternativa, el cual fue ejecutar las consultas desde AWS usando el editor de consultas de Athena, en la que nos pudo dar resultado. Por otra parte, por este cambio de planes le tuvimos que pedir a una IA que nos ayudara para crear el archivo `benchmark_resultados.md`. Toda esta información y las evidencias de los resultados se encuentran documentadas en esta ruta: `entregas/usuario/ bitácora_delegacion.md.` |
