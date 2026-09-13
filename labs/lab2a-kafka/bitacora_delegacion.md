---
title: Untitled

---

# Bitácora de Delegación de IA — ST1630

## Principio rector
**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Fecha:** 29/08/2026
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
|Funcionamiento de Docker Desktop |Sí |Se utilizó IA para consultar bien cómo eran las configuraciones necesarias para que esta herramienta funcionara correctamente para el desarrollo de la actividad. |
|Falta de dependencias de Python |Sí |Durante el desarrollo, nos dimos cuenta que nos hacían falta algunas herramientas de python. Es por ello que acudimos a la IA para que nos explicara cuáles necesitábamos y qué comandos correr para descargarlos. |
|Problemas de compatibilidad |Sí | Se experimentaron algunos problemas de compatibilidad entre Spark y Java en Windows. Primero se tuvo que arreglar esta parte, buscando que entre herramientas y sistema operativo coincidieranran entre si para luego continuar con el desarrollo del laboratorio. Para esto también se le preguntó a la IA para que nos explicara cuáles pueden ser las versiones que podemos usar y qué comandos usar para descargarlas.|
|Ejecución del consumidor desde Windows a WSL |Sí | Debido a los problemas de compatibilidad entre Spark y Java directamente en Windows, fue necesario cambiar el ambiente de ejecución del consumidor a WSL (Windows Subsystem for Linux). Se consultó a la IA para entender por qué WSL resolvía este problema y qué pasos seguir para configurar y correr el script desde ahí correctamente. |
