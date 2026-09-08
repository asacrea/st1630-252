# ADR-001 — decisión del modelo de arquitectura

> Copia este archivo a tu carpeta `equipo-N/adr.md` y complétalo.
> Formato basado en Nygard, "Documenting Architecture Decisions" (2011).
>
> Elige la decisión MÁS DIFÍCIL o la que más debate generó en tu
> equipo — no la trivial. Un ADR sobre "usamos S3 para archivos" no
> aporta nada; un ADR sobre "elegimos Kappa aunque perdemos
> re-procesabilidad barata" sí.

## Estado

 Aceptado

## Contexto

¿Qué problema u obligación técnica estamos resolviendo? ¿Qué
restricciones del caso (requisitos, presupuesto, equipo) son
relevantes para esta decisión específica?

>  Inicialmente discutimos que arquitectura usar ya que necesitamos cumplir la latencia porque los tableros se refrescan cada hora, throughput porque la información viene de una cadena de 400 tiendas + canal e-commerce, consistencia porque se necesita histórico de 5 años con reprocesamiento , costos/operación porque Presupuesto ajustado: no pueden mantener dos equipos de ingeniería (uno para batch, otro para streaming). que hace referencia al caso 2 . que son restricciones relevantes para una correcta implementación de la arquitectura.

## Opciones consideradas

Como mínimo dos opciones reales (no una opción real vs. un "hombre de
paja" obviamente malo).

### Opción A: Lakehouse

- Ventajas: Permite implementar streamming o batch, o ambos a la vez, describe principalmente almacenamiento, procesamiento y analítica. Organiza la información en capas. Combina datalake y datawarehouse
- Desventajas: Para este caso en específico no encontramos.  

### Opción B: Kappa

- Ventajas:  Reduce la duplicación de lambda (streamming y batch), el histórico puede reprocesarse volviendo  a reproducir los eventos almacenados.
- Desventajas: Es una arquitectura que todo se procesa como un flujo continuo de eventos, es decir es unicamente streamming lo que en el caso no nos serviria pues no es un requisito tener un seguimiento en tiempo real.

## Decisión

¿Qué opción eligieron?

> Se escogio la Opción A: lakehouse.

## Justificación

¿Por qué esta opción y no la otra? Cita explícitamente los ejes
(latencia, throughput, consistencia, costo) que inclinaron la balanza.

> Se escogio como arquitectura Lakehouse (medallion) porque no se necesitan procesar los datos en tiempo real (streamming) ya que en el caso se tiene una baja latencia, se busca conservar los datos y reprocesarlo (consistencia) y como esta arquitectura junta datalake y warehouse evita duplicados lo que reduce costos pues lo que hace es que los organiza y transforma, tambien almacena y procesa los datos (throughput).

## Consecuencias

¿Qué ganan con esta decisión? ¿Qué sacrifican o qué deuda técnica
asumen? ¿Qué tendría que cambiar en el futuro para revisitar esta
decisión?

> En un futuro si se llega a necesitar streamming se puede acoplar facilmente a esta arquitectura y ganamos un mejor costo ya que es la arquitectura más barata. Inicialmente estamos sacrificando la capacidad de procesar y visualizar información en tiempo real. De resto consideramos que no se esta sacrificando nada según los requisitos del caso.
