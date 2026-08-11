# Arquitectura — Lab 1a

**Curso:** ST1630-2026-2 · **Semana:** S4-S5 · **Fecha de entrega:** _(completar)_
**Estudiante:** _(nombre y correo @eafit.edu.co)_

> Copia este archivo a tu carpeta de entrega (`entregas/<tu-usuario>/architecture.md`)
> y complétalo. No lo edites aquí en `plantillas/`.

## 1. Diagrama de la arquitectura

Pega tu diagrama (bloque \`\`\`mermaid o ASCII) mostrando: bucket S3 con
las 3 capas, el rol IAM, y el clúster EMR leyendo desde S3.

> _(pega tu diagrama aquí)_

## 2. Decisiones de S3

| Decisión | Tu elección | Justificación |
|---|---|---|
| Nombre del bucket | | |
| Región | | |
| Estructura de prefijos | | |

**Justificación del particionamiento** (3-5 líneas): ¿por qué esa
estructura de prefijos y no otra? ¿Consideraste particionar además por
fecha o región dentro de cada capa?

> → [tu respuesta aquí]

## 3. Decisiones de IAM

- ¿Qué permisos otorgaste al rol de EMR, exactamente?

  → [tu respuesta aquí]

- ¿Qué permisos consideraste y descartaste? ¿Por qué?

  → [tu respuesta aquí]

- ¿Por qué importa el mínimo privilegio específicamente en un sistema
  **distribuido** como este (no solo "es buena práctica")? Conecta con
  el Teorema CAP: un agente/rol con acceso excesivo es, en cierto
  sentido, un riesgo análogo al de un nodo que retorna datos
  inconsistentes — ambos rompen una garantía que el resto del sistema
  asume que se sostiene.

  → [tu respuesta aquí]

## 4. Decisiones de EMR

- Tipo de instancia elegido y justificación (¿por qué es "mínimo
  viable" para este ejercicio, y qué cambiarías para producción?):

  → [tu respuesta aquí]

- Configuración de Spark/aplicaciones instaladas:

  → [tu respuesta aquí]

## 5. Estimación de costo

| Escenario | Costo estimado |
|---|---|
| Clúster encendido 24/7 durante un mes | → [tu cálculo aquí, con la calculadora de AWS] |
| Clúster encendido solo durante las ~3 horas que lo usaste para el lab | → [tu cálculo aquí] |

## 6. Reflexión — la era agéntica

¿En qué decisión de este lab dudaste más? ¿Qué le consultaste a un
agente de IA y qué terminaste decidiendo por tu cuenta?

> _(escribe aquí, 3-5 líneas)_

## 7. Bitácora de delegación

| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| | | |
| | | |

> Recuerda: los permisos IAM, la estructura de prefijos, las
> justificaciones de este documento y la interpretación de los
> resultados de Spark deben reflejar tu propio criterio (ver
> `../../../docs/politica-ia.md`).
