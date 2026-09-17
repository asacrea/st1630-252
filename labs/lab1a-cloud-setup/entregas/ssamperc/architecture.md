# Arquitectura — Lab 1a
**Curso:** ST1630-2026-2 · **Semana:** S4-S5 · **Fecha de entrega:** 13/08/2026
**Estudiantes:** Hellen Yanes Doria, Sebastian Salazar Henao, Andres Velez Alvarez, Samuel Samper Cardona

## 1. Diagrama de la arquitectura
## 2. Decisiones de S3
| Decisión | Tu elección | Justificación |
|---|---|---|
| Nombre del bucket | st1630-ssamperc-2026 | Convención del curso, único globalmente |
| Región | us-east-1 | Restricción de AWS Academy Learner Lab |
| Estructura de prefijos | bronze/, silver/, gold/ | Arquitectura medallion vista en clase |

**Justificación del particionamiento:** Separar por capas permite tratar cada etapa con reglas distintas (acceso, formato, calidad, costo de almacenamiento) sin que un cambio en los datos crudos (Bronze) rompa lo que consumen los usuarios finales (Gold).

## 3. Decisiones de IAM
- ¿Qué permisos otorgaste al rol de EMR, exactamente?
  → Se usó `EMR_EC2_DefaultRole` (rol predefinido de AWS Academy), ya que la cuenta no permite `iam:CreateRole` para roles custom — confirmado por el profesor como la solución esperada para este entorno.
- ¿Qué permisos consideraste y descartaste?
  → Se había diseñado una política de mínimo privilegio (GetObject/PutObject/DeleteObject/ListBucket solo sobre el bucket propio), pero no se pudo aplicar por la restricción de la cuenta.
- ¿Por qué importa el mínimo privilegio en un sistema distribuido?
  → Aunque en el lab se usa el rol default por restricción de la cuenta, en producción un rol amplio significa que si el clúster EMR se compromete, el atacante hereda todos esos permisos, no solo los que el job realmente necesita — análogo a un nodo que rompe una garantía de consistencia que el resto del sistema asume.

## 4. Decisiones de EMR
- Tipo de instancia: m5.xlarge, 1 master + 1 core — configuración mínima viable para correr Spark en modo distribuido real (no local). Para producción se consideraría más nodos core y/o instancias spot para reducir costo.
- Configuración: Spark + Hadoop instalados vía `--applications Name=Spark Name=Hadoop`.

## 5. Estimación de costo
| Escenario | Costo estimado |
|---|---|
| Clúster encendido 24/7 durante un mes | ~USD 317 (0.44 USD/hora promedio × 24 × 30) |
| Clúster encendido solo durante el lab (~30 min) | ~USD 0.22 |

## 6. Reflexión — la era agéntica
La decisión que más dudas generó fue el bloqueo de permisos IAM en la Parte 3: la cuenta de AWS Academy no permite crear roles custom. Se usó un agente de IA para diagnosticar el error (`AccessDenied` en `iam:CreateRole`) y confirmar mediante `aws iam list-roles` que la cuenta solo exponía roles predefinidos. La decisión de usar `EMR_EC2_DefaultRole` en lugar de insistir con un rol custom se tomó consultando directamente al profesor, no por indicación del agente.

## 7. Bitácora de delegación
| Tarea | ¿Delegado a agente? | Justificación |
|---|---|---|
| Setup de entorno (WSL, AWS CLI, túneles SSH, troubleshooting de conexión) | No | N/A |
| Decisión de usar EMR_EC2_DefaultRole en vez de rol custom | No | Restricción de la cuenta confirmada por el profesor, no una decisión de diseño propia |
| Interpretación del benchmark Parquet vs CSV (Celda 4 del notebook) | Parcial | Análisis propio; el agente ayudó a pulir la redacción |
