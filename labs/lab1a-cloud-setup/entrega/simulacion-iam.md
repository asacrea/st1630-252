# Evidencia — verificación de la política de mínimo privilegio

**Estudiante:** jjdiazr · **Cuenta AWS:** 040343073329 · **Fecha:** 2026-08-13
**Política evaluada:** [`politica-min-privilegio.json`](politica-min-privilegio.json)

## Restricción del entorno

El sandbox de AWS Academy (rol `voclabs`) **no permite `iam:CreateRole`**:

```
An error occurred (AccessDenied) when calling the CreateRole operation:
User: arn:aws:sts::040343073329:assumed-role/voclabs/user5062485=Juan_Jose_Diaz
is not authorized to perform: iam:CreateRole on resource:
arn:aws:iam::040343073329:role/EMR_EC2_jjdiazr_role
```

Por eso no fue posible crear el rol `EMR_EC2_jjdiazr_role` que propone
`scripts/setup_iam.sh`. La política se verificó igualmente con
`iam:SimulateCustomPolicy`, que evalúa un documento de política sin
necesidad de adjuntarlo a un principal existente.

## Comando usado

```bash
POL=$(python -c "import json;print(json.dumps(json.load(open('politica-min-privilegio.json'))))")

aws iam simulate-custom-policy \
    --policy-input-list "$POL" \
    --action-names <ACCION> \
    --resource-arns <ARN> \
    --query 'EvaluationResults[0].EvalDecision' --output text
```

## Resultados

| # | Acción | Recurso | Resultado | Esperado |
|---|---|---|---|---|
| 1 | `s3:GetObject` | `st1630-jjdiazr-2026/bronze/ventas/prueba_parquet.parquet` | `allowed` | ✅ |
| 2 | `s3:PutObject` | `st1630-jjdiazr-2026/bronze/test.txt` | `allowed` | ✅ |
| 3 | `s3:ListBucket` | `st1630-jjdiazr-2026` | `allowed` | ✅ |
| 4 | `s3:DeleteObject` | `st1630-darcilas1-2026/bronze/dato.csv` (bucket de otro estudiante) | `implicitDeny` | ✅ |
| 5 | `s3:DeleteBucket` | `st1630-jjdiazr-2026` (mi propio bucket) | `implicitDeny` | ✅ |
| 6 | `s3:GetObject` | `datos-confidenciales-eafit/nomina.csv` (bucket ajeno) | `implicitDeny` | ✅ |

Las tres primeras confirman que la política **sí alcanza** para lo que
EMR necesita: leer el Parquet de Bronze, escribir resultados y listar el
bucket. Las tres últimas confirman que **no alcanza para nada más** —
ni siquiera para destruir el propio bucket, porque `s3:DeleteBucket`
nunca se otorgó. `implicitDeny` significa denegado por ausencia de
permiso, no por una regla `Deny` explícita: es el comportamiento por
defecto de IAM y la razón por la que enumerar acciones una por una es
más seguro que usar `s3:*`.
