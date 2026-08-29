# Resultados del benchmark Athena — Lab 1b

**Curso:** ST1630-2026-2 · **Semana:** S5-S6 · **Generado:** ejecución de `04_athena_benchmark.py`

## Resultados crudos

| Query | Tiempo motor (ms) | Tiempo total (s) | Bytes escaneados |
|---|---:|---:|---:|
| 5.1 Top 5 regiones (Gold Parquet, Z-ordered) | 1399 | 1.40 | 29,768 |
| 5.2 Misma query (CSV sin particionar) | 624 | 0.62 | 774,922 |

## Ratio de bytes escaneados

**CSV / Parquet = 26.03x**

> Completa la Pregunta 5 de `pipeline_analysis.md` con este número:
> ¿coincide con el orden de magnitud teórico (~9x) visto en el slide
> de S4? Si no coincide, ¿a qué se lo atribuyes -- tamaño de la
> muestra, efecto del Z-ordering, selectividad de la query?

Profesor, un cordial saludo. Debido a que tuvimos problemas con la ejecución del archivo "04_athena_benchmark.py", el problema fue de autorización, similar al problema que teníamos a la hora de ejecutar "setup_iam.sh". Lo que decidimos hacer es ejecutar las consultas desde AWS usando el editor de consultas de Athena. Después de haber ejecutado las consultas, le pedimos a una IA que nos hiciera un archivo .md similar al que debió haber salido con nuestros resultados, que es este archivo.

Te contamos todo esto para tener transparencia y para generar más transparencia aún, adjuntamos las imágenes de las consultas:

## Consulta #1:

![alt text](<WhatsApp Image 2026-08-22 at 8.57.20 PM.jpeg>)

## Consulta #2:

![alt text](<WhatsApp Image 2026-08-22 at 8.56.52 PM.jpeg>)

## Respuesta a la pregunta:

El resultado obtenido de 26.03x no coincide con el teórico de 9x, ya que la diferencia puede atribuirse principalmente a las características de nuestra muestra de datos y al formato utilizado. En este caso, el CSV sin particionar requiere escanear una cantidad mucho mayor de datos, mientras que Parquet almacena la información de forma más eficiente y permite aprovechar la estructura de las columnas; además, el Z-ordering puede reducir aún más los datos que Athena necesita leer debido a la selectividad de la consulta por región y fecha. Por ello, el ratio observado puede ser considerablemente superior al valor teórico esperado.
