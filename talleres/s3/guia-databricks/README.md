# Guía de setup — Databricks Free Edition + GitHub

**Curso:** ST1630-2026-2 · **Semana:** S3 · **Última edición:** 2026-07-30

Guía paso a paso para crear tu cuenta de **Databricks Free Edition**,
conectarla con tu cuenta de GitHub y clonar allí tu fork de este
repositorio (`st1630-252`), para que puedas ejecutar el notebook del
taller (`../notebook_base/wordcount_reseñas.ipynb`) dentro de un
workspace real en vez de en local.

> Esta guía cubre el setup de la plataforma. El taller en sí (los 6
> pasos del lab, tiempos, rúbrica) está en `../README.md`.

## Checklist final

- [ ] Cuenta de GitHub creada y con correo verificado (deberías tenerla
      desde S1 — ver `../../../README.md`, sección "Cómo entregar").
- [ ] Fork de `st1630-252` creado en tu cuenta de GitHub.
- [ ] Cuenta de Databricks Free Edition creada y con acceso al workspace.
- [ ] GitHub integrado en Databricks mediante un Personal Access Token (PAT).
- [ ] Tu fork clonado en Databricks (Workspace → Git folder) y visible
      en tu árbol de archivos.

## Prerequisitos

- Correo institucional `@eafit.edu.co` (recomendado para el registro,
  aunque Databricks Free Edition también acepta correos personales).
- Fork de `st1630-252` ya creado en tu cuenta de GitHub. Si aún no lo
  tienes: entra al repositorio del curso en GitHub, haz clic en
  **Fork**, elige tu usuario como *Owner* y deja **desmarcada** la
  opción de copiar solo la rama principal (`main` only) — así tu fork
  llega con toda la estructura de ramas que puedas necesitar durante
  el semestre.

## 1. Crear la cuenta de Databricks Free Edition

1. Busca "Databricks Free Edition" en tu navegador y entra al
   resultado oficial (`databricks.com`), o ve directamente a
   [community.cloud.databricks.com](https://community.cloud.databricks.com).
2. Haz clic en **Registrarse** ("Sign up") y completa tus datos.
   Usa tu correo `@eafit.edu.co` si el formulario lo permite.
3. Si Databricks solicita un **código de verificación**, revisa tu
   correo y complétalo antes de continuar.
4. Al terminar, entrarás directamente al **workspace** de Databricks
   (la interfaz principal, con el menú lateral: *Workspace*, *Compute*,
   *Catalog*, etc.).

**Verifica:** puedes ver el menú lateral izquierdo con las secciones
*Workspace*, *Compute* y *Catalog*, y tu correo aparece arriba a la
derecha si abres el menú de usuario.

## 2. Conectar GitHub con Databricks (Personal Access Token)

Para que Databricks pueda clonar tu fork, primero hay que conectar
ambas cuentas con un **Personal Access Token (PAT)** de GitHub.

### 2.1 Crear el token en GitHub

1. En GitHub, haz clic en tu foto de perfil (arriba a la derecha) →
   **Settings**.
2. Baja hasta **Developer settings** (al final del menú lateral).
3. Entra a **Personal access tokens → Tokens (classic)**.
4. Haz clic en **Generate new token → Generate new token (classic)**.
5. Asigna un nombre descriptivo (p. ej. `databricks-st1630`) y una
   expiración corta — 30 días es suficiente para un semestre; puedes
   regenerarlo si expira.
6. En **Select scopes**, marca únicamente lo necesario para clonar y
   sincronizar tu fork:
   - `repo` (si tu fork es privado, o si quieres poder hacer push desde
     Databricks).
   - `public_repo` en vez de `repo` si tu fork es público y solo vas a
     leer/clonar (alcance más restringido, mejor práctica de seguridad).

   No marques scopes de administración de organización, workflows ni
   paquetes — no los necesitas para este flujo y ampliar el alcance del
   token innecesariamente es un riesgo si se filtra.
7. Haz clic en **Generate token**.

**Importante:** el token solo se muestra **una vez**. Cópialo de
inmediato a un gestor de contraseñas. Si lo pierdes, genera uno nuevo
(no hay forma de recuperarlo). Y nunca lo pegues en un chat, notebook,
commit o archivo del repositorio — trátalo como una contraseña.

### 2.2 Registrar el token en Databricks

1. En Databricks, haz clic en tu usuario (arriba a la derecha) →
   **Settings**.
2. Busca **Linked accounts** (cuentas conectadas) → **Git integration**.
3. Haz clic en **Add / Añadir Git credential**.
4. Proveedor: **GitHub**. Método: **Personal access token**.
5. Ingresa tu usuario de GitHub y pega el token que generaste.
6. Deja el campo **Host** vacío si aparece como opcional (aplica para
   GitHub estándar, no GitHub Enterprise).
7. Guarda los cambios.

**Verifica:** la credencial aparece listada en *Git integration* con tu
usuario de GitHub y sin errores.

## 3. Clonar tu fork en Databricks

1. En tu fork de `st1630-252` en GitHub, haz clic en **Code** y copia
   la URL **HTTPS** (termina en `.git`).
2. En Databricks, ve a **Workspace**.
3. Haz clic en **Create → Git folder**.
4. Pega la URL de tu fork. Verifica que el **Git provider** quede como
   **GitHub** (normalmente se detecta automático).
5. Haz clic en **Create Git folder**.

**Verifica:** el árbol de archivos de tu fork (`talleres/`, `labs/`,
`docs/`, `README.md`, etc.) aparece completo dentro de tu Workspace de
Databricks. Abre `talleres/s3/notebook_base/wordcount_reseñas.ipynb`
para confirmar que se ve y que puedes adjuntarlo a un cluster.

## Troubleshooting

| Error / síntoma | Causa probable | Solución |
|---|---|---|
| Error al clonar el Git folder | El token está mal pegado, expiró, o no tiene el scope `repo`/`public_repo` | Genera un token nuevo en GitHub con el scope correcto y actualiza la credencial en Databricks (*Settings → Linked accounts*) |
| Clonaste el repo equivocado | Pegaste la URL del repo original del curso en vez de tu **fork** | Ve a tu propia cuenta de GitHub, entra a tu fork, copia la URL desde ahí y vuelve a crear el Git folder |
| No encuentras "Workspace → Create → Git folder" | La UI de Databricks cambia de nombre entre versiones | Busca "Workspace" en el menú lateral izquierdo; el botón de creación puede decir "Create" o "+ New" según la versión |
| El cluster no arranca o queda "Terminated" | Los clusters de Free Edition se auto-terminan tras inactividad, o la cuenta gratuita tiene límites de cómputo | Reinicia el cluster desde **Compute**, reatáchalo al notebook, y vuelve a ejecutar desde la primera celda (el estado en memoria se pierde) |
| Compartiste el token sin querer (commit, chat, captura) | — | Revócalo de inmediato en GitHub (**Settings → Developer settings → Personal access tokens**) y genera uno nuevo |

## Referencias

- [Databricks Free Edition](https://community.cloud.databricks.com)
- [Documentación oficial de Databricks — Git folders](https://docs.databricks.com/)
- [GitHub — Managing your personal access tokens](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens)
- Ver también `../recursos.md` para las referencias teóricas de la Semana 3.
