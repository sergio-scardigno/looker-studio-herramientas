# Meta Ads to Google Sheets Pipeline

Este proyecto implementa un pipeline automatizado de datos (ETL) que extrae métricas de campañas de Meta Ads, las exporta a un archivo CSV y las carga automáticamente en una hoja de Google Sheets. El objetivo final es alimentar un dashboard en Looker Studio.

## 🏗 Arquitectura del Proyecto

El proyecto está modularizado en `src/` para facilitar el mantenimiento:

*   **`src/main.py`**: Orquestador principal. Define el rango de fechas (por defecto "ayer"), inicia la extracción y gestiona la subida.
*   **`src/meta_client.py`**: Maneja la autenticación con la API de Meta (Facebook Ads).
*   **`src/extract.py`**: Realiza la consulta a la API de Meta para obtener métricas (impresiones, clics, gasto, etc.) a nivel de anuncio (`ad`) y guarda los resultados en `meta_ads.csv`.
*   **`src/upload_sheets.py`**: Se conecta a Google Sheets usando una Service Account, limpia la hoja destino y sube los datos nuevos.

---

## 🚀 Guía de Configuración Paso a Paso

Para que este script funcione, necesitas configurar permisos en Meta (Facebook) y en Google Cloud. Sigue estos pasos detalladamente.

### Parte 1: Configuración de Meta Ads (Facebook)

1.  **Crear una App en Meta for Developers**:
    *   Ve a [developers.facebook.com](https://developers.facebook.com/).
    *   Crea una nueva App de tipo **"Negocios" (Business)**.
2.  **Obtener Identificador de la Cuenta Publicitaria (Ad Account ID)**:
    *   Ve a tu [Administrador de Anuncios](https://adsmanager.facebook.com/).
    *   Copia el número de cuenta que aparece en la URL o en el selector de cuentas (ej. `act_123456789`).
3.  **Obtener Access Token (Token de Acceso)**:
    *   **Opción Rápida (Pruebas)**: Usa la herramienta [Graph API Explorer](https://developers.facebook.com/tools/explorer/). Selecciona tu App y genera un token con los permisos `ads_read` y `read_insights`. *Nota: Estos tokens caducan rápido.*
    *   **Opción Producción (Recomendada)**:
        *   Ve a la **Configuración del Negocio** (Business Manager).
        *   Ve a **Usuarios** -> **Usuarios del sistema**.
        *   Añade un usuario del sistema (rol "Empleado" o "Admin").
        *   Haz clic en "Generar nuevo token".
        *   Selecciona tu App.
        *   Marca los permisos: **`ads_read`**, **`read_insights`**.
        *   Copia el token generado. Este token es permanente o de larga duración.

### Parte 2: Configuración de Google Cloud y Sheets

1.  **Crear Proyecto en Google Cloud**:
    *   Ve a [console.cloud.google.com](https://console.cloud.google.com/).
    *   Crea un nuevo proyecto.
2.  **Habilitar APIs**:
    *   En el menú, ve a **APIs y servicios** -> **Biblioteca**.
    *   Busca y habilita: **Google Sheets API**.
    *   Busca y habilita: **Google Drive API**.
3.  **Crear Service Account (Cuenta de Servicio)**:
    *   Ve a **APIs y servicios** -> **Credenciales**.
    *   Haz clic en "Crear credenciales" -> **Cuenta de servicio**.
    *   Dale un nombre (ej. `meta-ads-uploader`).
    *   En "Roles", puedes darle "Propietario" (rápido) o "Editor" (más seguro).
    *   Finaliza la creación.
4.  **Descargar Credenciales (JSON)**:
    *   Haz clic en la Service Account creada (la dirección de email que aparece).
    *   Ve a la pestaña **Claves** (Keys).
    *   "Agregar clave" -> "Crear nueva clave" -> **JSON**.
    *   Se descargará un archivo `.json`. **Renómbralo a `credentials.json`** y colócalo en la carpeta raíz de este proyecto.
5.  **Dar acceso al Google Sheet (¡CRUCIAL!)**:
    *   Abre tu archivo de Google Sheets en el navegador.
    *   Copia el **ID de la hoja** de la URL (la cadena larga entre `/d/` y `/edit`).
    *   Haz clic en el botón **Compartir** (Share).
    *   Copia el **email de la Service Account** (ej. `meta-ads-uploader@tu-proyecto.iam.gserviceaccount.com`) y pégalo en el cuadro de compartir.
    *   Dale permisos de **Editor**.

---

## 💻 Instalación y Ejecución

### 1. Preparar el entorno

```bash
# Clonar el repositorio (si aplica)
# cd meta-ads-pipeline

# Crear entorno virtual
python3 -m venv venv

# Activar entorno virtual
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt
```

### 2. Configurar Variables de Entorno

Copia el archivo de ejemplo y edítalo:

```bash
cp .env.example .env
nano .env  # O usa tu editor favorito
```

Rellena el archivo `.env` con los datos obtenidos en la Parte 1 y 2:

| Variable | Descripción | Ejemplo |
| :--- | :--- | :--- |
| `META_ACCESS_TOKEN` | Token obtenido en Meta Developers | `EAA...` |
| `META_AD_ACCOUNT_ID` | ID de la cuenta publicitaria | `act_123456789` |
| `START_DATE` | (Opcional) Fecha inicio extracción | `2023-01-01` |
| `END_DATE` | (Opcional) Fecha fin extracción | `2023-01-31` |
| `GOOGLE_SHEET_ID` | ID del Google Sheet destino | `1BxiM...` |
| `GOOGLE_CREDS_PATH` | Ruta al JSON de credenciales | `credentials.json` |

*Nota: Si no defines `START_DATE` ni `END_DATE`, el script extraerá automáticamente los datos de **ayer**.*

### 3. Ejecución Manual

Para probar que todo funciona:

```bash
python src/main.py
```

Deberías ver logs indicando la conexión a Meta, la descarga de datos, y la subida a Google Sheets.

### 4. Automatización (Cron Job)

Para que el script se ejecute solo todos los días (ej. a las 06:00 AM):

1.  Abre el editor de cron:
    ```bash
    crontab -e
    ```
2.  Añade la siguiente línea (ajusta las rutas a tu sistema):

    ```cron
    0 6 * * * cd /home/usuario/meta-ads-pipeline && /home/usuario/meta-ads-pipeline/venv/bin/python src/main.py >> /home/usuario/meta-ads-pipeline/execution.log 2>&1
    ```

---

## 🛠 Solución de Problemas

*   **Error: `SpreadsheetNotFound`**: Verifica que el `GOOGLE_SHEET_ID` sea correcto y que hayas compartido la hoja con el email de la Service Account.
*   **Error: `FacebookRequestError`**: Verifica que el `META_ACCESS_TOKEN` no haya caducado y que el `META_AD_ACCOUNT_ID` sea correcto (debe empezar con `act_` si no lo pusiste en el .env, el script lo añade, pero verifícalo).
*   **Datos vacíos**: Si el script dice "No data found", verifica que la cuenta publicitaria tenga actividad en las fechas seleccionadas.
