# Meta Ads to Google Sheets Pipeline

Este proyecto implementa un pipeline automatizado de datos (ETL) que extrae métricas de campañas de Meta Ads, las exporta a un archivo CSV y las carga automáticamente en una hoja de Google Sheets. El objetivo final es alimentar un dashboard en Looker Studio.

## 🏗 Arquitectura del Proyecto

El proyecto está modularizado en `src/` para facilitar el mantenimiento:

*   **`src/main.py`**: Orquestador principal. Define el rango de fechas (por defecto "ayer"), inicia la extracción, transformación y gestiona la subida.
*   **`src/meta_client.py`**: Maneja la autenticación con la API de Meta (Facebook Ads).
*   **`src/extract.py`**: Realiza la consulta a la API de Meta para obtener métricas (impresiones, clics, gasto, etc.) a nivel de anuncio (`ad`) y guarda los resultados en `meta_ads.csv`.
*   **`src/transform.py`**: Normaliza los datos raw y genera múltiples tablas optimizadas para reporting (por día, por campaña, por anuncio, etc.).
*   **`src/upload_sheets.py`**: Se conecta a Google Sheets usando una Service Account y sube múltiples tablas a pestañas separadas.

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
    *   **Opción Rápida (Pruebas)**: Usa la herramienta [Graph API Explorer](https://developers.facebook.com/tools/explorer/).
        *   Selecciona tu App en el dropdown "App de Meta".
        *   Selecciona el usuario o página en "Usuario o página".
        *   En la pestaña **"Permissions"**, busca y agrega los siguientes permisos:
            *   `ads_read` (OBLIGATORIO - para leer datos de anuncios)
            *   `read_insights` (OBLIGATORIO - para leer métricas e insights)
        *   Haz clic en **"Generate Access Token"** para generar un nuevo token.
        *   Copia el token generado (aparece en el campo "Token de acceso").
        *   *Nota: Estos tokens caducan rápido (1-2 horas). Para producción, usa la opción de Business Manager.*
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
#Windows
.\venv\Scripts\Activate.ps1

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
| `FETCH_FROM_BEGINNING` | (Opcional) Si es `true`, obtiene datos desde la primera campaña | `true` |
| `GOOGLE_SHEET_ID` | ID del Google Sheet destino | `1BxiM...` |
| `GOOGLE_CREDS_PATH` | Ruta al JSON de credenciales | `credentials.json` |

**Notas sobre fechas:**
* Si no defines `START_DATE` ni `FETCH_FROM_BEGINNING`, el script extraerá automáticamente los datos de **ayer**.
* Si defines `FETCH_FROM_BEGINNING=true`, el script consultará la API para obtener la fecha de inicio de la primera campaña y usará esa fecha como `START_DATE`.
* Si defines `START_DATE` explícitamente, se usará esa fecha (ignora `FETCH_FROM_BEGINNING`).
* Si no defines `END_DATE`, el script usará **ayer** como fecha final.

### 3. Ejecución Manual

Para probar que todo funciona:

```bash
python src/main.py
```

Deberías ver logs indicando la conexión a Meta, la descarga de datos, la transformación y la subida de múltiples tablas a Google Sheets.

El script generará las siguientes pestañas en tu Google Sheet:

1. **`ad_daily`**: Datos diarios por anuncio (granularidad máxima)
   - Incluye: IDs de campaña, conjunto y anuncio, nombres, métricas de rendimiento, mensajes totales, costos
   - Útil para: Análisis detallado de cada creativo/anuncio

2. **`messages_daily`**: Resumen diario de mensajes
   - Incluye: Fecha, mensajes totales, gasto, costo por mensaje, clics, CPC
   - Útil para: Ver tendencias diarias de mensajes y eficiencia

3. **`campaign_daily`**: Resumen diario por campaña
   - Incluye: Fecha, campaña, mensajes totales, gasto, costo por mensaje, clics, CPC, CTR
   - Útil para: Comparar rendimiento entre campañas

4. **`adset_daily`**: Resumen diario por conjunto de anuncios
   - Incluye: Fecha, conjunto, campaña, métricas agregadas
   - Útil para: Análisis a nivel de conjunto de anuncios

5. **`top_ads_period`**: Ranking de anuncios del período completo
   - Incluye: Anuncios ordenados por mensajes totales y costo por mensaje
   - Útil para: Identificar los mejores y peores creativos del período

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

*   **Error 403: "Ad account owner has NOT grant ads_management or ads_read permission"**:
    *   **Causa**: Tu token de acceso no tiene los permisos necesarios.
    *   **Solución**: 
        1. Ve al [Graph API Explorer](https://developers.facebook.com/tools/explorer/).
        2. Asegúrate de tener seleccionada tu App y tu usuario/página.
        3. En la pestaña "Permissions", agrega los permisos `ads_read` y `read_insights`.
        4. Genera un nuevo token haciendo clic en "Generate Access Token".
        5. Copia el nuevo token y actualízalo en tu archivo `.env`.
*   **Error: `SpreadsheetNotFound`**: Verifica que el `GOOGLE_SHEET_ID` sea correcto y que hayas compartido la hoja con el email de la Service Account.
*   **Error: `FacebookRequestError`**: Verifica que el `META_ACCESS_TOKEN` no haya caducado y que el `META_AD_ACCOUNT_ID` sea correcto (debe empezar con `act_` si no lo pusiste en el .env, el script lo añade, pero verifícalo).
*   **Datos vacíos**: Si el script dice "No data found", verifica que la cuenta publicitaria tenga actividad en las fechas seleccionadas.

---

## 📊 Conexión con Looker Studio

### Configuración de Data Sources

1. **Crear un nuevo Data Source en Looker Studio**:
   * Ve a [Looker Studio](https://lookerstudio.google.com/)
   * Clic en "Create" -> "Data Source"
   * Selecciona "Google Sheets" como conector
   * Ingresa la URL de tu Google Sheet o busca por nombre

2. **Seleccionar la pestaña apropiada**:
   * Para análisis de mensajes diarios: usa `messages_daily`
   * Para comparar campañas: usa `campaign_daily`
   * Para análisis detallado de anuncios: usa `ad_daily`
   * Para identificar top performers: usa `top_ads_period`

3. **Configurar campos**:
   * Asegúrate de que los campos de fecha (`date_start`) estén configurados como "Date"
   * Los campos numéricos (spend, clicks, etc.) deben ser "Number"
   * Los campos de texto (nombres, IDs) deben ser "Text"

### Ejemplos de Reportes Recomendados

**Reporte de Mensajes por Día**:
- Data Source: `messages_daily`
- Dimension: `date_start`
- Métricas: `messages_total`, `spend`, `cost_per_message`
- Visualización: Gráfico de líneas temporal

**Ranking de Campañas**:
- Data Source: `campaign_daily`
- Dimension: `campaign_name`
- Métricas: `messages_total`, `cost_per_message`, `cpc`
- Visualización: Tabla con ordenamiento

**Análisis de Creativos**:
- Data Source: `top_ads_period`
- Dimension: `ad_name`
- Métricas: `messages_total`, `cost_per_message`, `ctr`
- Visualización: Tabla con filtros y ordenamiento

**Comparación de Conjuntos de Anuncios**:
- Data Source: `adset_daily`
- Dimension: `adset_name`
- Métricas: `messages_total`, `spend`, `cpc`
- Visualización: Gráfico de barras comparativo
