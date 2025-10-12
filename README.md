# 📌 Open Data Analyzer 🏗️📊💰

Herramienta para la extracción, procesamiento y análisis de datos financieros publicados por la Superintendencia de Banca, Seguros y AFP (SBS) de Perú.

## 🚀 Características

- ✅ **Extracción Automatizada**: Descarga automáticamente los reportes de Estados Financieros (EEFF) y Tipo de Cambio (TC) desde el portal de la SBS.
- ✅ **Procesamiento de Datos**: Transforma los archivos Excel descargados en DataFrames de Pandas limpios y estructurados.
- ✅ **Persistencia en la Nube**: Almacena y versiona los datos procesados en Google Cloud Storage (GCS) para un acceso fácil y seguro.
- ✅ **Descarga Incremental**: Compara los datos existentes en GCS con los reportes disponibles en la web de la SBS y descarga únicamente la información faltante, optimizando el tiempo y los recursos.
- ✅ **Logging Detallado**: Registra cada paso del proceso, facilitando el seguimiento y la depuración.

## 📂 Estructura del Proyecto

```bash
📦 open_data_analyzer
 ┣ 📂 .venv/                      # Entorno virtual de Python
 ┣ 📂 src/                        # Código fuente del proyecto
 ┃ ┣ 📂 modules/                  # Módulos especializados
 ┃ ┃ ┣ 📜 gcs_manager.py          # Gestiona la conexión y operaciones con GCS
 ┃ ┃ ┣ 📜 sbs_data_fetcher.py     # Descarga datos desde la web de la SBS
 ┃ ┃ ┗ 📜 sbs_data_processing.py  # Procesa los archivos Excel descargados
 ┃ ┣ 📜 main_sbs.py              # Orquestador principal del proceso
 ┃ ┗ 📜 utils.py                  # Funciones de utilidad (ej. logger)
 ┣ 📂 notebooks/                  # Jupyter Notebooks para análisis exploratorio
 ┣ 📜 .env                        # Archivo para variables de entorno (no versionado)
 ┣ 📜 .gitignore                  # Archivos y carpetas ignorados por Git
 ┣ 📜 requirements.txt            # Dependencias de Python
 ┗ 📜 README.md                   # Esta documentación
```

Nota: La carpeta 📂.venv aparece solo si se instala un entorno virtual después de clonar el proyecto. Se recomienda su instalación.

## 📥 Instalación y Configuración

### 1️⃣ Clonar el Repositorio

#### 🔐 Repositorio Privado

1. **Autenticarse con GitHub** mediante **SSH** o **Token de Acceso Personal (PAT)**.

   - **SSH** (requiere configurar una clave SSH en GitHub):

     ```sh
     git clone git@github.com:edisonlmg/open_data_analyzer.git
     ```

   - **HTTPS con Token de Acceso Personal (PAT)**:

     ```sh
     git clone https://{TOKEN}git@github.com:edisonlmg/open_data_analyzer.git
     ```

2. Reemplaza `{TOKEN}` con tu token de acceso generado en [GitHub Tokens](https://github.com/settings/tokens).

### 2️⃣ Crear y Activar un Entorno Virtual

#### 🖥️ Desde la Terminal

```sh
python -m venv venv
# Activar en Windows
venv\Scripts\activate
# Activar en Mac/Linux
source venv/bin/activate
```

#### 🖥️ Desde VS Code

1. Abre **VS Code** en la carpeta del proyecto.
2. Pulsa `Ctrl + Shift + P` y busca `Python: Select Interpreter`.
3. Selecciona el intérprete de **venv**.
4. Abre una terminal (`Ctrl + Ñ`) y activa el entorno con:

   ```sh
   source venv/bin/activate  # Mac/Linux
   venv\Scripts\activate    # Windows
   ```

### 3️⃣ Actualizar pip (en caso sea necesario)

1. En entornos sin restricción de SSL

    ```sh
    python.exe -m pip install --upgrade pip
    ```

2. En entornos con restricción de SSL

    ```sh
    python.exe -m pip install --upgrade pip --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org
    ```


### 4️⃣ Instalar dependencias

1. En entornos sin restricción de SSL

    ```sh
    pip install -r requirements.txt
    ```

2. En entornos con restricción de SSL

    ```sh
    pip install --requirement requirements.txt --trusted-host pypi.org --trusted-host files.pythonhosted.org --trusted-host pypi.python.org
    ```

### 5️⃣ Instalar nuevas dependencias

1. En entornos sin restricción de SSL

    ```sh
    pip install {nombre_nueva_dependencia}
    ```

2. En entornos con restricción de SSL

    ```sh
    pip install {nombre_nueva_dependencia} --trusted-host pypi.org --trusted-host files.pythonhosted.org
    ```

3. Actualizar archivo de dependencias

    ```sh
    pip freeze > requirements.txt
    ```

### 6️⃣ Configuración de Variables de Entorno

1. Crea un archivo `.env` en la raíz del proyecto y define las variables necesarias, por ejemplo:

   ```ini
   SECRET_KEY={valor_aqui}
   DATABASE_URL={valor_aqui}
   ```

2. Para cargar estas variables en Python, usa `python-dotenv` y `os`:

   ```python
   from dotenv import load_dotenv
   import os

   load_dotenv()

   secret_key = os.getenv("SECRET_KEY")
   database_url = os.getenv("DATABASE_URL")
   ```

3. **El archivo `.env` no está incluido en el repositorio**, por lo que debes solicitarlo o crearlo manualmente.

### 7️⃣ Ordenar Importaciones

Para ordenar las liberías en los archivos 📜.py, se recomienta usar isort antes de hacer commit en el proyecto.

```sh
# Instalar isort si no está instalado

pip install isort
```

```sh
# Ejecutar isort en todos los archivos

isort .
```

```sh
# Ejecutar isort en un archivo específico

isort archivo.py
```

## 🚀 Uso

```markdown
## Instrucciones de Ejecución

El script principal `main_sbs.py` orquesta todo el proceso automatizado. Una vez completada la configuración, puedes ejecutarlo desde la raíz del proyecto:

```sh
python src/main_sbs.py
```

### ¿Qué hace el script?

El proceso de actualización sigue estos pasos:

1. **Conexión a GCS**  
   Se conecta a Google Cloud Storage usando las credenciales configuradas en el archivo `.env`.

2. **Descarga de Datos Base**  
   Descarga los archivos `SBS_EEFF_PROCESSED.csv` y `SBS_TC_PROCESSED.csv` desde tu bucket de GCS para identificar qué datos ya existen.

3. **Detección de Novedades**  
   Compara las fechas de los datos existentes con los reportes disponibles en la web de la SBS para identificar información faltante.

4. **Descarga de Nuevos Reportes**  
   Si encuentra meses o reportes faltantes, los descarga automáticamente en memoria.

5. **Procesamiento**  
   Transforma los nuevos archivos Excel a un formato tabular estructurado y normalizado.

6. **Actualización y Carga**  
   Concatena los datos nuevos con los existentes y sube las versiones actualizadas a GCS:
   - `SBS_EEFF_PROCESSED.csv`
   - `SBS_TC_PROCESSED.csv`
   - `SBS_EEFF_ANALYZED.csv` (archivo de análisis)

> **Nota:** Si no hay archivos nuevos por descargar, el proceso terminará informando que los datos ya están actualizados.

```

## 📜 Licencia
Este proyecto privado no está bajo la licencia.

---

💡 _Hecho por [Edison Mondragón](https://github.com/edisonlmg)
