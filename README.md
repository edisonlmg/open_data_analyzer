# 📌 Open Data Analyzer 🏗️📊💰

> Explora y analiza información proveniente de diversas fuentes de datos abiertos mediante herramientas interactivas y visuales.

## 🚀 Características

- ✅ Utiliza información de inversiones públicas registradas en el Banco de Inversiones y del gasto eejcutado en el Sistema Integrado de Administración Finanicera - SIAF del MEF.
- ✅ Algunas de las bases de datos del Banco de Inversiones y del SIAF del MEF se acceden desde el [repositorio de datos abiertos](https://datosabiertos.mef.gob.pe/) de dicha entidad y otros son consultados mediante la base de datos Hera de Contraloría, la cual contiene información proporcionada por el MEF mediante convenio.

## 📂 Estructura del Proyecto

```bash
📦 SESNC_SSI_SCRAPING
 ┣ 📂 .venv                       # Entorno virtual
 ┣ 📂 data                        # Datasets
 ┃ ┣ 📂 raw                       # Datasets sin procesar
 ┃ ┣ 📂 processing                # Datasets procesados
 ┣ 📂 report                      # Reporte (producto final)
 ┃ ┣ 📂 figures                   # Imagenes del reporte (jpg, png, etc)
 ┃ ┣ 📂 queries                   # Queries del reporte (csv, xlsx, etc)
 ┃ ┣ 📜 plantilla.docx            # Plantilla para automatizar reporte
 ┣ 📂 src                         # Scripts
 ┣ 📂 notebooks                   # Notebooks de jupyter
 ┣ 📜 .env                        # Variables de entorno
 ┣ 📜 .gitignore                  # Archivos ignorados por Git
 ┣ 📜 requirements.txt            # Dependencias
 ┣ 📜 README.md                   # Documentación
```

Nota: La carpeta 📂.venv aparece solo si se instala un entorno virtual después de clonar el proyecto. Se recomienda su instalación.

## 📥 Instalación y Configuración

### 1️⃣ Clonar el Repositorio

#### 🔐 Repositorio Privado

1. **Autenticarse con GitHub** mediante **SSH** o **Token de Acceso Personal (PAT)**.

   - **SSH** (requiere configurar una clave SSH en GitHub):

     ```sh
     git clone git@github.com:edisonlmg/SESNC_Informe_Inversiones.git
     ```

   - **HTTPS con Token de Acceso Personal (PAT)**:

     ```sh
     git clone https://{TOKEN}git@github.com:edisonlmg/SESNC_Informe_Inversiones.git
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

Instrucciones:

1. ✅ La carpeta 📂**data/raw** debe contener el listado de códigos de inversión a consultar en formato MS Excel. El archivo es proporcionado por el equipo de Infobras y debe contener los códigos únicos de inversión en la primera columna del excel sin filas en blanco por encima de la columna. La ruta del archivo debe especificarse en la sección **"Set Paths"** del archivo 📜**extract.py** ubicado en 📂**src/data**.
3. ✅ El archivo 📜.env debe contener los valores headers y payload correspondientes para cada archivo Fetch a extraer del SSI - MEF.
4. ✅ Ejecutar el script:

    Para extraer información:

    ```sh
    python src/data/extract.py
    ```
    
    Para procesar los datos solicitados por Infobras:

    ```sh
    python src/processing/transform_infobras.py
    ```

## 📜 Licencia
Este proyecto privado no está bajo la licencia.

---

💡 _Hecho con ❤️ por [Edison Mondragón](https://github.com/edisonlmg)
