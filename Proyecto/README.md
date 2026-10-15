# Implementación de un flujo moderno de datos con Python, Apache Airflow, dbt y PostgreSQL

**Universidad de San Carlos de Guatemala (USAC)**  
**Facultad de Ingeniería — Escuela de Ciencias y Sistemas**  
**Seminario de Sistemas 2 — Segundo Semestre 2026**  
**Proyecto 1**  

---

## 1. Información General y Equipo de Trabajo

* **Grupo:** Grupo 22 
* **Integrantes de la Pareja:**
  1. **Nombre:** Christian Alexandro Aragón García — **Carnet:** 202000308
  2. **Nombre:** Luis Carlos de León Torón — **Carnet:** 202212535

---

## 2. Resumen Ejecutivo y Descripción del Problema

**SG-Food** es una empresa dedicada a la distribución y comercialización de productos de consumo masivo en diversas categorías y marcas. La organización presentaba limitaciones críticas en su infraestructura analítica:
* **Procesos manuales y fragmentados** para consolidar datos de ventas, bodega, proveedores y devoluciones.
* **Falta de trazabilidad y linaje técnico** entre la base de datos transaccional (OLTP) y los reportes de negocio.
* **Impacto en el rendimiento del sistema operacional** al ejecutar consultas analíticas complejas directamente contra la base transaccional.
* **Ausencia de pruebas automatizadas de calidad de datos**, lo que provocaba inconsistencias en la toma de decisiones estratégicas.

### Solución Implementada
Se diseñó e implementó un pipeline automatizado bajo el paradigma **ELT (Extract, Load, Transform)**:
1. **Extracción y Carga (Python):** Ingesta controlada desde la base transaccional PostgreSQL (`oltp_sgfood`) y archivos planos delimitados (CSV) hacia la capa `raw` de un Data Warehouse en PostgreSQL. Cada carga aplica transacciones atómicas (Full Refresh) e inyecta metadatos técnicos de auditoría (`_batch_id`, `_fuente`, `_fecha_carga`).
2. **Orquestación (Apache Airflow):** DAG `sgfood_elt_pipeline` que coordina validaciones previas, creación de esquemas, cargas en paralelo, validaciones de integridad y la posterior ejecución de dbt.
3. **Calidad y Validación:** Conciliación estricta de conteos, reglas de dominio y de integridad referencial almacenadas en bitácoras dedicadas (`audit.etl_carga_log` y `audit.validacion_resultado`).
4. **Transformación Analítica (dbt Core):** Proyecto estructurado con fuentes declaradas (`sources`), pruebas automatizadas y contratos de datos para la futura construcción de dimensiones y hechos.

---

## 3. Arquitectura del Flujo de Datos

![Diagrama 1](./Enunciado/Diagrama1.png)

---

## 4. Estructura del Repositorio (`Proyecto1`)

```text
Proyecto1/
├── airflow/
│   ├── dags/
│   │   └── sgfood_elt_dag.py        # Definición del DAG de orquestación en Airflow
│   ├── Dockerfile                   # Imagen personalizada de Airflow 2.10.5 con dbt en venv
│   ├── requirements.txt             # Librerías Python para el entorno de Airflow (psycopg2, pandas)
│   ├── logs/                        # Logs de ejecución de Airflow
│   └── plugins/                     # Plugins personalizados
├── data/
│   ├── sgfood_oltp.sql              # Script DDL + DML de la base transaccional OLTP
│   ├── inventario_bodega.csv        # Snapshot mensual de inventarios
│   ├── proveedores_precios.csv      # Catálogo de precios y tiempos de entrega de proveedores
│   ├── promociones.csv              # Campañas promocionales por categoría
│   ├── metas_ventas.csv             # Metas mensuales de ventas y unidades por sucursal
│   ├── devoluciones.csv             # Registro de devoluciones de producto
│   ├── casos_calidad_opcionales.csv # Datos de prueba intencionales (no se cargan a raw)
│   └── catalogo_insumos_sgfood.xlsx # Diccionario de datos y guía de fuentes
├── dbt/
│   └── sgfood_dw/
│       ├── dbt_project.yml          # Configuración del proyecto dbt (schemas y materializaciones)
│       ├── profiles.yml             # Perfil de conexión a PostgreSQL Data Warehouse
│       ├── macros/
│       │   └── generate_schema_name.sql  # Macro para respetar nombres exactos de schemas
│       └── models/
│           └── staging/
│               └── _sources.yml     # Definición de fuentes raw, freshness y tests
├── scripts/
│   ├── etl/
│   │   ├── __init__.py              # Inicialización del paquete ETL
│   │   ├── config.py                # Configuración centralizada vía variables de entorno
│   │   ├── catalogo.py              # Catálogo declarativo de fuentes OLTP y CSV
│   │   ├── db.py                    # Conexiones psycopg2 con reintentos y transacciones
│   │   ├── extract.py               # Extracción OLTP y lectura/limpieza técnica de CSV
│   │   ├── load.py                  # Carga Full Refresh transaccional por tabla a raw
│   │   ├── auditoria.py             # Registro en audit.etl_carga_log y validacion_resultado
│   │   ├── validate.py              # Validaciones previas, conciliación y chequeos SQL
│   │   └── pipeline.py              # Funciones de alto nivel orquestables
│   └── run_el.py                    # CLI independiente para ejecutar la extracción/carga localmente
├── sql/
│   ├── ddl/
│   │   └── 01_schemas_raw_audit.sql # DDL de esquemas, tablas raw y tablas de auditoría
│   └── validaciones/
│       ├── 01_validaciones_raw.sql  # 25 reglas SQL de integridad y dominio en raw
│       └── post_dbt/
│           └── 01_validaciones_marts.sql # Verificación de esquemas post-dbt
├── docker-compose.yml               # Orquestación de contenedores (OLTP, DW, Airflow)
├── .env.example                     # Plantilla de variables de entorno
├── .gitignore                       # Exclusiones de Git (logs, venv, target de dbt)
└── README.md                        # Documentación técnica completa del proyecto
```

---

## 5. Descripción del Pipeline y Proceso de Extracción y Carga (Python)

El proceso de extracción y carga fue diseñado siguiendo las mejores prácticas de ingeniería de datos para garantizar **reproducibilidad, idempotencia y atomicidad**:

### 5.1 Catálogo Declarativo de Fuentes (`scripts/etl/catalogo.py`)
Todas las entidades se definen de forma desacoplada, basándose en el diccionario de datos:
* **7 Tablas OLTP:** `sucursal`, `categoria`, `marca`, `producto`, `cliente`, `venta`, `venta_detalle`.
* **5 Fuentes CSV Externas:** `inventario_bodega`, `proveedores_precios`, `promociones`, `metas_ventas`, `devoluciones`.

### 5.2 Estrategia de Ingesta y Limpieza Técnica (`scripts/etl/extract.py` y `load.py`)
1. **Extracción OLTP:** Se ejecuta una consulta SQL explícita ordenada por llave primaria utilizando `psycopg2`.
2. **Lectura y Limpieza de Archivos CSV:**
   * Detección y eliminación automática de BOM UTF-8 (`utf-8-sig`).
   * Normalización de encabezados (minúsculas, sin espacios superfluos).
   * Verificación estricta de columnas obligatorias definidas en el catálogo.
   * Eliminación de espacios en blanco (`strip`) y conversión de cadenas vacías a `NULL`.
   * Descarte de filas vacías.
3. **Carga Atómica (Full Refresh):**
   * Cada tabla se carga dentro de una transacción única (`BEGIN; TRUNCATE raw.<tabla>; INSERT ...; COMMIT;`).
   * Si ocurre cualquier error durante la transferencia, se ejecuta un `ROLLBACK` inmediato, preservando el último estado consistente de los datos.
   * Se utiliza `execute_values` para transferencias masivas optimizadas por bloques de 1,000 registros.
4. **Linaje Técnico:**
   Cada fila insertada en el esquema `raw` incorpora tres campos de metadatos:
   * `_batch_id`: Identificador de ejecución (`run_id` de Airflow o timestamp de CLI).
   * `_fuente`: Origen exacto del dato (`oltp:sgfood_oltp.oltp_sgfood.<tabla>` o `csv:<archivo>.csv`).
   * `_fecha_carga`: Marca de tiempo (`TIMESTAMPTZ`) de la inserción.

---

## 6. Orquestación con Apache Airflow (`sgfood_elt_pipeline`)

El flujo completo se orquesta a través del DAG `sgfood_elt_pipeline` implementado en `airflow/dags/sgfood_elt_dag.py`:

### 6.1 Topología y Tareas del DAG
1. **`inicio`:** `EmptyOperator` para señalizar el inicio del ciclo.
2. **`validaciones_previas` (`PythonOperator`):** Valida conectividad a PostgreSQL OLTP y DW, comprueba la existencia del esquema `oltp_sgfood` y confirma la integridad y tamaño de los archivos CSV en disco.
3. **`preparar_dw` (`PythonOperator`):** Ejecuta el DDL idempotente (`sql/ddl/01_schemas_raw_audit.sql`), garantizando que los esquemas (`raw`, `staging`, `intermediate`, `marts`, `audit`) y tablas existan antes de la carga.
4. **`extraccion_carga_oltp` (`TaskGroup`):** Contiene 7 tareas concurrentes (`cargar_sucursal`, `cargar_categoria`, etc.) para ingestar las tablas del sistema transaccional.
5. **`extraccion_carga_csv` (`TaskGroup`):** Contiene 5 tareas concurrentes (`cargar_inventario_bodega`, `cargar_promociones`, etc.) para ingestar los archivos delimitados.
6. **`validaciones_raw` (`TaskGroup`):**
   * **`conciliar_conteos`:** Compara uno a uno los registros en la fuente contra los cargados en `raw` para el `batch_id` actual. Falla si hay discrepancia.
   * **`validar_calidad_raw`:** Evalúa no-nulidad y unicidad de llaves primarias/lógicas, así como 25 reglas SQL de dominio y referencias cruzadas.
7. **`transformacion_dbt` (`TaskGroup`):**
   * **`dbt_debug`:** Verifica la conectividad de dbt hacia el Data Warehouse.
   * **`dbt_deps`:** Instala dependencias externas si existen.
   * **`dbt_source_freshness`:** Evalúa la frescura de las fuentes `raw`.
   * **`dbt_run`:** Compila y materializa los modelos analíticos.
   * **`dbt_test`:** Ejecuta las pruebas unitarias y de esquema de dbt.
8. **`validar_post_dbt` (`PythonOperator`):** Ejecuta verificaciones posteriores en el DW.
9. **`fin`:** `EmptyOperator` que consolida el fin exitoso del flujo.

### 6.2 Resiliencia y Manejo de Errores
* **Reintentos con Backoff Exponencial:** Las tareas críticas tienen configurados hasta 2 reintentos con intervalo progresivo para absorber fallas transitorias de red.
* **Callbacks de Alerta:** Funciones `notificar_fallo` y `notificar_reintento` que capturan el estado, URL de logs e información de la excepción.
* **Aislamiento de dbt:** Para evitar colisiones de dependencias entre Apache Airflow y dbt Core, dbt se instala en un entorno virtual aislado (`/home/airflow/dbt_venv`), ejecutándose limpiamente a través de `BashOperator`.

---

## 7. Esquemas del Data Warehouse y Auditoría (PostgreSQL)

### 7.1 Esquemas Lógicos
| Esquema | Propósito | Motor / Responsable |
|---|---|---|
| `raw` | Datos crudos cargados tal cual desde las fuentes, con columnas de linaje técnico. | Python / Airflow |
| `staging` | Vistas 1:1 con `raw` que aplican tipado estricto, renombramiento y filtros de calidad. | dbt Core |
| `intermediate` | Lógica de negocio reusable, cruces de inventarios y métricas intermedias. | dbt Core |
| `marts` | Modelo dimensional analítico final (dimensiones y hechos). | dbt Core |
| `audit` | Bitácoras de observabilidad, auditoría de cargas y resultados de pruebas. | Python / Airflow |

### 7.2 Tablas de Auditoría (`audit`)
* **`audit.etl_carga_log`:**
  Registra `batch_id`, tabla destino, fuente, tipo de fuente (`OLTP`/`CSV`), estado (`EN_PROCESO`, `OK`, `ERROR`), filas extraídas, filas cargadas, duración en segundos y mensaje de error si aplica.
* **`audit.validacion_resultado`:**
  Almacena cada validación ejecutada (`RAW` o `POST_DBT`), severidad (`ERROR` o `WARNING`), cantidad de registros con anomalía, resultado (`PASS`, `FAIL`, `WARN`) y detalle descriptivo de la regla.

---

## 8. Manual de Implementación y Ejecución

### 8.1 Requisitos Previos
* **Docker Desktop** (con soporte para Docker Compose v2 y Linux Containers habilitado).
* **Git** para clonar el repositorio.
* **Python 3.10+** (opcional, únicamente si se desea ejecutar el script CLI fuera de Docker).

### 8.2 Despliegue de la Infraestructura
1. Abrir una terminal en el directorio raíz del proyecto:
   ```bash
   cd Proyecto1
   ```
2. Construir las imágenes y levantar todos los contenedores en segundo plano:
   ```bash
   docker compose up -d --build
   ```
3. Comprobar que los servicios se encuentren en estado saludable (`healthy` / `Up`):
   ```bash
   docker compose ps
   ```
   Servicios que deben estar activos:
   * `sgfood-oltp`: Puerto `5433` (PostgreSQL Fuente con esquema `oltp_sgfood`).
   * `sgfood-dw`: Puerto `5434` (PostgreSQL Data Warehouse con esquemas `raw`, `audit`, etc.).
   * `sgfood-airflow-db`: Base de datos interna de Airflow.
   * `sgfood-airflow-init`: Inicializador de base de datos y usuario admin (estado `Exited (0)`).
   * `sgfood-airflow-webserver`: Puerto `8080` (Interfaz de usuario web).
   * `sgfood-airflow-scheduler`: Planificador y ejecutor de tareas.

### 8.3 Acceso a la Interfaz de Airflow y Ejecución del DAG
1. Ingresar en el navegador a: [http://localhost:8080](http://localhost:8080)
2. Credenciales por defecto:
   * **Usuario:** `admin`
   * **Contraseña:** `admin`
3. Localizar el DAG **`sgfood_elt_pipeline`**.
4. Activar el interruptor (Toggle) para habilitar el DAG.
5. Hacer clic en el botón de ejecución (**Trigger DAG**) para lanzar una corrida manual.
6. En la vista **Grid** o **Graph**, observar la ejecución en tiempo real de cada una de las fases.

### 8.4 Ejecución Alternativa por Línea de Comandos (CLI sin Airflow)
Si se desea ejecutar el proceso de extracción y carga directamente desde la máquina local (teniendo los contenedores de PostgreSQL encendidos):
```bash
# Instalar requerimientos locales si aplica
pip install psycopg2-binary pandas openpyxl

# Ejecutar el flujo EL completo con validaciones
python scripts/run_el.py

# Opcional: Ejecutar únicamente fuentes OLTP o CSV
python scripts/run_el.py --solo oltp
python scripts/run_el.py --solo csv
```

### 8.5 Consultas SQL de Validación y Auditoría
Para verificar los resultados de las cargas en el Data Warehouse, conectarse a PostgreSQL en el puerto `5434` (usuario: `dw_user`, password: `dw_password`, db: `sgfood_dw`) y ejecutar:

```sql
-- 1. Resumen de cargas registradas en la bitácora
SELECT batch_id, tabla_destino, estado, filas_extraidas, filas_cargadas, duracion_seg
  FROM audit.etl_carga_log
 ORDER BY id_log DESC;

-- 2. Resultados de las validaciones de calidad
SELECT batch_id, etapa, nombre_validacion, severidad, filas_con_problema, resultado
  FROM audit.validacion_resultado
 ORDER BY id_validacion DESC;

-- 3. Conteos actuales en el esquema raw con verificación de linaje
SELECT table_name,
       (xpath('/row/cnt/text()', xml_count))[1]::text::int AS total_filas
  FROM (
    SELECT table_name,
           query_to_xml(format('SELECT count(*) AS cnt FROM raw.%I', table_name), false, true, '') AS xml_count
      FROM information_schema.tables
     WHERE table_schema = 'raw'
  ) t
 ORDER BY table_name;
```

---

## 9. Justificación de Decisiones Técnicas y de Diseño
* **Arquitectura ELT frente a ETL:** Se cargan los datos en estado crudo (`raw`) preservando la fidelidad total del origen antes de aplicar transformaciones complejas en dbt. Esto permite auditar cualquier cálculo retrospectivamente sin requerir reextracciones.
* **Transacciones Atómicas por Tabla en Carga:** La combinación de `TRUNCATE` + `INSERT` dentro de un bloque transaccional asegura que una falla a mitad de carga nunca deje tablas vacías ni parcialmente corruptas en el Data Warehouse.
* **Separación de Entornos en Docker:** La instalación de dbt en un entorno virtual aislado (`/home/airflow/dbt_venv`) dentro del contenedor de Airflow elimina por completo el riesgo de incompatibilidad de librerías entre el runtime de Airflow y los adaptadores de dbt.
* **Manejo Gradual de Severidades (ERROR vs WARNING):** Anomalías operativas graves (como discrepancia en conteos o llaves primarias duplicadas) detienen inmediatamente el pipeline con severidad `ERROR`. Reglas de calidad de negocio (como devoluciones mayores a ventas o descuentos atípicos) se marcan con severidad `WARNING` y se registran en la bitácora de auditoría, permitiendo que la capa de modelado dbt aplique filtros o marque flags de calidad.
