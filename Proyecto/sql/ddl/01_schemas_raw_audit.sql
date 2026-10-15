-- =============================================================================
-- SG-Food Data Warehouse - DDL de schemas, capa RAW y auditoría
-- Motor: PostgreSQL 16
--
-- * Es idempotente (IF NOT EXISTS): se ejecuta al iniciar el contenedor del DW
--   y también desde la tarea "preparar_dw" del DAG de Airflow.
-- * La capa RAW replica la estructura de las fuentes SIN llaves primarias ni
--   foráneas, a propósito: los datos se cargan "tal cual" y la calidad se
--   valida después (validaciones Python + pruebas dbt).
-- * Cada tabla RAW incluye columnas técnicas de linaje:
--     _batch_id     -> identificador de la corrida (run_id de Airflow)
--     _fuente       -> origen del registro (tabla OLTP o archivo CSV)
--     _fecha_carga  -> timestamp de la carga
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;
CREATE SCHEMA IF NOT EXISTS audit;

COMMENT ON SCHEMA raw          IS 'Datos crudos cargados con Python desde OLTP y CSV';
COMMENT ON SCHEMA staging      IS 'Modelos dbt de limpieza/estandarización (1:1 con raw)';
COMMENT ON SCHEMA intermediate IS 'Modelos dbt intermedios (lógica de negocio reutilizable)';
COMMENT ON SCHEMA marts        IS 'Modelo dimensional (dimensiones y hechos) construido con dbt';
COMMENT ON SCHEMA audit        IS 'Bitácora de cargas y resultados de validaciones del pipeline';

-- -----------------------------------------------------------------------------
-- RAW - Fuente transaccional (oltp_sgfood)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.sucursal (
    id_sucursal     INTEGER,
    nombre          VARCHAR(100),
    ciudad          VARCHAR(100),
    departamento    VARCHAR(100),
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.categoria (
    id_categoria    INTEGER,
    nombre          VARCHAR(100),
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.marca (
    id_marca        INTEGER,
    nombre          VARCHAR(100),
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.producto (
    id_producto     INTEGER,
    sku             VARCHAR(20),
    nombre          VARCHAR(150),
    id_categoria    INTEGER,
    id_marca        INTEGER,
    unidad_medida   VARCHAR(30),
    costo_base      NUMERIC(12,2),
    precio_lista    NUMERIC(12,2),
    activo          BOOLEAN,
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.cliente (
    id_cliente      INTEGER,
    nit             VARCHAR(20),
    nombre          VARCHAR(150),
    tipo_cliente    VARCHAR(40),
    municipio       VARCHAR(100),
    departamento    VARCHAR(100),
    fecha_alta      DATE,
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.venta (
    id_venta        BIGINT,
    fecha           DATE,
    id_cliente      INTEGER,
    id_sucursal     INTEGER,
    canal           VARCHAR(30),
    metodo_pago     VARCHAR(30),
    estado          VARCHAR(20),
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.venta_detalle (
    id_detalle      BIGINT,
    id_venta        BIGINT,
    id_producto     INTEGER,
    cantidad        INTEGER,
    precio_unitario NUMERIC(12,2),
    descuento       NUMERIC(5,4),
    subtotal        NUMERIC(14,2),
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- RAW - Fuentes externas (archivos CSV)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.inventario_bodega (
    fecha_corte        DATE,
    id_sucursal        INTEGER,
    id_producto        INTEGER,
    stock_disponible   INTEGER,
    stock_minimo       INTEGER,
    stock_maximo       INTEGER,
    lote               VARCHAR(30),
    fecha_vencimiento  DATE,
    _batch_id          VARCHAR(250) NOT NULL,
    _fuente            VARCHAR(250) NOT NULL,
    _fecha_carga       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.proveedores_precios (
    id_proveedor     INTEGER,
    proveedor        VARCHAR(150),
    id_producto      INTEGER,
    costo_proveedor  NUMERIC(12,2),
    plazo_dias       INTEGER,
    fecha_vigencia   DATE,
    _batch_id        VARCHAR(250) NOT NULL,
    _fuente          VARCHAR(250) NOT NULL,
    _fecha_carga     TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.promociones (
    id_promocion          INTEGER,
    nombre                VARCHAR(150),
    fecha_inicio          DATE,
    fecha_fin             DATE,
    id_categoria          INTEGER,
    porcentaje_descuento  NUMERIC(5,4),
    _batch_id             VARCHAR(250) NOT NULL,
    _fuente               VARCHAR(250) NOT NULL,
    _fecha_carga          TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.metas_ventas (
    periodo         VARCHAR(7),
    id_sucursal     INTEGER,
    meta_ventas     NUMERIC(14,2),
    meta_unidades   INTEGER,
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.devoluciones (
    id_devolucion   INTEGER,
    fecha           DATE,
    id_venta        BIGINT,
    id_producto     INTEGER,
    cantidad        INTEGER,
    motivo          VARCHAR(150),
    _batch_id       VARCHAR(250) NOT NULL,
    _fuente         VARCHAR(250) NOT NULL,
    _fecha_carga    TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- AUDIT - Bitácora de cargas y validaciones
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit.etl_carga_log (
    id_log              BIGSERIAL PRIMARY KEY,
    batch_id            VARCHAR(250) NOT NULL,
    tabla_destino       VARCHAR(150) NOT NULL,
    fuente              VARCHAR(250) NOT NULL,
    tipo_fuente         VARCHAR(20)  NOT NULL,           -- OLTP | CSV
    estado              VARCHAR(20)  NOT NULL,           -- EN_PROCESO | OK | ERROR
    filas_extraidas     INTEGER,
    filas_cargadas      INTEGER,
    inicio              TIMESTAMPTZ  NOT NULL DEFAULT now(),
    fin                 TIMESTAMPTZ,
    duracion_seg        NUMERIC(10,2),
    mensaje_error       TEXT
);
CREATE INDEX IF NOT EXISTS idx_etl_carga_log_batch ON audit.etl_carga_log (batch_id);

CREATE TABLE IF NOT EXISTS audit.validacion_resultado (
    id_validacion       BIGSERIAL PRIMARY KEY,
    batch_id            VARCHAR(250) NOT NULL,
    etapa               VARCHAR(30)  NOT NULL,           -- RAW | POST_DBT
    nombre_validacion   VARCHAR(200) NOT NULL,
    tabla               VARCHAR(150),
    severidad           VARCHAR(10)  NOT NULL,           -- ERROR | WARNING
    filas_con_problema  INTEGER      NOT NULL,
    resultado           VARCHAR(10)  NOT NULL,           -- PASS | FAIL | WARN
    detalle             TEXT,
    fecha_ejecucion     TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_validacion_resultado_batch ON audit.validacion_resultado (batch_id);
