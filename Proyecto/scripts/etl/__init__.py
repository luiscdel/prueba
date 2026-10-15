"""Paquete de extracción y carga (EL) del proyecto SG-Food.

Módulos:
    config    -> parámetros de conexión y rutas (variables de entorno)
    catalogo  -> definición declarativa de fuentes (tablas OLTP y archivos CSV)
    db        -> utilidades de conexión a PostgreSQL
    extract   -> extracción desde OLTP y lectura/limpieza técnica de CSV
    load      -> carga full-refresh hacia el schema raw del DW
    auditoria -> bitácora de cargas (audit.etl_carga_log)
    validate  -> validaciones previas y posteriores a la carga
    pipeline  -> funciones de alto nivel invocadas por Airflow / CLI
"""

__version__ = "1.0.0"
