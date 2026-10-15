-- =============================================================================
-- SG-Food - Validaciones posteriores a dbt (capa marts)
--
-- La tarea "validar_post_dbt" del DAG ejecuta todos los *.sql de esta carpeta
-- con el mismo formato que 01_validaciones_raw.sql. Mientras no existan
-- modelos marts, este archivo solo contiene una verificación de los schemas.
-- Agregar aquí las validaciones de dimensiones y hechos en la siguiente fase.
-- =============================================================================

-- nombre: schemas_dw_existentes
-- tabla: information_schema.schemata
-- severidad: ERROR
-- descripcion: Deben existir los schemas raw, staging, intermediate y marts
SELECT 4 - count(*) FROM information_schema.schemata
 WHERE schema_name IN ('raw', 'staging', 'intermediate', 'marts');

-- nombre: modelos_dbt_esperados_existentes
-- tabla: information_schema.tables
-- severidad: ERROR
-- descripcion: Deben existir los 18 modelos esperados en staging, intermediate y marts
WITH modelos_esperados(schema_name, relation_name) AS (
    VALUES
        ('staging', 'stg_oltp_sucursal'),
        ('staging', 'stg_oltp_categoria'),
        ('staging', 'stg_oltp_marca'),
        ('staging', 'stg_oltp_producto'),
        ('staging', 'stg_oltp_cliente'),
        ('staging', 'stg_oltp_venta'),
        ('staging', 'stg_oltp_venta_detalle'),
        ('staging', 'stg_archivos_inventario_bodega'),
        ('staging', 'stg_archivos_proveedores_precios'),
        ('staging', 'stg_archivos_promociones'),
        ('staging', 'stg_archivos_metas_ventas'),
        ('staging', 'stg_archivos_devoluciones'),
        ('intermediate', 'int_ventas_detalle'),
        ('marts', 'dim_producto'),
        ('marts', 'dim_sucursal'),
        ('marts', 'dim_cliente'),
        ('marts', 'fct_ventas'),
        ('marts', 'fct_inventario_bodega')
)
SELECT count(*)
FROM modelos_esperados
WHERE to_regclass(format('%I.%I', schema_name, relation_name)) IS NULL;

-- nombre: fct_ventas_no_vacio
-- tabla: marts.fct_ventas
-- severidad: ERROR
-- descripcion: El hecho de ventas debe contener registros
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM marts.fct_ventas) THEN 0
    ELSE 1
END;

-- nombre: fct_inventario_no_vacio
-- tabla: marts.fct_inventario_bodega
-- severidad: ERROR
-- descripcion: El hecho de inventario debe contener registros
SELECT CASE
    WHEN EXISTS (SELECT 1 FROM marts.fct_inventario_bodega) THEN 0
    ELSE 1
END;

-- nombre: filas_intermedio_vs_hecho_ventas
-- tabla: marts.fct_ventas
-- severidad: ERROR
-- descripcion: El hecho debe conservar todas las líneas del modelo intermedio
SELECT abs(
    (SELECT count(*) FROM intermediate.int_ventas_detalle)
    -
    (SELECT count(*) FROM marts.fct_ventas)
);