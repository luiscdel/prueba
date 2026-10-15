SELECT
    id_proveedor,
    nullif(trim(proveedor), '') as proveedor,
    id_producto,
    costo_proveedor,
    plazo_dias,
    fecha_vigencia,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('archivos', 'proveedores_precios') }}