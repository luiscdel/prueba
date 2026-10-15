SELECT
    fecha_corte,
    id_sucursal,
    id_producto,
    stock_disponible,
    stock_minimo,
    stock_maximo,
    nullif(trim(lote), '') as lote,
    fecha_vencimiento,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('archivos', 'inventario_bodega') }}