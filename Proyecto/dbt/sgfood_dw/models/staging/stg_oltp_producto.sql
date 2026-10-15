SELECT
    id_producto,
    nullif(trim(sku), '') as sku,
    nullif(trim(nombre), '') as nombre,
    id_categoria,
    id_marca,
    nullif(trim(unidad_medida), '') as unidad_medida,
    costo_base,
    precio_lista,
    activo,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'producto') }}