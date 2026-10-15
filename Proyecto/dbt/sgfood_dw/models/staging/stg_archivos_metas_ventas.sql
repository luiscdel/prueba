SELECT
    nullif(trim(periodo), '') as periodo,
    id_sucursal,
    meta_ventas,
    meta_unidades,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('archivos', 'metas_ventas') }}