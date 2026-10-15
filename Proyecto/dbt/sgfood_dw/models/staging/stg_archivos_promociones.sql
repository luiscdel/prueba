SELECT
    id_promocion,
    nullif(trim(nombre), '') as nombre,
    fecha_inicio,
    fecha_fin,
    id_categoria,
    porcentaje_descuento,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('archivos', 'promociones') }}