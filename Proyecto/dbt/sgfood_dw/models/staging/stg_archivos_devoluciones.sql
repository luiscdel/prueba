SELECT
    id_devolucion,
    fecha,
    id_venta,
    id_producto,
    cantidad,
    nullif(trim(motivo), '') as motivo,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('archivos', 'devoluciones') }}