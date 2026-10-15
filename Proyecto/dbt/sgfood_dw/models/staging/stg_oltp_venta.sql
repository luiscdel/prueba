SELECT
    id_venta,
    fecha,
    id_cliente,
    id_sucursal,
    nullif(trim(canal), '') as canal,
    nullif(trim(metodo_pago), '') as metodo_pago,
    nullif(trim(estado), '') as estado,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'venta') }}