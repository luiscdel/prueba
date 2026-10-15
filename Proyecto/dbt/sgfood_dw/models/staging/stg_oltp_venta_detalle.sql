SELECT
    id_detalle,
    id_venta,
    id_producto,
    cantidad,
    precio_unitario,
    descuento,
    subtotal,
    _batch_id,
    _fuente,
    _fecha_carga
FROM {{ source('oltp', 'venta_detalle') }}