SELECT
    d.id_detalle,
    d.id_venta,
    v.fecha,
    v.id_cliente,
    v.id_sucursal,
    v.canal,
    v.metodo_pago,
    v.estado,
    d.id_producto,
    d.cantidad,
    d.precio_unitario,
    d.descuento,
    d.cantidad * d.precio_unitario as importe_bruto,
    d.cantidad * d.precio_unitario * d.descuento as importe_descuento,
    d.subtotal as importe_neto,
    d._batch_id,
    d._fuente,
    d._fecha_carga
FROM {{ ref('stg_oltp_venta_detalle') }} as d
INNER JOIN {{ ref('stg_oltp_venta') }} as v
    ON d.id_venta = v.id_venta

    