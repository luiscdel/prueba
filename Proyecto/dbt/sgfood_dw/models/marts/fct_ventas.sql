SELECT
    v.id_detalle,
    v.id_venta,
    v.fecha,
    c.id_cliente,
    s.id_sucursal,
    p.id_producto,
    v.estado,
    v.canal,
    v.metodo_pago,
    v.cantidad,
    v.precio_unitario,
    v.importe_bruto,
    v.importe_descuento,
    v.importe_neto
FROM {{ ref('int_ventas_detalle') }} as v
LEFT JOIN {{ ref('dim_cliente') }} as c
    ON v.id_cliente = c.id_cliente
LEFT JOIN {{ ref('dim_sucursal') }} as s
    ON v.id_sucursal = s.id_sucursal
LEFT JOIN {{ ref('dim_producto') }} as p
    ON v.id_producto = p.id_producto