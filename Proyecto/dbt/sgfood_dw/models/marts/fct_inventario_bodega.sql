SELECT
    i.fecha_corte,
    i.id_sucursal,
    i.id_producto,
    i.stock_disponible,
    i.stock_minimo,
    i.stock_maximo,
    i.lote,
    i.fecha_vencimiento,
    p.costo_base,
    i.stock_disponible * p.costo_base as valor_inventario_costo
FROM {{ ref('stg_archivos_inventario_bodega') }} as i
LEFT JOIN {{ ref('dim_producto') }} as p
    ON i.id_producto = p.id_producto