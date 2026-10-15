SELECT
    p.id_producto,
    p.sku,
    p.nombre as nombre_producto,
    p.unidad_medida,
    p.costo_base,
    p.precio_lista,
    p.activo,
    p.id_categoria,
    c.nombre as nombre_categoria,
    p.id_marca,
    m.nombre as nombre_marca
FROM {{ ref('stg_oltp_producto') }} as p
LEFT JOIN {{ ref('stg_oltp_categoria') }} as c
    ON p.id_categoria = c.id_categoria
LEFT JOIN {{ ref('stg_oltp_marca') }} as m
    ON p.id_marca = m.id_marca