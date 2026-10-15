SELECT
    fecha_corte,
    id_sucursal,
    id_producto,
    count(*) as filas
FROM {{ ref('fct_inventario_bodega') }}
GROUP BY
    fecha_corte,
    id_sucursal,
    id_producto
HAVING count(*) > 1