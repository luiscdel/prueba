-- Determinamos el ingreso mensual por sucursal de mayor a menor ingreso
SELECT
    date_trunc('month', v.fecha)::date AS mes,
    s.nombre AS sucursal,
    count(DISTINCT v.id_venta) AS cantidad_ventas,
    sum(v.importe_neto) AS ingreso
FROM marts.fct_ventas AS f
JOIN marts.dim_sucursal AS s ON v.id_sucursal = s.id_sucursal
WHERE v.estado = 'COMPLETADA'
GROUP BY date_trunc('month', v.fecha)::date, sucursal
ORDER BY mes, ingreso DESC ;

-- Determinamos el canal y metodo de pago con mayor ingreso
SELECT
    canal,
    metodo_pago,
    count(DISTINCT id_venta) AS venta,
    sum(importe_neto) as ingreso
FROM marts.fct_ventas
WHERE estado = 'COMPLETADA'
GROUP BY canal,metodo_pago
ORDER BY ingreso DESC;

--Determinamos el ingreso y cantidad por categoria de producto
SELECT
    p.nombre_categoria AS categoria,
    count(DISTINCT v.id_venta) AS numero_venta,
    sum(v.cantidad) as cantidad,
    sum(v.importe_neto) as importe
FROM marts.fct_ventas v
JOIN marts.dim_producto p ON p.id_producto = v.id_producto
WHERE v.estado = 'COMPLETADA'
GROUP BY categoria
ORDER BY numero_venta;

-- Determinamos el ingreso y cantidad de productos vendidos por marca
SELECT
    p.nombre_producto AS producto,
    count(DISTINCT v.id_venta) AS numero_venta,
    sum(v.cantidad) as cantidad,
    sum(v.importe_neto) as importe
FROM marts.fct_ventas v
JOIN marts.dim_producto p ON p.id_producto = v.id_producto
WHERE v.estado = 'COMPLETADA'
GROUP BY producto
ORDER BY numero_venta;