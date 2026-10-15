-- =============================================================================
-- SG-Food - Validaciones de integridad y reglas de negocio sobre la capa RAW
--
-- Este archivo lo ejecuta automáticamente la tarea "validar_calidad_raw" del
-- DAG (scripts/etl/validate.py), pero cada consulta también puede ejecutarse
-- manualmente en psql / DBeaver.
--
-- Formato: cada bloque declara metadatos en comentarios y una consulta que
-- devuelve UN entero = cantidad de filas que incumplen la regla (0 = OK).
--   severidad ERROR   -> detiene el pipeline
--   severidad WARNING -> se registra en audit.validacion_resultado y continúa
--
-- Las reglas cubren los tipos de incidencia de casos_calidad_opcionales.csv.
-- (Las llaves nulas/duplicadas y tablas vacías se generan automáticamente
--  desde scripts/etl/catalogo.py.)
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Integridad referencial (FK lógicas)
-- ---------------------------------------------------------------------------
-- nombre: fk_producto_categoria
-- tabla: raw.producto
-- severidad: WARNING
-- descripcion: Productos cuya categoría no existe
SELECT count(*) FROM raw.producto p
 WHERE NOT EXISTS (SELECT 1 FROM raw.categoria c WHERE c.id_categoria = p.id_categoria);

-- nombre: fk_producto_marca
-- tabla: raw.producto
-- severidad: WARNING
-- descripcion: Productos cuya marca no existe
SELECT count(*) FROM raw.producto p
 WHERE NOT EXISTS (SELECT 1 FROM raw.marca m WHERE m.id_marca = p.id_marca);

-- nombre: fk_venta_cliente
-- tabla: raw.venta
-- severidad: WARNING
-- descripcion: Ventas con cliente inexistente (FK huérfana)
SELECT count(*) FROM raw.venta v
 WHERE NOT EXISTS (SELECT 1 FROM raw.cliente c WHERE c.id_cliente = v.id_cliente);

-- nombre: fk_venta_sucursal
-- tabla: raw.venta
-- severidad: WARNING
-- descripcion: Ventas con sucursal inexistente
SELECT count(*) FROM raw.venta v
 WHERE NOT EXISTS (SELECT 1 FROM raw.sucursal s WHERE s.id_sucursal = v.id_sucursal);

-- nombre: fk_detalle_venta
-- tabla: raw.venta_detalle
-- severidad: WARNING
-- descripcion: Líneas de detalle sin encabezado de venta
SELECT count(*) FROM raw.venta_detalle d
 WHERE NOT EXISTS (SELECT 1 FROM raw.venta v WHERE v.id_venta = d.id_venta);

-- nombre: fk_detalle_producto
-- tabla: raw.venta_detalle
-- severidad: WARNING
-- descripcion: Líneas de detalle con producto inexistente
SELECT count(*) FROM raw.venta_detalle d
 WHERE NOT EXISTS (SELECT 1 FROM raw.producto p WHERE p.id_producto = d.id_producto);

-- nombre: fk_inventario_producto_sucursal
-- tabla: raw.inventario_bodega
-- severidad: WARNING
-- descripcion: Inventario con producto o sucursal inexistente
SELECT count(*) FROM raw.inventario_bodega i
 WHERE NOT EXISTS (SELECT 1 FROM raw.producto p WHERE p.id_producto = i.id_producto)
    OR NOT EXISTS (SELECT 1 FROM raw.sucursal s WHERE s.id_sucursal = i.id_sucursal);

-- nombre: fk_proveedores_producto
-- tabla: raw.proveedores_precios
-- severidad: WARNING
-- descripcion: Precios de proveedor para productos inexistentes
SELECT count(*) FROM raw.proveedores_precios pp
 WHERE NOT EXISTS (SELECT 1 FROM raw.producto p WHERE p.id_producto = pp.id_producto);

-- nombre: fk_promociones_categoria
-- tabla: raw.promociones
-- severidad: WARNING
-- descripcion: Promociones para categorías inexistentes
SELECT count(*) FROM raw.promociones pr
 WHERE NOT EXISTS (SELECT 1 FROM raw.categoria c WHERE c.id_categoria = pr.id_categoria);

-- nombre: fk_metas_sucursal
-- tabla: raw.metas_ventas
-- severidad: WARNING
-- descripcion: Metas para sucursales inexistentes
SELECT count(*) FROM raw.metas_ventas m
 WHERE NOT EXISTS (SELECT 1 FROM raw.sucursal s WHERE s.id_sucursal = m.id_sucursal);

-- nombre: fk_devoluciones_venta_producto
-- tabla: raw.devoluciones
-- severidad: WARNING
-- descripcion: Devoluciones cuya venta o producto no existe
SELECT count(*) FROM raw.devoluciones d
 WHERE NOT EXISTS (SELECT 1 FROM raw.venta v WHERE v.id_venta = d.id_venta)
    OR NOT EXISTS (SELECT 1 FROM raw.producto p WHERE p.id_producto = d.id_producto);

-- ---------------------------------------------------------------------------
-- Reglas de negocio / dominio de valores
-- ---------------------------------------------------------------------------
-- nombre: producto_precio_costo_negativo
-- tabla: raw.producto
-- severidad: WARNING
-- descripcion: Productos con precio de lista o costo base negativo o nulo
SELECT count(*) FROM raw.producto
 WHERE precio_lista IS NULL OR precio_lista < 0 OR costo_base IS NULL OR costo_base < 0;

-- nombre: detalle_cantidad_no_positiva
-- tabla: raw.venta_detalle
-- severidad: WARNING
-- descripcion: Líneas con cantidad cero, negativa o nula
SELECT count(*) FROM raw.venta_detalle WHERE cantidad IS NULL OR cantidad <= 0;

-- nombre: detalle_descuento_fuera_rango
-- tabla: raw.venta_detalle
-- severidad: WARNING
-- descripcion: Descuentos fuera del rango [0, 1]
SELECT count(*) FROM raw.venta_detalle WHERE descuento < 0 OR descuento > 1;

-- nombre: detalle_subtotal_inconsistente
-- tabla: raw.venta_detalle
-- severidad: WARNING
-- descripcion: Subtotal distinto de cantidad*precio*(1-descuento) (tolerancia 0.05)
SELECT count(*) FROM raw.venta_detalle
 WHERE abs(cantidad * precio_unitario * (1 - descuento) - subtotal) > 0.05;

-- nombre: venta_estado_desconocido
-- tabla: raw.venta
-- severidad: WARNING
-- descripcion: Ventas con estado fuera del dominio esperado
SELECT count(*) FROM raw.venta
 WHERE estado IS NULL OR estado NOT IN ('COMPLETADA', 'ANULADA', 'PENDIENTE');

-- nombre: inventario_stock_negativo
-- tabla: raw.inventario_bodega
-- severidad: WARNING
-- descripcion: Inventario con stock disponible negativo
SELECT count(*) FROM raw.inventario_bodega WHERE stock_disponible < 0;

-- nombre: inventario_min_mayor_max
-- tabla: raw.inventario_bodega
-- severidad: WARNING
-- descripcion: Stock mínimo mayor que el stock máximo
SELECT count(*) FROM raw.inventario_bodega WHERE stock_minimo > stock_maximo;

-- nombre: inventario_vencimiento_anterior_corte
-- tabla: raw.inventario_bodega
-- severidad: WARNING
-- descripcion: Lotes con vencimiento anterior a la fecha de corte
SELECT count(*) FROM raw.inventario_bodega WHERE fecha_vencimiento < fecha_corte;

-- nombre: proveedores_costo_nulo_negativo
-- tabla: raw.proveedores_precios
-- severidad: WARNING
-- descripcion: Costos de proveedor nulos o negativos
SELECT count(*) FROM raw.proveedores_precios WHERE costo_proveedor IS NULL OR costo_proveedor < 0;

-- nombre: promociones_rango_invalido
-- tabla: raw.promociones
-- severidad: WARNING
-- descripcion: Promociones con fecha fin < inicio o descuento fuera de [0, 1]
SELECT count(*) FROM raw.promociones
 WHERE fecha_fin < fecha_inicio OR porcentaje_descuento < 0 OR porcentaje_descuento > 1;

-- nombre: metas_no_positivas
-- tabla: raw.metas_ventas
-- severidad: WARNING
-- descripcion: Metas de ventas o unidades en cero o negativas
SELECT count(*) FROM raw.metas_ventas WHERE meta_ventas <= 0 OR meta_unidades <= 0;

-- nombre: metas_periodo_formato
-- tabla: raw.metas_ventas
-- severidad: WARNING
-- descripcion: Periodo con formato distinto de YYYY-MM
SELECT count(*) FROM raw.metas_ventas WHERE periodo !~ '^[0-9]{4}-(0[1-9]|1[0-2])$';

-- nombre: devolucion_mayor_a_venta
-- tabla: raw.devoluciones
-- severidad: WARNING
-- descripcion: Devoluciones con cantidad superior a la vendida para esa venta/producto
SELECT count(*) FROM raw.devoluciones d
  LEFT JOIN (SELECT id_venta, id_producto, sum(cantidad) AS vendida
               FROM raw.venta_detalle GROUP BY id_venta, id_producto) v
         ON v.id_venta = d.id_venta AND v.id_producto = d.id_producto
 WHERE d.cantidad > coalesce(v.vendida, 0);

-- nombre: devolucion_antes_de_venta
-- tabla: raw.devoluciones
-- severidad: WARNING
-- descripcion: Devoluciones registradas antes de la fecha de la venta
SELECT count(*) FROM raw.devoluciones d
  JOIN raw.venta v ON v.id_venta = d.id_venta
 WHERE d.fecha < v.fecha;
