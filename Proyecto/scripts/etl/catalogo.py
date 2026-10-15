"""Catálogo declarativo de fuentes de datos de SG-Food.

Basado en `catalogo_insumos_sgfood.xlsx` (hojas Resumen y Diccionario).
Agregar una nueva fuente al pipeline consiste en registrar una entrada aquí
y crear su tabla en `sql/ddl/01_schemas_raw_audit.sql`.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class FuenteOLTP:
    tabla: str                      # nombre en oltp_sgfood y en raw
    llave: tuple[str, ...]          # llave natural (para validaciones)
    columnas: tuple[str, ...]


@dataclass(frozen=True)
class FuenteCSV:
    nombre: str                     # nombre de la tabla destino en raw
    archivo: str                    # nombre del archivo en data/
    llave: tuple[str, ...]
    columnas: tuple[str, ...]       # encabezados esperados (en orden)
    columnas_fecha: tuple[str, ...] = field(default_factory=tuple)


# -----------------------------------------------------------------------------
# Fuente transaccional (PostgreSQL - schema oltp_sgfood)
# -----------------------------------------------------------------------------
FUENTES_OLTP: dict[str, FuenteOLTP] = {f.tabla: f for f in [
    FuenteOLTP("sucursal", ("id_sucursal",),
               ("id_sucursal", "nombre", "ciudad", "departamento")),
    FuenteOLTP("categoria", ("id_categoria",),
               ("id_categoria", "nombre")),
    FuenteOLTP("marca", ("id_marca",),
               ("id_marca", "nombre")),
    FuenteOLTP("producto", ("id_producto",),
               ("id_producto", "sku", "nombre", "id_categoria", "id_marca",
                "unidad_medida", "costo_base", "precio_lista", "activo")),
    FuenteOLTP("cliente", ("id_cliente",),
               ("id_cliente", "nit", "nombre", "tipo_cliente", "municipio",
                "departamento", "fecha_alta")),
    FuenteOLTP("venta", ("id_venta",),
               ("id_venta", "fecha", "id_cliente", "id_sucursal", "canal",
                "metodo_pago", "estado")),
    FuenteOLTP("venta_detalle", ("id_detalle",),
               ("id_detalle", "id_venta", "id_producto", "cantidad",
                "precio_unitario", "descuento", "subtotal")),
]}

# -----------------------------------------------------------------------------
# Fuentes externas (archivos CSV en data/)
# casos_calidad_opcionales.csv NO se carga en la corrida principal
# (según la hoja "Resumen" del catálogo de insumos).
# -----------------------------------------------------------------------------
FUENTES_CSV: dict[str, FuenteCSV] = {f.nombre: f for f in [
    FuenteCSV("inventario_bodega", "inventario_bodega.csv",
              ("fecha_corte", "id_sucursal", "id_producto"),
              ("fecha_corte", "id_sucursal", "id_producto", "stock_disponible",
               "stock_minimo", "stock_maximo", "lote", "fecha_vencimiento"),
              ("fecha_corte", "fecha_vencimiento")),
    FuenteCSV("proveedores_precios", "proveedores_precios.csv",
              ("id_proveedor", "id_producto", "fecha_vigencia"),
              ("id_proveedor", "proveedor", "id_producto", "costo_proveedor",
               "plazo_dias", "fecha_vigencia"),
              ("fecha_vigencia",)),
    FuenteCSV("promociones", "promociones.csv",
              ("id_promocion",),
              ("id_promocion", "nombre", "fecha_inicio", "fecha_fin",
               "id_categoria", "porcentaje_descuento"),
              ("fecha_inicio", "fecha_fin")),
    FuenteCSV("metas_ventas", "metas_ventas.csv",
              ("periodo", "id_sucursal"),
              ("periodo", "id_sucursal", "meta_ventas", "meta_unidades")),
    FuenteCSV("devoluciones", "devoluciones.csv",
              ("id_devolucion",),
              ("id_devolucion", "fecha", "id_venta", "id_producto", "cantidad",
               "motivo"),
              ("fecha",)),
]}


def todas_las_tablas_raw() -> list[str]:
    """Lista de todas las tablas destino en el schema raw."""
    return list(FUENTES_OLTP) + list(FUENTES_CSV)


def llave_de(tabla: str) -> tuple[str, ...]:
    if tabla in FUENTES_OLTP:
        return FUENTES_OLTP[tabla].llave
    return FUENTES_CSV[tabla].llave
