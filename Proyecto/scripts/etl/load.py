"""Carga de datos hacia el schema raw del Data Warehouse.

Estrategia: FULL REFRESH transaccional por tabla
    BEGIN; TRUNCATE raw.<tabla>; INSERT ...; COMMIT;
Si cualquier paso falla se hace ROLLBACK y la tabla conserva la última carga
válida (nunca queda vacía ni a medias).
"""
from __future__ import annotations

import logging

from psycopg2 import sql
from psycopg2.extras import execute_values

from . import config
from .db import conexion
from .extract import DatosExtraidos

log = logging.getLogger(__name__)


class ErrorCarga(RuntimeError):
    """Error al cargar datos en el DW."""


def columnas_tabla(cur, esquema: str, tabla: str) -> list[str]:
    """Columnas de negocio (sin columnas técnicas) de una tabla, en orden."""
    cur.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = %s AND table_name = %s
         ORDER BY ordinal_position
        """,
        (esquema, tabla),
    )
    return [r[0] for r in cur.fetchall() if r[0] not in config.COLUMNAS_TECNICAS]


def cargar_full_refresh(tabla: str, datos: DatosExtraidos, batch_id: str) -> int:
    """Reemplaza el contenido de raw.<tabla> con los datos extraídos.

    Devuelve el número de filas presentes en la tabla tras la carga.
    """
    esquema = config.RAW_SCHEMA
    destino = f"{esquema}.{tabla}"
    cfg = config.dw_config()

    with conexion(cfg) as conn, conn.cursor() as cur:
        cols_destino = columnas_tabla(cur, esquema, tabla)
        if not cols_destino:
            raise ErrorCarga(f"La tabla destino {destino} no existe. "
                             "Ejecute la tarea 'preparar_dw' (DDL) primero.")

        desconocidas = [c for c in datos.columnas if c not in cols_destino]
        if desconocidas:
            raise ErrorCarga(f"{destino}: columnas de la fuente sin destino {desconocidas}")
        sin_dato = [c for c in cols_destino if c not in datos.columnas]
        if sin_dato:
            log.warning("%s: columnas destino sin dato en la fuente (quedan NULL): %s",
                        destino, sin_dato)

        cur.execute(sql.SQL("TRUNCATE TABLE {}.{}").format(
            sql.Identifier(esquema), sql.Identifier(tabla)))

        insert = sql.SQL("INSERT INTO {sch}.{tbl} ({cols}) VALUES %s").format(
            sch=sql.Identifier(esquema),
            tbl=sql.Identifier(tabla),
            cols=sql.SQL(", ").join(
                sql.Identifier(c) for c in [*datos.columnas, "_batch_id", "_fuente"]),
        )
        registros = [(*fila, batch_id, datos.fuente) for fila in datos.filas]
        try:
            execute_values(cur, insert.as_string(conn), registros,
                           page_size=config.INSERT_PAGE_SIZE)
        except Exception as exc:
            raise ErrorCarga(f"Fallo al insertar en {destino}: {exc}".strip()) from exc

        cur.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
            sql.Identifier(esquema), sql.Identifier(tabla)))
        total = cur.fetchone()[0]

    log.info("  -> %s filas cargadas en %s (batch %s)", total, destino, batch_id)
    return total
