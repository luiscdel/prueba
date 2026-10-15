"""Bitácora de ejecución del pipeline (schema audit).

Se usa una conexión independiente en autocommit para que los registros de
auditoría persistan aunque la transacción de carga haga rollback.
"""
from __future__ import annotations

import logging

from . import config
from .db import conexion

log = logging.getLogger(__name__)


def registrar_inicio(batch_id: str, tabla: str, fuente: str, tipo_fuente: str) -> int | None:
    try:
        with conexion(config.dw_config(), autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO {config.AUDIT_SCHEMA}.etl_carga_log
                       (batch_id, tabla_destino, fuente, tipo_fuente, estado)
                VALUES (%s, %s, %s, %s, 'EN_PROCESO')
                RETURNING id_log
                """,
                (batch_id, f"{config.RAW_SCHEMA}.{tabla}", fuente, tipo_fuente),
            )
            return cur.fetchone()[0]
    except Exception as exc:  # la auditoría nunca debe tumbar la carga
        log.warning("No se pudo registrar inicio en auditoría: %s", exc)
        return None


def registrar_fin(id_log: int | None, estado: str, filas_extraidas: int | None = None,
                  filas_cargadas: int | None = None, mensaje_error: str | None = None) -> None:
    if id_log is None:
        return
    try:
        with conexion(config.dw_config(), autocommit=True) as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                UPDATE {config.AUDIT_SCHEMA}.etl_carga_log
                   SET estado = %s,
                       filas_extraidas = %s,
                       filas_cargadas = %s,
                       fin = now(),
                       duracion_seg = EXTRACT(EPOCH FROM (now() - inicio)),
                       mensaje_error = %s
                 WHERE id_log = %s
                """,
                (estado, filas_extraidas, filas_cargadas,
                 (mensaje_error or "")[:4000] or None, id_log),
            )
    except Exception as exc:
        log.warning("No se pudo registrar fin en auditoría: %s", exc)


def registrar_validacion(cur, batch_id: str, etapa: str, nombre: str, tabla: str | None,
                         severidad: str, filas_con_problema: int, resultado: str,
                         detalle: str | None = None) -> None:
    cur.execute(
        f"""
        INSERT INTO {config.AUDIT_SCHEMA}.validacion_resultado
               (batch_id, etapa, nombre_validacion, tabla, severidad,
                filas_con_problema, resultado, detalle)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (batch_id, etapa, nombre, tabla, severidad, filas_con_problema, resultado, detalle),
    )
