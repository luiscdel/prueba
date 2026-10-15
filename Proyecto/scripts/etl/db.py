"""Utilidades de conexión a PostgreSQL con reintentos."""
from __future__ import annotations

import logging
import time
from contextlib import contextmanager
from typing import Iterator

import psycopg2
from psycopg2.extensions import connection as PGConnection

from .config import DBConfig

log = logging.getLogger(__name__)


class ErrorConexion(RuntimeError):
    """No fue posible conectarse a la base de datos tras varios intentos."""


def conectar(cfg: DBConfig, intentos: int = 3, espera_seg: float = 3.0) -> PGConnection:
    """Abre una conexión reintentando ante fallos transitorios."""
    ultimo_error: Exception | None = None
    for intento in range(1, intentos + 1):
        try:
            conn = psycopg2.connect(**cfg.as_kwargs())
            conn.set_client_encoding("UTF8")
            return conn
        except psycopg2.OperationalError as exc:
            ultimo_error = exc
            log.warning("Intento %s/%s de conexión a %s falló: %s",
                        intento, intentos, cfg, str(exc).strip())
            if intento < intentos:
                time.sleep(espera_seg * intento)
    raise ErrorConexion(f"No se pudo conectar a {cfg}: {ultimo_error}") from ultimo_error


@contextmanager
def conexion(cfg: DBConfig, autocommit: bool = False) -> Iterator[PGConnection]:
    """Context manager: hace commit si todo sale bien y rollback ante errores."""
    conn = conectar(cfg)
    conn.autocommit = autocommit
    try:
        yield conn
        if not autocommit:
            conn.commit()
    except Exception:
        if not autocommit:
            conn.rollback()
        raise
    finally:
        conn.close()


def ejecutar_script_sql(cfg: DBConfig, sql: str) -> None:
    """Ejecuta un script SQL completo (varias sentencias) en una transacción."""
    with conexion(cfg) as conn, conn.cursor() as cur:
        cur.execute(sql)


def escalar(cfg: DBConfig, sql: str, params: tuple | None = None):
    """Devuelve el primer valor de la primera fila de una consulta."""
    with conexion(cfg) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        fila = cur.fetchone()
        return fila[0] if fila else None
