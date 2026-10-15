"""Extracción de datos desde las fuentes de SG-Food.

* OLTP: SELECT explícito de las columnas definidas en el catálogo.
* CSV : lectura con pandas + limpieza técnica mínima:
    - eliminación de BOM (utf-8-sig) y normalización de encabezados
    - validación de columnas esperadas (faltantes -> error, extras -> se ignoran)
    - recorte de espacios, cadenas vacías -> NULL
    - eliminación de filas completamente vacías
  Las conversiones de tipo las realiza PostgreSQL al insertar en las tablas
  raw tipadas; si un valor no es válido, la carga falla con un mensaje claro.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from psycopg2 import sql

from . import config
from .catalogo import FuenteCSV, FuenteOLTP
from .db import conexion

log = logging.getLogger(__name__)


class ErrorExtraccion(RuntimeError):
    """Error al extraer datos de una fuente."""


@dataclass
class DatosExtraidos:
    """Resultado de una extracción, independiente del tipo de fuente."""

    fuente: str                     # descripción del origen (para linaje)
    columnas: list[str]
    filas: list[tuple]

    @property
    def total(self) -> int:
        return len(self.filas)


# -----------------------------------------------------------------------------
# OLTP
# -----------------------------------------------------------------------------
def extraer_tabla_oltp(fuente: FuenteOLTP) -> DatosExtraidos:
    """Extrae todas las filas de una tabla del OLTP."""
    cfg = config.oltp_config()
    origen = f"oltp:{cfg.dbname}.{config.OLTP_SCHEMA}.{fuente.tabla}"
    consulta = sql.SQL("SELECT {cols} FROM {sch}.{tbl} ORDER BY {orden}").format(
        cols=sql.SQL(", ").join(sql.Identifier(c) for c in fuente.columnas),
        sch=sql.Identifier(config.OLTP_SCHEMA),
        tbl=sql.Identifier(fuente.tabla),
        orden=sql.SQL(", ").join(sql.Identifier(c) for c in fuente.llave),
    )
    log.info("Extrayendo %s", origen)
    try:
        with conexion(cfg) as conn, conn.cursor() as cur:
            cur.execute(consulta)
            filas = cur.fetchall()
            columnas = [d.name for d in cur.description]
    except Exception as exc:
        raise ErrorExtraccion(f"Fallo al extraer {origen}: {exc}") from exc

    log.info("  -> %s filas extraídas de %s", len(filas), origen)
    return DatosExtraidos(fuente=origen, columnas=columnas, filas=filas)


# -----------------------------------------------------------------------------
# CSV
# -----------------------------------------------------------------------------
def ruta_csv(fuente: FuenteCSV) -> Path:
    return config.data_dir() / fuente.archivo


def leer_csv(fuente: FuenteCSV) -> DatosExtraidos:
    """Lee un CSV, valida su estructura y aplica limpieza técnica mínima."""
    import pandas as pd  # import diferido: no penaliza el parseo del DAG

    ruta = ruta_csv(fuente)
    origen = f"csv:{fuente.archivo}"
    if not ruta.is_file():
        raise ErrorExtraccion(f"No existe el archivo fuente {ruta}")

    log.info("Leyendo %s", ruta)
    try:
        df = pd.read_csv(
            ruta,
            encoding="utf-8-sig",       # elimina BOM si existe
            dtype=str,                  # sin inferencia: los tipos los valida PostgreSQL
            keep_default_na=False,
            na_values=[""],
            skipinitialspace=True,
        )
    except Exception as exc:
        raise ErrorExtraccion(f"No se pudo leer {ruta}: {exc}") from exc

    # Normalización de encabezados
    df.columns = [str(c).strip().lower() for c in df.columns]

    faltantes = [c for c in fuente.columnas if c not in df.columns]
    if faltantes:
        raise ErrorExtraccion(
            f"{fuente.archivo}: faltan columnas obligatorias {faltantes}. "
            f"Encabezados encontrados: {list(df.columns)}")
    extras = [c for c in df.columns if c not in fuente.columnas]
    if extras:
        log.warning("%s: columnas no catalogadas serán ignoradas: %s", fuente.archivo, extras)

    df = df[list(fuente.columnas)]

    # Limpieza técnica mínima
    df = df.apply(lambda s: s.str.strip())
    df = df.replace({"": None})
    filas_antes = len(df)
    df = df.dropna(how="all")
    if len(df) < filas_antes:
        log.warning("%s: se descartaron %s filas completamente vacías",
                    fuente.archivo, filas_antes - len(df))

    duplicados = int(df.duplicated(subset=list(fuente.llave)).sum())
    if duplicados:
        log.warning("%s: %s filas con llave %s duplicada (se cargan igual en raw)",
                    fuente.archivo, duplicados, fuente.llave)

    df = df.astype(object).where(df.notna(), None)
    filas = [tuple(r) for r in df.itertuples(index=False, name=None)]
    log.info("  -> %s filas leídas de %s", len(filas), origen)
    return DatosExtraidos(fuente=origen, columnas=list(fuente.columnas), filas=filas)
