"""Funciones de alto nivel del proceso EL (invocadas por Airflow y por la CLI)."""
from __future__ import annotations

import logging
import time

from . import auditoria, config
from .catalogo import FUENTES_CSV, FUENTES_OLTP
from .db import ejecutar_script_sql
from .extract import DatosExtraidos, extraer_tabla_oltp, leer_csv
from .load import cargar_full_refresh

log = logging.getLogger(__name__)

DDL_RAW = "ddl/01_schemas_raw_audit.sql"


def preparar_dw() -> None:
    """Crea (si no existen) los schemas raw/staging/intermediate/marts/audit y tablas raw."""
    ruta = config.sql_dir() / DDL_RAW
    log.info("Ejecutando DDL %s en %s", ruta, config.dw_config())
    ejecutar_script_sql(config.dw_config(), ruta.read_text(encoding="utf-8"))
    log.info("DDL aplicado correctamente")


def _procesar(tabla: str, tipo: str, descripcion_fuente: str,
              extractor, batch_id: str) -> dict:
    """Plantilla común: auditoría -> extracción -> carga -> auditoría."""
    inicio = time.monotonic()
    id_log = auditoria.registrar_inicio(batch_id, tabla, descripcion_fuente, tipo)
    datos: DatosExtraidos | None = None
    try:
        datos = extractor()
        cargadas = cargar_full_refresh(tabla, datos, batch_id)
        if cargadas != datos.total:
            raise RuntimeError(
                f"raw.{tabla}: se extrajeron {datos.total} filas pero quedaron {cargadas}")
    except Exception as exc:
        auditoria.registrar_fin(id_log, "ERROR",
                                datos.total if datos else None, None, str(exc))
        log.error("Fallo procesando %s (%s): %s", tabla, descripcion_fuente, exc)
        raise

    auditoria.registrar_fin(id_log, "OK", datos.total, cargadas)
    resultado = {"tabla": f"{config.RAW_SCHEMA}.{tabla}", "fuente": datos.fuente,
                 "filas": cargadas, "segundos": round(time.monotonic() - inicio, 2)}
    log.info("Carga completada: %s", resultado)
    return resultado


def extraer_cargar_oltp(tabla: str, batch_id: str) -> dict:
    fuente = FUENTES_OLTP[tabla]
    return _procesar(tabla, "OLTP", f"{config.OLTP_SCHEMA}.{tabla}",
                     lambda: extraer_tabla_oltp(fuente), batch_id)


def extraer_cargar_csv(nombre: str, batch_id: str) -> dict:
    fuente = FUENTES_CSV[nombre]
    return _procesar(nombre, "CSV", fuente.archivo, lambda: leer_csv(fuente), batch_id)
