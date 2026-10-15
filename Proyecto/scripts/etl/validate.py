"""Validaciones del pipeline.

Pre-carga
    * verificar_conexiones : OLTP y DW accesibles, schema OLTP presente
    * verificar_archivos   : todos los CSV existen y no están vacíos
Post-carga (capa raw)
    * conciliar_conteos    : filas OLTP/CSV == filas cargadas en raw (por batch)
    * validar_calidad_raw  : llaves nulas/duplicadas (ERROR) + reglas de
                             integridad y negocio definidas en
                             sql/validaciones/01_validaciones_raw.sql
Post-dbt
    * validar_post_dbt     : ejecuta sql/validaciones/post_dbt/*.sql

Todas las validaciones registran su resultado en audit.validacion_resultado.
Las de severidad ERROR hacen fallar la tarea de Airflow; las WARNING solo se
reportan (la capa raw conserva los datos tal cual y dbt se encarga de depurar).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from pathlib import Path

from psycopg2 import sql

from . import config
from .auditoria import registrar_validacion
from .catalogo import FUENTES_CSV, FUENTES_OLTP, llave_de, todas_las_tablas_raw
from .db import conexion
from .extract import ruta_csv

log = logging.getLogger(__name__)


class ErrorValidacion(RuntimeError):
    """Una o más validaciones de severidad ERROR fallaron."""


@dataclass
class Chequeo:
    nombre: str
    tabla: str | None
    severidad: str          # ERROR | WARNING
    consulta: str           # debe devolver un único entero: filas con problema
    descripcion: str = ""


# -----------------------------------------------------------------------------
# Pre-carga
# -----------------------------------------------------------------------------
def verificar_conexiones() -> dict:
    """Verifica conectividad a OLTP y DW y la existencia del schema fuente."""
    resultado = {}
    with conexion(config.oltp_config()) as conn, conn.cursor() as cur:
        cur.execute("SELECT version()")
        resultado["oltp_version"] = cur.fetchone()[0]
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
            (config.OLTP_SCHEMA,))
        existentes = {r[0] for r in cur.fetchall()}
    faltantes = sorted(set(FUENTES_OLTP) - existentes)
    if faltantes:
        raise ErrorValidacion(
            f"El OLTP no tiene las tablas {faltantes} en el schema {config.OLTP_SCHEMA}")

    with conexion(config.dw_config()) as conn, conn.cursor() as cur:
        cur.execute("SELECT version()")
        resultado["dw_version"] = cur.fetchone()[0]

    log.info("Conexiones OK -> OLTP: %s | DW: %s", config.oltp_config(), config.dw_config())
    return resultado


def verificar_archivos() -> dict:
    """Verifica que todos los CSV catalogados existan y tengan contenido."""
    problemas, info = [], {}
    for fuente in FUENTES_CSV.values():
        ruta = ruta_csv(fuente)
        if not ruta.is_file():
            problemas.append(f"no existe {ruta}")
        elif ruta.stat().st_size == 0:
            problemas.append(f"archivo vacío {ruta}")
        else:
            info[fuente.archivo] = ruta.stat().st_size
    if problemas:
        raise ErrorValidacion("Archivos fuente con problemas: " + "; ".join(problemas))
    log.info("Archivos fuente OK: %s", info)
    return info


# -----------------------------------------------------------------------------
# Post-carga: conciliación de conteos
# -----------------------------------------------------------------------------
def conciliar_conteos(batch_id: str) -> list[dict]:
    """Compara filas de origen vs. filas cargadas en raw para el batch."""
    # Conteos actuales en el OLTP
    conteo_oltp = {}
    with conexion(config.oltp_config()) as conn, conn.cursor() as cur:
        for tabla in FUENTES_OLTP:
            cur.execute(sql.SQL("SELECT count(*) FROM {}.{}").format(
                sql.Identifier(config.OLTP_SCHEMA), sql.Identifier(tabla)))
            conteo_oltp[tabla] = cur.fetchone()[0]

    reporte, errores = [], []
    with conexion(config.dw_config()) as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT ON (tabla_destino) tabla_destino, estado,
                   filas_extraidas, filas_cargadas
              FROM {config.AUDIT_SCHEMA}.etl_carga_log
             WHERE batch_id = %s
             ORDER BY tabla_destino, id_log DESC
            """,
            (batch_id,))
        bitacora = {r[0].split(".", 1)[1]: r[1:] for r in cur.fetchall()}

        for tabla in todas_las_tablas_raw():
            cur.execute(sql.SQL("SELECT count(*) FROM {}.{} WHERE _batch_id = %s").format(
                sql.Identifier(config.RAW_SCHEMA), sql.Identifier(tabla)), (batch_id,))
            en_raw = cur.fetchone()[0]
            estado, extraidas, cargadas = bitacora.get(tabla, ("SIN_REGISTRO", None, None))
            origen = conteo_oltp.get(tabla, extraidas)
            ok = estado == "OK" and origen == extraidas == cargadas == en_raw
            fila = {"tabla": tabla, "estado_carga": estado, "filas_origen": origen,
                    "filas_extraidas": extraidas, "filas_cargadas": cargadas,
                    "filas_raw_batch": en_raw, "ok": ok}
            reporte.append(fila)
            registrar_validacion(
                cur, batch_id, "RAW", "conciliacion_conteos", f"raw.{tabla}", "ERROR",
                0 if ok else abs((origen or 0) - en_raw) or 1,
                "PASS" if ok else "FAIL",
                f"origen={origen} extraidas={extraidas} cargadas={cargadas} raw={en_raw} "
                f"estado={estado}")
            if not ok:
                errores.append(fila)

    for f in reporte:
        log.info("%-22s origen=%-6s extraidas=%-6s cargadas=%-6s raw=%-6s %s",
                 f["tabla"], f["filas_origen"], f["filas_extraidas"], f["filas_cargadas"],
                 f["filas_raw_batch"], "OK" if f["ok"] else "<< DIFERENCIA")
    if errores:
        raise ErrorValidacion(f"Conciliación fallida en {len(errores)} tabla(s): "
                              + ", ".join(e["tabla"] for e in errores))
    return reporte


# -----------------------------------------------------------------------------
# Post-carga: calidad de datos en raw
# -----------------------------------------------------------------------------
_PATRON_META = re.compile(r"^--\s*(\w+)\s*:\s*(.+?)\s*$")


def cargar_chequeos_sql(ruta: Path) -> list[Chequeo]:
    """Parsea un archivo SQL de validaciones.

    Formato: cada bloque inicia con comentarios de metadatos y contiene una
    consulta que devuelve un único entero (filas con problema):

        -- nombre: venta_cliente_huerfano
        -- tabla: raw.venta
        -- severidad: WARNING
        -- descripcion: ventas cuyo cliente no existe
        SELECT count(*) FROM ...;
    """
    chequeos: list[Chequeo] = []
    actual: dict | None = None
    cuerpo: list[str] = []

    def cerrar():
        if actual and "".join(cuerpo).strip():
            chequeos.append(Chequeo(
                nombre=actual["nombre"], tabla=actual.get("tabla"),
                severidad=actual.get("severidad", "WARNING").upper(),
                consulta="\n".join(cuerpo).strip().rstrip(";"),
                descripcion=actual.get("descripcion", "")))

    for linea in ruta.read_text(encoding="utf-8").splitlines():
        m = _PATRON_META.match(linea)
        if m and m.group(1).lower() in {"nombre", "tabla", "severidad", "descripcion"}:
            clave = m.group(1).lower()
            if clave == "nombre":
                cerrar()
                actual, cuerpo = {}, []
            if actual is not None:
                actual[clave] = m.group(2)
        elif actual is not None and not linea.strip().startswith("--"):
            cuerpo.append(linea)
    cerrar()
    return chequeos


def _chequeos_llaves() -> list[Chequeo]:
    """Chequeos generados automáticamente desde el catálogo: llaves nulas y duplicadas."""
    chequeos = []
    for tabla in todas_las_tablas_raw():
        llave = llave_de(tabla)
        cols = ", ".join(llave)
        nulos = " OR ".join(f"{c} IS NULL" for c in llave)
        chequeos.append(Chequeo(
            f"llave_no_nula_{tabla}", f"raw.{tabla}", "ERROR",
            f"SELECT count(*) FROM raw.{tabla} WHERE {nulos}",
            f"Registros con llave ({cols}) nula"))
        chequeos.append(Chequeo(
            f"llave_unica_{tabla}", f"raw.{tabla}", "ERROR",
            f"SELECT coalesce(sum(n - 1), 0) FROM (SELECT count(*) n FROM raw.{tabla} "
            f"GROUP BY {cols} HAVING count(*) > 1) d",
            f"Registros con llave ({cols}) duplicada"))
        chequeos.append(Chequeo(
            f"tabla_no_vacia_{tabla}", f"raw.{tabla}", "ERROR",
            f"SELECT CASE WHEN EXISTS (SELECT 1 FROM raw.{tabla}) THEN 0 ELSE 1 END",
            "La tabla raw no debe quedar vacía"))
    return chequeos


def _ejecutar_chequeos(batch_id: str, etapa: str, chequeos: list[Chequeo]) -> list[dict]:
    resultados, fallidos = [], []
    with conexion(config.dw_config()) as conn, conn.cursor() as cur:
        for ch in chequeos:
            try:
                cur.execute("SAVEPOINT chequeo")
                cur.execute(ch.consulta)
                problemas = int(cur.fetchone()[0] or 0)
                cur.execute("RELEASE SAVEPOINT chequeo")
                detalle = ch.descripcion
            except Exception as exc:
                cur.execute("ROLLBACK TO SAVEPOINT chequeo")
                problemas, detalle = -1, f"Error al ejecutar la validación: {exc}".strip()

            if problemas == 0:
                resultado = "PASS"
            elif ch.severidad == "ERROR" or problemas < 0:
                resultado = "FAIL"
            else:
                resultado = "WARN"

            registrar_validacion(cur, batch_id, etapa, ch.nombre, ch.tabla, ch.severidad,
                                 max(problemas, 0), resultado, detalle)
            nivel = logging.INFO if resultado == "PASS" else logging.WARNING
            log.log(nivel, "[%s] %-45s %-8s filas=%s", resultado, ch.nombre, ch.severidad, problemas)
            item = {"nombre": ch.nombre, "tabla": ch.tabla, "severidad": ch.severidad,
                    "filas_con_problema": problemas, "resultado": resultado}
            resultados.append(item)
            if resultado == "FAIL":
                fallidos.append(item)

    resumen = {r: sum(1 for x in resultados if x["resultado"] == r) for r in ("PASS", "WARN", "FAIL")}
    log.info("Resumen validaciones %s: %s", etapa, resumen)
    if fallidos:
        raise ErrorValidacion(
            f"{len(fallidos)} validación(es) {etapa} con severidad ERROR fallaron: "
            + ", ".join(f"{f['nombre']}({f['filas_con_problema']})" for f in fallidos))
    return resultados


def validar_calidad_raw(batch_id: str) -> list[dict]:
    archivo = config.sql_dir() / "validaciones" / "01_validaciones_raw.sql"
    chequeos = _chequeos_llaves()
    if archivo.is_file():
        chequeos += cargar_chequeos_sql(archivo)
    else:
        log.warning("No se encontró %s; solo se validan llaves", archivo)
    return _ejecutar_chequeos(batch_id, "RAW", chequeos)


def validar_post_dbt(batch_id: str) -> list[dict]:
    carpeta = config.sql_dir() / "validaciones" / "post_dbt"
    chequeos: list[Chequeo] = []
    for archivo in sorted(carpeta.glob("*.sql")) if carpeta.is_dir() else []:
        chequeos += cargar_chequeos_sql(archivo)
    if not chequeos:
        log.info("No hay validaciones post-dbt definidas en %s", carpeta)
        return []
    return _ejecutar_chequeos(batch_id, "POST_DBT", chequeos)
