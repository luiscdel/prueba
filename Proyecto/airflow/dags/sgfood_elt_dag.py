"""
DAG: sgfood_elt_pipeline
=========================

Flujo ELT completo de SG-Food orquestado con Apache Airflow:

    inicio
      └─ validaciones_previas  (conexiones OLTP/DW + archivos CSV)
           └─ preparar_dw      (DDL idempotente: schemas + tablas raw + audit)
                ├─ extraccion_carga_oltp.*   (7 tablas, en paralelo)
                └─ extraccion_carga_csv.*    (5 archivos, en paralelo)
                     └─ validaciones_raw
                          ├─ conciliar_conteos
                          └─ validar_calidad_raw
                               └─ transformacion_dbt
                                    ├─ dbt_debug
                                    ├─ dbt_deps
                                    ├─ dbt_source_freshness
                                    ├─ dbt_run
                                    └─ dbt_test
                                         └─ validar_post_dbt
                                              └─ fin

La lógica de extracción/carga vive en el paquete Python `scripts/etl`
(montado en /opt/airflow/scripts y agregado al PYTHONPATH). El DAG solo
orquesta: dependencias, reintentos, timeouts y registro de errores.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

# El catálogo no importa librerías pesadas: es seguro usarlo al parsear el DAG.
from etl.catalogo import FUENTES_CSV, FUENTES_OLTP

log = logging.getLogger(__name__)

DBT_BIN = os.getenv("DBT_BIN", "dbt")
DBT_DIR = os.getenv("SGFOOD_DBT_DIR", "/opt/airflow/dbt/sgfood_dw")
BATCH_ID = "{{ run_id }}"


# -----------------------------------------------------------------------------
# Callbacks de manejo de errores
# -----------------------------------------------------------------------------
def notificar_fallo(context) -> None:
    ti = context["task_instance"]
    log.error(
        "FALLO en DAG=%s tarea=%s run_id=%s intento=%s | excepción: %s | log: %s",
        ti.dag_id, ti.task_id, context.get("run_id"), ti.try_number,
        context.get("exception"), ti.log_url,
    )


def notificar_reintento(context) -> None:
    ti = context["task_instance"]
    log.warning("Reintentando tarea %s (intento %s): %s",
                ti.task_id, ti.try_number, context.get("exception"))


# -----------------------------------------------------------------------------
# Callables (imports diferidos para no penalizar el parseo del DAG)
# -----------------------------------------------------------------------------
def _validaciones_previas() -> dict:
    from etl import validate
    return {"conexiones": validate.verificar_conexiones(),
            "archivos": validate.verificar_archivos()}


def _preparar_dw() -> None:
    from etl import pipeline
    pipeline.preparar_dw()


def _cargar_oltp(tabla: str, batch_id: str) -> dict:
    from etl import pipeline
    return pipeline.extraer_cargar_oltp(tabla, batch_id)


def _cargar_csv(nombre: str, batch_id: str) -> dict:
    from etl import pipeline
    return pipeline.extraer_cargar_csv(nombre, batch_id)


def _conciliar_conteos(batch_id: str) -> list:
    from etl import validate
    return validate.conciliar_conteos(batch_id)


def _validar_calidad_raw(batch_id: str) -> dict:
    from etl import validate
    resultados = validate.validar_calidad_raw(batch_id)
    return {r: sum(1 for x in resultados if x["resultado"] == r) for r in ("PASS", "WARN", "FAIL")}


def _validar_post_dbt(batch_id: str) -> int:
    from etl import validate
    return len(validate.validar_post_dbt(batch_id))


def _dbt(comando: str) -> str:
    """Construye el comando bash para ejecutar dbt dentro del proyecto."""
    return (
        f"set -euo pipefail; cd {DBT_DIR} && "
        f"{DBT_BIN} {comando} --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
    )


# -----------------------------------------------------------------------------
# Definición del DAG
# -----------------------------------------------------------------------------
default_args = {
    "owner": "grupo22",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": notificar_fallo,
    "on_retry_callback": notificar_reintento,
}

with DAG(
    dag_id="sgfood_elt_pipeline",
    description="SG-Food: extracción OLTP+CSV con Python, carga raw en PostgreSQL, dbt y validaciones",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",          # diario a las 06:00 (America/Guatemala)
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=2),
    tags=["sgfood", "elt", "python", "dbt", "proyecto1"],
    doc_md=__doc__,
    params={
        "ejecutar_dbt": Param(True, type="boolean",
                              description="Ejecutar la fase de transformación con dbt"),
    },
) as dag:

    inicio = EmptyOperator(task_id="inicio")
    fin = EmptyOperator(task_id="fin")

    validaciones_previas = PythonOperator(
        task_id="validaciones_previas",
        python_callable=_validaciones_previas,
        retries=3,
        doc_md="Verifica conectividad a OLTP y DW y la existencia de los archivos fuente.",
    )

    preparar_dw = PythonOperator(
        task_id="preparar_dw",
        python_callable=_preparar_dw,
        doc_md="Aplica el DDL idempotente `sql/ddl/01_schemas_raw_audit.sql`.",
    )

    # ---- Extracción y carga: OLTP --------------------------------------------
    with TaskGroup("extraccion_carga_oltp",
                   tooltip="Tablas de oltp_sgfood -> raw") as tg_oltp:
        for tabla in FUENTES_OLTP:
            PythonOperator(
                task_id=f"cargar_{tabla}",
                python_callable=_cargar_oltp,
                op_kwargs={"tabla": tabla, "batch_id": BATCH_ID},
                doc_md=f"Extrae `oltp_sgfood.{tabla}` y la carga (full refresh) en `raw.{tabla}`.",
            )

    # ---- Extracción y carga: CSV ---------------------------------------------
    with TaskGroup("extraccion_carga_csv",
                   tooltip="Archivos CSV -> raw") as tg_csv:
        for nombre, fuente in FUENTES_CSV.items():
            PythonOperator(
                task_id=f"cargar_{nombre}",
                python_callable=_cargar_csv,
                op_kwargs={"nombre": nombre, "batch_id": BATCH_ID},
                doc_md=f"Lee `{fuente.archivo}` y lo carga (full refresh) en `raw.{nombre}`.",
            )

    # ---- Validaciones de la capa raw -----------------------------------------
    with TaskGroup("validaciones_raw", tooltip="Conciliación y calidad en raw") as tg_val:
        conciliar = PythonOperator(
            task_id="conciliar_conteos",
            python_callable=_conciliar_conteos,
            op_kwargs={"batch_id": BATCH_ID},
            retries=0,
        )
        calidad = PythonOperator(
            task_id="validar_calidad_raw",
            python_callable=_validar_calidad_raw,
            op_kwargs={"batch_id": BATCH_ID},
            retries=0,
        )
        conciliar >> calidad

    # ---- Transformación con dbt ----------------------------------------------
    with TaskGroup("transformacion_dbt", tooltip="dbt Core + dbt-postgres") as tg_dbt:
        dbt_debug = BashOperator(
            task_id="dbt_debug",
            bash_command=(
                "{% if not params.ejecutar_dbt %}echo 'dbt deshabilitado por parámetro'; exit 99;{% endif %} "
                + _dbt("debug --connection")
            ),
            skip_on_exit_code=99,
        )
        dbt_deps = BashOperator(
            task_id="dbt_deps",
            bash_command=(
                f"set -euo pipefail; cd {DBT_DIR}; "
                f"if [ -f packages.yml ] || [ -f dependencies.yml ]; then "
                f"{DBT_BIN} deps --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}; "
                f"else echo 'Sin paquetes dbt que instalar'; fi"
            ),
        )
        dbt_freshness = BashOperator(
            task_id="dbt_source_freshness",
            bash_command=_dbt("source freshness"),
        )
        dbt_run = BashOperator(
            task_id="dbt_run",
            bash_command=_dbt("run"),
        )
        dbt_test = BashOperator(
            task_id="dbt_test",
            bash_command=_dbt("test"),
            retries=0,
        )
        dbt_debug >> dbt_deps >> dbt_freshness >> dbt_run >> dbt_test

    validar_post_dbt = PythonOperator(
        task_id="validar_post_dbt",
        python_callable=_validar_post_dbt,
        op_kwargs={"batch_id": BATCH_ID},
        retries=0,
        trigger_rule="none_failed",   # corre aunque dbt se haya omitido por parámetro
    )

    inicio >> validaciones_previas >> preparar_dw >> [tg_oltp, tg_csv] >> tg_val
    tg_val >> tg_dbt >> validar_post_dbt >> fin
