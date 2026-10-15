"""CLI para ejecutar el proceso de extracción y carga sin Airflow.

Útil para desarrollo y pruebas locales (con los contenedores de PostgreSQL
levantados vía docker compose):

    python scripts/run_el.py                 # flujo completo EL + validaciones
    python scripts/run_el.py --solo oltp     # solo tablas del OLTP
    python scripts/run_el.py --solo csv      # solo archivos CSV
    python scripts/run_el.py --tabla venta   # una tabla específica
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from etl import pipeline, validate  # noqa: E402
from etl.catalogo import FUENTES_CSV, FUENTES_OLTP  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="SG-Food - Extracción y carga a raw")
    parser.add_argument("--solo", choices=["oltp", "csv"], help="Procesar solo un tipo de fuente")
    parser.add_argument("--tabla", help="Procesar una sola tabla/archivo del catálogo")
    parser.add_argument("--batch-id", default=f"cli__{datetime.now():%Y%m%dT%H%M%S}")
    parser.add_argument("--sin-validaciones", action="store_true")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(name)s - %(message)s")
    log = logging.getLogger("run_el")
    log.info("Batch: %s", args.batch_id)

    try:
        validate.verificar_conexiones()
        validate.verificar_archivos()
        pipeline.preparar_dw()

        oltp = [] if args.solo == "csv" else list(FUENTES_OLTP)
        csv = [] if args.solo == "oltp" else list(FUENTES_CSV)
        if args.tabla:
            oltp = [t for t in oltp if t == args.tabla]
            csv = [t for t in csv if t == args.tabla]
            if not oltp and not csv:
                parser.error(f"Tabla desconocida: {args.tabla}")

        for t in oltp:
            pipeline.extraer_cargar_oltp(t, args.batch_id)
        for t in csv:
            pipeline.extraer_cargar_csv(t, args.batch_id)

        if not args.sin_validaciones and not args.tabla and not args.solo:
            validate.conciliar_conteos(args.batch_id)
            validate.validar_calidad_raw(args.batch_id)
    except Exception as exc:
        log.error("Ejecución fallida: %s", exc)
        return 1

    log.info("Ejecución finalizada correctamente")
    return 0


if __name__ == "__main__":
    sys.exit(main())
