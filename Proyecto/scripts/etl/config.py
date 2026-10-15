"""Configuración centralizada del proceso EL.

Todos los parámetros se leen de variables de entorno, de modo que el mismo
código funciona:
  * dentro de Airflow (docker-compose define host=postgres-oltp/postgres-dw)
  * en local desde la máquina anfitriona (por defecto localhost:5433 / 5434)
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# scripts/etl/config.py -> parents[2] = carpeta Proyecto1 (o /opt/airflow en Docker)
_BASE_DIR = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class DBConfig:
    """Parámetros de conexión a una base PostgreSQL."""

    host: str
    port: int
    dbname: str
    user: str
    password: str

    def as_kwargs(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "dbname": self.dbname,
            "user": self.user,
            "password": self.password,
            "connect_timeout": int(os.getenv("SGFOOD_CONNECT_TIMEOUT", "10")),
            "application_name": "sgfood_el",
        }

    def __str__(self) -> str:  # nunca imprimir la contraseña en logs
        return f"postgresql://{self.user}:***@{self.host}:{self.port}/{self.dbname}"


def _db_config(prefix: str, default_port: int, default_db: str,
               default_user: str, default_password: str) -> DBConfig:
    return DBConfig(
        host=os.getenv(f"{prefix}_HOST", "localhost"),
        port=int(os.getenv(f"{prefix}_PORT", str(default_port))),
        dbname=os.getenv(f"{prefix}_DB", default_db),
        user=os.getenv(f"{prefix}_USER", default_user),
        password=os.getenv(f"{prefix}_PASSWORD", default_password),
    )


def oltp_config() -> DBConfig:
    """Conexión a la base transaccional fuente."""
    return _db_config("SGFOOD_OLTP", 5433, "sgfood_oltp", "sgfood", "sgfood")


def dw_config() -> DBConfig:
    """Conexión al Data Warehouse."""
    return _db_config("SGFOOD_DW", 5434, "sgfood_dw", "dw_user", "dw_password")


def data_dir() -> Path:
    return Path(os.getenv("SGFOOD_DATA_DIR", _BASE_DIR / "data"))


def sql_dir() -> Path:
    return Path(os.getenv("SGFOOD_SQL_DIR", _BASE_DIR / "sql"))


def dbt_dir() -> Path:
    return Path(os.getenv("SGFOOD_DBT_DIR", _BASE_DIR / "dbt" / "sgfood_dw"))


OLTP_SCHEMA = os.getenv("SGFOOD_OLTP_SCHEMA", "oltp_sgfood")
RAW_SCHEMA = os.getenv("SGFOOD_RAW_SCHEMA", "raw")
AUDIT_SCHEMA = os.getenv("SGFOOD_AUDIT_SCHEMA", "audit")

# Tamaño de lote para inserciones masivas (execute_values)
INSERT_PAGE_SIZE = int(os.getenv("SGFOOD_INSERT_PAGE_SIZE", "1000"))

# Columnas técnicas de linaje agregadas en cada tabla raw
COLUMNAS_TECNICAS = ("_batch_id", "_fuente", "_fecha_carga")
