FROM apache/airflow:2.10.5-python3.11

ARG AIRFLOW_VERSION=2.10.5
ARG PYTHON_VERSION=3.11
ARG DBT_CORE_VERSION=1.9.4
ARG DBT_POSTGRES_VERSION=1.9.0

USER airflow

# Dependencias del proceso de extracción/carga (respetando las constraints de Airflow)
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# dbt se instala en un entorno virtual aislado para evitar conflictos de
# dependencias con Airflow. Los DAGs lo invocan mediante $DBT_BIN.
RUN python -m venv /home/airflow/dbt_venv \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir --upgrade pip \
 && /home/airflow/dbt_venv/bin/pip install --no-cache-dir \
      "dbt-core==${DBT_CORE_VERSION}" "dbt-postgres==${DBT_POSTGRES_VERSION}"

ENV DBT_BIN=/home/airflow/dbt_venv/bin/dbt
