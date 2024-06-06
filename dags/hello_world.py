from __future__ import annotations

import logging
import sys
from pprint import pprint

import pendulum

from airflow.models.dag import DAG
from airflow.operators.python import (
    PythonOperator,
)

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable


with DAG(
    dag_id="alan_hello_world",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["teste"],
):
    # [START howto_operator_python]
    def print_hello(ds=None, **kwargs):
        # pprint(kwargs)
        print("HEEELLOOOO WORLLD")
        return "LOG DO RETORNO"

    run_this = PythonOperator(task_id="print_hello", python_callable=print_hello)
    # [END howto_operator_python]
