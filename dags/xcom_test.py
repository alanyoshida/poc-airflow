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
    dag_id="alan_xcom",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["teste"],
):

    def part1(ti, **kwargs):
        number = 3
        ti.xcom_push(key="number", value=number)
        # pprint(kwargs)
        print("PART 1 - SEND NUMBER {number}")
        return "LOG DO RETORNO"

    part1 = PythonOperator(task_id="part1", python_callable=part1)
    # [END howto_operator_python]

    def part2(ti, **kwargs):
        # pprint(kwargs)
        ti.xcom_pull(key="number", task_ids="part1")
        print(f"PART 2 = RECEIVED NUMBER")
        return "LOG DO RETORNO"

    part2 = PythonOperator(task_id="part2", python_callable=part2)

    part1 >> part2