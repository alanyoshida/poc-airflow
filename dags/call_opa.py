from __future__ import annotations

import logging
import sys
import requests
import json
import pendulum
from pprint import pprint
from datetime import datetime
import time
from airflow.models.dag import DAG
from airflow.operators.python import (
    PythonOperator,
)
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

with DAG(
    dag_id="policy_check_1",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["teste"],
):
  def request_resource(ti, **kwargs):
    # pprint(kwargs)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y-%H:%M:%S")

    # REQUEST DATA TO VALIDATE AGAINST OPA SERVER
    input = requests.get("http://golang-service.default.svc.cluster.local/servers",
                        headers={"Content-Type":"application/json"},)
    print(f"LOG=INFO DATE={dt_string} /servers RESPONSE:{input.json()}")

    # PREPARE REQUEST BODY
    request_body = json.dumps({"input": input.json()},ensure_ascii=False)
    ti.xcom_push(key="request_body", value=request_body)
    return request_body

  request_resource = PythonOperator(task_id="request_resource", python_callable=request_resource)

  def call_opa(ti, **kwargs):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    request_body = ti.xcom_pull(key="request_body", task_ids="request_resource")
    # VERIFY IN OPA IF IT ALLOW
    print(f"LOG=INFO DATE={dt_string} FN=call_opa REQUEST_BODY:{request_body}")
    allow_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/example/allow",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    print(f"LOG=INFO DATE={dt_string} FN=call_opa ROUTE=/v1/data/example/allow RESPONSE={allow_response.json()}")
    return allow_response.json()

  call_opa = PythonOperator(task_id="call_opa", python_callable=call_opa)

  def call_violation(ti, **kwargs):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    request_body = ti.xcom_pull(key="request_body", task_ids="request_resource")
    # VERIFY VIOLATION IN OPA
    violations_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/example/violation",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    print(f"LOG=INFO DATE={dt_string} FN=call_violation ROUTE=/v1/data/example/violation RESPONSE={violations_response.json()}")
    return json.dumps(violations_response.json())

  call_violation = PythonOperator(task_id="call_violation", python_callable=call_violation)

  save_violation = SQLExecuteQueryOperator(
      task_id="save_violation",
      conn_id="postgres_default",
      sql="INSERT INTO violations (date, violations, policies, severity, resource_type) VALUES (NOW(),%(violations)s,%(policies)s,%(severity)s,%(resource_type)s)",
      parameters={
        "violations": "{{ ti.xcom_pull(task_ids='call_violation', key='return_value') }}",
        "policies": json.dumps({
          "results": [
            "SSH",
            "TELNET"
          ]
        }),
        "severity": "HIGH",
        "resource_type": "NETWORK"
      },
  )

  request_resource >> call_opa >> call_violation >> save_violation
