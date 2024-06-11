from __future__ import annotations

import logging
import sys
import requests
import json
import pendulum
from pprint import pprint
from datetime import datetime
from airflow.models.dag import DAG
from airflow.operators.python import (
    PythonOperator,
)

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
    dt_string = now.strftime("%d/%m/%Y-%H:%M:%S")
    request_body = ti.xcom_pull(key="request_body", task_ids="request_resource")
    # VERIFY IN OPA IF IT ALLOW
    print(f"LOG=INFO DATE={dt_string} REQUEST_BODY:{request_body}")
    allow_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/example/allow",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    print(f"LOG=INFO DATE={dt_string} /v1/data/example/allow RESPONSE={allow_response.json()}")
    return request_body

  call_opa = PythonOperator(task_id="call_opa", python_callable=call_opa)

  def call_violation(ti, **kwargs):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y-%H:%M:%S")
    request_body = ti.xcom_pull(key="request_body", task_ids="request_resource")
    # VERIFY VIOLATION IN OPA
    violations_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/example/violation",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    print(f"LOG=INFO DATE={dt_string} /v1/data/example/violation RESPONSE={violations_response.json()}")
  call_violation = PythonOperator(task_id="call_violation", python_callable=call_violation)

  request_resource >> call_opa >> call_violation

# import requests
# import json
# from datetime import datetime

# def main():
#     now = datetime.now()
#     dt_string = now.strftime("%d/%m/%Y-%H:%M:%S")

#     # REQUEST DATA TO VALIDATE AGAINST OPA SERVER
#     input = requests.get("http://golang-service.default.svc.cluster.local/servers",
#                          headers={"Content-Type":"application/json"},)
#     print(f"LOG=INFO DATE={dt_string} /servers RESPONSE:{input.json()}")

#     # PREPARE REQUEST BODY
#     request_body = json.dumps({"input": input.json()},ensure_ascii=False)

#     # VERIFY IN OPA IF IT ALLOW
#     print(f"LOG=INFO DATE={dt_string} REQUEST_BODY:{request_body}")
#     allow_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/example/allow",
#                             headers={"Content-Type":"application/json"},
#                             data=request_body)
#     print(f"LOG=INFO DATE={dt_string} /v1/data/example/allow RESPONSE={allow_response.json()}")

#     # VERIFY VIOLATION IN OPA
#     violations_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/example/violation",
#                             headers={"Content-Type":"application/json"},
#                             data=request_body)
#     print(f"LOG=INFO DATE={dt_string} /v1/data/example/violation RESPONSE={violations_response.json()}")

#     # Execute function violation
#     fission_call = requests.post("http://router.default.svc.cluster.local/violation-pkg",
#                             headers={"Content-Type":"application/json"},
#                             json=violations_response.json())
#     print(f"LOG=INFO DATE={dt_string} FUNCTION=opa-violation RESPONSE_STATUS={fission_call.status_code} RESPONSE={fission_call.content}")

#     return allow_response.json(), allow_response.status_code
