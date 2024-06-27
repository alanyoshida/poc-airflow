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
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.models import Variable

import googleapiclient.discovery
from google.oauth2 import service_account

log = logging.getLogger(__name__)

PATH_TO_PYTHON_BINARY = sys.executable

with DAG(
    dag_id="iam_policy",
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["teste"],
):
  def request_resource(ti, **kwargs):
    # pprint(kwargs)
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y-%H:%M:%S")

    credentials = service_account.Credentials.from_service_account_file(
        filename='/home/airflow/gcp.json',
        scopes=['https://www.googleapis.com/auth/cloud-platform'])

    service = googleapiclient.discovery.build(
        'cloudresourcemanager', 'v1', credentials=credentials)

    project = Variable.get("project")
    input = service.projects().getIamPolicy(resource=project, body={}).execute()

    # print(json.dumps(input))

    # REQUEST DATA TO VALIDATE AGAINST OPA SERVER
    # input = requests.get("http://golang-service.default.svc.cluster.local/servers",
    #                     headers={"Content-Type":"application/json"},)
    print(f"LOG=INFO DATE={dt_string} /servers RESPONSE:{input} PROJECT={project}")

    # PREPARE REQUEST BODY
    request_body = json.dumps({"input": input},ensure_ascii=False)
    ti.xcom_push(key="request_body", value=request_body)
    return request_body

  request_resource = PythonOperator(task_id="request_resource", python_callable=request_resource)

  def call_opa(ti, **kwargs):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    request_body = ti.xcom_pull(key="request_body", task_ids="request_resource")
    # VERIFY IN OPA IF IT ALLOW
    print(f"LOG=INFO DATE={dt_string} FN=call_opa REQUEST_BODY:{request_body}")
    allow_response = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/iam/allow",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    print(f"LOG=INFO DATE={dt_string} FN=call_opa ROUTE=/v1/data/iam/allow RESPONSE={allow_response.json()}")
    return allow_response.json()

  call_opa = PythonOperator(task_id="call_opa", python_callable=call_opa)

  def call_violation(ti, **kwargs):
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    request_body = ti.xcom_pull(key="request_body", task_ids="request_resource")
    # VERIFY VIOLATION IN OPA
    has_admin = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/iam/has_admin",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    has_admin_json = has_admin.json()

    has_owner = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/iam/has_owner",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    has_owner_json = has_owner.json()
    owner_count = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/iam/owner_count",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    owner_count_json = owner_count.json()
    admin_count = requests.post("http://opa.default.svc.cluster.local:8181/v1/data/iam/admin_count",
                            headers={"Content-Type":"application/json"},
                            data=request_body)
    admin_count_json = admin_count.json()
    print(f"LOG=INFO DATE={dt_string} FN=call_violation ROUTE=/v1/data/iam/* RESPONSE={violations_response.json()}")

    total_violations = int(admin_count_json["result"]) + int(owner_count_json["result"])
    ti.xcom_push(key="total_violations", value=total_violations)

    violations = {
      "admin_count": admin_count_json["result"],
      "owner_count": owner_count_json["result"],
      "has_admin": has_admin_json,
      "has_owner": has_owner_json,
      "total_violations": total_violations
    }
    return json.dumps(violations)

  call_violation = PythonOperator(task_id="call_violation", python_callable=call_violation)

  save_violation = SQLExecuteQueryOperator(
      task_id="save_violation",
      conn_id="postgres_default",
      sql="INSERT INTO violations (date, violations, policies, severity, resource_type, total_violations) VALUES (NOW(),%(violations)s,%(policies)s,%(severity)s,%(resource_type)s)",
      parameters={
        "violations": "{{ ti.xcom_pull(task_ids='call_violation', key='return_value') }}",
        "policies": json.dumps({
          "results": [
            "ADMINISTRATIVE_ROLES"
          ]
        }),
        "severity": "HIGH",
        "resource_type": "IAM",
        "total_violations": "{{ ti.xcom_pull(task_ids='call_violation', key='total_violations') }}",
      },
  )

  request_resource >> call_opa >> call_violation >> save_violation
