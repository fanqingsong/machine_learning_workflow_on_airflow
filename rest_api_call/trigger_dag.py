import requests
import json
from pprint import pprint
from datetime import datetime

# datetime object containing current date and time
now = datetime.now()
 
print("now =", now)

dt_string = now.strftime("%Y-%m-%dT%H:%M:%SZ")
print("date and time =", dt_string)	

dag_id = "kmeans_with_workflow"
dag_run_id = "A_TEST_DAG_RUN4"

def trigger_dag():
    data = {
        # "dag_run_id": dag_run_id,
        "execution_date": dt_string,
        # "execution_date": None,
        # "state": None,
        "conf": { }
    }

    header = {"content-type": "application/json"}

    result = requests.post(
    f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns",
    data=json.dumps(data),
    headers=header,
    auth=("admin", "admin"))

    pprint(result.content.decode('utf-8'))


def get_dag_run():
    result = requests.get(
    f"http://localhost:8080/api/v1/dags/{dag_id}/dagRuns/{dag_run_id}",
    auth=("admin", "admin"))

    pprint(result.content.decode('utf-8'))


trigger_dag()
get_dag_run()
