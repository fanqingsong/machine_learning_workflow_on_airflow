# machine_learning_workflow_on_airflow

## Purpose
For demostrating how to apply workflow to machine learning system.

## Demo

![flowchart](flowchart.png)

## Run

based on python 3.6.8

```
# airflow needs a home, ~/airflow is the default,
# but you can lay foundation somewhere else if you prefer
# (optional)
export AIRFLOW_HOME=~/airflow

pip install -r requirement.txt

# initialize the database
airflow initdb

# deliver this file to airflow dags folder
cp -f kmeans_with_workflow.py  ~/airflow/dags

# startup scheduler
airflow scheduler

# startup web UI
airflow webserver -p 8080


# run on this project folder
airflow backfill -sd . kmeans_with_workflow  -s 2015-06-01 -e 2015-06-07

```

open http://127.0.0.1:8080 on browser, and enter WEB UI to watch DAG graph and run.


To be fixed:
Trigger workflow run on the web GUI, will raise an error "not find iris.csv", need to be investigated.
