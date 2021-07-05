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

# install dependency
pip install -r requirement.txt

# set option for mysql
vim /etc/mysql/my.cnf

set option:
explicit_defaults_for_timestamp=1

Note:
https://airflow.apache.org/docs/apache-airflow/1.10.12/howto/initialize-database.html
https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-mysql-database

# start database
/etc/init.d/mysql start

Note: to stop it
/etc/init.d/mysql stop

# create logic database
CREATE DATABASE airflow CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

Note:
https://airflow.apache.org/docs/apache-airflow/stable/howto/set-up-database.html#setting-up-a-mysql-database


# add mysql connection to airflow.cfg
vim ~/airflow/airflow.cfg
set parameter:
sql_alchemy_conn = mysql://{USERNAME}:{PASSWORD}@{MYSQL_HOST}:3306/airflow

sql_alchemy_conn = mysql://root:root@localhost:8806/airflow

Note:
https://stackoverflow.com/questions/61663681/configure-sql-server-in-airflow-with-sql-alchemy-conn
https://airflow.apache.org/docs/apache-airflow/1.10.12/howto/initialize-database.html


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
