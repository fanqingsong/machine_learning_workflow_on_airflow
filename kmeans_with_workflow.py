from csv import reader
from sklearn.cluster import KMeans
import joblib
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import airflow.utils
from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['qsfan@qq.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(seconds=5),
    'provide_context': True,
}

dag = DAG(
    dag_id='kmeans_with_workflow1',
    default_args=default_args,
    # schedule_interval="@once",
    # schedule_interval="00, *, *, *, *"  # support cron format
    # schedule_interval=timedelta(minutes=1)  # every minute
)


# Load a CSV file
def load_csv(filename):
    file = open(filename, "rt")
    lines = reader(file)
    dataset = list(lines)
    return dataset


# Convert string column to float
def str_column_to_float(dataset, column):
    for row in dataset:
        row[column] = float(row[column].strip())


# Convert string column to integer
def str_column_to_int(dataset, column):
    class_values = [row[column] for row in dataset]
    unique = set(class_values)
    lookup = dict()
    for i, value in enumerate(unique):
        lookup[value] = i
    for row in dataset:
        row[column] = lookup[row[column]]
    return lookup


def getRawIrisData(**context):
    # Load iris dataset
    filename = '/root/win10/mine/machine_learning_workflow_on_airflow/iris.csv'
    dataset = load_csv(filename)
    print('Loaded data file {0} with {1} rows and {2} columns'.format(filename, len(dataset), len(dataset[0])))
    print(dataset[0])
    # convert string columns to float
    for i in range(4):
        str_column_to_float(dataset, i)
    # convert class column to int
    lookup = str_column_to_int(dataset, 4)
    print(dataset[0])
    print(lookup)

    return dataset

# task for data
get_raw_iris_data = PythonOperator(
    task_id='get_raw_iris_data',
    python_callable=getRawIrisData,
    dag=dag,
    retries=2,
    provide_context=True,
)


def getTrainData(**context):
    dataset = context['task_instance'].xcom_pull(task_ids='get_raw_iris_data')

    trainData = [[one[0], one[1], one[2], one[3]] for one in dataset]

    print("Found {n_cereals} trainData".format(n_cereals=len(trainData)))

    return trainData

# task for getting training data
get_train_iris_data = PythonOperator(
    task_id='get_train_iris_data',
    python_callable=getTrainData,
    dag=dag,
    retries=2,
    provide_context=True,
)



def getNumClusters(**context):
    return 3


# task for getting cluster number
get_cluster_number = PythonOperator(
    task_id='get_cluster_number',
    python_callable=getNumClusters,
    dag=dag,
    retries=2,
    provide_context=True,
)


def train(**context):
    trainData = context['task_instance'].xcom_pull(task_ids='get_train_iris_data')
    numClusters = context['task_instance'].xcom_pull(task_ids='get_cluster_number')

    print("numClusters=%d" % numClusters)

    model = KMeans(n_clusters=numClusters)

    model.fit(trainData)

    # save model for prediction
    joblib.dump(model, 'model.kmeans')

    return trainData

# task for training
train_model = PythonOperator(
    task_id='train_model',
    python_callable=train,
    dag=dag,
    retries=2,
    provide_context=True,
)



def predict(**context):
    irisData = context['task_instance'].xcom_pull(task_ids='train_model')

    # test saved prediction
    model = joblib.load('model.kmeans')

    # cluster result
    labels = model.predict(irisData)

    print("cluster result")
    print(labels)


# task for predicting
predict_model = PythonOperator(
    task_id='predict_model',
    python_callable=predict,
    dag=dag,
    retries=2,
    provide_context=True,
)


def machine_learning_workflow_pipeline():
    get_raw_iris_data >> get_train_iris_data

    train_model << [get_cluster_number, get_train_iris_data]

    train_model >> predict_model


machine_learning_workflow_pipeline()

if __name__ == "__main__":
    dag.cli()
