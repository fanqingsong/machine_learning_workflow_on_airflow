import time
import airflow_client.client as client
from airflow_client.client.api import dag_run_api
from airflow_client.client.model.dag_run import DAGRun
from airflow_client.client.model.dag_state import DagState
from airflow_client.client.model.error import Error
from dateutil.parser import parse
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: Basic
configuration = client.Configuration(
    host = "http://localhost:8080/api/v1",
    username = 'admin',
    password = 'admin'
)

# Enter a context with an instance of the API client
with client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = dag_run_api.DAGRunApi(api_client)
    dag_id = "example_xcom_args_with_operators" # str | The DAG ID.
    dag_run = DAGRun(
        dag_id,
        dag_run_id=None,
        # execution_date=parse('1970-01-01T00:00:00.00Z'),
        # state=DagState("success"),
        # conf={},
    ) # DAGRun | 

    # example passing only required values which don't have defaults set
    try:
        # Trigger a new DAG run
        api_response = api_instance.post_dag_run(dag_id, dag_run)
        pprint(api_response)
    except client.ApiException as e:
        print("Exception when calling DAGRunApi->post_dag_run: %s\n" % e)
