import time
import airflow_client.client
from airflow_client.client.api import dag_api
from airflow_client.client.model.dag_detail import DAGDetail
from airflow_client.client.model.error import Error
from pprint import pprint
# Defining the host is optional and defaults to http://localhost/api/v1
# See configuration.py for a list of all supported configuration parameters.

# The client must configure the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.

# Configure HTTP basic authorization: Basic
configuration = airflow_client.client.Configuration(
    host = "http://localhost:8080/api/v1",
    username = 'admin',
    password = 'admin'
)

# Enter a context with an instance of the API client
with airflow_client.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = dag_api.DAGApi(api_client)
    dag_id = "kmeans_with_workflow1" # str | The DAG ID.

    # example passing only required values which don't have defaults set
    try:
        # Get a simplified representation of DAG
        api_response = api_instance.get_dag_details(dag_id)
        pprint(api_response)
    except airflow_client.client.ApiException as e:
        print("Exception when calling DAGApi->get_dag_details: %s\n" % e)