import airflow_client.client
from pprint import pprint
from airflow_client.client.api import config_api

# The client must use the authentication and authorization parameters
# in accordance with the API server security policy.
# Examples for each auth method are provided below, use the example that
# satisfies your auth use case.
#
# In case of the basic authentication below, make sure that Airflow is
# configured with the basic_auth as backend:
#
# auth_backend = airflow.api.auth.backend.basic_auth
#
# Make sure that your user/name are configured properly

# Configure HTTP basic authorization: Basic
configuration = airflow_client.client.Configuration(
    host="http://localhost:8080/api/v1",
    username='admin',
    password='admin'
)


# Enter a context with an instance of the API client
with airflow_client.client.ApiClient(configuration) as api_client:
    # Create an instance of the API class
    api_instance = config_api.ConfigApi(api_client)

    try:
        # Get current configuration
        api_response = api_instance.get_config()
        pprint(api_response)
    except airflow_client.client.ApiException as e:
        print("Exception when calling ConfigApi->get_config: %s\n" % e)
