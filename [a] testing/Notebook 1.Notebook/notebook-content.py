# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_url = 'https://api.powerbi.com/v1.0/myorg/datasets/62d404c8-59e8-44ce-9a2a-0da392d2efb8'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def issue_request(url):
    response = requests.get(url)
    resp_text = response.text

    return resp_text

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test = issue_request(base_url)
print(test)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# 2) Build the auth object
from azure.identity import ClientSecretCredential
from msrest.authentication import BasicTokenAuthentication

# 1) Service principal details


# 1) Acquire a token via azure-identity
cred = ClientSecretCredential(tenant_id, client_id, client_secret)
token = cred.get_token("https://analysis.windows.net/powerbi/api/.default").token
print(token)
url = "https://api.powerbi.com/v1.0/myorg/admin/capacities"   # e.g. list workspaces
headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}
resp = requests.get(url, headers=headers)
print(resp.text)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(resp.headers)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
