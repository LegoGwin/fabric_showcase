# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse_name": "",
# META       "default_lakehouse_workspace_id": ""
# META     }
# META   }
# META }

# CELL ********************

import requests
import json
from pyspark.sql.types import StringType
from pyspark.sql.functions import current_timestamp, date_format

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

%run internal_paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_path = 'lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry'
resource = 'berry'
partition_name = 'partition'
partition_format = 'yyyyMMddHHmmss'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_path = get_internal_path('abfss', target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_url = 'https://pokeapi.co/api/v2'

def issue_request(url):
    response = requests.get(url)
    resp_text = response.text

    return resp_text

def get_resource_urls(resource):
    urls = []

    items_url = f'{base_url}/{resource}?offset=0&limit=100'
    while items_url != None:
        resp_text = issue_request(items_url)
        resp_json = json.loads(resp_text)
        items_url = resp_json['next']
        urls.extend(item['url'] for item in resp_json['results'])

    return urls

def extract_pokemon_api(resource):
    results = []
    urls = get_resource_urls(resource)
    
    for url in urls:
        result = issue_request(url)
        results.append(result)

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text_list = extract_pokemon_api(resource)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def text_list_to_files(text_list, target_path, partition_name, partition_format):
    df = spark.createDataFrame(text_list, StringType())
    df = df.withColumn(partition_name, date_format(current_timestamp(), partition_format))

    df.write.mode('append').partitionBy(partition_name).text(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text_list_to_files(text_list, target_path, partition_name, partition_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
