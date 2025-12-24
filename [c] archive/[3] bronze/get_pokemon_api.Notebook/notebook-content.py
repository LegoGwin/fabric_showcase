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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

base_url = 'https://pokeapi.co/api/v2'

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

def get_resource_urls(resource):
    urls = []

    items_url = f'{base_url}/{resource}?offset=0&limit=100'
    while items_url != None:
        resp_text = issue_request(items_url)
        resp_json = json.loads(resp_text)
        items_url = resp_json['next']
        urls.extend(item['url'] for item in resp_json['results'])

    return urls

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

def text_list_to_files(text_list, logical_path, partition_name, partition_format):
    df = spark.createDataFrame(text_list, StringType())
    df = df.withColumn(partition_name, date_format(current_timestamp(), partition_format))

    output_path = get_internal_path('abfss', logical_path)
    df.write.mode('append').partitionBy(partition_name).text(output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def extract_pokemon_api(resource):
    results = []
    urls = get_resource_urls(resource)
    
    for url in urls:
        result = issue_request(url)
        results.append(result)

    return results

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
