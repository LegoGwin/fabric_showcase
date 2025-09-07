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

# CELL ********************

%run extract_pokemon_api

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_path = 'lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/item'
resource = 'item'
partition_name = 'partition'
partition_format = 'yyyyMMddHHmmss'

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

text_list_to_files(text_list, target_path, partition_name, partition_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
