# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "20afa95c-f7fc-45a4-b213-a47c9a2a6c5c",
# META       "default_lakehouse_name": "bronze_lakehouse",
# META       "default_lakehouse_workspace_id": "1d10f168-5ee0-487f-bfb4-4bc7e9fdb6ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "20afa95c-f7fc-45a4-b213-a47c9a2a6c5c"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run extract_pokemon_api

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_path = 'lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry'
resource = 'berry'
partition_name = 'Partition'
partition_format = 'yyyyMMddHHmmss'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

json_list = extract_pokemon_api(resource)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def text_list_to_files(text_list, logical_path, partition_name, partition_format):
    df = spark.createDataFrame(text_list, StringType())
    df = df.withColumn(partition_name, date_format(current_timestamp(), partition_format))

    output_path = get_lakehouse_path('relative', logical_path)
    df.write.mode('append').partitionBy(partition_name).text(output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

text_list_to_files(json_list, target_path, partition_name, partition_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
