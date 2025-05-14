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

%run internal_paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_bronze_table(logical_path, min_partition = None):
    source_path = get_deltalake_path('abfss', logical_path)
    df = spark.read.format('delta').load(source_path)

    if min_partition:
        df = df.filter(col('partition') >= min_partition)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_json, col, from_json,schema_of_json, lit, posexplode_outer

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.format('json').load('abfss://fabric_showcase@onelake.dfs.fabric.microsoft.com/bronze_lakehouse.Lakehouse/Files/pokemon/pokemon')
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df2 = df.selectExpr("to_json(abilities) as abilities")
display(df2)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df3 = df2.selectExpr(
    "posexplode_outer(from_json(abilities, "
    "'array<struct<ability:struct<name:string,url:string>,is_hidden:boolean,slot:bigint>>'"
    "))"
)
display(df3)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pyspark.sql.functions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
