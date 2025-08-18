# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1b9da1ef-0c8d-49bb-85aa-a9ef54fa3b0c",
# META       "default_lakehouse_name": "test_lakehouse",
# META       "default_lakehouse_workspace_id": "6499e4de-59d6-4a0b-bef7-5281edf8c372",
# META       "known_lakehouses": [
# META         {
# META           "id": "1b9da1ef-0c8d-49bb-85aa-a9ef54fa3b0c"
# META         },
# META         {
# META           "id": "643c3c4f-c922-4d7f-9995-46991a3933c9"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

df = spark.read.format('delta').load('abfss://fabric_showcase@onelake.dfs.fabric.microsoft.com/silver_lakehouse.Lakehouse/Tables/pokemon/berry')
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

df.write.format('delta').save('abfss://fabric_test@onelake.dfs.fabric.microsoft.com/test_lakehouse.Lakehouse/Tables/pokemon_berry')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
