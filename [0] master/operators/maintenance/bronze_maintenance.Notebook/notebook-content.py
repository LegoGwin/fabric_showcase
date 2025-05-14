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

tables = spark.catalog.listTables()
table_names = [table.name for table in tables if not table.name.startswith("bucs")]
print(table_names)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in table_names:
    spark.sql(f"VACUUM {table_name}")
    spark.sql(f"OPTIMIZE {table_name}")
    spark.sql(f"REFRESH TABLE {table_name}")
    print(table_name)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
