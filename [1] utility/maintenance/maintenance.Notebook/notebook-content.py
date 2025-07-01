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

schema_list = spark.sql('show schemas').select('namespace').rdd.map(lambda r: r[0]).collect()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_names = []
for schema in schema_list:
    spark.catalog.setCurrentDatabase(schema)
    tables = spark.catalog.listTables()
    table_names.extend([f"{schema}.{table.name}" for table in tables])

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
