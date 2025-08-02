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

from pyspark.sql.functions import current_timestamp, date_format, col, lit, posexplode_outer, to_json, max as sql_max, col
from pyspark.sql.types import StringType, ArrayType, StructType, MapType
from pyspark.sql.functions import max as sql_max, col
import re
import json

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_path = 'deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/berry'
source_path = 'lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry'
partition_name = 'Partition'
min_partition = '20250512141514'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_parquet_files(logical_path, partition_name, min_partition):
    path = get_internal_path('abfss', logical_path)

    df = spark.read \
        .option('mergeSchema', 'true') \
        .parquet(path)

    if min_partition:
        df = df.filter(f'{partition_name} >= {min_partition}')

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = read_parquet_files(source_path, partition_name, min_partition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_column_name(column_name):
    column_name = column_name.replace('.', '_')
    column_name = re.sub(r'[^0-9A-Za-z_]', '', column_name)

    return column_name

def format_columns(df):
    exprs = []
    for field in df.schema:
        column_name = field.name
        clean_name = clean_column_name(column_name)

        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            exprs.append(f"to_json(`{column_name}`) as `{clean_name}`")
        else:
            exprs.append(f"cast(`{column_name}` as string) as `{clean_name}`")

    df = df.selectExpr(*exprs)

    return df 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = format_columns(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dataframe_to_table(df, logical_path):
    api_path = get_lakehouse_path('api', logical_path)
    relative_path = get_lakehouse_path('relative', logical_path)

    if spark.catalog.tableExists(api_path):
        target_schema = spark.read.format('delta').load(relative_path).schema
        target_columns = [f'`{field.name}`' for field in target_schema]
    else:
        target_columns = []

    source_schema = df.schema
    source_columns = [f'`{field.name}`' for field in source_schema]

    old_columns = [column for column in target_columns if column in source_columns]
    new_columns = [column for column in source_columns if column not in target_columns]
    ordered_columns = old_columns + new_columns

    df_reordered = df.select(*ordered_columns)
    
    df_reordered.write \
        .option('mergeSchema', 'true') \
        .mode('append') \
        .partitionBy(partition_name) \
        .format('delta') \
        .save(relative_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe_to_table(df, target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_max_partition(df):
    result = df.select(sql_max(col(partition_name)).alias(partition_name)).collect()[0][partition_name]
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(get_max_partition(df))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
