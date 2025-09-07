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

from pyspark.sql.functions import current_timestamp, date_format
from pyspark.sql.types import StringType
from pyspark.sql.functions import max as sql_max, col

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

partition_name = 'partition'
partition_format = 'yyyyMMddHHmmss'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark",
# META   "frozen": false,
# META   "editable": true
# META }

# CELL ********************

def text_list_to_files(text_list, logical_path):
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

def read_json_files(logical_path, min_partition = None, multi_line = 'false'):
    path = get_lakehouse_path('abfs',logical_path)

    df = spark.read \
        .option('primitivesAsString', 'true') \
        .option('samplingRatio', 1) \
        .option('multiLine', multi_line) \
        .json(path)

    if min_partition:
        df = df.filter(f'{partition_name} >= {min_partition}')
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def dataframe_to_files(df, logical_path):
    df = df.withColumn(partition_name, date_format(current_timestamp(), partition_format))

    output_path = get_lakehouse_path('relative', logical_path)
    df.write.mode('append').partitionBy(partition_name).parquet(output_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_parquet_files(logical_path, min_partition = None):
    path = get_lakehouse_path('relative', logical_path)

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

def get_max_partition(df):
    result = df.select(sql_max(col(partition_name)).alias(partition_name)).collect()[0][partition_name]
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
