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

from pyspark.sql.functions import col, posexplode_outer, max
from pyspark.sql.types import ArrayType, StructType, MapType
import re
import json

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

target_path = 'deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/item'
source_path = 'lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/item'
flatten_mode = 'recursive'
flatten_settings = '[]'
multi_line = 'false'
partition_name = 'partition'
min_partition = '20250805134337'
full_refresh = 'false'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

full_refresh = full_refresh.strip().lower() == 'true'

api_path = get_internal_path('api', target_path)
if not spark.catalog.tableExists(api_path):
    full_refresh = True
elif min_partition is None:
    full_refresh = True

if full_refresh:
    min_partition = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_json_files(logical_path, multi_line, partition_name, min_partition):
    abfss_path = get_internal_path('abfss', logical_path)

    df = spark.read \
        .option('primitivesAsString', 'true') \
        .option('samplingRatio', 1) \
        .option('multiLine', multi_line) \
        .json(abfss_path)

    if min_partition:
        df = df.filter(f'{partition_name} >= {min_partition}')
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = read_json_files(source_path, multi_line, partition_name, min_partition)

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
        exprs.append(f"`{column_name}` as `{clean_name}`")

    df = df.selectExpr(*exprs)

    return df 
    
def get_flatten_settings(settings):
    settings = json.loads(settings)
    settings_list = []

    depths = sorted(set([setting['depth'] for setting in settings]))
    for depth in depths:
        setting = [setting['column'] for setting in settings if setting["depth"] == depth]
        settings_list.append(setting)

    return settings_list

def flatten_json_columns(df, column_names):
    for column_name in column_names:
        column_type = df.schema[column_name].dataType
        if isinstance(column_type, ArrayType):
            df = (df.selectExpr("*", f"posexplode_outer(`{column_name}`) as (pos, col)")
                .drop(column_name, 'pos')
                .withColumnRenamed('col', column_name))
        elif isinstance(column_type, StructType):
            for nested_field in column_type.fields:
                nested_name = f"{column_name}_{nested_field.name}"
                struct_name = f"`{column_name}`.`{nested_field.name}`"
                df = df.withColumn(nested_name, col(struct_name))
            df = df.drop(column_name)

    return df

def flatten_json(df, flatten_mode, flatten_settings = '[]'):
    flatten_mode = flatten_mode.strip().lower()

    if flatten_mode == 'recursive':
        fields = df.schema.fields
        while any(isinstance(field.dataType, (ArrayType, StructType)) for field in fields):
            column_names = [field.name for field in fields]
            df = flatten_json_columns(df, column_names)
            fields = df.schema.fields
    elif flatten_mode == 'column':
        flatten_settings = get_flatten_settings(flatten_settings)
        for column_names in flatten_settings:
            df = flatten_json_columns(df, column_names)

    return df

def transform_json(df, flatten_mode, settings):
    df = flatten_json(df, flatten_mode, settings)
    df = format_columns(df)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = transform_json(df, flatten_mode, flatten_settings)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_column_order(df, logical_path):
    api_path = get_internal_path('api', logical_path)
    abfss_path = get_internal_path('abfss', logical_path)

    if spark.catalog.tableExists(api_path):
        target_schema = spark.read.format('delta').load(abfss_path).schema
        target_columns = [f'`{field.name}`' for field in target_schema]
    else:
        target_columns = []

    source_schema = df.schema
    source_columns = [f'`{field.name}`' for field in source_schema]

    old_columns = [column for column in target_columns if column in source_columns]
    new_columns = [column for column in source_columns if column not in target_columns]
    column_order = old_columns + new_columns

    return column_order

def dataframe_to_table(df, logical_path, partition_name, full_refresh):
    abfss_path = get_internal_path('abfss', logical_path)

    column_order = get_column_order(df, logical_path)
    df = df.select(*column_order)
    
    if full_refresh:
        df.write \
            .mode('overwrite') \
            .option('overwriteSchema', 'true') \
            .partitionBy(partition_name) \
            .format('delta') \
            .save(abfss_path)
    else:
        df.write \
            .mode('append') \
            .option('mergeSchema', 'true') \
            .partitionBy(partition_name) \
            .format('delta') \
            .save(abfss_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

dataframe_to_table(df, target_path, partition_name, full_refresh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_max_partition(df, partition_name):
    result = df.select(max(col(partition_name)).alias(partition_name)).collect()[0][partition_name]
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(get_max_partition(df, partition_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
