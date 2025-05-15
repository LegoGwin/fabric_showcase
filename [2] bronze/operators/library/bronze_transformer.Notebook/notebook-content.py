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

import re
import json
from pyspark.sql.types import ArrayType, StructType, MapType
from pyspark.sql.functions import col, lit, posexplode_outer, to_json

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_json(df, flatten_mode, settings):
    df = flatten_json(df, flatten_mode, settings)
    df = format_columns(df)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
