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

from pyspark.sql.types import ArrayType, StructType
from pyspark.sql.functions import col, lit, posexplode_outer, to_json
from pyspark.sql.types import StructType, ArrayType, MapType
import re

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

def clean_col_name(name):
    name = name.replace('.', '_')
    name = re.sub(r'[^0-9A-Za-z_]', '', name)
    return name

def format_columns(df):
    exprs = []
    for field in df.schema:
        orig = field.name
        clean_name = clean_col_name(orig)

        if isinstance(field.dataType, (StructType, ArrayType, MapType)):
            exprs.append(f"to_json(`{orig}`) as `{clean_name}`")
        else:
            exprs.append(f"cast(`{orig}` as string) as `{clean_name}`")

    return df.selectExpr(*exprs)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def flatten_json(df, recursive_depth):
    if recursive_depth == 0:
        return df

    complex_fields = []
    for field in df.schema.fields:
        if isinstance(field.dataType, ArrayType):
            field_name = field.name
            df = df \
                .selectExpr("*", f"posexplode_outer(`{field_name}`) as (pos, `{field_name}_exploded`)") \
                .drop(field_name, "pos") \
                .withColumnRenamed(f"{field_name}_exploded", f'{field_name}')
        elif isinstance(field.dataType, StructType):
            for nested_field in field.dataType.fields:
                nested_field_name = f"{field.name}.{nested_field.name}"
                df = df.withColumn(
                    nested_field_name, 
                    col(f"`{field.name}`.`{nested_field.name}`")
                )
            complex_fields.append(field.name)


    df = df.drop(*complex_fields)
    if any(isinstance(field.dataType, (ArrayType, StructType)) for field in df.schema.fields):
        return flatten_json(df, recursive_depth - 1)
    
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
