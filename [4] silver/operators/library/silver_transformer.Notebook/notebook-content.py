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

import json
import re
from pyspark.sql.functions import col, to_timestamp, from_utc_timestamp, to_utc_timestamp, max as sql_max, row_number, to_date
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce
from operator import and_

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_json_map(column_map):
    result = json.loads(column_map)
    return result

def get_magic_expr(expression):
    name_regex = r"#\w+\(([^)]+)\)"
    source_name = re.match(name_regex, expression).group(1)

    func_regex = r"#([^()]+)\("
    function = re.match(func_regex, expression).group(1)

    if function == 'datetime1':
        result = f"to_timestamp(`{source_name}`, 'yyyy-MM-dd HH:mm:ss')"
    elif function == "datetime2":
        result = f"from_utc_timestamp(to_timestamp(`{source_name}`, 'yyyy-MM-dd HH:mm:ss'), America/New_York')"
    elif function == "datetime3":
        result = f"to_timestamp(regexp_replace(left(`{source_name}`, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"
    else:
        result = f"`{source_name}`"

    return result

def get_select_expr(column):
    expression = column["expression"]
    column_type = column["column_type"]
    column_name = column["column_name"]

    if expression.startswith("#"):
        expression = get_magic_expr(expression)
    elif expression.startswith("@"):
        expression = expression[1:]
    else:
        expression = f"`{expression}`"

    result = f"cast({expression} as {column_type}) as `{column_name}`"

    return result

def get_select_list(column_map):
    sorted_map = sorted(column_map, key = lambda x: x['column_order'])
    result = [get_select_expr(column) for column in sorted_map]

    return result

def get_filter_list(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_filter') == 1]
    return result

def get_primary_key_list(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_primary_key') == 1]
    return result

def get_batch_key_list(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_batch_key') == 1]
    return result

def get_order_by_list(column_map):
    sorted_map = sorted(column_map, key = lambda x: x['is_order_by'])
    result = [column['column_name'] for column in sorted_map if column.get('is_order_by') > 0]
    return result

def get_output_list(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_output') == 1]
    return result

def get_partition_by_list(column_map):
    sorted_map = sorted(column_map, key = lambda x: x['is_partition_by'])
    result = [column['column_name'] for column in column_map if column.get('is_partition_by') > 0]
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_df_selected(df, select_list):
    df = df.selectExpr(*select_list)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_df_filtered(df, filter_list):
    filter_expr = reduce(and_, (col(filter) for filter in filter_list))
    df = df.filter(filter_expr)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_last_batch(df, batch_key_list, order_by_list):
    agg_exprs = [sql_max(s).alias(f"max_{s}") for s in order_by_list]
    df_max = df.groupBy(*batch_key_list).agg(*agg_exprs)

    for k in batch_key_list:
        df_max = df_max.withColumnRenamed(k, f"max_{k}")

    join_condition = None
    for k in batch_key_list:
        cond = (df[k] == df_max[f"max_{k}"])
        join_condition = cond if join_condition is None else (join_condition & cond)

    for s in order_by_list:
        cond = (df[s] == df_max[f"max_{s}"])
        join_condition = join_condition & cond

    df_joined = df.join(df_max, join_condition, "inner")

    columns_to_drop = [f"max_{k}" for k in batch_key_list] + [f"max_{s}" for s in order_by_list]
    df_joined = df_joined.drop(*columns_to_drop)

    return df_joined

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_df_distinct(df, primary_key_list, order_by_list):
    if order_by_list:
        window_spec = Window.partitionBy(*primary_key_list).orderBy(*[col(col_name).desc() for col_name in order_by_list])
        df = df.withColumn("row_number", row_number().over(window_spec))
        df = df.filter(col("row_number") == 1)
        df = df.drop("row_number")
    else:
        df = df.dropDuplicates(primary_key_list)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_df_outputs(df, output_list):
    df = df.selectExpr(*output_list)
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def transform_df(df, column_map):
    column_map = get_json_map(column_map)

    select_list = get_select_list(column_map)
    df = get_df_selected(df, select_list)

    filter_list = get_filter_list(column_map)
    if filter_list:
        df = get_df_filtered(df, filter_list)

    batch_key_list = get_batch_key_list(column_map)
    order_by_list = get_order_by_list(column_map)
    if batch_key_list and order_by_list:
        df = get_last_batch(df, batch_key_list, order_by_list)

    primary_key_list = get_primary_key_list(column_map)
    if primary_key_list:
        df = get_df_distinct(df, primary_key_list, order_by_list)
    
    output_list = get_output_list(column_map)
    df = get_df_outputs(df, output_list)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
