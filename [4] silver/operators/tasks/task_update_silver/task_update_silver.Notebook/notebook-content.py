# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "643c3c4f-c922-4d7f-9995-46991a3933c9",
# META       "default_lakehouse_name": "silver_lakehouse",
# META       "default_lakehouse_workspace_id": "1d10f168-5ee0-487f-bfb4-4bc7e9fdb6ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "643c3c4f-c922-4d7f-9995-46991a3933c9"
# META         }
# META       ]
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

%run internal_paths

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# PARAMETERS CELL ********************

target_path = "deltalake:fabric_showcase/silver_lakehouse/tables/pokemon/berry"
source_path = 'deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/berry'
write_method = 'overwrite'
partition_update = 'True'
full_refresh = 'False'
schema = None
partition_name = 'Partition'
min_partition = '20250512105513'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

partition_update = partition_update.strip().lower() == 'true'
full_refresh = full_refresh.strip().lower() == 'true'

catalog_path = get_deltalake_path('api', target_path)
if not spark.catalog.tableExists(catalog_path):
    full_refresh = True
elif min_partition is None:
    full_refresh = True

if full_refresh:
    min_partition = None
    write_method = 'overwrite'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_bronze_table(logical_path, min_partition = None):
    source_path = get_internal_path('abfss', logical_path)
    df = spark.read.format('delta').load(source_path)

    if min_partition:
        df = df.filter(col(partition_name) >= min_partition)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source = read_bronze_table(source_path, min_partition)

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

def get_df_filtered(df, filter_list):
    filter_expr = reduce(and_, (col(filter) for filter in filter_list))
    df = df.filter(filter_expr)

    return df

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

def get_df_distinct(df, primary_key_list, order_by_list):
    if order_by_list:
        window_spec = Window.partitionBy(*primary_key_list).orderBy(*[col(col_name).desc() for col_name in order_by_list])
        df = df.withColumn("row_number", row_number().over(window_spec))
        df = df.filter(col("row_number") == 1)
        df = df.drop("row_number")
    else:
        df = df.dropDuplicates(primary_key_list)

    return df

def get_df_outputs(df, output_list):
    df = df.selectExpr(*output_list)
    return df

def transform_df(df, column_map):
    column_map = get_json_map(column_map)
    metrics = {}

    select_list = get_select_list(column_map)
    df = get_df_selected(df, select_list)

    rows_1 = df.count()
    filter_list = get_filter_list(column_map)
    if filter_list:
        df = get_df_filtered(df, filter_list)
    rows_2 = df.count()
    metrics['filtered_rows'] = rows_1 - rows_2

    batch_key_list = get_batch_key_list(column_map)
    order_by_list = get_order_by_list(column_map)
    if batch_key_list and order_by_list:
        df = get_last_batch(df, batch_key_list, order_by_list)

    primary_key_list = get_primary_key_list(column_map)
    if primary_key_list:
        df = get_df_distinct(df, primary_key_list, order_by_list)
    
    output_list = get_output_list(column_map)
    df = get_df_outputs(df, output_list)

    print(metrics)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = transform_df(df_source, schema)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_overwrite(df, logical_path, partition_by_list = None):
    writer = df.write \
        .mode('overwrite') \
        .option('overwriteSchema', 'true') \
        .format('delta')

    if partition_by_list:
        writer = writer.partitionBy(partition_by_list)

    save_path = get_deltalake_path('relative', logical_path)
    writer.save(save_path)

def write_append(df, logical_path, partition_by_list = None):
    writer = df.write \
        .mode('append') \
        .option('mergeSchema', 'false') \
        .format('delta')

    if partition_by_list:
        writer = writer.partitionBy(partition_by_list)

    save_path = get_deltalake_path('relative', logical_path)
    writer.save(save_path)

def write_bk_merge(df, logical_path, batch_key_list, partition_by_list = None):
    df_batch = df.select(*batch_key_list).dropDuplicates(batch_key_list)
    join_condition = " and ".join([f"target.{batch_key} = updates.{batch_key}" for batch_key in batch_key_list])

    delta_path = get_deltalake_path('relative', logical_path)
    DeltaTable.forPath(spark, delta_path).alias("target") \
        .merge(df_batch.alias("updates"), join_condition) \
        .whenMatchedDelete() \
        .execute()

    write_append(df, logical_path, partition_by_list)

def write_pk_merge(df, logical_path, primary_key_list, partition_by_list = None, partition_update = False):
    
    join_condition = " and ".join([f"target.`{primary_key}` = updates.`{primary_key}`" for primary_key in primary_key_list])
    if partition_by_list and partition_update:
        partition_df = df.select(partition_by_list).distinct().collect()
        partition_values = [f"'{row[partition_by_list]}'" for row in partition_df]
        partition_string = ', '.join(partition_values)
        partition_filter = f"target.`{partition_by_list}` in ({partition_string})"
        join_condition = f'{join_condition} and {partition_filter}'

    delta_path = get_deltalake_path('relative', logical_path)
    DeltaTable.forPath(spark, delta_path).alias("target") \
        .merge(df.alias("updates"), join_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

def write_to_silver(df, logical_path, column_map, write_method, partition_update):
    column_map = get_json_map(column_map)
    partition_by_list = get_partition_by_list(column_map)
    batch_key_list = get_batch_key_list(column_map)
    primary_key_list = get_primary_key_list(column_map)

    if write_method == 'overwrite':
        write_overwrite(df, logical_path, partition_by_list)
    elif batch_key_list:
        write_bk_merge(df, logical_path, batch_key_list, partition_by_list)
    elif primary_key_list:
        write_pk_merge(df, logical_path, primary_key_list, partition_by_list, partition_update)
    else:
        write_append(df, logical_path, partition_by_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_to_silver(df, target_path, schema, write_method, partition_update)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_max_partition(df, partition_name):
    result = df.select(sql_max(col(partition_name)).alias(partition_name)).collect()[0][partition_name]
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(get_max_partition(df_source, partition_name))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
