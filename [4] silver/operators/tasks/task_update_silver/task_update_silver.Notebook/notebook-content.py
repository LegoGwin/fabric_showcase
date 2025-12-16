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
schema = """
    [
        {"expression":"id","column_type":"int","column_name":"Id","column_order":1,"is_filter":0,"is_primary_key":1,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"name","column_type":"string","column_name":"Name","column_order":2,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"growth_time","column_type":"int","column_name":"GrowthTime","column_order":3,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"max_harvest","column_type":"int","column_name":"MaxHarvest","column_order":4,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"natural_gift_power","column_type":"int","column_name":"NaturalGiftPower","column_order":5,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"size","column_type":"int","column_name":"Size","column_order":6,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"smoothness","column_type":"int","column_name":"Smoothness","column_order":7,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"soil_dryness","column_type":"int","column_name":"SoilDyrness","column_order":8,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"firmness_name","column_type":"string","column_name":"FirmnessName","column_order":9,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"firmness_url","column_type":"string","column_name":"FirmnessUrl","column_order":10,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"item_name","column_type":"string","column_name":"ItemName","column_order":11,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"item_url","column_type":"string","column_name":"ItemUrl","column_order":12,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"natural_gift_type_name","column_type":"string","column_name":"NaturalGiftTypeName","column_order":13,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"natural_gift_type_url","column_type":"string","column_name":"NaturalGiftTypeUrl","column_order":14,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"flavors_potency","column_type":"int","column_name":"FlavorPotency","column_order":15,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"flavors_flavor_name","column_type":"string","column_name":"FlavorName","column_order":16,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"flavors_flavor_url","column_type":"string","column_name":"FlavorUrl","column_order":17,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"partition","column_type":"string","column_name":"Partition","column_order":18,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":1,"is_output":1,"is_partition_by":0}
    ]
    """
partition_name = 'partition'
min_partition = '20250512105513'
full_refresh = 'false'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

partition_update = partition_update.strip().lower() == 'true'
full_refresh = full_refresh.strip().lower() == 'true'

abfss_path = get_deltalake_path('abfss', target_path)
if not DeltaTable.isDeltaTable(spark, abfss_path):
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

def read_bronze_table(logical_path, partition_name, min_partition = None):
    abfss_path = get_internal_path('abfss', logical_path)
    df = spark.read.format('delta').load(abfss_path)

    if min_partition:
        df = df.filter(col(partition_name) >= min_partition)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source = read_bronze_table(source_path, partition_name, min_partition)

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
        result = f"from_utc_timestamp(to_timestamp(`{source_name}`, 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')"
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
    filtered = [column for column in column_map if column["is_order_by"] > 0]
    ordered = sorted(filtered, key = lambda column: column["is_order_by"])
    result = [column["column_name"] for column in ordered]
    return result

def get_output_list(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_output') == 1]
    return result

def get_partition_by_list(column_map):
    filtered = [column for column in column_map if column["is_partition_by"] > 0]
    ordered = sorted(filtered, key = lambda column: column["is_partition_by"])
    result = [column["column_name"] for column in ordered]
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
        window_spec = (
            Window
            .partitionBy(*[col(f"`{column}`") for column in primary_key_list])
            .orderBy(*[col(f"`{column}`").desc() for column in order_by_list])
        )

        df = (
            df.withColumn("_row_number", row_number().over(window_spec))
              .filter(col("_row_number") == 1)
              .drop("_row_number")
        )
    else:
        df = df.dropDuplicates(primary_key_list)

    return df

def get_df_outputs(df, output_list):
    df = df.selectExpr(*output_list)
    return df

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
    pk_condition = " and ".join([f"target.`{k}` = updates.`{k}`" for k in primary_key_list])
    join_condition = pk_condition

    if partition_by_list and partition_update:
        partition_filter = get_partition_filter(df, partition_by_list)
        if partition_filter:
            join_condition = f"{pk_condition} AND {partition_filter}"

    delta_path = get_deltalake_path("relative", logical_path)

    DeltaTable.forPath(spark, delta_path) \
        .alias("target") \
        .merge(df.alias("updates"), join_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

def get_partition_filter(df, partition_by_list):
    parts = df.select(*partition_by_list).distinct().collect()

    if not parts:
        return None

    if len(partition_by_list) == 1:
        partition_column = partition_by_list[0]
        partition_values = []

        for partition_row in parts:
            partition_value = partition_row[partition_column]
            if partition_value is None:
                partition_values.append("null")
            else:
                escaped_value = str(partition_value).replace("'", "''")
                partition_values.append(f"'{escaped_value}'")

        partition_filter = (f"target.`{partition_column}` in ({', '.join(partition_values)})")

    else:
        partition_disjunctions = []

        for partition_row in parts:
            partition_conjunctions = []

            for partition_column in partition_by_list:
                partition_value = partition_row[partition_column]

                if partition_value is None:
                    partition_conjunctions.append(f"target.`{partition_column}` is null")
                else:
                    escaped_value = str(partition_value).replace("'", "''")
                    partition_conjunctions.append(f"target.`{partition_column}` = '{escaped_value}'")

            partition_disjunctions.append("(" + " and ".join(partition_conjunctions) + ")")

        partition_filter = "(" + " or ".join(partition_disjunctions) + ")"

    return partition_filter

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

def get_max_partition(df, partition_column):
    row = (df.selectExpr(f"max(`{partition_column}`) as max_value").first())
    result =  row["max_value"] if row else None
    
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
