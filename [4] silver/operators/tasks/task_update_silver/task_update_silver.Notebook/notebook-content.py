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
import pyspark.sql.functions as sql_functions
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from functools import reduce
from operator import and_
from pyspark.sql.types import BooleanType

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
partition_update = 'true'
schema = """
    [
        {"expression":"id","column_type":"int","column_name":"Id","column_order":1,"is_filter":0,"is_primary_key":1,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"name","column_type":"string","column_name":"Name","column_order":2,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"growth_time","column_type":"int","column_name":"GrowthTime","column_order":3,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"max_harvest","column_type":"int","column_name":"MaxHarvest","column_order":4,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"natural_gift_power","column_type":"int","column_name":"NaturalGiftPower","column_order":5,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"size","column_type":"int","column_name":"Size","column_order":6,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"smoothness","column_type":"int","column_name":"Smoothness","column_order":7,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
        {"expression":"soil_dryness","column_type":"int","column_name":"SoilDryness","column_order":8,"is_filter":0,"is_primary_key":0,"is_batch_key":0,"is_order_by":0,"is_output":1,"is_partition_by":0},
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
extract_partition = 'partition'
min_extract_partition = '20250512105513'
full_refresh = 'false'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

schema_json = json.loads(schema)

partition_update = partition_update.strip().lower() == 'true'
full_refresh = full_refresh.strip().lower() == 'true'

source_path = get_internal_path('abfss', source_path)

target_path = get_internal_path('abfss', target_path)
if not DeltaTable.isDeltaTable(spark, target_path):
    full_refresh = True
elif min_extract_partition is None:
    full_refresh = True

if full_refresh:
    min_extract_partition = None
    write_method = 'overwrite'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_bronze_table(source_path, extract_partition, min_extract_partition = None):
    df = spark.read.format('delta').load(source_path)

    if min_extract_partition:
        df = df.filter(sql_functions.col(extract_partition) >= min_extract_partition)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source = read_bronze_table(source_path, extract_partition, min_extract_partition)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_magic_expr(expression):
    if expression is None:
        raise ValueError("Invalid magic expression: None. Expected format like #func(col), e.g. #datetime1(order_dt).")

    expr = str(expression).strip()

    name_regex = r"^#\w+\(([^)]+)\)$"
    name_match = re.match(name_regex, expr)
    if not name_match:
        raise ValueError(f"Invalid magic expression: {expr!r}. Expected format like #func(col), e.g. #datetime1(order_dt)")
    source_name = name_match.group(1).strip()

    func_regex = r"^#([^()]+)\("
    func_match = re.match(func_regex, expr)
    if not func_match:
        raise ValueError(f"Invalid magic expression: {expr!r}. Could not parse function name. Expected format like #func(col).")
    function = func_match.group(1).strip()

    if function == "datetime1":
        result = f"to_timestamp(`{source_name}`, 'yyyy-MM-dd HH:mm:ss')"
    elif function == "datetime2":
        result = f"from_utc_timestamp(to_timestamp(`{source_name}`, 'yyyy-MM-dd HH:mm:ss'), 'America/New_York')"
    elif function == "datetime3":
        result = f"to_timestamp(regexp_replace(left(`{source_name}`, 19), 'T', ' '), 'yyyy-MM-dd HH:mm:ss')"
    else:
        raise ValueError('Unrecognized magic expression.')

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

def get_select_list(schema_json):
    sorted_map = sorted(schema_json, key = lambda x: x['column_order'])
    result = [get_select_expr(column) for column in sorted_map]

    return result

def get_filter_list(schema_json):
    result = [column['column_name'] for column in schema_json if column.get('is_filter') == 1]
    return result

def get_primary_key_list(schema_json):
    result = [column['column_name'] for column in schema_json if column.get('is_primary_key') == 1]
    return result

def get_batch_key_list(schema_json):
    result = [column['column_name'] for column in schema_json if column.get('is_batch_key') == 1]
    return result

def get_order_by_list(schema_json):
    filtered = [column for column in schema_json if column["is_order_by"] > 0]
    ordered = sorted(filtered, key = lambda column: column["is_order_by"])
    result = [column["column_name"] for column in ordered]
    return result

def get_output_list(schema_json):
    result = [column['column_name'] for column in schema_json if column.get('is_output') == 1]
    return result

def get_partition_by_list(schema_json):
    filtered = [column for column in schema_json if column["is_partition_by"] > 0]
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
    schema_by_name = {field.name: field.dataType for field in df.schema.fields}

    missing = [column for column in filter_list if column not in schema_by_name]
    if missing:
        raise ValueError(f"Filter_list contains columns not in dataframe: {missing}")

    non_bool = [(c, schema_by_name[c]) for c in filter_list if not isinstance(schema_by_name[c], BooleanType)]
    if non_bool:
        details = ", ".join([f"{c}={t.simpleString()}" for c, t in non_bool])
        raise TypeError(f"Filter_list columns must be boolean. Non-boolean columns: {details}")

    filter_expr = reduce(and_, (sql_functions.col(c) for c in filter_list))
    df_filtered = df.filter(filter_expr)

    return df_filtered

def apply_latest_keep_ties(df, batch_key_list, order_by_list):
    window_spec = (
        Window
        .partitionBy(*[sql_functions.col(c) for c in batch_key_list])
        .orderBy(*[sql_functions.col(c).desc_nulls_last() for c in order_by_list])
    )

    df = (
        df
        .withColumn("_dense_rank", sql_functions.dense_rank().over(window_spec))
        .filter(sql_functions.col("_dense_rank") == 1)
        .drop("_dense_rank")
    )

    return df

def apply_latest_break_ties(df, primary_key_list, order_by_list):
    if order_by_list:
        window_spec = (
            Window
            .partitionBy(*[sql_functions.col(column) for column in primary_key_list])
            .orderBy(*[sql_functions.col(column).desc_nulls_last() for column in order_by_list])
        )

        df = (
            df.withColumn("_row_number", sql_functions.row_number().over(window_spec))
              .filter(sql_functions.col("_row_number") == 1)
              .drop("_row_number")
        )
    else:
        df = df.dropDuplicates(primary_key_list)

    return df

def get_df_outputs(df, output_list):
    df = df.selectExpr(*output_list)
    return df

def transform_df(df, schema_json):
    select_list = get_select_list(schema_json)
    df = get_df_selected(df, select_list)

    filter_list = get_filter_list(schema_json)
    if filter_list:
        df = get_df_filtered(df, filter_list)

    batch_key_list = get_batch_key_list(schema_json)
    order_by_list = get_order_by_list(schema_json)
    if batch_key_list and order_by_list:
        df = apply_latest_keep_ties(df, batch_key_list, order_by_list)

    primary_key_list = get_primary_key_list(schema_json)
    if primary_key_list:
        df = apply_latest_break_ties(df, primary_key_list, order_by_list)
    
    output_list = get_output_list(schema_json)
    df = get_df_outputs(df, output_list)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = transform_df(df_source, schema_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_overwrite(df, target_path, partition_by_list = None):
    writer = df.write \
        .mode('overwrite') \
        .option('overwriteSchema', 'true') \
        .format('delta')

    if partition_by_list:
        writer = writer.partitionBy(*partition_by_list)

    writer.save(target_path)

def write_append(df, target_path, partition_by_list = None):
    writer = df.write \
        .mode('append') \
        .option('mergeSchema', 'false') \
        .format('delta')

    if partition_by_list:
        writer = writer.partitionBy(*partition_by_list)

    writer.save(target_path)

def drop_null_keys(df, key_list):
    null_pred = reduce(lambda a,b: a | b, (F.col(k).isNull() for k in key_list))
    return df.filter(~null_pred)

def write_rebuild_by_batch_key(df, target_path, batch_key_list, partition_by_list = None, partition_update = False):
    df = drop_null_keys(df, batch_key_list)
    
    df_batch = df.select(*batch_key_list).dropDuplicates(batch_key_list)

    join_condition = " and ".join([f"target.`{k}` = updates.`{k}`" for k in batch_key_list])

    if partition_by_list and partition_update:
        partition_filter = get_partition_filter(df, partition_by_list)
        if partition_filter:
            join_condition = f"({join_condition} and {partition_filter})"

    DeltaTable.forPath(spark, target_path).alias("target") \
        .merge(df_batch.alias("updates"), join_condition) \
        .whenMatchedDelete() \
        .execute()

    write_append(df, target_path, partition_by_list)

def write_pk_merge(df, target_path, primary_key_list, partition_by_list = None, partition_update = False):
    df = drop_null_keys(df, primary_key_list)

    pk_condition = " and ".join([f"target.`{k}` = updates.`{k}`" for k in primary_key_list])
    join_condition = pk_condition

    if partition_by_list and partition_update:
        partition_filter = get_partition_filter(df, partition_by_list)
        if partition_filter:
            join_condition = f"({pk_condition} and {partition_filter})"

    DeltaTable.forPath(spark, target_path) \
        .alias("target") \
        .merge(df.alias("updates"), join_condition) \
        .whenMatchedUpdateAll() \
        .whenNotMatchedInsertAll() \
        .execute()

def get_partition_filter(df, partition_by_list, *, max_partitions = 1024, max_predicate_chars = 32768):
    if not partition_by_list:
        raise ValueError("partition_by_list must be a non-empty list of columns.")

    partition_df = df.select(*partition_by_list).distinct().limit(max_partitions + 1).collect()

    if not partition_df:
        return None

    if len(partition_df) > max_partitions:
        raise ValueError(f"Too many distinct partitions (> {max_partitions}). Check partition_by_list/metadata.")

    partition_disjunctions = []

    for partition_row in partition_df:
        predicates = []

        for partition_column in partition_by_list:
            partition_value = partition_row[partition_column]

            if partition_value is None:
                predicates.append(f"target.`{partition_column}` is null")
            else:
                escaped_value = str(partition_value).replace("'", "''")
                predicates.append(f"target.`{partition_column}` = '{escaped_value}'")

        if len(predicates) == 1:
            partition_disjunctions.append(predicates[0])
        else:
            partition_disjunctions.append("(" + " and ".join(predicates) + ")")

    result = "(" + " or ".join(partition_disjunctions) + ")"
    
    if len(result) > max_predicate_chars:
        raise ValueError('Predicate is too large for a partition merge.')

    return result 

def write_to_silver(df, target_path, schema_json, write_method, partition_update):
    partition_by_list = get_partition_by_list(schema_json)

    if write_method == 'overwrite':
        write_overwrite(df, target_path, partition_by_list)
    elif write_method == 'bk_merge':
        batch_key_list = get_batch_key_list(schema_json)
        if batch_key_list:
            write_rebuild_by_batch_key(df, target_path, batch_key_list, partition_by_list, partition_update)
        else:
            raise ValueError('Batch key list is empty.')
    elif write_method == 'pk_merge':
        primary_key_list = get_primary_key_list(schema_json)
        if primary_key_list:
            write_pk_merge(df, target_path, primary_key_list, partition_by_list, partition_update)
        else:
            raise ValueError('Primary key list is empty.')
    elif write_method == 'append':
        write_append(df, target_path, partition_by_list)
    else:
        raise ValueError('Write method not recognized')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

write_to_silver(df, target_path, schema_json, write_method, partition_update)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_max_extract_partition(df, partition_column):
    row = df.selectExpr(f"max(`{partition_column}`) as max_value").first()
    result =  row["max_value"] if row else None
    
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(get_max_extract_partition(df_source, extract_partition))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
