# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import json
import pyspark.sql.functions as sql_functions
from pyspark.sql.window import Window
from delta.tables import DeltaTable

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

target_path = ''
source_path = ''
schema = """
    [
        {"column_name": "Id", "is_business_key": 1, "is_order_by": 0, "is_surrogate_key": 0},
        {"column_name": "Partition", "is_business_key": 0, "is_order_by": 1, "is_surrogate_key": 0},
        {"column_name": "BerrySk", "is_business_key": 0, "is_order_by": 0, "is_surrogate_key": 1}
    ]
    """
full_refresh = 'false'
min_partition = None
partition_column = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_path = get_internal_path('abfss', target_path)
source_path = get_internal_path('abfss', source_path)

schema_json = json.loads(schema)

full_refresh = full_refresh.strip().lower() == 'true'
if not DeltaTable.isDeltaTable(spark, target_path):
    full_refresh = True

if full_refresh:
    min_partition = None
    partition_column = None


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_business_key_list(schema_json):
    result = [column['column_name'] for column in schema_json if column.get('is_business_key') == 1]
    if result is None:
        raise ValueError('business_key_list must not be empty.')
        
    return result

def get_order_by_list(schema_json):
    filtered = [column for column in schema_json if column["is_order_by"] > 0]
    
    if not filtered:
        return None

    ordered = sorted(filtered, key = lambda column: column["is_order_by"])
    result = [column["column_name"] for column in ordered]

    return result

def get_sk_column(schema_json):
    sk_columns = [column['column_name'] for column in schema_json if column.get('is_surrogate_key') == 1]
    if len(sk_columns) != 1:
        raise ValueError(f'Exactly one surrogate key is required. Total surrogate keys provied is {len(sk_columns)}.')
    else:
        result = sk_columns[0]

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

business_key_list = get_business_key_list(schema_json)
order_by_list = get_order_by_list(schema_json)
sk_column = get_sk_column(schema_json)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def clean_df_source(source_path, business_key_list, order_by_list = None, partition_column = None, min_partition = None):
    df_source = spark.read.format('delta').load(source_path)
    
    if partition_column and min_partition:
        df_source = df_source.filter(sql_functions.col(partition_column) >= min_partition)

    df_source = df_source.dropna(subset = business_key_list)

    if order_by_list:
        window_spec = (
            Window
            .partitionBy(*[sql_functions.col(column) for column in business_key_list])
            .orderBy(*[sql_functions.col(column).desc_nulls_last() for column in order_by_list])
        )
        df_source = (
            df_source
            .withColumn("_row_number", sql_functions.row_number().over(window_spec))
            .filter(sql_functions.col("_row_number") == 1)
            .drop("_row_number")
        )
    else:
        df_source = df_source.dropDuplicates(business_key_list)

    return df_source

def clean_df_target(target_path, full_refresh):
    if not full_refresh:
        df_target = spark.read.format('delta').load(target_path)
    else:
        df_target = None

    return df_target

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source = clean_df_source(source_path, business_key_list, order_by_list)
df_target = clean_df_target(target_path, full_refresh)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_new_records(df_source, df_target, business_key_list, sk_column):
    df_updates = df_source.join(df_target, on = business_key_list, how = 'left_anti')

    max_sk_value = (
        df_target.agg(sql_functions.max(sql_functions.col(sk_column)).alias("max_sk"))
                .first()["max_sk"]
    )
    max_sk_value = 0 if max_sk_value is None else int(max_sk_value)
 
    window_spec = Window.orderBy(*[sql_functions.col(column).asc_nulls_last() for column in business_key_list])
    df_updates = df_updates.withColumn(sk_column, sql_functions.row_number().over(window_spec) + sql_functions.lit(max_sk_value))

    return df_updates

def get_existing_records(df_source, df_target, business_key_list, sk_column):
    df_existing = df_source.join(df_target, on = business_key_list, how = 'left_semi')
    df_existing = df_existing.withColumn(sk_column, sql_functions.lit(None))

    return df_existing

def update_scd1(df_source, df_target, target_path, business_key_list, sk_column):
    df_new = get_new_records(df_source, df_target, business_key_list, sk_column)
    df_existing = get_existing_records(df_source, df_target, business_key_list, sk_column)

    df = df_existing.unionByName(df_new)
    join_condition = " and ".join([f"target.`{column}` = updates.`{column}`" for column in business_key_list])

    update_cols = [column for column in df_source.columns if column != sk_column]
    set_expr = {column: f"updates.`{column}`" for column in update_cols}

    (
        DeltaTable.forPath(spark, target_path)
        .alias("target")
        .merge(df.alias("updates"), join_condition)
        .whenMatchedUpdate(set = set_expr)
        .whenNotMatchedInsertAll()
        .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_full_refresh_df(df, business_key_list, sk_column, order_by_list = None):
    df0 = df.dropna(subset = business_key_list)

    if not order_by_list:
        # Assert uniqueness: if more than 1 row per key, we refuse to guess
        has_dups = (
            df0.groupBy(*business_key_list)
               .count()
               .filter(sql_functions.col("count") > 1)
               .limit(1)
               .count() > 0
        )
        if has_dups:
            raise ValueError(
                "Found duplicate rows per business key but no order_by_list was provided. "
                "Provide an ordering column (e.g., updated_at/extract_tstamp) or a deterministic tie-breaker."
            )
        else:
            df_dim = df0
    else:
        # Pick the “latest” row per business key deterministically
        w_pick = (
            Window.partitionBy(*business_key_list)
                  .orderBy(*[sql_functions.col(c).desc_nulls_last() for c in order_by_list])
        )
        df_dim = (
            df0.withColumn("_rn", sql_functions.row_number().over(w_pick))
               .filter(sql_functions.col("_rn") == 1)
               .drop("_rn")
        )

    # Deterministic SK assignment for this rebuild
    w_sk = Window.orderBy(*[sql_functions.col(c).asc_nulls_last() for c in business_key_list])
    df_dim = df_dim.withColumn(sk_column, sql_functions.row_number().over(w_sk))

    return df_dim

def rebuild_scd1(df_source, target_path, business_key_list, sk_column, order_by_list):
    df = get_full_refresh_df(df_source, business_key_list, sk_column, order_by_list)
    df.write.format('delta').mode('overwrite').option('overwriteSchema', 'true').save(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if full_refresh:
    rebuild_scd1(df_source, target_path, business_key_list, sk_column, order_by_list)
else:
    update_scd1(df_source, df_target, target_path, business_key_list, sk_column)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_max_partition(df, partition_column):
    if partition_column:
        max_partition = df.agg(sql_functions.max(sql_functions.col(partition_column)).alias('max_partition')).first()['max_partition']
    else:
        max_partition = None

    return max_partition

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.exitValue(get_max_partition(df_source, partition_column))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
