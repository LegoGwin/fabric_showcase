# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "31d88d62-8db6-461e-b773-aa3396d3db9d",
# META       "default_lakehouse_name": "gold_lakehouse",
# META       "default_lakehouse_workspace_id": "1d10f168-5ee0-487f-bfb4-4bc7e9fdb6ab",
# META       "known_lakehouses": [
# META         {
# META           "id": "31d88d62-8db6-461e-b773-aa3396d3db9d"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import json
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
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

target_path = 'deltalake:fabric_showcase/gold_lakehouse/tables/dbo/DimBerry2'
source_path = 'deltalake:fabric_showcase/silver_lakehouse/tables/pokemon/berry_history'
full_refresh = 'false'
schema = """
        [
            {
                "column_name": "Id",
                "is_primary_key": 1,
                "is_business_key": 0,
                "is_date_key": 0
            },
            {
                "column_name": "Name",
                "is_primary_key": 0,
                "is_business_key": 1,
                "is_date_key": 0
            },
            {
                "column_name": "Partition",
                "is_primary_key": 0,
                "is_business_key": 1,
                "is_date_key": 0
            },
            {
                "column_name": "ExtractDate",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 1
            }
        ]
    """
min_partition = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

full_refresh = full_refresh.strip().lower() == 'true'

catalog_path = get_deltalake_path('api', target_path)
if not spark.catalog.tableExists(catalog_path):
    full_refresh = True
    
if full_refresh:
    min_partition = None

row_hash_col = 'RowHash'
valid_from_col = 'ValidFrom'
valid_to_col = 'ValidTo'
is_current_col = 'IsCurrent'
surrogate_key_col = 'SurrogateKey'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_json_map(column_map):
    result = json.loads(column_map)
    return result

def get_primary_keys(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_primary_key') == 1]
    return result

def get_business_keys(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_business_key') == 1]
    return result

def get_date_key(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_date_key') == 1][0]
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_silver_scd2(logical_path, partition_by = None, min_partition = None):
    abfs_path = get_internal_path('abfs', logical_path)
    df = spark.read.format('delta').load(abfs_path)

    if partition_by and min_partition:
        df = df.filter(col(partition_by) >= min_partition)

    return df

def read_gold_scd2(logical_path, return_type = 'dataframe'):
    abfs_path = get_internal_path('abfs', logical_path)

    if return_type == 'dataframe':
        result = spark.read.format('delta').load(abfs_path)
    elif return_type == 'delta':
        result = DeltaTable.forPath(spark, abfs_path)
    else:
        result = None

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def incremental_scd2(source_path, target_path, schema, min_partition):
    schema = get_json_map(schema)
    primary_keys = get_primary_keys(schema)
    business_keys = get_business_keys(schema)
    date_key = get_date_key(schema)

    df_source = read_silver_scd2(source_path, date_key, min_partition)
    df_target = read_gold_scd2(target_path, 'dataframe')
    delta_target = read_gold_scd2(target_path, 'delta')

    source_path = get_internal_path('abfs', source_path)
    target_path = get_internal_path('abfs', target_path)

    df_updates = prepare_updates(df_source, df_target, primary_keys, business_keys, date_key, valid_from_col, valid_to_col, is_current_col, row_hash_col, surrogate_key_col)
    insert_new(df_updates, df_target, target_path, primary_keys)
    expire_updated(delta_target, df_updates, primary_keys, is_current_col, row_hash_col, valid_to_col, valid_from_col)
    insert_updated(delta_target, df_updates, primary_keys, is_current_col, row_hash_col, target_path)
    expire_deleted(delta_target, df_updates, primary_keys, date_key, is_current_col, valid_to_col)

def prepare_updates(df_source, df_target, primary_keys, business_keys, date_key, valid_from_col, valid_to_col, is_current_col, row_hash_col, surrogate_key_col):
    max_date_key = df_source.select(F.max(F.col(date_key)).alias(date_key)).collect()[0][date_key]
    df_updates = df_source.filter(F.col(date_key) == F.lit(max_date_key))

    df_updates = df_updates.withColumn(valid_from_col, F.lit(max_date_key))
    df_updates = df_updates.withColumn(valid_to_col, F.lit("9999-12-31").cast("date"))
    df_updates = df_updates.withColumn(is_current_col, F.lit(True))

    df_updates = df_updates.withColumn(row_hash_col, F.sha2(F.concat_ws('||', *[F.col(c).cast('string') for c in business_keys]), 256))

    max_surrogate_key = df_target.agg(F.max(F.col(surrogate_key_col))).collect()[0][0] or 0
    window = Window.orderBy(*primary_keys)
    df_updates = df_updates.withColumn(surrogate_key_col, F.row_number().over(window) + max_surrogate_key)

    return df_updates

def insert_new(df_updates, df_target, target_path, primary_keys):
    current_keys_df = df_target.filter(F.col(is_current_col) == True).select(*primary_keys).distinct()

    new_rows = df_updates.join(current_keys_df, on = primary_keys, how = "left_anti")
    if not new_rows.isEmpty():
        new_rows.write.format("delta").mode("append").save(target_path)

def expire_updated(delta_target, df_updates, primary_keys, is_current_col, row_hash_col, valid_to_col, valid_from_col):
    join_cond = " AND ".join([f"updates.{k} = target.{k}" for k in primary_keys])
    delta_target.alias("target").merge(
        source=df_updates.alias("updates"),
        condition=join_cond,
    ).whenMatchedUpdate(
        condition=f"target.{is_current_col} = true AND target.{row_hash_col} != updates.{row_hash_col}",
        set={
            valid_to_col: f"updates.{valid_from_col}",
            is_current_col: "false"
        }
    ).execute()

def insert_updated(delta_target, df_updates, primary_keys, is_current_col, row_hash_col, target_table_path):
    changed_rows = df_updates.alias("updates").join(
        delta_target.toDF().alias("target"),
        on=primary_keys,
        how="inner"
    ).filter(
        (F.col(f"target.{is_current_col}") == True) &
        (F.col(f"target.{row_hash_col}") != F.col(f"updates.{row_hash_col}"))
    )

    if changed_rows.isEmpty():
        print("No changed rows to insert.")
        return

    changed_rows.select(df_updates.columns).write.format("delta").mode("append").save(target_table_path)

def expire_deleted(delta_target, df_updates, primary_keys, date_key, is_current_col, valid_to_col):
    """
    Expires target records (sets is_current=false and validto=max_date) when keys in active rows
    are missing from df_updates (i.e., deletions).
    Uses DeltaTable.update() instead of appending new records.
    """
    max_date = df_updates.select(F.max(F.col(date_key)).alias(date_key)).collect()[0][date_key]
    # Prepare condition: active rows in target whose keys are NOT in current updates
    current_keys_df = df_updates.select(*primary_keys).distinct()
    target_df = delta_target.toDF().filter(F.col(is_current_col) == True)

    # Identify keys to expire using anti-join
    keys_to_expire_df = target_df.join(
        current_keys_df,
        on=primary_keys,
        how="left_anti"
    ).select(*primary_keys)

    if keys_to_expire_df.isEmpty():
        print("No deleted records to expire.")
        return

    # Build condition to match keys to expire
    expire_cond = " AND ".join([
        f"target.{k} = expire_keys.{k}" for k in primary_keys
    ]) + f" AND target.{is_current_col} = true"

    # Create temporary view for join in update
    keys_to_expire_df.createOrReplaceTempView("expire_keys")

    # Perform update on delta table to expire rows
    delta_target.alias("target").merge(
        source=spark.table("expire_keys").alias("expire_keys"),
        condition=expire_cond
    ).whenMatchedUpdate(
        set={
            valid_to_col: f"CAST('{max_date}' AS DATE)",  # or appropriate type cast
            is_current_col: "false"
        }
    ).execute()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def full_refresh_scd2(source_path, target_path, schema):
    schema = get_json_map(schema)
    primary_keys = get_primary_keys(schema)
    business_keys = get_business_keys(schema)
    date_key = get_date_key(schema)

    df_source = read_silver_scd2(source_path)
    target_path = get_internal_path('abfs', target_path)

    df_updates = prepare_full_refresh(df_source, primary_keys, business_keys, date_key)
    overwrite_target(df_updates, target_path)

def prepare_full_refresh(df, primary_keys, business_keys, date_key):
    df = df.withColumn(row_hash_col, F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_keys]), 256))

    # Step 2: Assign group based on lead(row_hash) != current
    w_ordered = Window.partitionBy(*primary_keys).orderBy(date_key)
    df = df.withColumn("prev_row_hash", F.lag(row_hash_col).over(w_ordered))
    df = df.withColumn("hash_change", F.when(F.col(row_hash_col) != F.col("prev_row_hash"), 1).otherwise(0))

    # Step 3: Cumulative sum of hash_change *in reverse order*
    # So we can assign the same group_id to contiguous rows with same hash
    df = df.withColumn("group_id", F.sum("hash_change").over(w_ordered.rowsBetween(Window.unboundedPreceding, 0)))

    # Step 4: Collapse periods
    group_window = Window.partitionBy(*primary_keys, "group_id")
    df = df.withColumn(valid_from_col, F.min(date_key).over(group_window))

    df = df.dropDuplicates(primary_keys + [valid_from_col, row_hash_col])
    group_window2 = Window.partitionBy(*primary_keys,).orderBy(valid_from_col)
    df = df.withColumn(valid_to_col, F.lead(valid_from_col).over(group_window2))

    df = df.withColumn(is_current_col, F.when(F.col(valid_to_col).isNull(), F.lit(True)).otherwise(F.lit(False)))

    window = Window.orderBy(*primary_keys)
    df = df.withColumn(surrogate_key_col, F.row_number().over(window))

    df = df.drop('prev_row_hash', 'hash_change', 'group_id')

    return df

def overwrite_target(df_updates, target_path):
    df_updates.write \
        .option('overwriteSchema', 'true') \
        .mode('overwrite') \
        .format('delta') \
        .save(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

if full_refresh:
    full_refresh_scd2(source_path, target_path, schema)
else:
    incremental_scd2(source_path, target_path, schema, min_partition)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
