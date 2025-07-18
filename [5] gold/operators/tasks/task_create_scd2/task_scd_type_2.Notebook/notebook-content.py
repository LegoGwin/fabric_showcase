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
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
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

target_path = 'deltalake:fabric_showcase/gold_lakehouse/tables/dbo/DimBerry4'
source_path = 'deltalake:fabric_showcase/silver_lakehouse/tables/pokemon/berry_history'
full_refresh = 'false'

schema = """
        [
            {
                "column_name": "Id",
                "is_primary_key": 1,
                "is_business_key": 0,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "Name",
                "is_primary_key": 0,
                "is_business_key": 1,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "Partition",
                "is_primary_key": 0,
                "is_business_key": 1,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "ExtractDate",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 1,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "ValidFrom",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 0,
                "is_valid_from": 1,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "ValidTo",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 1,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "IsCurrent",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 1,
                "is_row_hash": 0,
                "is_surrogate_key": 0
            },
            {
                "column_name": "RowHash",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 1,
                "is_surrogate_key": 0
            },
            {
                "column_name": "SurrogateKey",
                "is_primary_key": 0,
                "is_business_key": 0,
                "is_date_key": 0,
                "is_valid_from": 0,
                "is_valid_to": 0,
                "is_is_current": 0,
                "is_row_hash": 0,
                "is_surrogate_key": 1
            }
        ]
    """

min_date_key = None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_json_schema(schema):
    result = json.loads(schema)
    return result

def get_primary_keys(schema):
    result = [column['column_name'] for column in schema if column.get('is_primary_key') == 1]
    return result

def get_business_keys(schema):
    result = [column['column_name'] for column in schema if column.get('is_business_key') == 1]
    return result

def get_date_key(schema):
    result = [column['column_name'] for column in schema if column.get('is_date_key') == 1][0]
    return result

def get_valid_from(schema):
    result = [column['column_name'] for column in schema if column.get('is_valid_from') == 1][0]
    return result

def get_valid_to(schema):
    result = [column['column_name'] for column in schema if column.get('is_valid_to') == 1][0]
    return result

def get_is_current(schema):
    result = [column['column_name'] for column in schema if column.get('is_is_current') == 1][0]
    return result

def get_row_hash(schema):
    result = [column['column_name'] for column in schema if column.get('is_row_hash') == 1][0]
    return result

def get_surrogate_key(schema):
    result = [column['column_name'] for column in schema if column.get('is_surrogate_key') == 1][0]
    return result

def get_schema_fields(schema):
    schema = get_json_schema(schema)
    result = (
        get_primary_keys(schema),
        get_business_keys(schema),
        get_date_key(schema),
        get_valid_from(schema),
        get_valid_to(schema),
        get_is_current(schema),
        get_row_hash(schema),
        get_surrogate_key(schema)
    )
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_silver_scd2(logical_path, date_key = None, min_date_key = None):
    abfs_path = get_internal_path('abfs', logical_path)
    df = spark.read.format('delta').load(abfs_path)

    if date_key and min_date_key:
        df = df.filter(col(date_key) >= min_date_key)

    return df

def read_gold_scd2(logical_path):
    abfs_path = get_internal_path('abfs', logical_path)
    delta = DeltaTable.forPath(spark, abfs_path)

    return delta

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def incremental_scd2(source_path, target_path, schema, min_date_key):
    primary_keys, business_keys, date_key, valid_from, valid_to, is_current, row_hash, surrogate_key = get_schema_fields(schema)

    df_source = read_silver_scd2(source_path, date_key, min_date_key)
    source_path = get_internal_path('abfs', source_path)

    delta_target = read_gold_scd2(target_path)
    df_target = delta_target.toDF()
    target_path = get_internal_path('abfs', target_path)

    df_updates = inc_prepare_updates(df_source, delta_target, primary_keys, business_keys, date_key, valid_from, valid_to, is_current, row_hash, surrogate_key)

    insert_new(df_updates, delta_target, primary_keys, is_current)
    insert_updated(df_updates, df_target, target_path, primary_keys, is_current, row_hash)
    expire_updated(df_updates, delta_target, primary_keys, valid_to, valid_from, is_current, row_hash)
    expire_deleted(df_updates, delta_target, primary_keys, date_key, is_current, valid_to)

    return df_updates.select(F.max(F.col(date_key)).alias(date_key)).collect()[0][date_key]
    
def inc_prepare_updates(df_source, delta_target, primary_keys, business_keys, date_key, valid_from_col, valid_to_col, is_current_col, row_hash_col, surrogate_key_col):
    max_date_key = df_source.select(F.max(F.col(date_key)).alias(date_key)).collect()[0][date_key]
    df_updates = df_source.filter(F.col(date_key) == F.lit(max_date_key))

    df_updates = df_updates.withColumn(valid_from_col, F.lit(max_date_key))
    df_updates = df_updates.withColumn(valid_to_col, F.lit("9999-12-31").cast("date"))
    df_updates = df_updates.withColumn(is_current_col, F.lit(True))

    df_updates = df_updates.withColumn(row_hash_col, F.sha2(F.concat_ws('||', *[F.col(c).cast('string') for c in business_keys]), 256))

    max_surrogate_key = delta_target.toDF().agg(F.max(F.col(surrogate_key_col))).collect()[0][0] or 0
    window = Window.orderBy(*primary_keys)
    df_updates = df_updates.withColumn(surrogate_key_col, F.row_number().over(window) + max_surrogate_key)

    return df_updates

def insert_new(df_updates, delta_target, primary_keys, is_current_col):
    (
        delta_target.alias("target")
            .merge(
                source = df_updates.alias("updates"),
                condition = (" and ".join([f"updates.{k} = target.{k}" for k in primary_keys])) + f" and target.{is_current_col} = true")
            .whenNotMatchedInsertAll()
            .execute()
    )

def insert_updated(df_updates, df_target, target_path, primary_keys, is_current_col, row_hash_col):
    changed_rows = (
        df_updates.alias("updates")
        .join(df_target.alias("target"), on=primary_keys, how="inner")
        .filter(f"target.{is_current_col} = true and target.{row_hash_col} != updates.{row_hash_col}")
    )

    if not changed_rows.isEmpty():
        changed_rows.select(df_updates.columns).write.format("delta").mode("append").save(target_path)

def expire_updated(df_updates, delta_target, primary_keys, valid_to_col, valid_from_col, is_current_col, row_hash_col):
    (
        delta_target.alias("target")
            .merge(
                source = df_updates.alias("updates"),
                condition = " AND ".join([f"updates.{k} = target.{k}" for k in primary_keys]))
            .whenMatchedUpdate(
                condition = f"target.{is_current_col} = true AND target.{row_hash_col} != updates.{row_hash_col}",
                set = {
                    valid_to_col: f"updates.{valid_from_col}",
                    is_current_col: "false"
                })
            .execute()
    )

def expire_deleted(df_updates, delta_target, primary_keys, date_key, is_current_col, valid_to_col):
    df_current_keys = df_updates.select(*primary_keys).distinct()
    df_target = delta_target.toDF().filter(f"{is_current_col} = true")
    df_expired = df_target.join(df_current_keys, on = primary_keys, how = "left_anti").select(*primary_keys)

    if not df_expired.isEmpty():
        max_date = df_updates.select(F.max(F.col(date_key)).alias(date_key)).collect()[0][date_key]
        (
            delta_target.alias("target")
                .merge(
                    source = df_expired.alias("expire_keys"),
                    condition = " AND ".join([f"target.{k} = expire_keys.{k}" for k in primary_keys]) + f" AND target.{is_current_col} = true"
                ).whenMatchedUpdate(
                    set={
                        valid_to_col: f"CAST('{max_date}' AS DATE)",
                        is_current_col: "false"
                    }
                ).execute()
        )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def full_refresh_scd2(source_path, target_path, schema):
    primary_keys, business_keys, date_key, valid_from, valid_to, is_current, row_hash, surrogate_key = get_schema_fields(schema)

    df_source = read_silver_scd2(source_path)
    target_path = get_internal_path('abfs', target_path)

    df_updates = fr_prepare_updates(df_source, primary_keys, business_keys, date_key, valid_from, valid_to, is_current, row_hash, surrogate_key)

    overwrite_target(df_updates, target_path)

    return df_updates.select(F.max(F.col(date_key)).alias(date_key)).collect()[0][date_key]

def fr_prepare_updates(df, primary_keys, business_keys, date_key, valid_from, valid_to, is_current, row_hash, surrogate_key):
    df = df.withColumn(row_hash, F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_keys]), 256))

    w_ordered = Window.partitionBy(*primary_keys).orderBy(date_key)
    df = df.withColumn("prev_row_hash", F.lag(row_hash).over(w_ordered))
    df = df.withColumn("hash_change", F.when(F.col(row_hash) != F.col("prev_row_hash"), 1).otherwise(0))

    df = df.withColumn("group_id", F.sum("hash_change").over(w_ordered.rowsBetween(Window.unboundedPreceding, 0)))

    group_window = Window.partitionBy(*primary_keys, "group_id")
    df = df.withColumn(valid_from, F.min(date_key).over(group_window))

    df = df.dropDuplicates(primary_keys + [valid_from, row_hash])
    group_window2 = Window.partitionBy(*primary_keys,).orderBy(valid_from)
    df = df.withColumn(
        valid_to,
        F.when(
            F.lead(valid_from).over(group_window2).isNotNull(),
            F.lead(valid_from).over(group_window2)
        ).otherwise(F.lit("9999-12-31").cast("date"))
    )
    df.withColumn(
        is_current, en(F.col(valid_to) == F.lit("9999-12-31").cast("date"), F.lit(True))
        erwise(F.lit(False))
        )

  
    ow = Window.orderBy(*primary_keys)
    df.withColumn(surrogate_key, F.row_number().over(window))

    df.drop('prev_row_hash', 'hash_change', 'group_id')

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

full_refresh = full_refresh.strip().lower() == 'true'

catalog_path = get_internal_path('api', target_path)
if not spark.catalog.tableExists(catalog_path):
    full_refresh = True

if full_refresh:
    max_date_key = full_refresh_scd2(source_path, target_path, schema)
else:
    max_date_key = incremental_scd2(source_path, target_path, schema, min_date_key)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.notebook.exit(max_date_key)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
