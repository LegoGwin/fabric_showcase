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

# CELL ********************

target_path = ''
source_path = ''
business_keys = """
    [
        {"business_key": "Id"}
    ]
    """
deletes_possible = 'false'
updates_possible = 'false'
source_has_history = 'false'
order_by_columns = None
sk_column = 'berry_sk'
full_refresh = 'false'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_path = get_internal_path('abfss', target_path)
source_path = get_internal_path('abfss', source_path)

full_refresh = full_refresh.strip().lower() == 'true'
if not DeltaTable.isDeltaTable(spark, target_path):
    full_refresh = True

business_key_json = json.loads(business_keys)
business_key_list = [key["business_key"] for key in business_key_json]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source = spark.read.format('delta').load(source_path)
df_target = spark.read.format('delta').load(target_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def insert_new_keys(df_source, df_target, business_keys_list):
    max_df_sk = df_target.agg(sql_functions.max(surrogate_key).alias('max_sk').first()['max_sk'])

    new_key_df = df_source.join(df_target, on = business_keys_list, how = "left_anti")

def update_old_keys():
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


def rebuild_dimension_scd1(df, business_key_list, sk_column, *, order_by_list = None, sk_start=1):
    if not business_key_list:
        raise ValueError("business_key_list must be non-empty.")

    df0 = df.dropna(subset=business_key_list)

    if not order_by_list:
        # Assert uniqueness: if more than 1 row per key, we refuse to guess
        has_dups = (
            df0.groupBy(*business_key_list)
               .count()
               .filter(F.col("count") > 1)
               .limit(1)
               .count() > 0
        )
        if has_dups:
            raise ValueError(
                "Found duplicate rows per business key but no order_by_list was provided. "
                "Provide an ordering column (e.g., updated_at/extract_tstamp) or a deterministic tie-breaker."
            )
    else:
        # Pick the “latest” row per business key deterministically
        w_pick = (
            Window.partitionBy(*business_key_list)
                  .orderBy(*[F.col(c).desc_nulls_last() for c in order_by_list])
        )
        df_dim = (
            df0.withColumn("_rn", F.row_number().over(w_pick))
               .filter(F.col("_rn") == 1)
               .drop("_rn")
        )

    # Deterministic SK assignment for this rebuild
    w_sk = Window.orderBy(*[F.col(c).asc_nulls_last() for c in business_key_list])
    df_dim = df_dim.withColumn(sk_column, F.row_number().over(w_sk) + (F.lit(int(sk_start)) - 1))

    return df_dim


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
