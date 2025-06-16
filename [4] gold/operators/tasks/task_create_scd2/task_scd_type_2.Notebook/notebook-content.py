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

target_path = 'deltalake:fabric_showcase/gold_lakehouse/tables/dbo/DimBerry'
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

catalog_path = get_deltalake_path('api', target_path)
full_refresh = full_refresh.strip().lower() == 'true'
if not spark.catalog.tableExists(catalog_path):
    full_refresh = True

row_hash_col = 'RowHash'
valid_from_col = 'ValidFrom'
valid_to_col = 'ValidTo'
is_current_col = 'IsCurrent'
scd_key_col = 'PrimaryKey'

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

def get_date_keys(column_map):
    result = [column['column_name'] for column in column_map if column.get('is_date_key') == 1][0]
    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_silver_scd2(logical_path, partition_by = None, min_partition = None):
    source_path = get_deltalake_path('abfss', logical_path)
    df = spark.read.format('delta').load(source_path)

    if partition_by and min_partition:
        df = df.filter(col(partition_by) >= min_partition)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def read_gold_scd2(logical_path):
    catalog_path = get_deltalake_path('api', logical_path)
    abfs_path = get_deltalake_path('abfs', logical_path)
    if spark.catalog.tableExists(catalog_path):
        df = spark.read.format('delta').load(abfs_path)
    else:
        df = None

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_max_primary_key(df):
    try:
        result = df.select(F.max(scd_key_col).alias("max_pk")).collect()[0]["max_pk"]
        return int(result) if result is not None else 0
    except Exception:
        return 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def format_silver_scd2(df, primary_keys, business_keys, max_primary_key):
    df = df.withColumn(row_hash_col, F.sha2(F.concat_ws("||", *[F.col(c).cast("string") for c in business_keys]), 256))

    window = Window.orderBy(*primary_keys)
    starting_id = max_primary_key + 1
    df = df.withColumn(scd_key_col, F.row_number().over(window) + starting_id - 1)

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def union_scd2(df_source, df_target, primary_keys, date_key, max_primary_key):
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
    starting_id = max_primary_key + 1
    df = df.withColumn(scd_key_col, F.row_number().over(window) + starting_id - 1)

    df = df.drop('prev_row_hash', 'hash_change', 'group_id')

    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_scd2(df, logical_path):
    relative_path = get_lakehouse_path('relative', logical_path)
    df.write \
        .option('overwriteSchema', 'true') \
        .mode('overwrite') \
        .format('delta') \
        .save(relative_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_source = read_silver_scd2(source_path)
df_target = read_gold_scd2(target_path)
jcolumn_map = get_json_map(schema)
max_primary_key = get_max_primary_key(df_target)
df_source2 = format_silver_scd2(df_source, get_primary_keys(jcolumn_map), get_business_keys(jcolumn_map), max_primary_key)
df_source3 = union_scd2(df_source2, df_target, get_primary_keys(jcolumn_map), get_date_keys(jcolumn_map))
write_scd2(df_source3, target_path)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
