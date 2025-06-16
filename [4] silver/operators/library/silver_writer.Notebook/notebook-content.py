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

%run silver_transformer

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_append(df, logical_path, partition_by_list = None):
    writer = df.write \
        .mode('append') \
        .option('mergeSchema', 'false') \
        .format('delta')

    if partition_by_list:
        writer = writer.partitionBy(partition_by_list)

    save_path = get_deltalake_path('relative', logical_path)
    writer.save(save_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def write_bk_merge(df, logical_path, batch_key_list, partition_by_list = None):
    df_batch = df.select(*batch_key_list).dropDuplicates(batch_key_list)
    join_condition = " and ".join([f"target.{batch_key} = updates.{batch_key}" for batch_key in batch_key_list])

    delta_path = get_deltalake_path('relative', logical_path)
    DeltaTable.forPath(spark, delta_path).alias("target") \
        .merge(df_batch.alias("updates"), join_condition) \
        .whenMatchedDelete() \
        .execute()

    write_append(df, logical_path, partition_by_list)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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
