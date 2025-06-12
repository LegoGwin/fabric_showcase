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

import os

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def split_logical_path(logical_path):
    path_type, logical_path = logical_path.split(":", 1)
    path_type = path_type.lower()
    components = logical_path.split("/")

    if path_type == "lakefiles":
        result = {
            'path_type':    path_type,
            "workspace":    components[0],
            "lakehouse":    components[1],
            "file_system":  components[2],
            "directory":    "/".join(components[3:])
        }
    elif path_type == "deltalake":
        result = {
            'path_type':    path_type,
            "workspace":    components[0],
            "lakehouse":    components[1],
            "file_system":  components[2],
            "schema":       components[3],
            "table_name":   components[4]
        }
    elif path_type == 'warehouse':
        result = {
            'path_type':    path_type,
            'workspace':    components[0],
            'warehouse':    components[1],
            'schema':       components[2],
            'table_name':   components[3]
        }
    elif path_type == 'database':
        result = {
            path_type: 'database'
        }
    elif path_type == 'eventhouse':
        result = {
            path_type: 'eventhouse'
        }
    else:
        result = None

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_lakefiles_path(path_format, logical_path):
    split_path = split_logical_path(logical_path)
    path_format = path_format.lower()

    if path_format == 'api':
        default_root = '/lakehouse/default/Files/'
    elif path_format == 'relative':
        default_root = 'Files/'
    else:
        default_root = f"abfss://{split_path['workspace']}@onelake.dfs.fabric.microsoft.com/{split_path['lakehouse']}.Lakehouse/Files/"

    file_path = os.path.join(default_root, split_path['directory'])

    return file_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_deltalake_path(path_format, logical_path):
    split_path = split_logical_path(logical_path)
    path_format = path_format.lower()

    if path_format == 'api':
        target_path = f"{split_path['lakehouse']}.{split_path['schema']}.{split_path['table_name']}"
    elif path_format == 'catalog':
        target_path = f"{split_path['lakehouse']}.{split_path['table_name']}"
    elif path_format == 'relative':
        default_root = 'Tables/'
        target_path = os.path.join(default_root, split_path['schema'], split_path['table_name'])
    else:
        default_root = f"abfss://{split_path['workspace']}@onelake.dfs.fabric.microsoft.com/{split_path['lakehouse']}.Lakehouse/Tables/"
        target_path = os.path.join(default_root, split_path['schema'], split_path['table_name'])

    return target_path

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_lakehouse_path(path_format, logical_path):
    split_path = split_logical_path(logical_path)
    path_type = split_path['path_type']

    if path_type == 'lakefiles':
        result = get_lakefiles_path(path_format, logical_path)
    elif path_type == 'deltalake':
        result = get_deltalake_path(path_format, logical_path)

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_warehouse_path(path_format, logical_path):
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_database_path(path_format, logical_path):
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_eventhouse_path(path_format, logical_path):
    return None

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_internal_path(path_format, logical_path):
    split_path = split_logical_path(logical_path)
    path_type = split_path['path_type']

    if path_type == 'lakefiles' or path_type == 'deltalake':
        result = get_lakehouse_path(path_format, logical_path)
    elif path_type == 'warehouse':
        result = get_warehouse_path(path_format, logical_path)
    elif path_type == 'database':
        result = get_database_path(path_format, logical_path)
    elif path_type == 'eventhouse':
        result = get_eventhouse_path(path_format, logical_path)
    else:
        result = None

    return result

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
