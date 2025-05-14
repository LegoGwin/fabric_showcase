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

def get_meta_data(query):

    jdbc_hostname = "64k76qoso54upbqqkmfdnabrny-ndyrahpalz7urp5ujpd6t7nwvm.database.fabric.microsoft.com"
    jdbc_port     = 1433
    jdbc_database = "meta_data-d6cac2ad-6f10-4d76-9d78-d8159d2f8001"

    jdbc_url = (
        f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};"
        f"database={jdbc_database};"
        "encrypt=true;trustServerCertificate=false;"
    )
    subquery = "(select * from dbo.test) as test"


    df = (
        spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", query)               # ← your query here
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
            .option("authentication", "ActiveDirectoryServicePrincipal")
            .option("user", client_id)
            .option("password", client_secret)
            .option("tenantId", tenant_id)
            .load()
    )
    return df

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

result = get_meta_data('(select * from mas.dependencies) as a')
display(result).


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
