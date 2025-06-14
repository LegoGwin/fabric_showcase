
# Microsoft Fabric Medallion Architecture Library

This repository provides a generalized solution for handling common data transformation and ingestion tasks in a **Microsoft Fabric medallion architecture**.

---

## 🏗️ Architecture Overview

The solution is structured around three Lakehouses—**bronze**, **silver**, and **gold**—corresponding to the standard medallion pattern.

From an operational perspective, the architecture is metadata-driven. Each pipeline retrieves its configuration from the `meta_data` SQL database. Currently, the metadata is sourced from an Excel file and refreshed prior to pipeline execution. Orchestration is handled by the `control_jobs` pipeline, which accepts a job ID as a parameter and executes the required sequence of tasks. Execution order is governed by the `dataset_lineage` metadata, which defines table dependencies and allows task batches to run in the correct order.

---

### 📊 Sample: `dataset_lineage` Metadata

This table defines dependencies between datasets to ensure proper task execution ordering:


| dataset_path                                              		| parent_path                                                 		|
|-----------------------------------------------------------------------|-----------------------------------------------------------------------|
| lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry 	|                                                             		|
| lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/pokemon 	|                                                          		|
| deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/berry 	| lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry 	|


---

### 📊 Sample: `control_tasks` Metadata

This table defines the characteristics of each task and the dataset it targets:


| task_id             | dataset_path                                                  	| is_active | stage_name | lineage_name |
|---------------------|-----------------------------------------------------------------|-----------|------------|---------------|
| task_pokemon_api:1  | lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry  | 1         | raw        | berry         |
| task_pokemon_api:2  | lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/pokemon| 1         | bronze     | berry         |
| task_json_to_delta:1| deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/berry	| 1         | raw        | pokemon       |


---

### 📊 Sample: `control_jobs` Metadata

This table defines how jobs query task definitions dynamically using SQL:


| job_id | job_name     | task_sql                                                               |
|--------|--------------|------------------------------------------------------------------------|
| 1      | active_tasks | select [task_id] from [dbo].[control_tasks] where [is_active] = 1      |
| 2      | raw_tasks    | select [task_id] from [dbo].[control_tasks] where [stage_name] = 'raw' |


---

## 🪙 Bronze Layer

The **files section** of the bronze Lakehouse contains raw data ingested in its original source format. Even JDBC sources are staged in the files area to avoid naming and compatibility issues. These datasets are append-only and partitioned by the extraction timestamp in `yyyyMMddHHmmss` format.

The **tables section** of the bronze layer contains flattened versions of the data, where column names are cleaned and all values are typed as strings.

For demo purposes, the solution includes a dummy connector to the Pokémon API that extracts JSON data and lands it in the bronze files section for testing.

Two general-purpose pipelines are provided in the bronze layer:

- `json_to_delta`
- `parquet_to_delta`

These pipelines reliably and securely transfer data from the files section to the tables section.

---

## 🥈 Silver Layer

The silver layer begins full-scale data cleaning and transformation. Only the **tables section** is used in this layer.

The process is managed by a single pipeline: `task_update_silver`. This pipeline supports a wide range of transformations, including:

- Deduplication using primary keys or batch keys
- Column selection and type casting
- Expression-based column creation
- Row-level filtering
- Insert strategies including append, overwrite, primary key merge, and batch merge

At this stage, the data is intended to closely reflect the source system’s structure, with potential upgrades for single-row enhancements.

### 📊 Sample: `task_update_silver_schema` Metadata

This table defines the transformation logic for the `task_update_silver` pipeline:


| task_id             | expression | column_type | column_name | column_order | is_filter | is_primary_key | is_batch_key | is_order_by | is_output | is_partition_by |
|---------------------|------------|-------------|-------------|--------------|-----------|----------------|--------------|-------------|-----------|------------------|
| task_update_silver:1| id         | int         | Id          | 1            | 0         | 1              | 0            | 0           | 1         | 0                |
| task_update_silver:1| name       | string      | Name        | 2            | 0         | 0              | 0            | 0           | 1         | 0                |


---

## 🥇 Gold Layer

The gold layer is implemented with a lakehouse named gold_lakehouse. The gold layer is meant to be the source of data used for reporting via tools such as Power BI. The data here is denormalized and includes constructions like slowly changing dimensions, snapshot fact tables, running total fact tables, aggregegations, and so on. 

Currently, there is a standard pipeline for slowly changing dimensions type 2 that leverages the meta-data shown below.

### 📊 Sample: `task_create_scd2_schema` Metadata
| task_id           | column_name | is_primary_key | is_business_key | is_date_key |
|------------------|-------------|----------------|------------------|-------------|
| task_create_scd2:1 | Id          | 1              | 0                | 0           |
| task_create_scd2:1 | Name        | 0              | 1                | 0           |
| task_create_scd2:1 | ExtractDate | 0              | 0                | 1           |

There is also a basic passthrough pipeline to transfer data unmodified from silver to gold.

Work is in progress to implement other standard gold-layer patterns.

---

## 🚀 Usage Overview

This solution enables metadata-driven orchestration of pipelines within a Microsoft Fabric medallion architecture. The system is modular, extensible, and supports dependency-aware scheduling across multiple pipelines.

**To use this framework:**

1. **Configure task-level metadata**  
   Each task pipeline (e.g., bronze ingestion, SCD2 processing) has its **own dedicated metadata table** (e.g., `task_json_to_delta`, `task_update_silver_schema`).  
   You must configure these tables with the information about the task to perform.

2. **Configure scheduling metadata**  
   Once individual task metadata is in place, define scheduling rules:
   - `control_tasks` — maps task metadata tables to runnable task IDs
   - `control_jobs` — maps job IDs to lists of tasks using SQL expressions
   - `dataset_lineage` — defines dependency chains between datasets to enforce proper batch execution

3. **Run the `control_jobs` pipeline**  
   Execute the orchestration by passing a job ID. This will:
   - Query `control_jobs` to find relevant task IDs
   - Order tasks based on dependency levels (`dataset_lineage`)
   - Dynamically route each task to its respective pipeline logic (e.g., notebook, SQL, operator)

4. **Review output**  
   - `bronze` stores raw staged data (often file-based)
   - `silver` holds cleaned, enriched, and conformed data (e.g., pre-SCD2 outputs)
   - `gold` contains final analytical tables like facts and dimensions
"""
