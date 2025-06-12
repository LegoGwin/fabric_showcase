
# Microsoft Fabric Medallion Architecture Library

This repository provides a generalized solution for handling common data transformation and ingestion tasks in a **Microsoft Fabric medallion architecture**.

---

## 🏗️ Architecture Overview

The solution is structured around three Lakehouses—**bronze**, **silver**, and **gold**—corresponding to the standard medallion pattern.

From an operational perspective, the architecture is metadata-driven. Each pipeline retrieves its configuration from the `meta_data` SQL database. Currently, the metadata is sourced from an Excel file and refreshed prior to pipeline execution. Orchestration is handled by the `control_jobs` pipeline, which accepts a job ID as a parameter and executes the required sequence of tasks. Execution order is governed by the `dataset_lineage` metadata, which defines table dependencies and allows task batches to run in the correct order.

---

### 📊 Sample: `dataset_lineage` Metadata

This table defines dependencies between datasets to ensure proper task execution ordering:

```text
| dataset_path                                              		| parent_path                                                 		|
|-----------------------------------------------------------------------|-----------------------------------------------------------------------|
| lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry 	|                                                             		|
| lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/pokemon 	|                                                          		|
| deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/berry 	| lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry 	|
```

---

### 🧩 Sample: `control_tasks` Metadata

This table defines the characteristics of each task and the dataset it targets:

```text
| task_id             | dataset_path                                                  	| is_active | stage_name | lineage_name |
|---------------------|-----------------------------------------------------------------|-----------|------------|---------------|
| task_pokemon_api:1  | lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/berry  | 1         | raw        | berry         |
| task_pokemon_api:2  | lakefiles:fabric_showcase/bronze_lakehouse/files/pokemon/pokemon| 1         | bronze     | berry         |
| task_json_to_delta:1| deltalake:fabric_showcase/bronze_lakehouse/tables/pokemon/berry	| 1         | raw        | pokemon       |
```

---

### 🧪 Sample: `control_jobs` Metadata

This table defines how jobs query task definitions dynamically using SQL:

```text
| job_id | job_name     | task_sql                                                               |
|--------|--------------|------------------------------------------------------------------------|
| 1      | active_tasks | select [task_id] from [dbo].[control_tasks] where [is_active] = 1      |
| 2      | raw_tasks    | select [task_id] from [dbo].[control_tasks] where [stage_name] = 'raw' |
```

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

```text
| task_id             | expression | column_type | column_name | column_order | is_filter | is_primary_key | is_batch_key | is_order_by | is_output | is_partition_by |
|---------------------|------------|-------------|-------------|--------------|-----------|----------------|--------------|-------------|-----------|------------------|
| task_update_silver:1| id         | int         | Id          | 1            | 0         | 1              | 0            | 0           | 1         | 0                |
| task_update_silver:1| name       | string      | Name        | 2            | 0         | 0              | 0            | 0           | 1         | 0                |
```

---

## 🥇 Gold Layer

The gold layer is currently a placeholder, with a single Lakehouse leveraging only the **tables section**.

A basic passthrough pipeline is included to transfer data unmodified from silver to gold. Work is in progress to implement standard pipelines for **Slowly Changing Dimensions (SCD)** and other common gold-layer patterns.
