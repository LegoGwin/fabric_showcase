Microsoft Fabric Showcase

This repository contains a reusable data engineering framework built on Microsoft Fabric using PySpark, Delta Lake, and metadata-driven transformations. It implements a medallion architecture (Bronze, Silver, Gold layers) and supports scalable, maintainable pipeline patterns suitable for enterprise workloads.

📐 Architecture Overview

[Source APIs / Files]
        ↓
     [Bronze Layer]  ← Ingest raw JSON, Parquet, or API data
        ↓
     [Silver Layer]  ← Apply metadata-driven cleaning and transformation
        ↓
      [Gold Layer]   ← Join, aggregate, and prepare for reporting

Bronze Layer: Raw data is landed with minimal processing.

Silver Layer: Cleansing, deduplication, and business rules are applied using a configurable column map.

Gold Layer: Optional layer for modeling, joining, or reporting.

🚀 Demo Notebook (Coming Soon)

A runnable demo will be added to simulate:

Ingesting mock data

Applying transformations

Showing the final DataFrame output

This is useful to show the framework in action without requiring external APIs.

🔁 Core Components

Folder

Description

[0] master

Control jobs for orchestrating pipelines

[1] multi

Shared maintenance and configuration logic

[2] bronze

Ingestion notebooks and pipelines

[3] silver

Transformation logic using column maps

[4] gold

(Optional) Aggregation layer

[a] testing

Testing and stubbed utilities

📁 Example: Metadata-Driven Transformation

Your column_map JSON controls:

Data type casting

Timestamp formatting

Primary key & batch deduplication

Output selection

This makes transformations declarative and repeatable across datasets.

📦 Future Enhancements

Add a complete demo_run.Notebook

Include Power BI visuals or export steps

Create robust error logging and retry support

📬 Contact / Credit

Built by a data engineering consultant experienced in Fabric, Power BI, and scalable lakehouse solutions.

Feel free to fork this repo or reach out with questions!

