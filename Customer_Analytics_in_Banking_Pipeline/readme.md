# Customer Analytics in Banking Pipeline Using Databricks Spark Declarative Pipelines

## Overview
This project implements a modern, scalable data engineering and analytics platform on **Databricks**, designed to ingest, process, validate, govern, and visualize data. The solution uses **Databricks Auto Loader** for incremental file ingestion, **Spark Declarative Pipelines** for declarative data processing, and **Delta Lake** to maintain reliable Landing, Bronze, Silver, and Gold data layers.

## Architecture
<img width="1139" height="617" alt="image" src="https://github.com/user-attachments/assets/31be21ca-4735-4808-85e9-6bc74b6bba15" />

## Tech Stack
* Ingestion: Databricks Auto Loader (cloudFiles)
* Orchestration: Databricks Lakeflow Job, Spark Declarative Pipelines
* Storage: Delta Lake (Bronze / Silver / Gold)
* Governance: Unity Catalog
* Consumption: Databricks Dashboard
  
## Key Features
- Incremental file ingestion using **Databricks Auto Loader**
- `cloudFiles`-based streaming ingestion
- Medallion architecture using **Landing, Bronze, Silver, and Gold** layers
- Delta Lake-based storage
- Spark Declarative Pipelines for declarative transformations
- Streaming tables for Landing, Bronze, and Silver layers
- Materialized view for the Gold layer
- Data validation using pipeline expectations
- SCD Type 2 implementation using `create_auto_cdc_flow`
- Historical tracking of customer data
- Automated workflow orchestration using Databricks Lakeflow Jobs
- Centralized governance using Unity Catalog
- Business reporting through Databricks Dashboards

## Data Flow

Incoming data is automatically detected and ingested from cloud storage using Auto Loader (cloudFiles), eliminating the need to manually track newly arrived files. The data then flows through the Landing, Bronze, Silver, and Gold layers, with each stage progressively improving data quality and business readiness.

Spark Declarative Pipelines provide the orchestration and transformation framework. Streaming tables are used for the Landing, Bronze, and Silver layers to support incremental processing, while a materialized view is used in the Gold layer to provide optimized, aggregated data for analytics and reporting.

Data quality is incorporated directly into the pipeline through pipeline expectations, allowing invalid or unexpected records to be identified and handled during processing. The project also implements Slowly Changing Dimension (SCD) Type 2 processing using create_auto_cdc_flow, enabling historical customer changes to be captured while preserving previous versions of customer records.

The complete solution is orchestrated through a Databricks Lakeflow Job, providing an automated workflow from ingestion through transformation and analytics. Unity Catalog provides a centralized governance layer for managing data access, permissions, lineage, and discoverability across the platform.

The final Gold-layer datasets are consumed through Databricks Dashboards, providing business users with aggregated and trusted data for reporting and insights.

## Dashboard
<img width="1654" height="814" alt="image" src="https://github.com/user-attachments/assets/ef6836b7-bed6-4a8f-92db-813299651054" />













