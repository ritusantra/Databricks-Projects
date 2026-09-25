# Ecommerce Orders Analytics Pipeline - Medallion Architecture with Delta Lake, Unity Catalog, and Spark Declarative Pipelines

## Overview
This project implements a scalable **Ecommerce Orders Analytics Pipeline** using the **Medallion Architecture** on **Databricks**. Databricks **Lakeflow Job** ingests raw CSV data from Volume Storage and progressively transforms it through Bronze, Silver, and Gold layers using **Spark Declarative Pipelines** on **Delta Lake**. Gold layer outputs are exposed to a **Dashboard** for reporting and to a **Genie AI Agent** for natural-language business insights, with all pipeline assets governed end-to-end under **Unity Catalog**.



## Architecture 
<img width="1212" height="531" alt="image" src="https://github.com/user-attachments/assets/0b34012a-e76c-450b-97cd-806d449f7fa3" />


## Tech Stack
* **Ingestion:** Databricks Auto Loader (cloudFiles)
* **Orchestration:** Databricks Lakeflow Job, Spark Declarative Pipelines
* **Storage:** Delta Lake (Bronze / Silver / Gold)
* **Governance:** Unity Catalog
* **Consumption:** Databricks Dashboard, Genie AI Agent

## Data Pipeline
### Key Features
* **Auto Loader for incremental ingestion** - new CSV files dropped into Volume Storage are automatically detected and processed without full reprocessing, using Auto Loader's `cloudFiles`
* **Declarative pipeline orchestration** - Bronze, Silver, and Gold transformations are defined declaratively via Spark Declarative Pipelines, giving automatic dependency resolution, and lineage tracking
* **Streaming-first design** - Bronze and Silver are implemented as streaming tables so data flows through the medallion layers continuously as new files arrive, instead of relying on scheduled batch runs
* **Quarantine of invalid records** - rows that fail data quality expectations at the Silver stage are routed to a quarantine path rather than silently dropped or allowed to corrupt downstream tables
* **Materialized Gold layer** - the Gold layer is a materialized view, so it is automatically and efficiently refreshed whenever upstream Silver data changes, without needing a separate manual refresh job
* **Unity Catalog governance** - every table across Bronze, Silver, and Gold is registered in Unity Catalog, giving centralized access control, lineage, and discoverability across the whole job
* **Genie AI Agent for self-serve insights** - business users can ask plain-language questions over the Gold layer without needing to write SQL
* **Single orchestrated job** - the entire flow (ingestion through Gold) runs as one Databricks Lakeflow Job, simplifying scheduling, monitoring, and failure handling

## Data Flow
 
### Ingestion
* Raw CSV files land in Volume Storage as they arrive from the source system
* Auto Loader (`cloudFiles` format) continuously watches the volume and picks up new files incrementally, tracking already-processed files so nothing is re-ingested
* Auto Loader handles schema inference on first read and evolves the schema automatically as new columns appear in later files
  
### Bronze
* A Bronze schema/streaming table is created via Spark Declarative Pipelines to land the raw data coming out of Auto Loader
* Data is written with minimal transformation, preserving the original structure, column names, and values from the source CSVs for full traceability back to the raw files
* Because it is a streaming table, Bronze is continuously appended to as Auto Loader delivers new micro-batches, rather than being rebuilt on each run

### Silver
* A Silver streaming table is created from the Bronze streaming table using Spark Declarative Pipelines
* Data transformations are applied to clean and standardize the data, including:
  * Handling null and invalid values
  * Standardizing date formats
  * Trimming leading/trailing whitespace
  * Normalizing inconsistent data formats
* Records that fail the data validation rules are quarantined instead of being dropped, so bad data is visible and auditable rather than silently lost
* Processing remains incremental end-to-end, so only new/changed Bronze data is processed into Silver on each run

### Gold
* A Gold Materialized View is built by combining and aggregating Silver streaming tables into an analytics-ready dataset
* Because it's a materialized view rather than a plain table, Databricks automatically keeps it up to date as the underlying Silver data changes, recomputing only what's needed
* This is the layer intended for direct consumption by BI tools and the Genie AI Agent - it should already be shaped for reporting (clean grain, business-friendly column names, key metrics/aggregates pre-computed where useful)

### Consumption
* **Dashboard** - the Gold Materialized View is connected directly to a Databricks Dashboard for standard visual reporting and monitoring of key metrics
* **Genie AI Agent** - the same Gold layer also powers a Genie AI Agent, which is configured to answer natural-language business questions (e.g. sales trends, customer/product breakdowns) directly against the Gold tables, without requiring the end user to write any SQL
### Governance
* Unity Catalog governs every asset in the pipeline - Bronze, Silver, and Gold tables/views are all registered under it
* This provides a single place for access control (who can query which layer), column/table-level lineage (tracing a Gold metric all the way back to the source CSV), and discoverability for other teams who want to build on top of these tables
* Running the whole thing as a single Databricks Lakeflow Job means scheduling, monitoring, retries, and alerting are all managed centrally rather than being spread across disconnected notebooks


## Project Folder









