# Customer Analytics in Banking Pipeline

## Overview


## Architecture
<img width="1139" height="617" alt="image" src="https://github.com/user-attachments/assets/31be21ca-4735-4808-85e9-6bc74b6bba15" />

## Tech Stack
* Ingestion: Databricks Auto Loader (cloudFiles)
* Orchestration: Databricks Lakeflow Job, Spark Declarative Pipelines
* Storage: Delta Lake (Bronze / Silver / Gold)
* Governance: Unity Catalog
* Consumption: Databricks Dashboard
  
## Key Features
* Incremental ingestion of data from new files using Auto Loader's cloudFiles
* Declarative pipeline orchestration
* Streaming tables for landing, bronze, and silver layer
* Materialized view for gold layer
* Data validation using pipeline expectations
* SCD type 2 implementation using create_auto_cdc_flow for tracking historical customer data
* Dashboard for insights
* Unity catalog governance

## Dashboard
<img width="1654" height="814" alt="image" src="https://github.com/user-attachments/assets/ef6836b7-bed6-4a8f-92db-813299651054" />
