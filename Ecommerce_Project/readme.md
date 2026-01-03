# Ecommerce Data Engineering Project using Databricks

Designed and implemented an end-to-end ETL pipeline for e-commerce sales transaction data using Databricks, adhering to the Medallion Architecture (Bronze, Silver, and Gold layers) to ensure scalable data ingestion, transformation, and analytics readiness.

Architecture:
1. Data from local in .csv format
2. Data uploaded in Databricks in volume
3. Created catalog and bronze, silver, gold schemas using SQL
4. Performed ETL and data transformations in PySpark
5. Utilized Genie to explore the key insights from the data
6. Developed the dashboard using Databricks inbuild Dashboard feature

Steps:
1. Uploaded data in .csv format from local to Databricks volume
2. Extracted data in the bronze layer
3. Performed data cleansing and transformation in the silver layer
4. Build analytics ready tables in the gold layer
5. Build a dashboard based on the data from gold layer








# E-commerce Data Engineering Project using Databricks

Designed and implemented an end-to-end ETL pipeline for e-commerce sales transaction data using Databricks, following the Medallion Architecture (Bronze, Silver, and Gold layers) to enable scalable data processing and analytics-ready datasets.

## Architecture & Databricks Features
- Ingested raw e-commerce transaction data in CSV format from a local source into Databricks Volumes  
- Created a unified data catalog and defined Bronze, Silver, and Gold schemas using Databricks SQL  
- Performed data extraction, cleansing, and transformation using PySpark  
- Leveraged Databricks Genie to explore and derive key business insights  
- Developed interactive dashboards using Databricks’ built-in Dashboard functionality  

## Implementation Workflow
- Uploaded raw CSV files from a local environment to Databricks Volumes  
- Ingested raw data into the Bronze layer for initial storage and schema enforcement  
- Applied data cleansing, validation, and transformation logic in the Silver layer  
- Built analytics-ready fact and dimension tables in the Gold layer  
- Designed a dashboard using Gold-layer data for business reporting and insights  

