# Ecommerce Data Engineering Project using Databricks

Designed and implemented an end-to-end ETL pipeline for e-commerce sales transaction data using Databricks, adhering to the Medallion Architecture (Bronze, Silver, and Gold layers) to ensure scalable data ingestion, transformation, and analytics readiness.

Architecture:
1. Data from local in .csv format
2. Data uploaded in Databricks in volume
3. Created catalog

Steps:
1. Uploaded data in .csv format from local to Databricks volume
2. Extracted data in the bronze layer
3. Performed data cleansing and transformation in the silver layer
4. Build analytics ready tables in the gold layer
5. Build a dashboard based on the data from gold layer
