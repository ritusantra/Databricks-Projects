# Ecommerce Orders Analytics Pipeline - Medallion Architecture with Delta Lake, Unity Catalog, and Spark Declarative Pipelines

## Overview
This project implements a scalable Ecommerce Orders Analytics Pipeline using the Medallion Architecture on Databricks. The pipeline ingests raw ecommerce order data, progressively transforms it through Bronze, Silver, and Gold layers, and publishes analytics-ready datasets for reporting and business intelligence.



## Architecture 



## Data Pipeline


### Key Features
* Auto Loader for incremental data ingestion
* Quarantine invalid records
* Genie AI Agent for insights

## Data Flow

### Ingestion
* Data is ingested as .csv from volume storage
* Ingested six source datasets consisting of customer, product, category, location, and sales transaction data

### Bronze
* Created the bronze schema to store raw ingested data using AutoLoader (cloudFiles)
* Streaming bronze tables are created to ingest incremental data
Loaded source .csv files into Bronze tables using full-load ingestion
Preserved raw data structure with minimal transformations


### Silver
Created the silver schema for cleaned and standardized streaming tables
Applied data quality transformations including:
Handling null and invalid values
Standardizing date formats
Trimming leading and trailing spaces
Normalizing data formats
Quarantined invalid records

### Gold
created materialized view by combining tables from silver layer


### Genie AI Agent for Sales and Customer Insights
Created a Genie AT Agent using the gold layer tables as the source, and configured it to answer business questions on sales, products, and customers.


## Project Folder








