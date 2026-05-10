# Building End-to-End Medallion Architecture

## Methodology

### 🔗 Source Data Ingestion

### 🟤 Bronze Layer 
* Analyzed source systems and data structure
* Uploaded .csv files from local storage to Databricks volume
* Defined bronze table schema and loaded data from .csv files
* Included print statements for load progress: "loading...", "load successful", and time taken

### ⚪ Silver Layer
* Defined silver table schema
* Applied data transformations and loaded silver table with dwh_create_date
* Trimmed whitespace, standardized categories, created derived columns, adjusted dates, handled invalid data

### 🟡 Gold Layer
* To be completed

### 📊 BI Dashboard