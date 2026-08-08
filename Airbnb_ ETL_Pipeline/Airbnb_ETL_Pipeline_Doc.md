# Airbnb Data Pipeline Using Lakeflow Designer

# Resume
* Built an end-to-end ETL pipeline using Lakeflow Designer to ingest Airbnb data from APIs and CSV files, performing data cleansing, transformation, and integration using Python, PySpark, and SQL.
* Designed analytics-ready data models and SQL views by creating business metrics and optimizing datasets for Revenue, Bookings, Hosts, and Listings analytics.

## Objective

This project demonstrates the development of an end-to-end data pipeline using Lakeflow Designer to integrate Airbnb booking data from multiple sources and transform it into analytics-ready datasets. The pipeline follows a layered approach, enabling business users and stakeholders to analyze booking performance, revenue, hosts, and listings efficiently.

## Methodology

1. Ingest Airbnb booking, host, and listing data from multiple data sources.
2. Clean and standardize the data to improve quality.
3. Create a unified bookings-level dataset (OBT) by integrating all source tables.
4. Generate business metrics to support analytical reporting.
5. Build analytics views for revenue, bookings, hosts, and listings.
6. Create output tables in the enriched schema.

## Data Sources

1. Bookings API
2. Hosts CSV file
3. Listings CSV file

## Data Flow and Metrics

### Data Ingestion

1. Utilized the Bookings API to ingest booking data using Python.
2. Uploaded the CSV files to Unity Catalog and created source tables under a defined schema.
3. Cleaned the data using PySpark by:

   * Dropping duplicate records.
   * Handling missing values by filling:

     * **"Unknown"** for categorical columns.
     * **0** for numerical columns.
   * Applied these transformations only to important columns to avoid losing valuable business data.
4. Joined the datasets using the primary key to build a one-big table:

   * Used the **Bookings** table as the left table.
   * Performed a **Left Join** with the **Listings** table.
   * Performed another **Left Join** with the **Hosts** table.
   * This ensured that all booking records were retained, even if listing or host information was missing.
5. Built additional business metrics such as **Total Amount**, **Accommodation Type**, and **Response Rate Quality** using SQL.
6. Created separate analytics views for **Revenue**, **Bookings**, **Hosts**, and **Listings** using SQL to analyze the performance of each business entity.
7. Create output tables of the final views for queries and analysis.

### Metrics

1. **Total Amount**
   **Formula:**
   `(Nights Booked × Price per Night) + Cleaning Fee + Service Fee`

2. **Accommodation Type**

   * Accommodates > 4 → **Large**
   * Accommodates > 2 → **Medium**
   * Otherwise → **Single**

3. **Response Rate Quality**

   * Response Rate > 90 → **Very Good**
   * Response Rate > 80 → **Good**
   * Response Rate > 60 → **Average**
   * Otherwise → **Poor**

   ![image_1785806232686.png](./image_1785806232686.png "image_1785806232686.png")
