# 🔢🧱Databricks Lakehouse Pipeline with Medallion Architecture and SCD Type 2 Dimension Modeling

This project implements an end-to-end Data Lakehouse pipeline on Databricks using the Medallion Architecture (Bronze, Silver, and Gold layers). It processes raw sales data through incremental ingestion and transformation to build analytics-ready datasets. The solution includes a Product dimension with SCD Type 2 modeling to track historical attribute changes using surrogate keys. Finally, a fact table is created by joining dimensions to enable efficient and reliable business reporting.

## Data Dictionary

| Column Name       | Data Type | Description |
|------------------|----------|-------------|
| OrderId          | Integer   | Unique identifier for each order |
| OrderDate        | Date      | Date when the order was placed |
| ShipMode         | String    | Shipping method used for delivery (e.g., Second Class, First Class) |
| Segment          | String    | Customer segment (e.g., Consumer, Corporate, Home Office) |
| Country          | String    | Country where the order was placed |
| City             | String    | City of delivery |
| State            | String    | State of delivery |
| PostalCode       | Integer   | Postal code of delivery location |
| Region           | String    | Region within the country (e.g., South, West) |
| Category         | String    | Product category (e.g., Furniture, Technology) |
| SubCategory      | String    | Product sub-category (e.g., Bookcases, Phones) |
| ProductId        | String    | Unique identifier for each product |
| CostPrice        | Float     | Cost price of the product |
| ListPrice        | Float     | Listed selling price of the product |
| Quantity         | Integer   | Number of units ordered |
| DiscountPercent  | Float     | Discount applied on the product (in %) |

## Methodology

### Bronze Layer
- Ingested sales data in `.csv` format from volume storage  
- Implemented incremental load to bring only the latest orders into the Bronze table  
- Added `loaded_at` timestamp to track ingestion time for each batch  

---

### Silver Layer
- Ingested latest order records from the Bronze table  
- Transformed data by adding derived columns: `Total_Sales` and `Final_Price`  
- Added `processed_date` to track transformation time  
- Loaded the transformed data into the Silver table  

---

### Gold Layer
- Ingested latest order records from the Silver table  
- Created a **Product Dimension table** and implemented **SCD Type 2 modeling**  
  - Introduced a surrogate key to track historical changes in product attributes  
  - Added `is_current` flag along with `start_date` and `end_date` to manage record validity  
- Created a **Fact table** containing numerical measures and relevant foreign keys  
- Joined the fact table with the Product Dimension to include the **Product surrogate key**, ensuring only the latest active product record is used during mapping  
- Loaded both Dimension and Fact tables into the Gold layer  