"""
Silver Layer Script

# This script creates the silver schema, tables, and loads data from the bronze layer after applying data transformations.

"""

from pyspark.sql import SparkSession
import logging
import time

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_silver_schema(spark):
    """Create the silver schema if it doesn't exist"""
    logger.info('Creating silver schema...')
    spark.sql('''
              CREATE SCHEMA IF NOT EXISTS sales_catalog.silver
            ''')
    logger.info('Silver schema created successfully.')


def create_silver_tables(spark):
    """Create silver tables"""
    logger.info('Creating silver tables...')

    # s_crm_cust_info
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.silver.s_crm_cust_info
        (
            cst_id             INT,
            cst_key            STRING,
            cst_firstname      STRING,
            cst_lastname       STRING,
            cst_marital_status STRING,
            cst_gndr           STRING,
            cst_create_date    DATE,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
        ''')
    logger.info('Created table: s_crm_cust_info')

    # s_crm_prd_info
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.silver.s_crm_prd_info
        (
            prd_id       INT,
            cat_id      STRING,        
            prd_key      STRING,
            prd_nm       STRING,
            prd_cost     DOUBLE,
            prd_line     STRING,
            prd_start_dt DATE,
            prd_end_dt   DATE,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
    ''')
    logger.info('Created table: s_crm_prd_info')

    # s_sales_details
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.silver.s_sales_details
        (
            sls_ord_num  STRING,
            sls_prd_key  STRING,
            sls_cust_id  INT,
            sls_order_dt DATE,
            sls_ship_dt  DATE,
            sls_due_dt   DATE,
            sls_sales    DOUBLE,
            sls_quantity INT,
            sls_price    DOUBLE,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
    ''')
    logger.info('Created table: s_sales_details')

    # s_cust_az12
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.silver.s_cust_az12
        (
            CID   STRING,
            BDATE DATE,
            GEN   STRING,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
    ''')
    logger.info('Created table: s_cust_az12')

    # s_loc_a101_cntry
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.silver.s_loc_a101_cntry
        (
            CID   STRING,
            CNTRY STRING,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
    ''')
    logger.info('Created table: s_loc_a101_cntry')

    # s_px_cat_g1v2
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.silver.s_px_cat_g1v2
        (
            ID          STRING,
            CAT         STRING,
            SUBCAT      STRING,
            MAINTENANCE STRING,
            dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
        )
        TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
    ''')
    logger.info('Created table: s_px_cat_g1v2')

    logger.info('All silver tables created successfully.')

def load_silver_data(spark):
    """Load data from bronze tables to silver tables after transformations."""
    logger.info('Loading data to silver tables...')

    start_time_b = time.time()

    # Load s_crm_cust_info
    logger.info('Loading s_crm_cust_info...')
    spark.sql('''TRUNCATE TABLE sales_catalog.silver.s_crm_cust_info''')
    spark.sql('''INSERT INTO sales_catalog.silver.s_crm_cust_info
                (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)

                WITH silver_transformation AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                    FROM
                        sales_catalog.bronze.b_crm_cust_info
                        WHERE cst_id IS NOT NULL
                )
                SELECT
                    cst_id,
                    cst_key,
                    TRIM(cst_firstname) AS cst_firstname,
                    TRIM(cst_lastname) AS cst_lastname,
                    CASE 
                        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                        ELSE 'Unknown'
                    END AS cst_marital_status,
                    CASE 
                        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                        ELSE 'Unknown'
                    END AS cst_gndr,
                    cst_create_date
                FROM
                    silver_transformation
                WHERE
                    flag_last = 1''')
    row_counts = spark.sql('''select count(*) as cnt from sales_catalog.silver.s_crm_cust_info''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into s_crm_cust_info.')

    # Load s_crm_prd_info
    logger.info('Loading s_crm_prd_info...')
    spark.sql('''TRUNCATE TABLE sales_catalog.silver.s_crm_prd_info''')
    spark.sql('''INSERT INTO sales_catalog.silver.s_crm_prd_info
                    (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
                    SELECT 
                        prd_id, 
                        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
                        SUBSTRING(prd_key, 7) AS prd_key,
                        prd_nm,
                        COALESCE(prd_cost, 0) AS prd_cost,
                        CASE 
                            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
                            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
                            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
                            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Tour'
                            ELSE 'Unknown'
                        END AS prd_line,
                        prd_start_dt,
                        LEAD(prd_start_dt) OVER (
                            PARTITION BY prd_key 
                            ORDER BY prd_start_dt ASC
                        ) - 1 AS prd_end_dt
                    FROM 
                        sales_catalog.bronze.b_crm_prd_info
                    WHERE 
                        prd_id IS NOT NULL''')
    row_counts = spark.sql('''SELECT count(*) as cnt FROM sales_catalog.silver.s_crm_prd_info''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into s_crm_prd_info.')

    # Load s_sales_details
    logger.info('Loading s_sales_details...')
    spark.sql('''TRUNCATE TABLE sales_catalog.silver.s_sales_details''')
    spark.sql('''
              INSERT INTO sales_catalog.silver.s_sales_details
                    (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
                    SELECT 
                        sls_ord_num,
                        sls_prd_key,
                        sls_cust_id,
                        CASE 
                            WHEN sls_order_dt = 0 OR LENGTH(CAST(sls_order_dt AS STRING)) != 8 THEN NULL 
                            ELSE TO_DATE(CAST(sls_order_dt AS STRING), 'yyyyMMdd') 
                        END AS sls_order_dt,
                        CASE 
                            WHEN sls_ship_dt = 0 OR LENGTH(CAST(sls_ship_dt AS STRING)) != 8 THEN NULL 
                            ELSE TO_DATE(CAST(sls_ship_dt AS STRING), 'yyyyMMdd') 
                        END AS sls_ship_dt,
                        CASE 
                            WHEN sls_due_dt = 0 OR LENGTH(CAST(sls_due_dt AS STRING)) != 8 THEN NULL 
                            ELSE TO_DATE(CAST(sls_due_dt AS STRING), 'yyyyMMdd') 
                        END AS sls_due_dt,
                        CASE 
                            WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) THEN sls_quantity * ABS(sls_price) 
                            ELSE sls_sales
                        END AS sls_sales,
                        sls_quantity,
                        CASE 
                            WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / COALESCE(sls_quantity, 0) 
                            ELSE sls_price 
                        END AS sls_price
                    FROM sales_catalog.bronze.b_sales_details
              ''')
    row_counts = spark.sql('''SELECT count(*) AS cnt FROM sales_catalog.silver.s_sales_details''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into s_sales_details.')

    # Load s_cust_az12
    logger.info('Loading b_cust_az12...')
    spark.sql('''TRUNCATE TABLE sales_catalog.silver.s_cust_az12''')
    spark.sql('''
              INSERT INTO sales_catalog.silver.s_cust_az12
                    (CID, BDATE, GEN)
                    SELECT 
                        CASE 
                            WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4)
                            ELSE CID 
                        END AS CID, 
                        CASE 
                            WHEN BDATE > CURRENT_DATE() THEN NULL 
                            ELSE BDATE
                        END AS BDATE, 
                        CASE 
                            WHEN UPPER(TRIM(GEN)) IN ('F', 'FEMALE') THEN 'Female'
                            WHEN UPPER(TRIM(GEN)) IN ('M', 'MALE') THEN 'Male'
                            ELSE 'Unknown'
                        END AS GEN
                    FROM 
                    sales_catalog.bronze.b_cust_az12
              ''')
    row_counts = spark.sql('''SELECT count(*) as cnt from sales_catalog.silver.s_cust_az12''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into s_cust_az12.')

    # Load s_loc_a101_cntry
    logger.info('Loading s_loc_a101_cntry...')
    spark.sql('''TRUNCATE TABLE sales_catalog.silver.s_loc_a101_cntry''')
    spark.sql('''
              INSERT INTO sales_catalog.silver.s_loc_a101_cntry
                (CID, CNTRY)
                SELECT 
                    REPLACE(CID, '-', '') AS CID,
                    CASE 
                        WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
                        WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
                        WHEN TRIM(CNTRY) = '' OR CNTRY IS NULL THEN 'Unknown'
                        ELSE TRIM(CNTRY) 
                    END AS CNTRY
                FROM 
                    sales_catalog.bronze.b_loc_a101_cntry  
              ''')
    row_counts = spark.sql('''SELECT count(*) as cnt from sales_catalog.silver.s_loc_a101_cntry''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into s_loc_a101_cntry.')

    # Load s_px_cat_g1v2
    logger.info('Loading s_px_cat_g1v2...')
    spark.sql('''TRUNCATE TABLE sales_catalog.silver.s_px_cat_g1v2''')
    spark.sql('''
              INSERT INTO sales_catalog.silver.s_px_cat_g1v2
                (ID, CAT, SUBCAT, MAINTENANCE)
                SELECT 
                    ID,
                    CAT,
                    SUBCAT,
                    MAINTENANCE
                FROM sales_catalog.bronze.b_px_cat_g1v2
             ''')
    row_counts = spark.sql('''SELECT count(*) as cnt from sales_catalog.silver.s_px_cat_g1v2''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into s_px_cat_g1v2.')

    end_time_b = time.time()
    duration = end_time_b - start_time_b

    logger.info(f'Silver layer data load completed successfully in {round(duration)} seconds.')

def main():
    """Main execution function."""
    try:
        # Get or create a spark session
        spark = SparkSession.builder.appName('Silver Layer').getOrCreate()

        # Execute the silver layer creation steps
        create_silver_schema(spark)
        create_silver_tables(spark)
        load_silver_data(spark)

        logger.info('Silver layer created successfully.')

    except Exception as e:
        logger.error(f'Silver layer creation failed: {str(e)}')
        raise

if __name__ == "__main__":
    main()
