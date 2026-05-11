"""
Gold Layer Script

# This script creates the gold schema, tables, and loads data from the silver layer after applying data aggregations.

"""

from pyspark.sql import SparkSession
import logging
import time

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_gold_schema(spark):
    """Create the gold schema if it doesn't exist"""
    logger.info('Creating gold schema...')
    spark.sql('''
              CREATE SCHEMA IF NOT EXISTS sales_catalog.gold
              ''')
    logger.info('Gold schema created successfully.')


def create_gold_tables(spark):
    """Create gold tables"""
    logger.info('Creating gold tables...')
    
    # dim_customer
    spark.sql('''
               CREATE OR REPLACE TABLE sales_catalog.gold.dim_customers
                (
                    customer_key INT,
                    customer_id INT,
                    custmer_number STRING,
                    first_name STRING,
                    last_name STRING,
                    country STRING,
                    marital_status STRING,
                    gender STRING,
                    birthdate DATE,
                    create_date DATE
                )
                TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
            ''')
    logger.info('Created table: dim_customers')
    
    # dim_products
    spark.sql('''
              CREATE OR REPLACE TABLE sales_catalog.gold.dim_products
                (
                    product_key INT,
                    product_id INT,
                    product_number STRING,
                    product_name STRING,
                    category_id STRING,
                    category STRING,
                    subcategory STRING,
                    maintenance STRING,
                    cost DOUBLE,
                    product_line STRING,
                    start_date DATE
                )
                TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
              ''')
    logger.info('Create table: dim_products')

    # fact_sales
    spark.sql('''
              CREATE OR REPLACE TABLE sales_catalog.gold.fact_sales
                (
                    order_number STRING,
                    product_key INT,
                    customer_key INT,
                    order_date DATE,
                    shipping_date DATE,
                    due_date DATE,
                    sales_amount DOUBLE,
                    quantity INT,
                    price DOUBLE
                )
                TBLPROPERTIES('delta.feature.allowColumnDefaults' = 'supported')
              
              ''')
    logger.info('Create table: fact_sales')


def load_gold_data(spark):
    """Load data from silver tables to gold tables after aggregation."""

    logger.info('Loading data to gold tables...')
    start_time_g = time.time()

    # Load dim_customers  
    logger.info('Loading dim_customers...')
    spark.sql('''TRUNCATE TABLE sales_catalog.gold.dim_customers''')
    spark.sql('''
              INSERT INTO sales_catalog.gold.dim_customers(
                    customer_key,
                    customer_id,
                    custmer_number,
                    first_name,
                    last_name,
                    country,
                    marital_status,
                    gender,
                    birthdate,
                    create_date
                )
                SELECT 
                    ROW_NUMBER() OVER(ORDER BY ci.cst_id) AS customer_key,
                    ci.cst_id AS customer_id, 
                    ci.cst_key AS custmer_number, 
                    ci.cst_firstname AS first_name, 
                    ci.cst_lastname AS last_name, 
                    la.CNTRY AS country,
                    ci.cst_marital_status AS marital_status, 
                    CASE 
                        WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr
                        ELSE COALESCE(ca.gen, 'Unknown') 
                    END AS gender,
                    ca.BDATE AS birthdate,
                    ci.cst_create_date AS create_date
                FROM sales_catalog.silver.s_crm_cust_info ci
                LEFT JOIN sales_catalog.silver.s_cust_az12 ca
                    ON ci.cst_key = ca.CID
                LEFT JOIN sales_catalog.silver.s_loc_a101_cntry la
                    ON ci.cst_key = la.CID
              ''')
    row_counts = spark.sql('''select count(*) as cnt from sales_catalog.gold.dim_customers''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into dim_customers.')

    # Load dim_products
    logger.info('Loading dim_products...')
    spark.sql('''TRUNCATE TABLE sales_catalog.gold.dim_products''')
    spark.sql('''
              INSERT INTO sales_catalog.gold.dim_products (
                        product_key,
                        product_id,
                        product_number,
                        product_name,
                        category_id,
                        category,
                        subcategory,
                        maintenance,
                        cost,
                        product_line,
                        start_date
                    )
                    SELECT 
                        ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
                        pn.prd_id AS product_id, 
                        pn.prd_key AS product_number, 
                        pn.prd_nm AS product_name, 
                        pn.cat_id AS category_id, 
                        pc.CAT AS category, 
                        pc.SUBCAT AS subcategory, 
                        pc.MAINTENANCE AS maintenance, 
                        pn.prd_cost AS cost, 
                        pn.prd_line AS product_line, 
                        pn.prd_start_dt AS start_date
                    FROM sales_catalog.silver.s_crm_prd_info pn
                    LEFT JOIN sales_catalog.silver.s_px_cat_g1v2 pc
                        ON pn.cat_id = pc.ID
                    WHERE pn.prd_end_dt IS NULL
              ''')
    row_counts = spark.sql('''select count(*) as cnt from sales_catalog.gold.dim_products''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into dim_products.')

    # Load fact_sales
    logger.info('Loading fact_sales...')
    spark.sql('''TRUNCATE TABLE sales_catalog.gold.fact_sales''')
    spark.sql('''
              INSERT INTO sales_catalog.gold.fact_sales (
                            order_number,
                            product_key,
                            customer_key,
                            order_date,
                            shipping_date,
                            due_date,
                            sales_amount,
                            quantity,
                            price
                        )
                        SELECT 
                            sd.sls_ord_num AS order_number,
                            pr.product_key,
                            cu.customer_key,
                            sd.sls_order_dt AS order_date,
                            sd.sls_ship_dt AS shipping_date,
                            sd.sls_due_dt AS due_date,
                            sd.sls_sales AS sales_amount,
                            sd.sls_quantity AS quantity,
                            sd.sls_price AS price
                        FROM sales_catalog.silver.s_sales_details sd
                        LEFT JOIN sales_catalog.gold.dim_products pr 
                        ON sd.sls_prd_key = pr.product_number
                        LEFT JOIN sales_catalog.gold.dim_customers cu 
                        ON sd.sls_cust_id = cu.customer_id
              
              ''')
    row_counts = spark.sql('''select count(*) AS cnt from sales_catalog.gold.fact_sales''').collect()[0]['cnt']
    logger.info(f'Loaded {row_counts} records into fact_sales.')

    end_time_g = time.time()
    duration = end_time_g - start_time_g
    
    logger.info(f'Gold layer data load completed successfully in {round(duration)} seconds.')

def main():
    """Main execution function."""
    try:
        # Get or create a spark session
        spark = SparkSession.builder.appName('Gold Layer').getOrCreate()

        # Execute gold layer creation steps
        create_gold_schema(spark)
        create_gold_tables(spark)
        load_gold_data(spark)

        logger.info('Gold layer created successfully.')

    except Exception as e:
        logger.info(f'Gold layer creation failed: {str(e)}')
        raise

if __name__ == '__main__':
    main()
