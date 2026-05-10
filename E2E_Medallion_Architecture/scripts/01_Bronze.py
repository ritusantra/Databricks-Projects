"""
Bronze Layer Script

This script creates the bronze schema and tables, then loads .csv data from source volume.

"""

from pyspark.sql import SparkSession
import logging
import time

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_bronze_schema(spark):
    """Create the bronze schema if it doesn't exist"""
    logger.info('Creating bronze schema...')
    spark.sql('''
              CREATE SCHEMA IF NOT EXISTS sales_catalog.bronze
            ''')
    logger.info('Bronze schema created successfully.')


def create_bronze_tables(spark):
    """Create bronze tables"""
    logger.info('Creating bronze tables...')

    # b_crm_cust_info
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.bronze.b_crm_cust_info
        (
            cst_id             INT,
            cst_key            STRING,
            cst_firstname      STRING,
            cst_lastname       STRING,
            cst_marital_status STRING,
            cst_gndr           STRING,
            cst_create_date    DATE
        )
        ''')
    logger.info('Created table: b_crm_cust_info')

    # b_crm_prd_info
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.bronze.b_crm_prd_info
        (
            prd_id       INT,
            prd_key      STRING,
            prd_nm       STRING,
            prd_cost     DOUBLE,
            prd_line     STRING,
            prd_start_dt DATE,
            prd_end_dt   DATE
        )
    ''')
    logger.info('Created table: b_crm_prd_info')

    # b_sales_details
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.bronze.b_sales_details
        (
            sls_ord_num  STRING,
            sls_prd_key  STRING,
            sls_cust_id  INT,
            sls_order_dt INT,
            sls_ship_dt  INT,
            sls_due_dt   INT,
            sls_sales    DOUBLE,
            sls_quantity INT,
            sls_price    DOUBLE
        )
    ''')
    logger.info('Created table: b_sales_details')

    # b_cust_az12
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.bronze.b_cust_az12
        (
            CID   STRING,
            BDATE DATE,
            GEN   STRING
        )
    ''')
    logger.info('Created table: b_cust_az12')

    # b_loc_a101_cntry
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.bronze.b_loc_a101_cntry
        (
            CID   STRING,
            CNTRY STRING
        )
    ''')
    logger.info('Created table: b_loc_a101_cntry')

    # b_px_cat_g1v2
    spark.sql('''
        CREATE OR REPLACE TABLE sales_catalog.bronze.b_px_cat_g1v2
        (
            ID          STRING,
            CAT         STRING,
            SUBCAT      STRING,
            MAINTENANCE STRING
        )
    ''')
    logger.info('Created table: b_px_cat_g1v2')

    logger.info('All bronze tables created successfully.')


def load_bronze_data(spark):
    """Load .csv data from source volume into bronze tables."""
    logger.info('Loading data to bronze tables...')

    start_time_b = time.time()

    # Load b_crm_cust_info
    logger.info('Loading b_crm_cust_info...')
    spark.sql('''TRUNCATE TABLE sales_catalog.bronze.b_crm_cust_info''')
    df_cust_info = spark.read.format('csv').option('header', 'true').load('/Volumes/sales_catalog/source_crm/cust_info')
    df_cust_info.write.mode('overwrite').insertInto('sales_catalog.bronze.b_crm_cust_info')
    logger.info(f'Loaded {df_cust_info.count()} records into b_crm_cust_info.')

    # Load b_crm_prd_info
    logger.info('Loading b_crm_prd_info...')
    spark.sql('''TRUNCATE TABLE sales_catalog.bronze.b_crm_prd_info''')
    df_prd_info = spark.read.format('csv').option('header', 'true').load('/Volumes/sales_catalog/source_crm/prd_info')
    df_prd_info.write.mode('overwrite').insertInto('sales_catalog.bronze.b_crm_prd_info')
    logger.info(f'Loaded {df_prd_info.count()} records into b_crm_prd_info')

    # Load b_sales_details
    logger.info('Loading b_sales_details...')
    spark.sql('''TRUNCATE TABLE sales_catalog.bronze.b_sales_details''')
    df_sales_details = spark.read.format('csv').option('header', 'true').load('/Volumes/sales_catalog/source_crm/sales_details')
    df_sales_details.write.mode('overwrite').insertInto('sales_catalog.bronze.b_sales_details')
    logger.info(f'Loaded {df_sales_details.count()} records into b_sales_details')

    # Load b_cust_az12
    logger.info('Loading b_cust_az12...')
    spark.sql('''TRUNCATE TABLE sales_catalog.bronze.b_cust_az12''')
    df_cust_az12 = spark.read.format('csv').option('header', 'true').load('/Volumes/sales_catalog/source_erp/cust_az12')
    df_cust_az12.write.mode('overwrite').insertInto('sales_catalog.bronze.b_cust_az12')
    logger.info(f'Loaded {df_cust_az12.count()} records into b_cust_az12')

    # Load b_loc_a101_cntry
    logger.info('Loading b_loc_a101_cntry...')
    spark.sql('''TRUNCATE TABLE sales_catalog.bronze.b_loc_a101_cntry''')
    df_loc_a101 = spark.read.format('csv').option('header', 'true').load('/Volumes/sales_catalog/source_erp/loc_a101')
    df_loc_a101.write.mode('overwrite').insertInto('sales_catalog.bronze.b_loc_a101_cntry')
    logger.info(f'Loaded {df_loc_a101.count()} records into b_loc_a101_cntry')

    # Load b_px_cat_g1v2
    logger.info('Loading b_px_cat_g1v2...')
    spark.sql('''TRUNCATE TABLE sales_catalog.bronze.b_px_cat_g1v2''')
    df_px_cat_g1v2 = spark.read.format('csv').option('header', 'true').load('/Volumes/sales_catalog/source_erp/px_cat_g1v2')
    df_px_cat_g1v2.write.mode('overwrite').insertInto('sales_catalog.bronze.b_px_cat_g1v2')
    logger.info(f'Loaded {df_px_cat_g1v2.count()} records into b_px_cat_g1v2')

    end_time_b = time.time()
    duration = end_time_b - start_time_b

    logger.info(f'Bronze layer data load completed successfully in {round(duration)} seconds.')


def main():
    """Main execution function."""
    try:
        # Get or create a spark session
        spark = SparkSession.builder.appName('Bronze Layer').getOrCreate()

        # Execute the bronze layer creation steps
        create_bronze_schema(spark)
        create_bronze_tables(spark)
        load_bronze_data(spark)

        logger.info('Bronze layer created successfully.')

    except Exception as e:
        logger.error(f'Bronze layer creation failed: {str(e)}')
        raise


if __name__ == "__main__":
    main()
