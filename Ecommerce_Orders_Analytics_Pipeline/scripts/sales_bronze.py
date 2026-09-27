from pyspark import pipelines as dp
from pyspark.sql.functions import *


# source_crm: sales_details_schema

sales_details_schema = '''
            sls_ord_num  STRING,
            sls_prd_key  STRING,
            sls_cust_id  INT,
            sls_order_dt INT,
            sls_ship_dt  INT,
            sls_due_dt   INT,
            sls_sales    DOUBLE,
            sls_quantity INT,
            sls_price    DOUBLE
            
            '''

@dp.table(name='br_sales_details')
def br_sales_details():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .schema(sales_details_schema)
          .option('header', 'true')
          .option('dateFormat','yyyy-MM-dd')
          .load('/Volumes/sales_catalog/source_crm/sales_details'))\
          .withColumn('ingestion_date', current_timestamp())
    return df