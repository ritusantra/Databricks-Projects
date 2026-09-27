from pyspark import pipelines as dp
from pyspark.sql.functions import *


# source_crm: prd_info

prd_info_schema = '''
            prd_id       INT,
            prd_key      STRING,
            prd_nm       STRING,
            prd_cost     DOUBLE,
            prd_line     STRING,
            prd_start_dt DATE,
            prd_end_dt   DATE
            
            '''

@dp.table(name='br_prd_info')
def br_prd_info():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .schema(prd_info_schema)
          .option('header', 'true')
          .option('dateFormat','yyyy-MM-dd')
          .load('/Volumes/sales_catalog/source_crm/prd_info'))\
          .withColumn('ingestion_date', current_timestamp())
    return df


# source_erp: px_cat_g1v2

px_cat_g1v2_schema = '''
            ID          STRING,
            CAT         STRING,
            SUBCAT      STRING,
            MAINTENANCE STRING
            '''

@dp.table(name='br_px_cat_g1v2')
def br_px_cat_g1v2():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .schema(px_cat_g1v2_schema)
          .option('header', 'true')
          .option('dateFormat','yyyy-MM-dd')
          .load('/Volumes/sales_catalog/source_erp/px_cat_g1v2'))\
          .withColumn('ingestion_date', current_timestamp())
    return df