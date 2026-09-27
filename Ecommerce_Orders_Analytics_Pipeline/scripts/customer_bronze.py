from pyspark import pipelines as dp
from pyspark.sql.functions import *


# source_crm: cust_info

cust_info_schema = '''
            cst_id             INT,
            cst_key            STRING,
            cst_firstname      STRING,
            cst_lastname       STRING,
            cst_marital_status STRING,
            cst_gndr           STRING,
            cst_create_date    DATE
            
            '''

@dp.table(name='br_cust_info')
def br_cust_info():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .schema(cust_info_schema)
          .option('header', 'true')
          .option('dateFormat','yyyy-MM-dd')
          .load('/Volumes/sales_catalog/source_crm/cust_info/'))\
          .withColumn('ingestion_date', current_timestamp())
    return df

# source_erp: cust_az12

cust_az12_schema = '''
            CID   STRING,
            BDATE DATE,
            GEN   STRING          
            '''

@dp.table(name='br_cust_az12')
def br_cust_az12():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format','csv')
          .option('cloudFiles.includeExistingFiles','true')
          .schema(cust_az12_schema)
          .option('header', 'true')
          .option('dateFormat','yyyy-MM-dd')
          .load('/Volumes/sales_catalog/source_erp/cust_az12/')\
          .withColumn('ingestion_date', current_timestamp())
          )
    return df

# source_erp: loc_a101

loc_a101_schema = '''
            CID   STRING,
            CNTRY STRING
'''

@dp.table(name='br_loc_a101')
def br_loc_a101():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format','csv')
          .option('cloudFiles.includeExistingFiles','true')
          .schema(loc_a101_schema)
          .option('header', 'true')
          .option('dateFormat','yyyy-MM-dd')
          .load('/Volumes/sales_catalog/source_erp/loc_a101')\
          .withColumn('ingestion_date', current_timestamp())
          )
    return df
