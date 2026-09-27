from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# gl_customer

@dp.materialized_view(name='sales_catalog.gold.gl_customer')
def gl_customer():
    cust_info = spark.table('sales_catalog.silver.sl_cust_info')
    cust_az12 = spark.table('sales_catalog.silver.sl_cust_az12')
    loc_a101 = spark.table('sales_catalog.silver.sl_loc_a101')

    df = cust_info.join(cust_az12, cust_info['cst_key'] == cust_az12['cid'], how='left')\
        .join(loc_a101, cust_info['cst_key'] == loc_a101['cid'], how='left')

    df = df.withColumn('latest_cust', row_number().over(Window.partitionBy(col('cst_id')).orderBy(col('cst_create_date').desc())))
    df = df.where((col('latest_cust')==1) & (col('cst_id').isNotNull()))
                    
    df = df.withColumn('gender', when(col('cst_gndr') != 'Unknown', col('cst_gndr')).otherwise(col('gen')))
    df = df.withColumn('customer_key', row_number().over(Window.orderBy(col('cst_id'))))

    df = df.select(col('customer_key'),
                   col('cst_id').alias('customer_id'),
                   col('cst_key').alias('customer_number'),
                   col('cst_firstname').alias('first_name'),
                   col('cst_lastname').alias('last_name'),
                   col('cntry').alias('country'),
                   col('cst_marital_status').alias('marital_status'),
                   col('gender'),
                   col('BDATE').alias('birthdate'),
                   col('cst_create_date').alias('create_date'))

    return df



