from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# br_sales_details --> sl_sales_deatils

sales_val = """sls_ord_num IS NOT NULL AND
            sls_prd_key IS NOT NULL AND
            sls_cust_id IS NOT NULL
"""

@dp.table(name='sl_sales_deatils')
def sl_sales_deatils():
    df = spark.readStream.table('sales_catalog.bronze.br_sales_details')

    df = df.withColumns({

        'sls_order_dt' : when((
                            (col('sls_order_dt')==0) | 
                            (length(col('sls_order_dt').cast('string')) != 8)),None).
                        otherwise(to_date(col('sls_order_dt'),'yyyyMMdd')),

        'sls_ship_dt' : when((
                            (col('sls_ship_dt')==0) |
                            (length(col('sls_ship_dt').cast('string')) != 8)),None).
                        otherwise(to_date(col('sls_ship_dt'),'yyyyMMdd')),

        'sls_due_dt' : when((
                            (col('sls_due_dt')==0) |
                            (length(col('sls_due_dt').cast('string')) != 8)),None).
                        otherwise(to_date(col('sls_due_dt'),'yyyyMMdd')),

        'sls_sales' : when((
                           (col('sls_sales').isNull()) |
                           (col('sls_sales') <= 0) |
                           (col('sls_sales') != col('sls_quantity')*abs(col('sls_price')))
                           ), 
                           col('sls_quantity')*abs(col('sls_price'))
                           ).otherwise(col('sls_sales')),

        'modified_date' : current_timestamp()

    })

    return df.where(sales_val)

# br_sales_details --> qtr_sl_sales_deatils

@dp.table(name='qtr_sl_sales_deatils')
def qtr_sl_sales_deatils():
    df = spark.readStream.table('sales_catalog.bronze.br_sales_details')
    df = df.withColumn('quarantined_date',current_timestamp())
    return df.where(f'NOT({sales_val})')
                       





