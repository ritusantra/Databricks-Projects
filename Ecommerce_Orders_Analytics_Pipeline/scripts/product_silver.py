from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# br_prd_info --> sl_prd_info

prd_info_val = """prd_id IS NOT NULL AND 
    prd_key IS NOT NULL"""


@dp.table(name='sl_prd_info')
def sl_prd_info():
    df = spark.readStream.table('sales_catalog.bronze.br_prd_info')
    
    df = df.withColumns(
        {
            'cat_id' : regexp_replace(substring(col('prd_key'),1,5), '-', '_'),
            'prd_key' : substring(col('prd_key'), 7, length(col('prd_key'))),
            'prd_cost' : when(col('prd_cost').isNull(),0).otherwise(col('prd_cost')),
            'prd_line' : (when(upper(trim(col('prd_line')))=='M','Mountain').
                            when(upper(trim(col('prd_line')))=='R','Road').
                            when(upper(trim(col('prd_line')))=='S','Other Sales').
                            when(upper(trim(col('prd_line')))=='T','Touring').
                            otherwise('N/A')),
            'modified_date' : current_timestamp()
        }
    )

    return df.where(prd_info_val)

# br_prd_info --> qtr_sl_prd_info

@dp.table(name='qtr_sl_prd_info')
def qtr_sl_prd_info():
    df = spark.readStream.table('sales_catalog.bronze.br_prd_info')
    df = df.withColumn('quarantined_date', current_timestamp())
    return df.where(f'NOT({prd_info_val})')


# br_px_cat_g1v2 -> sl_px_cat_g1v2

cat_val = """ID IS NOT NULL"""

@dp.table(name='sl_px_cat_g1v2')
def sl_px_cat_g1v2():
    df = spark.readStream.table('sales_catalog.bronze.br_px_cat_g1v2')

    df = df.withColumns(
        {
            'modified_date' : current_timestamp()
        })

    return df.where(cat_val)


# br_px_cat_g1v2 -> qtr_sl_px_cat_g1v2

@dp.table(name='qtr_sl_px_cat_g1v2')
def qtr_sl_prd_info():
    df = spark.readStream.table('sales_catalog.bronze.br_px_cat_g1v2')
    df = df.withColumn('quarantined_date', current_timestamp())
    return df.where(f'NOT({cat_val})')
    







