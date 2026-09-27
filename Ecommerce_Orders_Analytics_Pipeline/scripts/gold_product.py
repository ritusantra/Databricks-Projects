from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# gl_product

@dp.materialized_view(name='sales_catalog.gold.gl_product')
def gl_product():
    prd_info = spark.table('sales_catalog.silver.sl_prd_info')
    px_cat = spark.table('sales_catalog.silver.sl_px_cat_g1v2')

    df = prd_info.join(px_cat, prd_info['cat_id'] == px_cat['ID'], how='left').where(col('prd_id').isNotNull())

    df = df.withColumn('prd_end_dt', date_sub(lead(col('prd_start_dt'),1).over(Window.partitionBy(col('prd_key')).orderBy(col('prd_start_dt'))), 1))

    df = df.withColumn('product_key', row_number().over(Window.orderBy(col('prd_start_dt'),col('prd_key'))))

    df = df.select(col('product_key'),
                   col('prd_id').alias('product_id'),
                   col('prd_key').alias('product_number'),
                   col('prd_nm').alias('product_name'),
                   col('cat_id').alias('category_id'),
                   col('cat').alias('category'),
                   col('subcat').alias('subcategory'),
                   col('maintenance'),
                   col('prd_cost').alias('cost'),
                   col('prd_line').alias('product_line'),
                   col('prd_start_dt').alias('start_date'),
                   col('prd_end_dt').alias('end_date'))

    return df



