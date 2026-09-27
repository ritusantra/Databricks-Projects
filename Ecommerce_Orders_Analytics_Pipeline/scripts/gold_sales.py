from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import *

# gl_sales

@dp.materialized_view(name='sales_catalog.gold.gl_sales')
def gl_sales():
    sl_sales = spark.table('sales_catalog.silver.sl_sales_deatils')
    cust = spark.table('sales_catalog.gold.gl_customer')
    prd = spark.table('sales_catalog.gold.gl_product')

    df = sl_sales.join(prd, sl_sales['sls_prd_key'] == prd['product_number'], how='left')\
        .join(cust, sl_sales['sls_cust_id'] == cust['customer_id'], how='left')

    df = df.select(col('sls_ord_num').alias('order_number'),
                   col('product_key'),
                   col('customer_key'),
                   col('sls_order_dt').alias('order_date'),
                   col('sls_ship_dt').alias('shipping_date'),
                   col('sls_due_dt').alias('due_date'),
                   col('sls_sales').alias('sales_amount'),
                   col('sls_quantity').alias('quantity'),
                   col('sls_price').alias('price')
                    )


    return df 

