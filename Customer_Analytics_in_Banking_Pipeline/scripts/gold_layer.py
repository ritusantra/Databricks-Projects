from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

@dp.materialized_view(name='gold_cust_txn')
def gold_cust_txn():
    cust = spark.table('silver_customer_trf_scd')
    txn = spark.table('silver_accounts_trf')
    df = cust.join(txn,on='customer_id',how='inner')
    return df


@dp.materialized_view(name='gold_cust_txn_agg')
def gold_cust_txn_agg():
    df = spark.table('gold_cust_txn')
    df = df.groupBy('customer_id','name','gender','city','status',
                    'income_range', 'risk_segment', 'customer_age', 'tenure').agg(
                            countDistinct('account_id').alias('accounts'),
                            count('txn_id').alias('transactions'),
                            round(avg('txn_amount'),2).alias('avg_txn_amt'),
                            min('txn_date').alias('first_txn_dt'),
                            max('txn_date').alias('last_txn_dt')
                            )
    return df




