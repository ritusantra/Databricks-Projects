from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Customer Data Transformation

@dp.table(name='silver_customer_trf')
def silver_customer_trf():
    df = spark.readStream.table('bronze_customer_ingestion')
    df = df.withColumn('customer_age', when(col('dob').isNotNull(),floor(months_between(current_date(),col('dob'))/12)).otherwise('None'))
    df = df.withColumn('tenure', when(col('join_date').isNotNull(), datediff(current_date(),col('join_date'))).otherwise('None'))
    df = df.withColumn('invalid_dob', when((col('dob')>current_date()) | (col('dob') < lit('1900-01-01')),lit('Y')).otherwise(lit('N')))
    df = df.withColumn('transformation_date', current_timestamp())
    return df

dp.create_streaming_table(name='silver_customer_trf_scd')
dp.create_auto_cdc_flow(
    target = 'silver_customer_trf_scd',
    source = 'silver_customer_trf',
    keys = ['customer_id'],
    sequence_by = col('transformation_date'),
    except_column_list = ['transformation_date'],
    stored_as_scd_type = 2)

# Accounts Transactions Data Transformation

@dp.table(name='silver_accounts_trf')
def silver_accounts_trf():
    df = spark.readStream.table('bronze_accounts_ingestion')
    df = df.withColumn('channel_type',when(col('txn_channel').isin('ATM','BRANCH'),'PHYSICAL').otherwise('DIGITAL'))
    df = df.withColumn('txn_year', year(col('txn_date'))).withColumn('txn_month', month(col('txn_date'))).withColumn('txn_day',dayofmonth(col('txn_date')))
    df = df.withColumn('transformation_date', current_timestamp())
    return df


