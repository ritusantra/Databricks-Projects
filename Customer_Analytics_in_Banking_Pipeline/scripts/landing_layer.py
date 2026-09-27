from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Customer Streaming Table

customer_schema = StructType([
    StructField('customer_id', IntegerType(), True),
    StructField('name', StringType(), True),
    StructField('dob', DateType(), True),
    StructField('gender', StringType(), True),
    StructField('city', StringType(), True),
    StructField('join_date', DateType(), True),
    StructField('status', StringType(), True),
    StructField('email', StringType(), True),
    StructField('phone_number', StringType(), True),
    StructField('preferred_channel', StringType(), True),
    StructField('occupation', StringType(), True),
    StructField('income_range', StringType(), True),
    StructField('risk_segment', StringType(), True)
])

@dp.table(name='landing_customers_incremental')
def landing_customers_incremental():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .option('header', 'true')
          .schema(customer_schema)
          .load('/Volumes/banking/banking_schema/banking_volume/customers/'))
    return df


# Accounts Transactions Streaming Table

accounts_schema = StructType([
    StructField('account_id', IntegerType(), True),
    StructField('customer_id', IntegerType(), True),
    StructField('account_type', StringType(), True),
    StructField('balance', DoubleType(), True),
    StructField('txn_id', IntegerType(), True),
    StructField('txn_date', DateType(), True),
    StructField('txn_type', StringType(), True),
    StructField('txn_amount', DoubleType(), True),
    StructField('txn_channel', StringType(), True),
])

@dp.table(name='landing_accounts_incremental')
def landing_accounts_incremental():
    df = (spark.readStream.format('cloudFiles')
          .option('cloudFiles.format', 'csv')
          .option('cloudFiles.includeExistingFiles', 'true')
          .option('header', 'true')
          .schema(accounts_schema)
          .load('/Volumes/banking/banking_schema/banking_volume/accounts/'))
    return df






