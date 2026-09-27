from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *


# Customer Streaming Table

expect_all_or_fail = {
    'customer_id_valid': 'customer_id IS NOT NULL',
    'customer_name_valid': 'name IS NOT NULL'
}
expect_all_or_drop = {
    'dob_valid': 'dob IS NOT NULL',
    'city_valid': 'city IS NOT NULL',
    'join_date_valid': 'join_date IS NOT NULL',
    'email_valid': r"email IS NOT NULL AND email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}$'",
    'phone_valid': 'phone_number IS NOT NULL',
    'channel_valid': 'preferred_channel IS NOT NULL',
    'occupation_valid': 'occupation IS NOT NULL',
    'income_valid': 'income_range IS NOT NULL',
    'risk_segment_valid': 'risk_segment IS NOT NULL',
    'status_valid': 'status IS NOT NULL'
}

@dp.table(name='bronze_customer_ingestion')
@dp.expect_all_or_fail(expect_all_or_fail)
@dp.expect_all_or_drop(expect_all_or_drop)
@dp.expect('gender_valid', 'gender IS NOT NULL')
def bronze_customer_ingestion():
    df = spark.readStream.table('landing_customers_incremental')
    df = df.withColumn('name',upper(col('name')))
    df = df.withColumn('email',lower(col('email')))
    df = df.withColumn('occupation',upper(col('occupation')))
    df = df.withColumn('city',upper(col('city')))
    df = df.withColumn('income_range',upper(col('income_range')))
    df = df.withColumn('risk_segment',upper(col('risk_segment')))
    df = df.withColumn('preferred_channel',upper(col('preferred_channel')))
    df = df.withColumn('gender',when(col('gender') == 'M', 'MALE').when(col('gender') == 'F', 'FEMALE').otherwise('UNKNOWN'))
    df = df.withColumn('status',when((col('status').isNull()) | (trim(col('status'))==""), 'INACTIVE').otherwise(col('status')))
    df = df.withColumn('phone_number', trim(col('phone_number')))  
    df = df.withColumn('phone_number', regexp_replace(col('phone_number'),r"[^0-9]",""))
    df = df.where(col('phone_number').rlike(r"^44\d{10}$"))
    df = df.withColumn('preferred_channel', regexp_replace('preferred_channel','Onlne','Online'))
    return df


# Accounts Transactions Streaming Table

expect_all_or_fail = {
    'account_id_valid' : 'account_id IS NOT NULL',
    'customer_id_valid' : 'customer_id IS NOT NULL'
}

expect_or_drop = {
    'txn_id_valid' : 'txn_id IS NOT NULL',
    'account_type_valid' : 'account_type IS NOT NULL',
    'balance_valid': 'balance IS NOT NULL',
    'txn_date_valid' : 'txn_date IS NOT NULL',
    'txn_amount_valid' : 'txn_amount IS NOT NULL',
    'txn_channel_valid' : 'txn_channel IS NOT NULL'

}


@dp.table(name='bronze_accounts_ingestion')
@dp.expect_all_or_fail(expect_all_or_fail)
@dp.expect_all_or_drop(expect_or_drop)
def bronze_accounts_ingestion():
    df = spark.readStream.table('landing_accounts_incremental')
    df = df.withColumn('account_type',upper(col('account_type')))
    df = df.withColumn('txn_channel',upper(col('txn_channel')))
    df = df.withColumn('txn_type',upper(col('txn_type')))
    df = df.withColumn('txn_type', when(col('txn_type')=='DEBITT','DEBIT').when(col('txn_type')=='CREDIIT','CREDIT').otherwise(col('txn_type')))
    return df












