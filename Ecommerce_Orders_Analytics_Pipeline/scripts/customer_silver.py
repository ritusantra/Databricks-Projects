from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import Window

# br_cust_info -> sl_cust_info

cust_info_val = """
    cst_id IS NOT NULL AND
    cst_key IS NOT NULL
"""

@dp.table(name='sl_cust_info')
def sl_cust_info():
    df = spark.readStream.table('sales_catalog.bronze.br_cust_info')
                 
    df = df.withColumns({
        'cst_firstname' : trim(col('cst_firstname')),
        'cst_lastname' : trim(col('cst_lastname')),
        'cst_marital_status' : when(upper(trim(col('cst_marital_status')))=='S','Single').
                                when(upper(trim(col('cst_marital_status')))=='M','Married').
                                otherwise('Unknown'),
        'cst_gndr' : when(upper(trim(col('cst_gndr')))=='F','Female').
                    when(upper(trim(col('cst_gndr')))=='M','Male').
                    otherwise('Unknown'),
        'modified_date': current_timestamp()        
    })

    return df.where(cust_info_val)

# br_cust_info -> qtr_sl_cust_info

@dp.table(name='qtr_sl_cust_info')
def qtr_br_cust_info():
    df = spark.readStream.table('sales_catalog.bronze.br_cust_info')
    df = df.withColumn('quarantined_date',current_timestamp())
    return df.where(f'NOT({cust_info_val})')


# br_cust_az12 -> sl_cust_az12

cust_az12_val = """cid IS NOT NULL"""

@dp.table(name='sl_cust_az12')
def sl_cust_az12():
    df = spark.readStream.table('sales_catalog.bronze.br_cust_az12')

    df = df.withColumns({
        'CID': when(col('CID').like('NAS%'),substring(col('CID'),4,length(col('CID')))).otherwise(col('CID')),
        'BDATE': when(col('BDATE')>current_date(),None).otherwise(col('BDATE')),
        'GEN': when((upper(trim(col('GEN')))=='F') | (upper(trim(col('GEN')))=='FEMALE'), 'Female').
            when((upper(trim(col('GEN')))=='M') | (upper(trim(col('GEN')))=='Male'), 'Male').
            otherwise('Unknown'),
        'modified_date': current_timestamp()

    })

    return df.where(cust_az12_val)

# br_cust_az12 -> qtr_sl_cust_az12

@dp.table(name='qtr_sl_cust_az12')
def qtr_sl_cust_az12():
    df = spark.readStream.table('sales_catalog.bronze.br_cust_az12')
    df = df.withColumn('quarantined_date',current_timestamp())
    return df.where(f'NOT({cust_az12_val})')

# br_loc_a101 -> sl_loc_a101

loc_val = """cid IS NOT NULL"""

@dp.table(name='sl_loc_a101')
def sl_loc_a101():
    df = spark.readStream.table('sales_catalog.bronze.br_loc_a101')

    df = df.withColumns({
        'CID': regexp_replace(col('CID'),'-',''),
        'CNTRY': (when(trim(col('CNTRY'))=='DE','Germany').
                when((trim(col('CNTRY'))=='US')|(trim(col('CNTRY'))=='USA'),'United States').
                when((trim(col('CNTRY'))=='')|(trim(col('CNTRY')).isNull()),'Unknown').
                otherwise(trim(col('CNTRY')))),
        'modified_date': current_timestamp()
    })

    return df.where(loc_val)


# br_loc_a101 -> qtr_sl_loc_a101

@dp.table(name='qtr_sl_loc_a101')
def qtr_sl_loc_a101():
    df = spark.readStream.table('sales_catalog.bronze.br_loc_a101')
    df = df.withColumn('quarantined_date',current_timestamp())
    return df.where(f'NOT({loc_val})')









