###############################################################################
# Script Name:amtfServiceTc360AcctLoad.py                                     #
# Purpose: To load Tc360 account atributes into hive table                    #
# Created by: Upendra Kumar                                                   #
# Create Date: 10/22/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql.types import FloatType, IntegerType, ArrayType, StringType, DataType, StructField, StructType,TimestampType, DateType, LongType, DecimalType
from pyspark.sql import SparkSession, SQLContext, HiveContext, Window, Row
from pyspark.sql.functions import sum as _sum, mean as _mean, stddev_pop as _stddev, col, coalesce, lit, split, trim,size, lpad, length, to_date, concat, substring, current_date, expr, datediff, udf, array, desc, date_sub, count,col, max, min, to_timestamp, row_number, rank, col, explode, round, current_timestamp, date_format,broadcast, count, regexp_replace, when,collect_set
from math import sqrt, exp, log
from ConfigParser import ConfigParser
from time import *
import commands
import getopt
import sys
import os
sys.dont_write_bytecode = True
import datetime
from dateutil.relativedelta import relativedelta
import amtf_reusable_functions as rFunctions

############################################ Function definition section #####################################################################
## Context Build and Variable Setting
def validateArg():
    runDay = None
    printError = 'spark-submit script_name.py -f <config_file_path>/<config_file_name> -r runDay'
    try:
        opts, args = getopt.getopt(sys.argv[1:], 'f:r:')
    except getopt.error as msg:
        print('Something went wrong!')
        print('Example for entering argument to the script is:')
        sys.exit(printError)

    runDay = opts[1:]
    for opt, arg in opts:
        if (opt == '-f'):
            configFileName = arg
        elif (opt == '-r'):
            runDay = arg
    if (configFileName is None or configFileName == ''):
        print(printError)
        sys.exit('ERROR: Config File Name not provided with argument -f')
    elif (runDay is None or runDay == ''):
        print(printError)
        sys.exit('ERROR: runDay not provided with argument -r')
    return configFileName, runDay


###################################################################################################################
##Validate config file
def validateConfigFile(configFileName):
    if (not (os.path.exists(configFileName))):
        sys.exit('ERROR: The Configuration file ' + configFileName + ' does not exist')
    else:
        config = ConfigParser()
        config.optionxform = str
        config.read(configFileName)
        ## Checking the Config File Sections
        if ('SCHEMA' not in config.sections()):
            sys.exit('ERROR: The Configuration file does not have section: SCHEMA')
        elif ('HIVE' not in config.sections()):
            sys.exit('ERROR: The Configuration file does not have section: HIVE')
        elif ('HDFSPATH' not in config.sections()):
            sys.exit('ERROR: The Configuration file does not have section: HDFSPATH')
        else:
            # check the options in each section
            if ('readModeRawFile' not in config.options('FUNCTIONAL')):
                sys.exit('ERROR: Section FUNCTIONAL does not contain readModeRawFile')
            else:
                readMode = config.get('FUNCTIONAL', 'readModeRawFile')
                readOptions = ['DROPMALFORMED', 'PERMISSIVE']
            if (readMode in readOptions):
                readMode = readMode
            if ('writeModeTokenFile' not in config.options('FUNCTIONAL')):
                sys.exit('ERROR: Section FUNCTIONAL does not contain readModeRawFile')
            else:
                writeMode = config.get('FUNCTIONAL', 'writeModeTokenFile')
                writeOptions = ['overwrite', 'append', 'ignore', 'error']
                if writeMode in writeOptions:
                    writeMode = writeMode
                else:
                    sys.exit('ERROR: input write mode seems invalid, please check back the write mode in config file')
            if ('inputFileDemiliter' not in config.options('SCHEMA')):
                sys.exit('ERROR: Section HIVE does not contain inputFileDemiliter')
            else:
                inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
            if ('runDay' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain runDay')
            else:
                runDay = config.get('HIVE', 'runDay')
            if ('wrksRawUIColumns' not in config.options('SCHEMA')):
                sys.exit('ERROR: Section SCHEMA does not contain wrksRawUIColumns')
            else:
                wrksRawUIColumns = config.get('SCHEMA', 'wrksRawUIColumns')
            if ('falconInjectionDate' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain falconInjectionDate')
            else:
                falconInjectionDate = config.get('HIVE', 'falconInjectionDate')
            if ('pcStmtFact' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain pcStmtFact')
            else:
                pcStmtFact = config.get('HIVE', 'pcStmtFact')
            if ('pcAcctLexid' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain pcAcctLexid')
            else:
                pcAcctLexid = config.get('HIVE', 'pcAcctLexid')
            if ('tc360fcc' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain tc360fcc')
            else:
                tc360fcc = config.get('HDFSPATH', 'tc360fcc')
            if ('colsAcctTable' not in config.options('SCHEMA')):
                sys.exit('ERROR: Section SCHEMA does not contain colsAcctTable')
            else:
                colsAcctTable = config.get('SCHEMA', 'colsAcctTable')
            if ('pcAppFact' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain pcAppFact')
            else:
                pcAppFact = config.get('HIVE', 'pcAppFact')
            if ('falconRawPath' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain falconRawPath')
            else:
                falconRawPath = config.get('HIVE', 'falconRawPath')
            if ('ts797ExtPath' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain ts797ExtPath')
            else:
                ts797ExtPath = config.get('HDFSPATH', 'ts797ExtPath')
            if ('ts323ExtPath' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain ts323ExtPath')
            else:
                ts323ExtPath = config.get('HDFSPATH', 'ts323ExtPath')
            if ('ts257ExtPath' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain ts257ExtPath')
            else:
                ts257ExtPath = config.get('HDFSPATH', 'ts257ExtPath')
            if ('wrksUIRawExtPath' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain wrksUIRawExtPath')
            else:
                wrksUIRawExtPath = config.get('HDFSPATH', 'wrksUIRawExtPath')
            if ('gibberish_corpus' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain gibberish_corpus')
            else:
                gibberish_corpus = config.get('HDFSPATH', 'gibberish_corpus')
            if ('gibberish_threshold' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain gibberish_threshold')
            else:
                gibberish_threshold = config.get('HDFSPATH', 'gibberish_threshold')
            if ('extPathTBAttr' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain extPathTBAttr')
            else:
                extPathTBAttr = config.get('HDFSPATH', 'extPathTBAttr')
            if ('tc360_account_hive' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain tc360_account_hive')
            else:
                tc360_account_hive = config.get('HIVE', 'tc360_account_hive')
            if ('extPathAcct' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain extPathAcct')
            else:
                extPathAcct = config.get('HDFSPATH', 'extPathAcct')
            if ('extPathAcctDelta' not in config.options('HDFSPATH')):
                sys.exit('ERROR: Section HDFSPATH does not contain extPathAcctDelta')
            else:
                extPathAcctDelta = config.get('HDFSPATH', 'extPathAcctDelta')
            if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                sys.exit('ERROR: Section HIVE does not contain edl_amf_falcon_raw')
            else:
                edl_amf_falcon_raw = config.get('HIVE', 'edl_amf_falcon_raw')
            if ('extPathCustomer' not in config.options('HDFSPATH')):
                sys.exit("ERROR: Section HDFSPATH doesn't contain extPathCustomer")
            else:
                extPathCustomer = config.get('HDFSPATH', 'extPathCustomer')
            if ('debug' not in config.options('FUNCTIONAL')):
                sys.exit('ERROR: Section FUNCTIONAL does not contain debug')
            else:
                debug = config.get('FUNCTIONAL', 'debug')
            if ('arr_size' not in config.options('FUNCTIONAL')):
                sys.exit('ERROR: Section FUNCTIONAL does not contain arr_size')
            else:
                arr_size = config.get('FUNCTIONAL', 'arr_size')

        return {

            'readMode': readMode
            , 'inputFileDemiliter': inputFileDemiliter
            , 'writeMode': writeMode
            , 'pcAcctLexid': pcAcctLexid
            , 'falconRawPath': falconRawPath
            , 'tc360_account_hive': tc360_account_hive
            , 'pcStmtFact': pcStmtFact
            , 'pcAppFact': pcAppFact
            , 'tc360fcc': tc360fcc
            , 'ts797ExtPath': ts797ExtPath
            , 'ts323ExtPath': ts323ExtPath
            , 'ts257ExtPath': ts257ExtPath
            , 'wrksUIRawExtPath': wrksUIRawExtPath
            , 'extPathAcct': extPathAcct
            , 'extPathTBAttr': extPathTBAttr
            , 'extPathCustomer': extPathCustomer
            , 'extPathAcctDelta': extPathAcctDelta
            , 'runDay': runDay
            , 'wrksRawUIColumns': wrksRawUIColumns
            , 'falconInjectionDate': falconInjectionDate
            , 'gibberish_corpus': gibberish_corpus
            , 'gibberish_threshold': gibberish_threshold
            , 'edl_amf_falcon_raw': edl_amf_falcon_raw
            , 'colsAcctTable': colsAcctTable
            , 'debug': debug
            , 'arr_size': arr_size
        }


####################################################################################################################
## Create spark session
def createSparkSession():
    spark = SparkSession.builder. \
        enableHiveSupport().appName('TC360AcctLoad').getOrCreate()
    spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
    spark.conf.set('spark.sql.autoBroadcastJoinThreshold', '200000000')
    spark.conf.set('spark.sql.broadcastTimeout', '-1')
    spark.sparkContext.addFile('/data1/EDL/domains/amf_qa/edl_amf_qa/python/amtf_reusable_functions.py')
    spark.sql('SET spark.hadoop.hive.mapred.supports.subdirectories=true')
    spark.sql('SET mapreduce.input.fileinputformat.input.dir.recursive=true')
    status, protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
    spark.sql('add jar ' + protegrityFile)
    spark.sql('add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar')
    spark.sql('add jar /usr/share/java/mysql-connector-java-5.1.17.jar')
    spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
    spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
    spark.sql(
        'add jar /data1/EDL/domains/cdl/synapps/edlservices-crypto-sparkudf-extensions/crypto-sparkudf-extensions-jar-with-dependencies.jar')
    return spark


####################################################################################################################
##Create sql context
def createSQLContext(spark):
    sc = spark.sparkContext
    sqlContext = SQLContext(sc)
    return sqlContext


######################################################################################################################
##Create hive context
def readHiveContext(spark):
    hive = HiveContext(spark)
    return hive


def createSchema(columnNames):
    if (not columnNames):
        sys.exit('The Column Names string is blank. Please provide valid column names')
    else:
        columnStructFields = []
        for column in columnNames.split(','):
            if (column in (
            'MERGER_DATE', 'ACQ_SCORE_STRATEGY_FLIP_DATE', 'ACQ_LASER_STRATEGY_FLIP_DATE', 'ACM_SCORE_FLIP_DATE')):
                columnStructFields.append(StructField(column, LongType(), True))
            elif (column in ('EDL_LOAD_TS', 'load_timestamp')):
                columnStructFields.append(StructField(column, TimestampType(), True))
            elif (column in ('load_time', 'unload_time')):
                columnStructFields.append(StructField(column, DecimalType(13, 0), True))
            elif (column in ('dynamic_field_length')):
                columnStructFields.append(StructField(column, DecimalType(5, 0), True))
            elif (column in ('d_date', 'as_of_date')):
                columnStructFields.append(StructField(column, DateType(), True))
            else:
                columnStructFields.append(StructField(column, StringType(), True))
    schema = StructType(columnStructFields)
    return schema


########################################################################################################################

## Function to read pre-cursors table for lexid,stmt, and app fact data
def readPrecursorTables(pcAcctLexid, pcStmtFact, pcAppFact, spark):
    try:
        lexidDf = spark.read.parquet(pcAcctLexid).drop(*['edl_load_ts'])
        stmtDf = spark.read.parquet(pcStmtFact).drop(*['edl_load_ts'])
        appFactDf = spark.read.parquet(pcAppFact).drop(*['edl_load_ts']).withColumnRenamed('credit_limit_amt','acct_credit_line_orig')

        lexidDf.cache()
        stmtDf.cache()
        appFactDf.cache()
        return lexidDf, stmtDf, appFactDf
    except Exception as e:
        sys.exit('ERROR: while reading pre-cursor tables - ' + str(e))


# Function to read timeboxed attribute
def readTimeBoxedAttributes(extPathTBAttr, runDay, spark):
    try:
        extPathTBAttr = extPathTBAttr + "/as_of_date=" + runDay
        timeBoxedAttrDf = spark.read.parquet(extPathTBAttr).drop(*['as_of_date', 'edl_load_ts'])
        timeBoxedAttrDf.cache()
        return timeBoxedAttrDf
    except Exception as e:
        sys.exit('ERROR: while reading tme boxed attributes - ' + str(e))


def readAcctIncrAttr(extPathAcctDelta, runDay, spark):
    try:
        extPathAcctDelta = extPathAcctDelta + "/as_of_date=" + runDay
        acctIncrAttrDf = spark.read.parquet(extPathAcctDelta).drop(*['as_of_date', 'edl_load_ts'])
        acctIncrAttrDf.cache()
        return acctIncrAttrDf
    except Exception as e:
        sys.exit('ERROR: while account incremental attributes - ' + str(e))


def computeCAAttributes(extPathCustomer, lexidDf, runDay, spark):
    # Computing customer account attributes
    try:
        readCustDf = spark.read.parquet(extPathCustomer).where(col('as_of_date') == lit(runDay))
        lexidDf = lexidDf['cust_lexid', 'orig_account_nbr'].withColumn('cust_lexid', explode(split(regexp_replace(col('cust_lexid').cast('string'), "([^0-9a-zA-Z,])", ""), ",")))
        custAcctAttrDf = lexidDf.join(broadcast(readCustDf), ['cust_lexid'], 'left')
        custAcctAttrDf = custAcctAttrDf.drop(*['cust_lexid'])
        return custAcctAttrDf
    except Exception as e:
        sys.exit("ERROR: while computing customer account attributes - " + str(e))


# formatting df's to use in account attribute computation
def deltaFormatDfs(lexidDf, stmtDf, edl_amf_falcon_raw, falconRawPath, debug, runDay, previousDay, spark):
    try:
        wdSpecAccStatus = Window.partitionBy('orig_account_nbr').orderBy(lit('X'))
        accXref = lexidDf[['orig_account_nbr', 'current_account_nbr', 'customer_acct_nbr', 'external_status']] \
            .withColumn('count', count('current_account_nbr').over(wdSpecAccStatus)) \
            .withColumn('allClsCnt',_sum(expr("case when trim(external_status) not in ('A','') and external_status is not null and orig_account_nbr <> '0000000000000000' then 1 else 0 end")).over(wdSpecAccStatus)) \
            .where(col('count') != col('allClsCnt')) \
            .drop(*['external_status', 'count', 'allClsCnt', 'current_account_nbr']) \
            .dropDuplicates() \
            .withColumnRenamed('customer_acct_nbr', 'current_account_nbr_pty')

        lexidDf = lexidDf.filter("trim(external_status) in ('A','') or external_status is null and orig_account_nbr <> '0000000000000000'") \
            .drop(*['external_status', 'external_status_reason_code']) \
            .dropDuplicates().groupBy('orig_account_nbr') \
            .agg(coalesce(collect_set('lexid1'), array().cast('array<bigint>')).alias('cust_lexid')
                 , coalesce(collect_set('current_account_nbr'), array().cast('array<string>')).alias('current_account_nbr')
                 , coalesce(collect_set('customer_acct_nbr'), array().cast('array<string>')).alias('current_account_nbr_pty')
                 , coalesce(collect_set('acct_bin'), array().cast('array<string>')).alias('acct_bin')
                 , max('sys').alias('sys')
                 , to_date(max('last_plastic_date')).alias('acct_last_card_issue_date')
                 , max('nbr_of_cards_current').alias('acct_total_cards_issued')
                 , min('open_date').alias('acct_open_date')) \
            .withColumn('acct_days_on_book', datediff(lit(runDay), to_date('acct_open_date'))) \
            .withColumn('acct_days_since_last_card', datediff(lit(runDay), to_date('acct_last_card_issue_date'))) \
            .withColumn('acct_card_network', expr("case when sys in ('6362') then 'DISC' \
                                                                        when sys in ('1364','1366','1409','1192','1246','1192','1468','1469','1312','1410','1408','1136','1363','1365','1327','1326','1407','1411','3935') then 'MC' \
                                                                        when sys in ('5156','5166','3179','5160','3941','3394','3394','4543','3659','3665','5168','5931','3895','3894') then 'VISA' \
                                                                        else 'PLCC' end"))

        stmtDf = stmtDf.filter("trim(external_status) in ('A','') or external_status is null") \
            .drop(*['external_status', 'writeoff_amt']) \
            .withColumnRenamed('current_account_nbr', 'stmt_current_account_nbr') \
            .withColumn('acct_open_to_buy', lit(col('credit_limit_amt') - col('curr_mnth_balance_amt'))) \
            .withColumn('acct_util_ratio',lit(col('curr_mnth_balance_amt') / col('credit_limit_amt')))\
            .withColumnRenamed('credit_limit_amt', 'acct_credit_line_curr') \
            .withColumnRenamed('curr_mnth_balance_amt', 'acct_bal') \
            .dropDuplicates()

        lexidDf.cache()
        stmtDf.cache()
        accXref.cache()
        return lexidDf, stmtDf, accXref
    except Exception as e:
        sys.exit('ERROR: while formatting the dataframes as per the script requirements - ' + str(e))


def deriveACI_0_2(df):
    # Logic to derive aci_0_2 field for account table
    try:
        df = df.withColumn('cf_001', col('ACCT_BAL'))\
               .withColumn('cf_002',round(col('ACCT_SALE_AMT_PER_DAY_30_STD'), 6).cast('double'))\
               .withColumn('cf_003', round(col('ACCT_TOP_MCC_90D_AMT_PER_DAY'), 2).cast('double'))\
               .withColumn('cf_004',col('ACCT_AS_AUTH_90day'))\
               .withColumn('cf_005', col('ACCT_FRAUD_CLAIM_NON_TNF_TOT'))\
               .withColumn('cf_006',round(col('ACCT_WORLD_AUTH_PCT_90d'), 2).cast('double'))
        df = df.na.fill({'cf_001': -99999999, 'cf_002': 0, 'cf_003': 0, 'cf_004': 0, 'cf_005': 0, 'cf_006': 0})
        df = df.withColumn('IND_ACCT_AS_AUTH_60D', when(col('ACCT_AS_AUTH_60day') > lit(float(0.5)), 1).otherwise(0)) \
               .withColumn('IND_ACCT_POS81_AUTH_PCT_30D',when(round(col('ACCT_POS81_AUTH_PCT_30D'), 2).cast('double') > lit(float(0.005)), 1).otherwise(0)) \
               .withColumn('IND_ACCT_SALE_PER_DAY_7',when(round(col('ACCT_SALE_PER_DAY_7'), 2).cast('double') > lit(float(0.5)), 1).otherwise(0)) \
               .withColumn('IND_ACCT_DECLINED_AUTH_30D',when(col('ACCT_DECLINED_AUTH_30day') > lit(float(0.5)), 1).otherwise(0)) \
               .withColumn('NUM_INDICATORS', col('IND_ACCT_AS_AUTH_60D') + col('IND_ACCT_POS81_AUTH_PCT_30D') + col('IND_ACCT_SALE_PER_DAY_7') + col('IND_ACCT_DECLINED_AUTH_30D'))
        df = df.withColumn('aci_h', ((col('cf_006') > lit(int(0))) \
                                     & (col('cf_001') < lit(int(41))) \
                                     & (col('cf_001') > lit(float(1.49))) \
                                     & (col('cf_002') < lit(float(2.59))) \
                                     & (col('cf_002') > lit(float(2.33))) \
                                     & ((col('cf_003') < lit(float(0.185))) | (col('cf_003').isNull())) \
                                     & (col('cf_004') > lit(int(0))) \
                                     ))
        df = df.withColumn('aci_m', ((col('cf_006') > 0) & ((col('cf_005') >= lit(float(9.5))) | ((col('cf_005') >= lit(float(5.5))) & (col('NUM_INDICATORS') >= lit(int(2)))))))
        df = df.withColumn('aci_l', ((col('cf_006') > 0) & (col('cf_005') >= lit(float(3.5))) & (col('NUM_INDICATORS') >= lit(int(3)))))
        df = df.withColumn('aci_world', (col('cf_006') > lit(float(0))))
        df = df.withColumn('aci_plcc', col('ACCT_CARD_NETWORK') != 'PLCC')
        df = df.withColumn('aci_0_2', when(col('aci_h'), 5) \
                           .when(col('aci_m'), 4) \
                           .when(col('aci_l'), 3) \
                           .when(col('aci_world'), 2) \
                           .when(col('aci_plcc'), 1) \
                           .otherwise(0))
        df = df.drop(*['cf_001', 'cf_002', 'cf_003', 'cf_004', 'cf_005', 'cf_006', 'aci_h', 'aci_m', 'aci_l', 'aci_world','aci_plcc', 'NUM_INDICATORS', 'IND_ACCT_AS_AUTH_60D', 'IND_ACCT_POS81_AUTH_PCT_30D','IND_ACCT_SALE_PER_DAY_7', 'IND_ACCT_DECLINED_AUTH_30D'])
        df.cache()
        return df
    except Exception as e:
        sys.exit("ERROR: while deriving aci_0_2 attribute - " + str(e))


def combineDf(lexidDf, acctIncrAttrDf, stmtDf, appFactDf, timeBoxedAttrDf, custAcctAttrDf, falconInjectionDate, runDay):
    try:
        finalIncrAttrDf = lexidDf.join(timeBoxedAttrDf, ['orig_account_nbr'], 'left')
        finalIncrAttrDf = finalIncrAttrDf.join(custAcctAttrDf, ['orig_account_nbr'], 'left')
        finalIncrAttrDf = finalIncrAttrDf.join(acctIncrAttrDf, ['orig_account_nbr'], 'left')
        finalIncrAttrDf = finalIncrAttrDf.join(stmtDf, ['orig_account_nbr'], 'left')
        finalIncrAttrDf = finalIncrAttrDf.join(appFactDf, finalIncrAttrDf['orig_account_nbr'] == appFactDf['logical_application_key'], how='left')
        finalIncrAttrDf = finalIncrAttrDf.withColumn('acct_sale_per_day',col('acct_total_sales_cnt') / datediff(lit(runDay), expr("case when acct_open_date<= ' + falconInjectionDate + ' then acct_open_date else ' + falconInjectionDate + ' end"))) \
                                         .withColumn('acct_sale_amt_per_day', col('acct_total_sales_amt') / datediff(lit(runDay), expr("case when acct_open_date<= ' + falconInjectionDate + ' then acct_open_date else ' + falconInjectionDate + ' end")))
        finalIncrAttrDf.cache()
        return finalIncrAttrDf
    except Exception as e:
        sys.exit('ERROR: while combining all dataframes - ' + str(e))


def createLoadReadyDf(finalIncrAttrDf, colsAcctTable, runDay, spark):
    ##Forming final tc360 account level attributes data frame
    try:
        finalIncrAttrDf = deriveACI_0_2(finalIncrAttrDf)
        finalIncrAttrDf.createOrReplaceTempView('v_Tc360AcctAttrDF')
        tc360AcctAttrDF = spark.sql("""select """ + colsAcctTable + """ from v_Tc360AcctAttrDF""") \
            .dropDuplicates() \
            .withColumn('cust_lexid', col('cust_lexid').cast('string')) \
            .withColumn('orig_account_nbr', col('orig_account_nbr').cast('string')) \
            .withColumn('current_account_nbr', col('current_account_nbr').cast('string')) \
            .withColumn('current_account_nbr_pty', col('current_account_nbr_pty').cast('string')) \
            .withColumn('acct_open_date', col('acct_open_date').cast('date')) \
            .withColumn('acct_days_on_book', col('acct_days_on_book').cast('bigint')) \
            .withColumn('acct_days_since_last_card', col('acct_days_since_last_card').cast('bigint')) \
            .withColumn('acct_total_cards_issued', col('acct_total_cards_issued').cast('int')) \
            .withColumn('acct_card_network', col('acct_card_network').cast('string')) \
            .withColumn('acct_total_sales_cnt', col('acct_total_sales_cnt').cast('bigint')) \
            .withColumn('acct_total_sales_amt', round(col('acct_total_sales_amt'), 2).cast("double")) \
            .withColumn('acct_total_sales_amt_avg', round(col('acct_total_sales_amt_avg'), 2).cast("double")) \
            .withColumn('acct_total_sales_amt_std', round(col('acct_total_sales_amt_std'), 2).cast("double")) \
            .withColumn('acct_sale_per_day', round(col('acct_sale_per_day'), 6).cast("double")) \
            .withColumn('acct_sale_amt_per_day', round(col('acct_sale_amt_per_day'), 6).cast("double")) \
            .withColumn('acct_sale_per_day_1', round(col('acct_sale_per_day_1'), 2).cast("double")) \
            .withColumn('acct_sale_per_day_7', round(col('acct_sale_per_day_7'), 2).cast("double")) \
            .withColumn('acct_sale_per_day_30', round(col('acct_sale_per_day_30'), 2).cast("double")) \
            .withColumn('acct_sale_per_day_60', round(col('acct_sale_per_day_60'), 2).cast("double")) \
            .withColumn('acct_sale_per_day_90', round(col('acct_sale_per_day_90'), 2).cast("double")) \
            .withColumn('acct_sale_amt_per_day_1', round(col('acct_sale_amt_per_day_1'), 6).cast("double")) \
            .withColumn('acct_sale_amt_per_day_7', round(col('acct_sale_amt_per_day_7'), 6).cast("double")) \
            .withColumn('acct_sale_amt_per_day_30', round(col('acct_sale_amt_per_day_30'), 6).cast("double")) \
            .withColumn('acct_sale_amt_per_day_60', round(col('acct_sale_amt_per_day_60'), 6).cast("double")) \
            .withColumn('acct_sale_amt_per_day_90', round(col('acct_sale_amt_per_day_90'), 6).cast("double")) \
            .withColumn('acct_credit_line_orig', round(col('acct_credit_line_orig'), 2).cast("double")) \
            .withColumn('acct_credit_line_curr', round(col('acct_credit_line_curr'), 2).cast("double")) \
            .withColumn('acct_bal', round(col('acct_bal'), 2).cast("double")) \
            .withColumn('acct_open_to_buy', round(col('acct_open_to_buy'), 2).cast("double")) \
            .withColumn('acct_util_ratio', round(col('acct_util_ratio'), 2).cast("double")) \
            .withColumn('acct_total_payment', round(col('acct_total_payment'), 2).cast("double")) \
            .withColumn('acct_as_auth_1day', col('acct_as_auth_1day').cast("int")) \
            .withColumn('acct_as_auth_2day', col('acct_as_auth_2day').cast("int")) \
            .withColumn('acct_as_auth_7day', col('acct_as_auth_7day').cast("int")) \
            .withColumn('acct_as_auth_30day', col('acct_as_auth_30day').cast("int")) \
            .withColumn('acct_as_auth_60day', col('acct_as_auth_60day').cast("int")) \
            .withColumn('acct_as_auth_90day', col('acct_as_auth_90day').cast("int")) \
            .withColumn('acct_declined_auth_1day', col('acct_declined_auth_1day').cast("int")) \
            .withColumn('acct_declined_auth_2day', col('acct_declined_auth_2day').cast("int")) \
            .withColumn('acct_declined_auth_7day', col('acct_declined_auth_7day').cast("int")) \
            .withColumn('acct_declined_auth_30day', col('acct_declined_auth_30day').cast("int")) \
            .withColumn('acct_declined_auth_60day', col('acct_declined_auth_60day').cast("int")) \
            .withColumn('acct_declined_auth_90day', col('acct_declined_auth_90day').cast("int")) \
            .withColumn('acct_fraud_claim_ls_tot', col('acct_fraud_claim_ls_tot').cast("int")) \
            .withColumn('acct_fraud_claim_other_tot', col('acct_fraud_claim_other_tot').cast("int")) \
            .withColumn('acct_fraud_claim_cnp_tot', col('acct_fraud_claim_cnp_tot').cast("int")) \
            .withColumn('acct_fraud_claim_tnf_tot', col('acct_fraud_claim_tnf_tot').cast("int")) \
            .withColumn('acct_fraud_claim_non_tnf_tot', col('acct_fraud_claim_non_tnf_tot').cast("int")) \
            .withColumn('acct_total_sales_declined_cnt', col('acct_total_sales_declined_cnt').cast("int")) \
            .withColumn('acct_total_sales_declined_amt', round(col('acct_total_sales_declined_amt'), 2).cast("double")) \
            .withColumn('acct_total_sales_declined_amt_avg',round(col('acct_total_sales_declined_amt_avg'), 2).cast("double")) \
            .withColumn('acct_total_sales_declined_amt_std',round(col('acct_total_sales_declined_amt_std'), 2).cast("double")) \
            .withColumn('acct_fraud_claim_non_tnf_gross_amt',round(col('acct_fraud_claim_non_tnf_gross_amt'), 2).cast("double")) \
            .withColumn('acct_fraud_claim_non_tnf_net_amt',round(col('acct_fraud_claim_non_tnf_net_amt'), 2).cast("double")) \
            .withColumn('acct_fraud_claim_tnf_gross_amt', round(col('acct_fraud_claim_tnf_gross_amt'), 2).cast("double")) \
            .withColumn('acct_fraud_claim_tnf_net_amt', round(col('acct_fraud_claim_tnf_net_amt'), 2).cast("double")) \
            .withColumn('acct_crypto_purchase_1d', col('acct_crypto_purchase_1d').cast('int')) \
            .withColumn('acct_crypto_purchase_7d', col('acct_crypto_purchase_7d').cast('int')) \
            .withColumn('acct_crypto_purchase_amt_1d', round(col('acct_crypto_purchase_amt_1d'), 2).cast('double')) \
            .withColumn('acct_crypto_purchase_amt_7d', round(col('acct_crypto_purchase_amt_7d'), 2).cast('double')) \
            .withColumn('acct_tot_highrisk_purchase_1d', col('acct_tot_highrisk_purchase_1d').cast('int')) \
            .withColumn('acct_tot_highrisk_purchase_7d', col('acct_tot_highrisk_purchase_7d').cast('int')) \
            .withColumn('acct_3ds_attempt_1d', col('acct_3ds_attempt_1d').cast('int')) \
            .withColumn('acct_3ds_cb_1d', col('acct_3ds_cb_1d').cast('int')) \
            .withColumn('acct_3ds_attempt_amt_1d', round(col('acct_3ds_attempt_amt_1d'), 2).cast('double')) \
            .withColumn('acct_3ds_cb_amt_1d', round(col('acct_3ds_cb_amt_1d'), 2).cast('double')) \
            .withColumn('acct_sales_amt_1', round(col('acct_sales_amt_1'), 2).cast('double')) \
            .withColumn('acct_sales_amt_2', round(col('acct_sales_amt_2'), 2).cast('double')) \
            .withColumn('acct_sales_amt_3', round(col('acct_sales_amt_3'), 2).cast('double')) \
            .withColumn('acct_sales_amt_4', round(col('acct_sales_amt_4'), 2).cast('double')) \
            .withColumn('acct_sales_amt_5', round(col('acct_sales_amt_5'), 2).cast('double')) \
            .withColumn('acct_sales_amt_6', round(col('acct_sales_amt_6'), 2).cast('double')) \
            .withColumn('acct_sales_amt_7', round(col('acct_sales_amt_7'), 2).cast('double')) \
            .withColumn('acct_sales_amt_8', round(col('acct_sales_amt_8'), 2).cast('double')) \
            .withColumn('acct_sales_amt_9', round(col('acct_sales_amt_9'), 2).cast('double')) \
            .withColumn('acct_sales_amt_10', round(col('acct_sales_amt_10'), 2).cast('double')) \
            .withColumn('acct_sales_ts_1', col('acct_sales_ts_1').cast('timestamp')) \
            .withColumn('acct_sales_ts_2', col('acct_sales_ts_2').cast('timestamp')) \
            .withColumn('acct_sales_ts_3', col('acct_sales_ts_3').cast('timestamp')) \
            .withColumn('acct_sales_ts_4', col('acct_sales_ts_4').cast('timestamp')) \
            .withColumn('acct_sales_ts_5', col('acct_sales_ts_5').cast('timestamp')) \
            .withColumn('acct_sales_ts_6', col('acct_sales_ts_6').cast('timestamp')) \
            .withColumn('acct_sales_ts_7', col('acct_sales_ts_7').cast('timestamp')) \
            .withColumn('acct_sales_ts_8', col('acct_sales_ts_8').cast('timestamp')) \
            .withColumn('acct_sales_ts_9', col('acct_sales_ts_9').cast('timestamp')) \
            .withColumn('acct_sales_ts_10', col('acct_sales_ts_10').cast('timestamp')) \
            .withColumn('acct_sales_vaa_scr_1', col('acct_sales_vaa_scr_1').cast('int')) \
            .withColumn('acct_sales_vaa_scr_2', col('acct_sales_vaa_scr_2').cast('int')) \
            .withColumn('acct_sales_vaa_scr_3', col('acct_sales_vaa_scr_3').cast('int')) \
            .withColumn('acct_sales_vaa_scr_4', col('acct_sales_vaa_scr_4').cast('int')) \
            .withColumn('acct_sales_vaa_scr_5', col('acct_sales_vaa_scr_5').cast('int')) \
            .withColumn('acct_sales_vaa_scr_6', col('acct_sales_vaa_scr_6').cast('int')) \
            .withColumn('acct_sales_vaa_scr_7', col('acct_sales_vaa_scr_7').cast('int')) \
            .withColumn('acct_sales_vaa_scr_8', col('acct_sales_vaa_scr_8').cast('int')) \
            .withColumn('acct_sales_vaa_scr_9', col('acct_sales_vaa_scr_9').cast('int')) \
            .withColumn('acct_sales_vaa_scr_10', col('acct_sales_vaa_scr_10').cast('int')) \
            .withColumn('acct_sales_r3_scr_1', col('acct_sales_r3_scr_1').cast('int')) \
            .withColumn('acct_sales_r3_scr_2', col('acct_sales_r3_scr_2').cast('int')) \
            .withColumn('acct_sales_r3_scr_3', col('acct_sales_r3_scr_3').cast('int')) \
            .withColumn('acct_sales_r3_scr_4', col('acct_sales_r3_scr_4').cast('int')) \
            .withColumn('acct_sales_r3_scr_5', col('acct_sales_r3_scr_5').cast('int')) \
            .withColumn('acct_sales_r3_scr_6', col('acct_sales_r3_scr_6').cast('int')) \
            .withColumn('acct_sales_r3_scr_7', col('acct_sales_r3_scr_7').cast('int')) \
            .withColumn('acct_sales_r3_scr_8', col('acct_sales_r3_scr_8').cast('int')) \
            .withColumn('acct_sales_r3_scr_9', col('acct_sales_r3_scr_9').cast('int')) \
            .withColumn('acct_sales_r3_scr_10', col('acct_sales_r3_scr_10').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_1', col('acct_sales_mcdi_scr_1').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_2', col('acct_sales_mcdi_scr_2').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_3', col('acct_sales_mcdi_scr_3').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_4', col('acct_sales_mcdi_scr_4').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_5', col('acct_sales_mcdi_scr_5').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_6', col('acct_sales_mcdi_scr_6').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_7', col('acct_sales_mcdi_scr_7').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_8', col('acct_sales_mcdi_scr_8').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_9', col('acct_sales_mcdi_scr_9').cast('int')) \
            .withColumn('acct_sales_mcdi_scr_10', col('acct_sales_mcdi_scr_10').cast('int')) \
            .withColumn('acct_sale_per_day_30_std', round(col('acct_sale_per_day_30_std'), 6).cast('double')) \
            .withColumn('acct_sale_per_day_60_std', round(col('acct_sale_per_day_60_std'), 6).cast('double')) \
            .withColumn('acct_sale_per_day_90_std', round(col('acct_sale_per_day_90_std'), 6).cast('double')) \
            .withColumn('acct_sale_amt_per_day_30_std', round(col('acct_sale_amt_per_day_30_std'), 6).cast('double')) \
            .withColumn('acct_sale_amt_per_day_60_std', round(col('acct_sale_amt_per_day_60_std'), 6).cast('double')) \
            .withColumn('acct_sale_amt_per_day_90_std', round(col('acct_sale_amt_per_day_90_std'), 6).cast('double')) \
            .withColumn('acct_top_mrch_country_1d_amt', round(col('acct_top_mrch_country_1d_amt'), 2).cast('double')) \
            .withColumn('acct_top_mrch_country_1d', col('acct_top_mrch_country_1d').cast('string')) \
            .withColumn('acct_top_mcc_1d', col('acct_top_mcc_1d').cast('string')) \
            .withColumn('acct_top_mcc_7d', col('acct_top_mcc_7d').cast('string')) \
            .withColumn('acct_top_mcc_30d', col('acct_top_mcc_30d').cast('string')) \
            .withColumn('acct_top_mcc_60d', col('acct_top_mcc_60d').cast('string')) \
            .withColumn('acct_top_mcc_90d', col('acct_top_mcc_90d').cast('string')) \
            .withColumn('acct_top_mcc_1d_amt_per_day', round(col('acct_top_mcc_1d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_mcc_7d_amt_per_day', round(col('acct_top_mcc_7d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_mcc_30d_amt_per_day', round(col('acct_top_mcc_30d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_mcc_60d_amt_per_day', round(col('acct_top_mcc_60d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_mcc_90d_amt_per_day', round(col('acct_top_mcc_90d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_pos_1d', col('acct_top_pos_1d').cast('string')) \
            .withColumn('acct_top_pos_7d', col('acct_top_pos_7d').cast('string')) \
            .withColumn('acct_top_pos_30d', col('acct_top_pos_30d').cast('string')) \
            .withColumn('acct_top_pos_60d', col('acct_top_pos_60d').cast('string')) \
            .withColumn('acct_top_pos_90d', col('acct_top_pos_90d').cast('string')) \
            .withColumn('acct_top_pos_1d_amt_per_day', round(col('acct_top_pos_1d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_pos_7d_amt_per_day', round(col('acct_top_pos_7d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_pos_30d_amt_per_day', round(col('acct_top_pos_30d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_pos_60d_amt_per_day', round(col('acct_top_pos_60d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_top_pos_90d_amt_per_day', round(col('acct_top_pos_90d_amt_per_day'), 2).cast('double')) \
            .withColumn('acct_sales_amt_1d_from_newmerchant_90d',round(col('acct_sales_amt_1d_from_newmerchant_90d'), 2).cast('double')) \
            .withColumn('acct_authe_passive_pass_1d', col('acct_authe_passive_pass_1d').cast('int')) \
            .withColumn('acct_authe_passive_pass_7d', col('acct_authe_passive_pass_7d').cast('int')) \
            .withColumn('acct_authe_passive_pass_30d', col('acct_authe_passive_pass_30d').cast('int')) \
            .withColumn('acct_authe_passive_fail_1d', col('acct_authe_passive_fail_1d').cast('int')) \
            .withColumn('acct_authe_passive_fail_7d', col('acct_authe_passive_fail_7d').cast('int')) \
            .withColumn('acct_authe_passive_fail_30d', col('acct_authe_passive_fail_30d').cast('int')) \
            .withColumn('acct_authe_passive_payfone_pass_1d', col('acct_authe_passive_payfone_pass_1d').cast('int')) \
            .withColumn('acct_authe_passive_payfone_pass_7d', col('acct_authe_passive_payfone_pass_7d').cast('int')) \
            .withColumn('acct_authe_passive_payfone_pass_30d', col('acct_authe_passive_payfone_pass_30d').cast('int')) \
            .withColumn('acct_authe_passive_payfone_fail_1d', col('acct_authe_passive_payfone_fail_1d').cast('int')) \
            .withColumn('acct_authe_passive_payfone_fail_7d', col('acct_authe_passive_payfone_fail_7d').cast('int')) \
            .withColumn('acct_authe_passive_payfone_fail_30d', col('acct_authe_passive_payfone_fail_30d').cast('int')) \
            .withColumn('acct_world_auth_pct_1d', round(col('acct_world_auth_pct_1d'), 2).cast('double')) \
            .withColumn('acct_world_auth_pct_7d', round(col('acct_world_auth_pct_7d'), 2).cast('double')) \
            .withColumn('acct_world_auth_pct_30d', round(col('acct_world_auth_pct_30d'), 2).cast('double')) \
            .withColumn('acct_world_auth_pct_60d', round(col('acct_world_auth_pct_60d'), 2).cast('double')) \
            .withColumn('acct_world_auth_pct_90d', round(col('acct_world_auth_pct_90d'), 2).cast('double')) \
            .withColumn('acct_world_auth_amt_pct_1d', round(col('acct_world_auth_amt_pct_1d'), 2).cast('double')) \
            .withColumn('acct_world_auth_amt_pct_7d', round(col('acct_world_auth_amt_pct_7d'), 2).cast('double')) \
            .withColumn('acct_world_auth_amt_pct_30d', round(col('acct_world_auth_amt_pct_30d'), 2).cast('double')) \
            .withColumn('acct_world_auth_amt_pct_60d', round(col('acct_world_auth_amt_pct_60d'), 2).cast('double')) \
            .withColumn('acct_world_auth_amt_pct_90d', round(col('acct_world_auth_amt_pct_90d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_pct_1d', round(col('acct_pos81_auth_pct_1d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_pct_7d', round(col('acct_pos81_auth_pct_7d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_pct_30d', round(col('acct_pos81_auth_pct_30d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_pct_60d', round(col('acct_pos81_auth_pct_60d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_pct_90d', round(col('acct_pos81_auth_pct_90d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_amt_pct_1d', round(col('acct_pos81_auth_amt_pct_1d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_amt_pct_7d', round(col('acct_pos81_auth_amt_pct_7d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_amt_pct_30d', round(col('acct_pos81_auth_amt_pct_30d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_amt_pct_60d', round(col('acct_pos81_auth_amt_pct_60d'), 2).cast('double')) \
            .withColumn('acct_pos81_auth_amt_pct_90d', round(col('acct_pos81_auth_amt_pct_90d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_pct_1d', round(col('acct_cb_auth_pct_1d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_pct_7d', round(col('acct_cb_auth_pct_7d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_pct_30d', round(col('acct_cb_auth_pct_30d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_pct_60d', round(col('acct_cb_auth_pct_60d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_pct_90d', round(col('acct_cb_auth_pct_90d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_amt_pct_1d', round(col('acct_cb_auth_amt_pct_1d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_amt_pct_7d', round(col('acct_cb_auth_amt_pct_7d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_amt_pct_30d', round(col('acct_cb_auth_amt_pct_30d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_amt_pct_60d', round(col('acct_cb_auth_amt_pct_60d'), 2).cast('double')) \
            .withColumn('acct_cb_auth_amt_pct_90d', round(col('acct_cb_auth_amt_pct_90d'), 2).cast('double')) \
            .withColumn('acct_authe_instant_link_pass_1d', col('acct_authe_instant_link_pass_1d').cast('int')) \
            .withColumn('acct_authe_instant_link_pass_7d', col('acct_authe_instant_link_pass_7d').cast('int')) \
            .withColumn('acct_authe_instant_link_pass_30d', col('acct_authe_instant_link_pass_30d').cast('int')) \
            .withColumn('acct_authe_instant_link_fail_1d', col('acct_authe_instant_link_fail_1d').cast('int')) \
            .withColumn('acct_authe_instant_link_fail_7d', col('acct_authe_instant_link_fail_7d').cast('int')) \
            .withColumn('acct_authe_instant_link_fail_30d', col('acct_authe_instant_link_fail_30d').cast('int')) \
            .withColumn('acct_authe_trust_stamp_pass_1d', col('acct_authe_trust_stamp_pass_1d').cast('int')) \
            .withColumn('acct_authe_trust_stamp_pass_7d', col('acct_authe_trust_stamp_pass_7d').cast('int')) \
            .withColumn('acct_authe_trust_stamp_pass_30d', col('acct_authe_trust_stamp_pass_30d').cast('int')) \
            .withColumn('acct_authe_trust_stamp_fail_1d', col('acct_authe_trust_stamp_fail_1d').cast('int')) \
            .withColumn('acct_authe_trust_stamp_fail_7d', col('acct_authe_trust_stamp_fail_7d').cast('int')) \
            .withColumn('acct_authe_trust_stamp_fail_30d', col('acct_authe_trust_stamp_fail_30d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_1d', col('acct_cvv2_invalid_auth_1d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_2d', col('acct_cvv2_invalid_auth_2d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_7d', col('acct_cvv2_invalid_auth_7d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_30d', col('acct_cvv2_invalid_auth_30d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_60d', col('acct_cvv2_invalid_auth_60d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_90d', col('acct_cvv2_invalid_auth_90d').cast('int')) \
            .withColumn('acct_cvv2_invalid_auth_amt_1d', round(col('acct_cvv2_invalid_auth_amt_1d'), 2).cast('double')) \
            .withColumn('acct_cvv2_invalid_auth_amt_2d', round(col('acct_cvv2_invalid_auth_amt_2d'), 2).cast('double')) \
            .withColumn('acct_cvv2_invalid_auth_amt_7d', round(col('acct_cvv2_invalid_auth_amt_7d'), 2).cast('double')) \
            .withColumn('acct_cvv2_invalid_auth_amt_30d', round(col('acct_cvv2_invalid_auth_amt_30d'), 2).cast('double')) \
            .withColumn('acct_cvv2_invalid_auth_amt_60d', round(col('acct_cvv2_invalid_auth_amt_60d'), 2).cast('double')) \
            .withColumn('acct_cvv2_invalid_auth_amt_90d', round(col('acct_cvv2_invalid_auth_amt_90d'), 2).cast('double')) \
            .withColumn('acct_avs_fail_auth_1d', col('acct_avs_fail_auth_1d').cast('int')) \
            .withColumn('acct_avs_fail_auth_2d', col('acct_avs_fail_auth_2d').cast('int')) \
            .withColumn('acct_avs_fail_auth_7d', col('acct_avs_fail_auth_7d').cast('int')) \
            .withColumn('acct_avs_fail_auth_30d', col('acct_avs_fail_auth_30d').cast('int')) \
            .withColumn('acct_avs_fail_auth_60d', col('acct_avs_fail_auth_60d').cast('int')) \
            .withColumn('acct_avs_fail_auth_90d', col('acct_avs_fail_auth_90d').cast('int')) \
            .withColumn('acct_avs_fail_auth_amt_1d', round(col('acct_avs_fail_auth_amt_1d'), 2).cast('double')) \
            .withColumn('acct_avs_fail_auth_amt_2d', round(col('acct_avs_fail_auth_amt_2d'), 2).cast('double')) \
            .withColumn('acct_avs_fail_auth_amt_7d', round(col('acct_avs_fail_auth_amt_7d'), 2).cast('double')) \
            .withColumn('acct_avs_fail_auth_amt_30d', round(col('acct_avs_fail_auth_amt_30d'), 2).cast('double')) \
            .withColumn('acct_avs_fail_auth_amt_60d', round(col('acct_avs_fail_auth_amt_60d'), 2).cast('double')) \
            .withColumn('acct_avs_fail_auth_amt_90d', round(col('acct_avs_fail_auth_amt_90d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_non_tnf_bin6_1d', col('acct_fraud_claim_non_tnf_bin6_1d').cast('int')) \
            .withColumn('acct_fraud_claim_non_tnf_bin6_7d', col('acct_fraud_claim_non_tnf_bin6_7d').cast('int')) \
            .withColumn('acct_fraud_claim_non_tnf_bin6_30d', col('acct_fraud_claim_non_tnf_bin6_30d').cast('int')) \
            .withColumn('acct_fraud_claim_non_tnf_bin6_60d', col('acct_fraud_claim_non_tnf_bin6_60d').cast('int')) \
            .withColumn('acct_fraud_claim_non_tnf_bin6_90d', col('acct_fraud_claim_non_tnf_bin6_90d').cast('int')) \
            .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_1d', round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_1d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_7d',round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_7d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_30d', round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_30d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_60d', round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_60d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_90d', round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_90d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_tnf_bin6_1d', col('acct_fraud_claim_tnf_bin6_1d').cast('int')) \
            .withColumn('acct_fraud_claim_tnf_bin6_7d', col('acct_fraud_claim_tnf_bin6_7d').cast('int')) \
            .withColumn('acct_fraud_claim_tnf_bin6_30d', col('acct_fraud_claim_tnf_bin6_30d').cast('int')) \
            .withColumn('acct_fraud_claim_tnf_bin6_60d', col('acct_fraud_claim_tnf_bin6_60d').cast('int')) \
            .withColumn('acct_fraud_claim_tnf_bin6_90d', col('acct_fraud_claim_tnf_bin6_90d').cast('int')) \
            .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_1d', round(col('acct_fraud_claim_tnf_gross_amt_bin6_1d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_7d', round(col('acct_fraud_claim_tnf_gross_amt_bin6_7d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_30d', round(col('acct_fraud_claim_tnf_gross_amt_bin6_30d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_60d', round(col('acct_fraud_claim_tnf_gross_amt_bin6_60d'), 2).cast('double')) \
            .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_90d', round(col('acct_fraud_claim_tnf_gross_amt_bin6_90d'), 2).cast('double')) \
            .withColumn('cust_bk_loss_cnt_tot', col('cust_bk_loss_cnt_tot').cast('int')) \
            .withColumn('cust_aged_loss_cnt_tot', col('cust_aged_loss_cnt_tot').cast('int')) \
            .withColumn('cust_fraud_loss_cnt_tot', col('cust_fraud_loss_cnt_tot').cast('int')) \
            .withColumn('cust_other_loss_cnt_tot', col('cust_other_loss_cnt_tot').cast('int')) \
            .withColumn('cust_total_loss_cnt_tot', col('cust_total_loss_cnt_tot').cast('int')) \
            .withColumn('cust_fraud_claim_ls_tot', col('cust_fraud_claim_ls_tot').cast('int')) \
            .withColumn('cust_fraud_claim_cnp_tot', col('cust_fraud_claim_cnp_tot').cast('int')) \
            .withColumn('cust_fraud_claim_tnf_tot', col('cust_fraud_claim_tnf_tot').cast('int')) \
            .withColumn('cust_fraud_claim_other_tot', col('cust_fraud_claim_other_tot').cast('int')) \
            .withColumn('cust_fraud_claim_non_tnf_tot', col('cust_fraud_claim_non_tnf_tot').cast('int')) \
            .withColumn('cust_aged_loss_amt_tot', round(col('cust_aged_loss_amt_tot'), 2).cast('double')) \
            .withColumn('cust_bk_loss_amt_tot', round(col('cust_bk_loss_amt_tot'), 2).cast('double')) \
            .withColumn('cust_fraud_loss_amt_tot', round(col('cust_fraud_loss_amt_tot'), 2).cast('double')) \
            .withColumn('cust_other_loss_amt_tot', round(col('cust_other_loss_amt_tot'), 2).cast('double')) \
            .withColumn('cust_total_loss_amt_tot', round(col('cust_total_loss_amt_tot'), 2).cast('double')) \
            .withColumn('cust_multi_account_claim_same_day', col('cust_multi_account_claim_same_day').cast('int')) \
            .withColumn('acct_sale_bin6_1d', col('acct_sale_bin6_1d').cast('int')) \
            .withColumn('acct_sale_bin6_7d', col('acct_sale_bin6_7d').cast('int')) \
            .withColumn('acct_sale_bin6_30d', col('acct_sale_bin6_30d').cast('int')) \
            .withColumn('acct_sale_bin6_60d', col('acct_sale_bin6_60d').cast('int')) \
            .withColumn('acct_sale_bin6_90d', col('acct_sale_bin6_90d').cast('int')) \
            .withColumn('acct_sale_amt_bin6_1d', round(col('acct_sale_amt_bin6_1d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_bin6_7d', round(col('acct_sale_amt_bin6_7d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_bin6_30d', round(col('acct_sale_amt_bin6_30d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_bin6_60d', round(col('acct_sale_amt_bin6_60d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_bin6_90d', round(col('acct_sale_amt_bin6_90d'), 2).cast('double')) \
            .withColumn('acct_cnm_addr_chg_1d',col('acct_cnm_addr_chg_1d').cast('int').alias('acct_cnm_addr_chg_1d')) \
            .withColumn('acct_cnm_addr_chg_7d',col('acct_cnm_addr_chg_7d').cast('int').alias('acct_cnm_addr_chg_7d')) \
            .withColumn('acct_cnm_addr_chg_30d',col('acct_cnm_addr_chg_30d').cast('int').alias('acct_cnm_addr_chg_30d')) \
            .withColumn('acct_cnm_addr_chg_60d',col('acct_cnm_addr_chg_60d').cast('int').alias('acct_cnm_addr_chg_60d')) \
            .withColumn('acct_cnm_addr_chg_90d',col('acct_cnm_addr_chg_90d').cast('int').alias('acct_cnm_addr_chg_90d')) \
            .withColumn('acct_cnm_email_chg_1d',col('acct_cnm_email_chg_1d').cast('int').alias('acct_cnm_email_chg_1d')) \
            .withColumn('acct_cnm_email_chg_7d',col('acct_cnm_email_chg_7d').cast('int').alias('acct_cnm_email_chg_7d')) \
            .withColumn('acct_cnm_email_chg_30d',col('acct_cnm_email_chg_30d').cast('int').alias('acct_cnm_email_chg_30d')) \
            .withColumn('acct_cnm_email_chg_60d',col('acct_cnm_email_chg_60d').cast('int').alias('acct_cnm_email_chg_60d')) \
            .withColumn('acct_cnm_email_chg_90d',col('acct_cnm_email_chg_90d').cast('int').alias('acct_cnm_email_chg_90d')) \
            .withColumn('acct_cnm_home_phn_chg_1d',col('acct_cnm_home_phn_chg_1d').cast('int').alias('acct_cnm_home_phn_chg_1d')) \
            .withColumn('acct_cnm_home_phn_chg_7d',col('acct_cnm_home_phn_chg_7d').cast('int').alias('acct_cnm_home_phn_chg_7d')) \
            .withColumn('acct_cnm_home_phn_chg_30d',col('acct_cnm_home_phn_chg_30d').cast('int').alias('acct_cnm_home_phn_chg_30d')) \
            .withColumn('acct_cnm_home_phn_chg_60d',col('acct_cnm_home_phn_chg_60d').cast('int').alias('acct_cnm_home_phn_chg_60d')) \
            .withColumn('acct_cnm_home_phn_chg_90d',col('acct_cnm_home_phn_chg_90d').cast('int').alias('acct_cnm_home_phn_chg_90d')) \
            .withColumn('acct_cnm_business_phn_chg_1d',col('acct_cnm_business_phn_chg_1d').cast('int').alias('acct_cnm_business_phn_chg_1d')) \
            .withColumn('acct_cnm_business_phn_chg_7d',col('acct_cnm_business_phn_chg_7d').cast('int').alias('acct_cnm_business_phn_chg_7d')) \
            .withColumn('acct_cnm_business_phn_chg_30d',col('acct_cnm_business_phn_chg_30d').cast('int').alias('acct_cnm_business_phn_chg_30d')) \
            .withColumn('acct_cnm_business_phn_chg_60d',col('acct_cnm_business_phn_chg_60d').cast('int').alias('acct_cnm_business_phn_chg_60d')) \
            .withColumn('acct_cnm_business_phn_chg_90d',col('acct_cnm_business_phn_chg_90d').cast('int').alias('acct_cnm_business_phn_chg_90d')) \
            .withColumn('acct_cnm_credit_line_chg_1d',col('acct_cnm_credit_line_chg_1d').cast('int').alias('acct_cnm_credit_line_chg_1d')) \
            .withColumn('acct_cnm_credit_line_chg_7d',col('acct_cnm_credit_line_chg_7d').cast('int').alias('acct_cnm_credit_line_chg_7d')) \
            .withColumn('acct_cnm_credit_line_chg_30d',col('acct_cnm_credit_line_chg_30d').cast('int').alias('acct_cnm_credit_line_chg_30d')) \
            .withColumn('acct_cnm_credit_line_chg_60d',col('acct_cnm_credit_line_chg_60d').cast('int').alias('acct_cnm_credit_line_chg_60d')) \
            .withColumn('acct_cnm_credit_line_chg_90d',col('acct_cnm_credit_line_chg_90d').cast('int').alias('acct_cnm_credit_line_chg_90d')) \
            .withColumn('acct_cnm_rush_plastic_1d',col('acct_cnm_rush_plastic_1d').cast('int').alias('acct_cnm_rush_plastic_1d')) \
            .withColumn('acct_cnm_rush_plastic_7d',col('acct_cnm_rush_plastic_7d').cast('int').alias('acct_cnm_rush_plastic_7d')) \
            .withColumn('acct_cnm_rush_plastic_30d',col('acct_cnm_rush_plastic_30d').cast('int').alias('acct_cnm_rush_plastic_30d')) \
            .withColumn('acct_cnm_rush_plastic_60d',col('acct_cnm_rush_plastic_60d').cast('int').alias('acct_cnm_rush_plastic_60d')) \
            .withColumn('acct_cnm_rush_plastic_90d',col('acct_cnm_rush_plastic_90d').cast('int').alias('acct_cnm_rush_plastic_90d')) \
            .withColumn('acct_cnm_new_plastic_auth_1d',col('acct_cnm_new_plastic_auth_1d').cast('int').alias('acct_cnm_new_plastic_auth_1d')) \
            .withColumn('acct_cnm_new_plastic_auth_7d',col('acct_cnm_new_plastic_auth_7d').cast('int').alias('acct_cnm_new_plastic_auth_7d')) \
            .withColumn('acct_cnm_new_plastic_auth_30d',col('acct_cnm_new_plastic_auth_30d').cast('int').alias('acct_cnm_new_plastic_auth_30d')) \
            .withColumn('acct_cnm_new_plastic_auth_60d',col('acct_cnm_new_plastic_auth_60d').cast('int').alias('acct_cnm_new_plastic_auth_60d')) \
            .withColumn('acct_cnm_new_plastic_auth_90d',col('acct_cnm_new_plastic_auth_90d').cast('int').alias('acct_cnm_new_plastic_auth_90d')) \
            .withColumn('acct_cnm_pin_chg_1d',col('acct_cnm_pin_chg_1d').cast('int').alias('acct_cnm_pin_chg_1d')) \
            .withColumn('acct_cnm_pin_chg_7d',col('acct_cnm_pin_chg_7d').cast('int').alias('acct_cnm_pin_chg_7d')) \
            .withColumn('acct_cnm_pin_chg_30d',col('acct_cnm_pin_chg_30d').cast('int').alias('acct_cnm_pin_chg_30d')) \
            .withColumn('acct_cnm_pin_chg_60d',col('acct_cnm_pin_chg_60d').cast('int').alias('acct_cnm_pin_chg_60d')) \
            .withColumn('acct_cnm_pin_chg_90d',col('acct_cnm_pin_chg_90d').cast('int').alias('acct_cnm_pin_chg_90d')) \
            .withColumn('acct_cnm_mother_maiden_name_chg_1d',col('acct_cnm_mother_maiden_name_chg_1d').cast('int').alias('acct_cnm_mother_maiden_name_chg_1d')) \
            .withColumn('acct_cnm_mother_maiden_name_chg_7d',col('acct_cnm_mother_maiden_name_chg_7d').cast('int').alias('acct_cnm_mother_maiden_name_chg_7d')) \
            .withColumn('acct_cnm_mother_maiden_name_chg_30d',col('acct_cnm_mother_maiden_name_chg_30d').cast('int').alias('acct_cnm_mother_maiden_name_chg_30d')) \
            .withColumn('acct_cnm_mother_maiden_name_chg_60d',col('acct_cnm_mother_maiden_name_chg_60d').cast('int').alias('acct_cnm_mother_maiden_name_chg_60d')) \
            .withColumn('acct_cnm_mother_maiden_name_chg_90d',col('acct_cnm_mother_maiden_name_chg_90d').cast('int').alias('acct_cnm_mother_maiden_name_chg_90d')) \
            .withColumn('acct_cnm_dob_chg_1d',col('acct_cnm_dob_chg_1d').cast('int').alias('acct_cnm_dob_chg_1d')) \
            .withColumn('acct_cnm_dob_chg_7d',col('acct_cnm_dob_chg_7d').cast('int').alias('acct_cnm_dob_chg_7d')) \
            .withColumn('acct_cnm_dob_chg_30d',col('acct_cnm_dob_chg_30d').cast('int').alias('acct_cnm_dob_chg_30d')) \
            .withColumn('acct_cnm_dob_chg_60d',col('acct_cnm_dob_chg_60d').cast('int').alias('acct_cnm_dob_chg_60d')) \
            .withColumn('acct_cnm_dob_chg_90d',col('acct_cnm_dob_chg_90d').cast('int').alias('acct_cnm_dob_chg_90d')) \
            .withColumn('acct_cnm_name_chg_1d',col('acct_cnm_name_chg_1d').cast('int').alias('acct_cnm_name_chg_1d')) \
            .withColumn('acct_cnm_name_chg_7d',col('acct_cnm_name_chg_7d').cast('int').alias('acct_cnm_name_chg_7d')) \
            .withColumn('acct_cnm_name_chg_30d',col('acct_cnm_name_chg_30d').cast('int').alias('acct_cnm_name_chg_30d')) \
            .withColumn('acct_cnm_name_chg_60d',col('acct_cnm_name_chg_60d').cast('int').alias('acct_cnm_name_chg_60d')) \
            .withColumn('acct_cnm_name_chg_90d',col('acct_cnm_name_chg_90d').cast('int').alias('acct_cnm_name_chg_90d')) \
            .withColumn('acct_cnm_ssn_chg_1d',col('acct_cnm_ssn_chg_1d').cast('int').alias('acct_cnm_ssn_chg_1d')) \
            .withColumn('acct_cnm_ssn_chg_7d',col('acct_cnm_ssn_chg_7d').cast('int').alias('acct_cnm_ssn_chg_7d')) \
            .withColumn('acct_cnm_ssn_chg_30d',col('acct_cnm_ssn_chg_30d').cast('int').alias('acct_cnm_ssn_chg_30d')) \
            .withColumn('acct_cnm_ssn_chg_60d',col('acct_cnm_ssn_chg_60d').cast('int').alias('acct_cnm_ssn_chg_60d')) \
            .withColumn('acct_cnm_ssn_chg_90d',col('acct_cnm_ssn_chg_90d').cast('int').alias('acct_cnm_ssn_chg_90d')) \
            .withColumn('acct_cnm_total_1d',col('acct_cnm_total_1d').cast('int').alias('acct_cnm_total_1d')) \
            .withColumn('acct_cnm_total_7d',col('acct_cnm_total_7d').cast('int').alias('acct_cnm_total_7d')) \
            .withColumn('acct_cnm_total_30d',col('acct_cnm_total_30d').cast('int').alias('acct_cnm_total_30d')) \
            .withColumn('acct_cnm_total_60d',col('acct_cnm_total_60d').cast('int').alias('acct_cnm_total_60d')) \
            .withColumn('acct_cnm_total_90d',col('acct_cnm_total_90d').cast('int').alias('acct_cnm_total_90d')) \
            .withColumn('acct_auth_amazon_prime_1d', col('acct_auth_amazon_prime_1d').cast('int')) \
            .withColumn('acct_auth_amazon_prime_7d', col('acct_auth_amazon_prime_7d').cast('int')) \
            .withColumn('acct_auth_amazon_prime_30d', col('acct_auth_amazon_prime_30d').cast('int')) \
            .withColumn('acct_auth_amazon_prime_60d', col('acct_auth_amazon_prime_60d').cast('int')) \
            .withColumn('acct_auth_amazon_prime_90d', col('acct_auth_amazon_prime_90d').cast('int')) \
            .withColumn('acct_auth_amt_amazon_prime_1d', round(col('acct_auth_amt_amazon_prime_1d'), 6).cast('double')) \
            .withColumn('acct_auth_amt_amazon_prime_7d', round(col('acct_auth_amt_amazon_prime_7d'), 6).cast('double')) \
            .withColumn('acct_auth_amt_amazon_prime_30d',round(col('acct_auth_amt_amazon_prime_30d'), 6).cast('double')) \
            .withColumn('acct_auth_amt_amazon_prime_60d',round(col('acct_auth_amt_amazon_prime_60d'), 6).cast('double')) \
            .withColumn('acct_auth_amt_amazon_prime_90d',round(col('acct_auth_amt_amazon_prime_90d'), 6).cast('double')) \
            .withColumn('acct_unique_pos_1d', col('acct_unique_pos_1d').cast('int')) \
            .withColumn('acct_unique_pos_7d', col('acct_unique_pos_7d').cast('int')) \
            .withColumn('acct_unique_pos_30d', col('acct_unique_pos_30d').cast('int')) \
            .withColumn('acct_unique_pos_60d', col('acct_unique_pos_60d').cast('int')) \
            .withColumn('acct_unique_pos_90d', col('acct_unique_pos_90d').cast('int')) \
            .withColumn('acct_unique_mcc_1d', col('acct_unique_mcc_1d').cast('int')) \
            .withColumn('acct_unique_mcc_7d', col('acct_unique_mcc_7d').cast('int')) \
            .withColumn('acct_unique_mcc_30d', col('acct_unique_mcc_30d').cast('int')) \
            .withColumn('acct_unique_mcc_60d', col('acct_unique_mcc_60d').cast('int')) \
            .withColumn('acct_unique_mcc_90d', col('acct_unique_mcc_90d').cast('int')) \
            .withColumn('acct_sale_amt_per_tran_1d', round(col('acct_sale_amt_per_tran_1d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_per_tran_7d', round(col('acct_sale_amt_per_tran_7d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_per_tran_30d', round(col('acct_sale_amt_per_tran_30d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_per_tran_60d', round(col('acct_sale_amt_per_tran_60d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_per_tran_90d', round(col('acct_sale_amt_per_tran_90d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_std_1d', round(col('acct_sale_amt_std_1d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_std_7d', round(col('acct_sale_amt_std_7d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_std_30d', round(col('acct_sale_amt_std_30d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_std_60d', round(col('acct_sale_amt_std_60d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_std_90d', round(col('acct_sale_amt_std_90d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_change_1d', round(col('acct_sale_amt_change_1d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_change_7d', round(col('acct_sale_amt_change_7d'), 2).cast('double')) \
            .withColumn('acct_sale_amt_change_30d', round(col('acct_sale_amt_change_30d'), 2).cast('double')) \
            .withColumn('acct_sale_acceleration_7d', round(col('acct_sale_acceleration_7d'), 2).cast('double')) \
            .withColumn('acct_sale_acceleration_30d', round(col('acct_sale_acceleration_30d'), 2).cast('double')) \
            .withColumn('acct_pos_shift_7d', col('acct_pos_shift_7d').cast('string')) \
            .withColumn('acct_pos_shift_30d', col('acct_pos_shift_30d').cast('string')) \
            .withColumn('acct_pos_shift_60d', col('acct_pos_shift_60d').cast('string')) \
            .withColumn('acct_pos_shift_90d', col('acct_pos_shift_90d').cast('string')) \
            .withColumn('acct_bin', col('acct_bin').cast('string')) \
            .withColumn('acct_last_card_issue_date', col('acct_last_card_issue_date').cast('date')) \
            .withColumn('aci_0_2', col('aci_0_2').cast('int')) \
            .withColumn('edl_load_ts', lit(current_timestamp())) \
            .withColumn('as_of_date', lit(runDay))
        return tc360AcctAttrDF
    except Exception as e:
        sys.exit('ERROR: while creating load ready dataframe - ' + str(e))


#######################################################################################################################
# Function for loading the C360 attributes hive table                                                                 #
#######################################################################################################################
def loadC360AttributeHive(tc360AcctAttrDF, tc360_account_hive, writeMode, spark):
    print('######################## Start loading Tc360 account attribute load ######################################')
    try:
        spark.sql('set hive.exec.dynamic.partition=true')
        spark.sql('set hive.exec.dynamic.partition.mode=nonstrict')
        tc360AcctAttrDF.write.mode(writeMode).insertInto(tc360_account_hive, overwrite=True)
        print(
            '######################## Hive Tc360 account attribute load complete ###########################################')
    except Exception as e:
        sys.exit('ERROR: while loading incremental account attributes into hive - ' + str(e))


########################################################################################################################
## defining main function for the c360 attribute script
def main():
    spark = createSparkSession()
    spark.sparkContext.setLogLevel('ERROR')
    sqlContext = createSQLContext(spark)
    hive = readHiveContext(spark)
    configFileName, runDay = validateArg()
    inputParams = validateConfigFile(configFileName)
    previousDay = rFunctions.getPreviousRunDay(runDay)
    timeBoxedAttrDf = readTimeBoxedAttributes(inputParams['extPathTBAttr'], runDay, spark)
    acctIncrAttrDf = readAcctIncrAttr(inputParams['extPathAcctDelta'], runDay, spark)
    lexidDf, stmtDf, appFactDf = readPrecursorTables(inputParams['pcAcctLexid'], inputParams['pcStmtFact'],inputParams['pcAppFact'], spark)
    lexidDf, stmtDf, accXref = deltaFormatDfs(lexidDf, stmtDf, inputParams['edl_amf_falcon_raw'],inputParams['falconRawPath'], inputParams['debug'], runDay, previousDay,spark)
    custAcctAttrDf = computeCAAttributes(inputParams['extPathCustomer'], lexidDf, runDay, spark)
    acctAttrDf = combineDf(lexidDf, acctIncrAttrDf, stmtDf, appFactDf, timeBoxedAttrDf, custAcctAttrDf,inputParams['falconInjectionDate'], runDay)
    tc360AcctAttrDF = createLoadReadyDf(acctAttrDf, inputParams['colsAcctTable'], runDay, spark)
    loadC360AttributeHive(tc360AcctAttrDF, inputParams['tc360_account_hive'], inputParams['writeMode'], spark)


if __name__ == '__main__':
    main()
