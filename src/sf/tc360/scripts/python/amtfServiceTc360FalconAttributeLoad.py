###############################################################################
# Script Name:amtfServiceTc360FalconAttributeLoad.py                          #
# Purpose: To calculate falcon attributes and load into EDL                   #
# Created by: Upendra Kumar                                                   #
# Create Date: 02/28/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql.types import FloatType,IntegerType,ArrayType,StringType,DataType,StructField,StructType
from pyspark.sql import SparkSession,SQLContext,HiveContext,Window,Row
from pyspark.sql.functions import sum as _sum,mean as _mean, stddev_pop as _stddev,col,coalesce,lit,split,trim,size,lpad,length,to_date,concat,substring,current_date,expr,datediff,udf,array,desc,date_sub,count,collect_list,max,min,to_timestamp,row_number,rank,collect_set,explode,explode_outer,round,current_timestamp,broadcast,regexp_replace,when,countDistinct
from math import sqrt,exp,log
from ConfigParser import ConfigParser
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
    printError = "spark-submit script_name.py -f <config_file_path>/<config_file_name> -r runDay"
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:r:")
    except getopt.error as msg:
        print("Something went wrong!")
        print("Example for entering argument to the script is:")
        sys.exit(printError)

    runDay = opts[1:]
    for opt, arg in opts:
        if(opt == "-f"):
            configFileName = arg
        elif(opt == "-r"):
            runDay = arg
    if(configFileName is None or configFileName == ''):
        print(printError)
        sys.exit("ERROR: Config File Name not provided with argument -f")
    elif(runDay is None or runDay == ''):
        print(printError)
        sys.exit("ERROR: runDay not provided with argument -r")
    return configFileName, runDay

###################################################################################################################
##Validate config file
def validateConfigFile(configFileName):
    if(not(os.path.exists(configFileName))):
        sys.exit("ERROR: The Configuration file " + configFileName + " doesn't exist")
    else:
        config = ConfigParser()
        config.optionxform = str
        config.read(configFileName)
        ## Checking the Config File Sections
        if('SCHEMA' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: SCHEMA")
        elif('HIVE' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: HIVE")
        elif('HDFSPATH' not in config.sections()):
            sys.exit("ERROR: The Configuration file doesn't have section: HDFSPATH")
        else:
                # check the options in each section
                if('readModeRawFile' not in config.options('FUNCTIONAL')):
                    sys.exit("ERROR: Section FUNCTIONAL doesn't contain readModeRawFile")
                else:
                    readMode = config.get('FUNCTIONAL', 'readModeRawFile')
                    readOptions = ['DROPMALFORMED', 'PERMISSIVE']
                if (readMode in readOptions):
                    readMode = readMode
                if ('writeModeTokenFile' not in config.options('FUNCTIONAL')):
                   sys.exit("ERROR: Section FUNCTIONAL doesn't contain readModeRawFile")
                else:
                     writeMode = config.get('FUNCTIONAL','writeModeTokenFile')
                     writeOptions = ['overwrite','append','ignore','error']
                     if writeMode in writeOptions:
                        writeMode = writeMode
                     else:
                       sys.exit("ERROR: input write mode seems invalid, please check back the write mode in config file")
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section HIVE doesn't contain inputFileDemiliter")
                else:
                    inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
                if ('runDay' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain runDay")
                else:
                    runDay = config.get('HIVE', 'runDay')
                if ('pcAcctLexid' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcAcctLexid")
                else:
                    pcAcctLexid = config.get('HIVE', 'pcAcctLexid')
                if ('falconRawPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain falconRawPath")
                else:
                    falconRawPath = config.get('HIVE', 'falconRawPath')
                if ('falcon_hive_table' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain falcon_hive_table")
                else:
                    falcon_hive_table = config.get('HIVE','falcon_hive_table')
                if ('gibberish_corpus' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain gibberish_corpus")
                else:
                    gibberish_corpus = config.get('HDFSPATH','gibberish_corpus')
                if ('gibberish_threshold' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain gibberish_threshold")
                else:
                    gibberish_threshold = config.get('HDFSPATH','gibberish_threshold')
                if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_amf_falcon_raw")
                else:
                    edl_amf_falcon_raw = config.get('HIVE','edl_amf_falcon_raw')
                if ('extFalconAttr' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extFalconAttr")
                else:
                    extFalconAttr = config.get('HDFSPATH','extFalconAttr')
                if ('extGibberishPath' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HIVE doesn't contain extGibberishPath")
                else:
                    extGibberishPath = config.get('HDFSPATH','extGibberishPath')
                if ('debug' not in config.options('FUNCTIONAL')):
                  sys.exit("ERROR: Section FUNCTIONAL doesn't contain debug")
                else:
                    debug = config.get('FUNCTIONAL','debug')
                
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'pcAcctLexid': pcAcctLexid
                            ,'falconRawPath': falconRawPath
                            ,'falcon_hive_table': falcon_hive_table
                            ,'runDay': runDay
                            ,'gibberish_corpus': gibberish_corpus
                            ,'gibberish_threshold': gibberish_threshold
                            ,'edl_amf_falcon_raw': edl_amf_falcon_raw
                            ,'extGibberishPath': extGibberishPath
                            ,'extFalconAttr': extFalconAttr
                            ,'debug': debug
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('tc360_falcon_attribute_app').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.conf.set('spark.sql.broadcastTimeout','-1')
 spark.sparkContext.addFile("/data1/EDL/domains/amf_qa/edl_amf_qa/python/amtf_reusable_functions.py")
 spark.sql("SET spark.hadoop.hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar")
 spark.sql("add jar /usr/share/java/mysql-connector-java-5.1.17.jar")
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'") 
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

##############################################################
## Function to creating schema                               #
##############################################################
def createSchema(columnNames):
    if (not columnNames):
        sys.exit("The Column Names string is blank. Please provide valid column names")
    else:
        columnStructFields = []
        for column in columnNames.split(","):
           if (column in ('MERGER_DATE','ACQ_SCORE_STRATEGY_FLIP_DATE','ACQ_LASER_STRATEGY_FLIP_DATE','ACM_SCORE_FLIP_DATE')):
              columnStructFields.append(StructField(column, LongType(), True))
           elif (column in ('EDL_LOAD_TS','load_timestamp')):
              columnStructFields.append(StructField(column, TimestampType(), True))
           elif (column in ('load_time','unload_time')):
              columnStructFields.append(StructField(column,DecimalType(13,0),True))
           elif (column in ('dynamic_field_length')):
              columnStructFields.append(StructField(column,DecimalType(5,0),True))
           elif(column in ('d_date','as_of_date')):
              columnStructFields.append(StructField(column,DateType(),True))
           else:
              columnStructFields.append(StructField(column, StringType(), True))
    schema = StructType(columnStructFields)
    return schema
########################################################################################################################

## Function to compute falcon attributes 
def computeFalconAttributes(readFalconDf,lexidDf,runDay):
 try:
  # joining falcon with accxref to bring orig_account_nbr in the aggregation
  falconAggDf = lexidDf.join(readFalconDf, on = (lexidDf["current_account_nbr_pty"] == readFalconDf["ffsl_account_number"]), how = "inner")
  falconAttrDf = falconAggDf.groupBy("orig_account_nbr")\
                                 .agg(
                                  _sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then 1 else 0 end")).alias("acct_total_sales_cnt")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount else 0 end")).alias("acct_total_sales_amt")\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount end")).alias("acct_total_sales_amt_avg")\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias('acct_gibberish_mrch_auth_1d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 2 then 1 else 0 end")).alias('acct_gibberish_mrch_auth_2d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias('acct_gibberish_mrch_auth_7d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")).alias('acct_gibberish_mrch_auth_30d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")).alias('acct_gibberish_mrch_auth_60d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")).alias('acct_gibberish_mrch_auth_90d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias('acct_gibberish_mrch_auth_amt_1d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 2 then tran_amount else 0 end")).alias('acct_gibberish_mrch_auth_amt_2d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")).alias('acct_gibberish_mrch_auth_amt_7d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")).alias('acct_gibberish_mrch_auth_amt_30d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")).alias('acct_gibberish_mrch_auth_amt_60d')\
                                 ,_sum(expr("case when gibberish_flag == true and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")).alias('acct_gibberish_mrch_auth_amt_90d')\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias("acct_auth_amazon_prime_1d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias("acct_auth_amazon_prime_7d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")).alias("acct_auth_amazon_prime_30d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")).alias("acct_auth_amazon_prime_60d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")).alias("acct_auth_amazon_prime_90d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias("acct_auth_amt_amazon_prime_1d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")).alias("acct_auth_amt_amazon_prime_7d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")).alias("acct_auth_amt_amazon_prime_30d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")).alias("acct_auth_amt_amazon_prime_60d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') and lower(trim(ptyUnprotectStr(ffsl_mrch_nm,'dtStrNPL'))) = 'amazon prime' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")).alias("acct_auth_amt_amazon_prime_90d")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias("acct_sale_per_day_1")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias("acct_sale_amt_per_day_1")\
                                 ,_sum(expr("case when  ffsl_fraud_trancode = 'AS' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias("acct_as_auth_1day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode = 'AS' and datediff(to_date('" + runDay + "'),tran_date) <= 2 then 1 else 0 end")).alias("acct_as_auth_2day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode = 'AS' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias("acct_as_auth_7day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode = 'AS' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")).alias("acct_as_auth_30day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode = 'AS' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")).alias("acct_as_auth_60day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode = 'AS' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")).alias("acct_as_auth_90day")\
                                 ,_sum(expr("case when  ffsl_fraud_trancode in ('MD','CD','DP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias("acct_declined_auth_1day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') and datediff(to_date('" + runDay + "'),tran_date) <= 2 then 1 else 0 end")).alias("acct_declined_auth_2day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias("acct_declined_auth_7day")\
                                 ,_sum(expr("case when  ffsl_fraud_trancode in ('MD','CD','DP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")).alias("acct_declined_auth_30day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")).alias("acct_declined_auth_60day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MD','CD','DP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")).alias("acct_declined_auth_90day")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP') then ffsl_total_pay_amt else 0 end")).alias("acct_total_payment")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('DP','MD','CD') then 1 else 0 end")).alias("acct_total_sales_declined_cnt")\
                                 ,_sum(expr("case when ffsl_fraud_trancode in ('DP','MD','CD') then tran_amount else 0 end")).alias("acct_total_sales_declined_amt")\
                                 ,_sum(expr("case when ffsl_spcl_cndt_in = '7' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias("acct_crypto_purchase_1d")\
                                 ,_sum(expr("case when ffsl_spcl_cndt_in = '7' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias("acct_crypto_purchase_7d")\
                                 ,_sum(expr("case when ffsl_spcl_cndt_in = '7' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias("acct_crypto_purchase_amt_1d")\
                                 ,_sum(expr("case when ffsl_spcl_cndt_in = '7' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")).alias("acct_crypto_purchase_amt_7d")\
                                 ,_sum(expr("case when (ffsl_spcl_cndt_in is not null or trim(ffsl_spcl_cndt_in) !='' ) and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias("acct_tot_highrisk_purchase_1d")\
                                 ,_sum(expr("case when (ffsl_spcl_cndt_in is not null or trim(ffsl_spcl_cndt_in) !='' ) and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias("acct_tot_highrisk_purchase_7d")\
                                 ,_sum(expr("case when (substr(upper(ffsl_mc_aav_ctrl),1,1) = 'H' or substr(upper(ffsl_mc_aav_ctrl),1,2) in ('KC','KE') or ffsl_ecom_in = '06') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias('acct_3ds_attempt_1d')\
                                 ,_sum(expr("case when (substr(upper(ffsl_mc_aav_ctrl),1,1) = 'H' or substr(upper(ffsl_mc_aav_ctrl),1,2) in ('KC','KE') or ffsl_ecom_in = '06') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias('acct_3ds_attempt_amt_1d')\
                                 ,_sum(expr("case when (ffsl_ecmm_scrt_ucaf_cd in ('211','212') or ffsl_ecom_in in ('05','06')) and ffsl_mrch_ctry_cd = '840' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias('acct_3ds_cb_1d')\
                                 ,_sum(expr("case when (ffsl_ecmm_scrt_ucaf_cd in ('211','212') or ffsl_ecom_in in ('05','06')) and ffsl_mrch_ctry_cd = '840' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias('acct_3ds_cb_amt_1d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias('acct_cvv2_invalid_auth_1d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 2 then 1 else 0 end")).alias('acct_cvv2_invalid_auth_2d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias('acct_cvv2_invalid_auth_7d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")).alias('acct_cvv2_invalid_auth_30d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")).alias('acct_cvv2_invalid_auth_60d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")).alias('acct_cvv2_invalid_auth_90d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias('acct_cvv2_invalid_auth_amt_1d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 2 then tran_amount else 0 end")).alias('acct_cvv2_invalid_auth_amt_2d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")).alias('acct_cvv2_invalid_auth_amt_7d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")).alias('acct_cvv2_invalid_auth_amt_30d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")).alias('acct_cvv2_invalid_auth_amt_60d')\
                                 ,_sum(expr("case when ffsl_cvv2_cvc2_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")).alias('acct_cvv2_invalid_auth_amt_90d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")).alias('acct_avs_fail_auth_1d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 2 then 1 else 0 end")).alias('acct_avs_fail_auth_2d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")).alias('acct_avs_fail_auth_7d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")).alias('acct_avs_fail_auth_30d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")).alias('acct_avs_fail_auth_60d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")).alias('acct_avs_fail_auth_90d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")).alias('acct_avs_fail_auth_amt_1d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 2 then tran_amount else 0 end")).alias('acct_avs_fail_auth_amt_2d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")).alias('acct_avs_fail_auth_amt_7d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")).alias('acct_avs_fail_auth_amt_30d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")).alias('acct_avs_fail_auth_amt_60d')\
                                 ,_sum(expr("case when ffsl_addr_vrfc_otcm_cd = 1 and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")).alias('acct_avs_fail_auth_amt_90d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end"))/7).alias("acct_sale_per_day_7")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end"))/30).alias("acct_sale_per_day_30")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end"))/60).alias("acct_sale_per_day_60")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end"))/90).alias("acct_sale_per_day_90")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end"))/7).alias("acct_sale_amt_per_day_7")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end"))/30).alias("acct_sale_amt_per_day_30")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end"))/60).alias("acct_sale_amt_per_day_60")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end"))/90).alias("acct_sale_amt_per_day_90")\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")))).alias('acct_world_auth_pct_1d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")))).alias('acct_world_auth_pct_7d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")))).alias('acct_world_auth_pct_30d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")))).alias('acct_world_auth_pct_60d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")))).alias('acct_world_auth_pct_90d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")))).alias('acct_world_auth_amt_pct_1d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")))).alias('acct_world_auth_amt_pct_7d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")))).alias('acct_world_auth_amt_pct_30d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")))).alias('acct_world_auth_amt_pct_60d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_frad_cdk01_tx='WRLD' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")))).alias('acct_world_auth_amt_pct_90d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")))).alias('acct_pos81_auth_pct_1d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")))).alias('acct_pos81_auth_pct_7d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")))).alias('acct_pos81_auth_pct_30d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")))).alias('acct_pos81_auth_pct_60d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")))).alias('acct_pos81_auth_pct_90d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")))).alias('acct_pos81_auth_amt_pct_1d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")))).alias('acct_pos81_auth_amt_pct_7d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")))).alias('acct_pos81_auth_amt_pct_30d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")))).alias('acct_pos81_auth_amt_pct_60d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_entry_mode_ind='81' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")))).alias('acct_pos81_auth_amt_pct_90d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then 1 else 0 end")))).alias('acct_cb_auth_pct_1d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then 1 else 0 end")))).alias('acct_cb_auth_pct_7d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then 1 else 0 end")))).alias('acct_cb_auth_pct_30d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then 1 else 0 end")))).alias('acct_cb_auth_pct_60d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then 1 else 0 end")))).alias('acct_cb_auth_pct_90d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount else 0 end")))).alias('acct_cb_auth_amt_pct_1d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount else 0 end")))).alias('acct_cb_auth_amt_pct_7d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount else 0 end")))).alias('acct_cb_auth_amt_pct_30d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount else 0 end")))).alias('acct_cb_auth_amt_pct_60d')\
                                 ,(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and ffsl_mrch_ctry_cd <> '840' and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end"))/(_sum(expr("case when ffsl_fraud_trancode in ('MA','CA','AP','DP','MD','CD') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount else 0 end")))).alias('acct_cb_auth_amt_pct_90d')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('DP','MD','CD') then tran_amount end")).alias("acct_total_sales_declined_amt_avg")\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount end")).alias('acct_sale_amt_per_tran_1d')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount end")).alias('acct_sale_amt_per_tran_7d')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount end")).alias('acct_sale_amt_per_tran_30d')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount end")).alias('acct_sale_amt_per_tran_60d')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount end")).alias('acct_sale_amt_per_tran_90d')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and tran_date between date_sub(to_date('" + runDay + "'),int(14)) and date_sub(to_date('" + runDay +"'),int(8)) then tran_amount end")).alias('avg_bw_7_14_days')\
                                 ,_mean(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and tran_date between date_sub(to_date('" + runDay + "'),int(60)) and date_sub(to_date('" + runDay +"'),int(31)) then tran_amount end")).alias('avg_bw_30_60_days')\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') then tran_amount end")).alias("acct_total_sales_amt_std")\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('DP','MD','CD') then tran_amount end")).alias("acct_total_sales_declined_amt_std")\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then tran_amount end")).alias('acct_sale_amt_std_1d')\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then tran_amount end")).alias('acct_sale_amt_std_7d')\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then tran_amount end")).alias('acct_sale_amt_std_30d')\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then tran_amount end")).alias('acct_sale_amt_std_60d')\
                                 ,_stddev(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then tran_amount end")).alias('acct_sale_amt_std_90d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then ffsl_entry_mode_ind end")).alias('acct_unique_pos_1d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then ffsl_entry_mode_ind end")).alias('acct_unique_pos_7d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then ffsl_entry_mode_ind end")).alias('acct_unique_pos_30d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then ffsl_entry_mode_ind end")).alias('acct_unique_pos_60d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then ffsl_entry_mode_ind end")).alias('acct_unique_pos_90d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 1 then ffsl_merch_cat_code end")).alias('acct_unique_mcc_1d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 7 then ffsl_merch_cat_code end")).alias('acct_unique_mcc_7d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 30 then ffsl_merch_cat_code end")).alias('acct_unique_mcc_30d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 60 then ffsl_merch_cat_code end")).alias('acct_unique_mcc_60d')\
                                 ,countDistinct(expr("case when ffsl_fraud_trancode in ('MA','CA','AP') and datediff(to_date('" + runDay + "'),tran_date) <= 90 then ffsl_merch_cat_code end")).alias('acct_unique_mcc_90d')\
                                  ).dropDuplicates()
           
  ## Computing sales timestamp attributes
  salesAttrDf = computeSalesAttr(falconAggDf)
  ## Computing sales standard deviations
  salesTBAttrDf_30d = computeTimeBoxedStddev(falconAggDf,30,runDay)
  salesTBAttrDf_60d = computeTimeBoxedStddev(falconAggDf,60,runDay)
  salesTBAttrDf_90d = computeTimeBoxedStddev(falconAggDf,90,runDay)
  ## Computing top merchant attributes
  topMrchCountryDf = computeTopMrchCountry(falconAggDf,runDay,1)
  ## Computing top merchant by category attributes
  topMerchByCatAttrDf_1d = computeTopMerchByCatAttr(falconAggDf,runDay,1)
  topMerchByCatAttrDf_7d = computeTopMerchByCatAttr(falconAggDf,runDay,7)
  topMerchByCatAttrDf_30d = computeTopMerchByCatAttr(falconAggDf,runDay,30)
  topMerchByCatAttrDf_60d = computeTopMerchByCatAttr(falconAggDf,runDay,60)
  topMerchByCatAttrDf_90d = computeTopMerchByCatAttr(falconAggDf,runDay,90)
  ## Computing top pos transactions by sales amount
  topPOSAttrDf_1d = computeTopPOSAttr(falconAggDf,runDay,1)
  topPOSAttrDf_7d = computeTopPOSAttr(falconAggDf,runDay,7)
  topPOSAttrDf_30d = computeTopPOSAttr(falconAggDf,runDay,30)
  topPOSAttrDf_60d = computeTopPOSAttr(falconAggDf,runDay,60)
  topPOSAttrDf_90d = computeTopPOSAttr(falconAggDf,runDay,90)
  ## Computing sales amount on last day for new merchant
  saNewMrchPreviousDayDf = salesAmtNewMrchPreviousDay(falconAggDf,runDay,90)
  
  ## joining back to falcon attributes df to get a single view of incremental variables
  falconAttrDf = falconAttrDf.join(salesAttrDf,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(salesTBAttrDf_30d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(salesTBAttrDf_60d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(salesTBAttrDf_90d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topMrchCountryDf,["orig_account_nbr"],"left")
  falconAttrDf = falconAttrDf.join(topMerchByCatAttrDf_1d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topMerchByCatAttrDf_7d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topMerchByCatAttrDf_30d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topMerchByCatAttrDf_60d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topMerchByCatAttrDf_90d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topPOSAttrDf_1d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topPOSAttrDf_7d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topPOSAttrDf_30d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topPOSAttrDf_60d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(topPOSAttrDf_90d,['orig_account_nbr'],'left')
  falconAttrDf = falconAttrDf.join(saNewMrchPreviousDayDf,['orig_account_nbr'],'left')
  
  falconAttrDf.cache()
  return falconAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing falcon attributes - " + str(e))

## Function to compute sales attributes
def computeSalesAttr(falconAggDf):
 try:
  wdSpecSales = Window.partitionBy('orig_account_nbr').orderBy(desc('timestamp'))
  salesAttrDf = falconAggDf.filter("ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP')")\
                            .withColumn('timestamp',to_timestamp(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'),lit(' '),col('ffsl_time_hh'),lit(':'),col('ffsl_time_mm'),lit(':'),col('ffsl_time_ss')),'yyyy-MM-dd HH:mm:ss'))\
                            .withColumn('rowNumber',row_number().over(wdSpecSales)).where(col('rowNumber') <= 10)\
                            .withColumn('listAmount',collect_list('tran_amount').over(wdSpecSales))\
                            .withColumn('listTimestamp',collect_list('timestamp').over(wdSpecSales))\
                            .withColumn('listRiskScoreVa',collect_list('ffsl_vaa_risk_scor_nr').over(wdSpecSales))\
                            .withColumn('listR3Scores',collect_list('ffsl_score_2').over(wdSpecSales))\
                            .withColumn('listMcdiScore',collect_list('ffsl_ems_frad_scor_nr').over(wdSpecSales))\
                            .groupBy('orig_account_nbr')\
                            .agg(max('listAmount').alias('sortedListAmount')\
                                ,max('listTimestamp').alias('sortedListTimestamp')\
                                ,max('listRiskScoreVa').alias('sortedRiskScoreVa')\
                                ,max('listR3Scores').alias('sortedR3Scores')\
                                ,max('listMcdiScore').alias('sortedMcdiScore')\
                                ).dropDuplicates()
                            
  salesAttrDf = salesAttrDf.select(['orig_account_nbr']+[expr('sortedListAmount['+str(i)+']') for i in range(0,10)]+[expr('sortedListTimestamp['+str(i)+']') for i in range(0,10)]+[expr('sortedRiskScoreVa['+str(i)+']') for i in range(0,10)]+[expr('sortedR3Scores['+str(i)+']') for i in range(0,10)]+[expr('sortedMcdiScore['+str(i)+']') for i in range(0,10)])
  cols = ['orig_account_nbr']+['acct_sales_amt_' + str(i+1) for i in range(0,10)]+['acct_sales_ts_' + str(i+1) for i in range(0,10)]+['acct_sales_vaa_scr_'+str(i+1) for i in range(0,10)]+['acct_sales_r3_scr_'+str(i+1) for i in range(0,10)]+['acct_sales_mcdi_scr_'+str(i+1) for i in range(0,10)]
  salesAttrDf = salesAttrDf.toDF(*cols)
  return salesAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing sales attributes - " + str(e))

def computeTimeBoxedStddev(falconAggDf,n,runDay):
 ###############################################################################################
 # Function to compute sales 30,60,90 days standard deviation for count as well as tran_amount #
 # Creating window partition and registering udf function                                      #
 # Creating a base dataframe and missing dates dataframe                                       #
 # Joining both the dataframes to get the final dataframe for computation of stddev            #
 ###############################################################################################
 try:
  wdSalesSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('tran_date'))
  extend_list_amount_udf = udf(rFunctions.extend_list_amount,ArrayType(FloatType()))
  extend_list_count_udf = udf(rFunctions.extend_list_count,ArrayType(IntegerType()))
  compute_timeboxed_stddev_udf = udf(rFunctions.compute_timeboxed_stddev,FloatType())
  cols = ['orig_account_nbr','acct_sale_amt_per_day_'+str(n)+'_std','acct_sale_per_day_'+str(n)+'_std']
  tbStdDevAttrDf = falconAggDf.filter("ffsl_fraud_trancode in ('MA','CA','AP')")\
                         .where(col('tran_date') >= date_sub(lit(runDay),int(n)))\
                         .groupBy('orig_account_nbr','tran_date')\
                         .agg(count('tran_amount').alias('countTxn'),_sum(col('tran_amount').cast('double')).alias('sumTxnAmt'))\
                         .withColumn('listTxnCnt',collect_list(col('countTxn')).over(wdSalesSpec))\
                         .withColumn('listTxnAmt',collect_list(col('sumTxnAmt')).over(wdSalesSpec))\
                         .groupBy('orig_account_nbr').agg(max('listTxnAmt').alias('sortedTxnAmt'),max('listTxnCnt').alias('sortedTxnCnt'))\
                         .withColumn('extendedCountList',extend_list_count_udf(col('sortedTxnCnt'),lit(n)))\
                         .withColumn('extendedTxnAmtList',extend_list_amount_udf(col('sortedTxnAmt'),lit(n)))\
                         .withColumn('stdDevAmt',compute_timeboxed_stddev_udf(col('extendedTxnAmtList')))\
                         .withColumn('stdDevCnt',compute_timeboxed_stddev_udf(col('extendedCountList'))).drop(*['sortedTxnAmt','sortedTxnCnt','extendedCountList','extendedTxnAmtList'])
  
  tbStdDevAttrDf = tbStdDevAttrDf.toDF(*cols)
  return tbStdDevAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing sales time boxed standard deviation-" + str(e))

## Function to compute top merchant attributes 
def computeTopMrchCountry(falconAggDf,runDay,n):
 try:
  removeDuplicatesListUdf = udf(rFunctions.removeDuplicatesList,ArrayType(StringType()))
  wdSalesSpecs = Window.partitionBy('orig_account_nbr','ffsl_mrch_ctry_cd').orderBy(lit('X'))
  wdTopMrchSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('sum_sa'))
  wdTopEdgeSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('ffsl_mrch_ctry_cd'))
  cols = ['orig_account_nbr']+['acct_top_mrch_country_' + str(n) + 'd']+['acct_top_mrch_country_' + str(n) + 'd_amt']
  topMrchCountryDf = falconAggDf.where(col('tran_date') >= date_sub(lit(runDay),int(n)))\
                             .withColumn('sum_sa',_sum(col('tran_amount')).over(wdSalesSpecs))\
                             .withColumn('rk',rank().over(wdTopMrchSpec))\
                             .where(col('rk') == 1).withColumn('mrchCtryCodeSet',collect_list('ffsl_mrch_ctry_cd').over(wdTopEdgeSpec))\
                             .groupBy('orig_account_nbr').agg(max('mrchCtryCodeSet').alias('maxMrchCtryCodeSet'),max(col('sum_sa')).alias('sum_sa')).withColumn('ddupMrchCtryCodeSet',removeDuplicatesListUdf('maxMrchCtryCodeSet'))\
                             .select(col('orig_account_nbr'),col('ddupMrchCtryCodeSet'),col('sum_sa')).dropDuplicates()
  topMrchCountryDf = topMrchCountryDf.toDF(*cols)
  return topMrchCountryDf
 except Exception as e:
  sys.exit("ERROR: while computing top merchant country attribute -" + str(e))
 
 ## Function to compute top merchant categary attributes
def computeTopMerchByCatAttr(falconAggDf,runDay,n):
 try:
  removeDuplicatesListUdf = udf(rFunctions.removeDuplicatesList,ArrayType(StringType()))
  wdMerchCatSpec = Window.partitionBy('orig_account_nbr','ffsl_merch_cat_code').orderBy(lit('X'))
  wdTopMerchCatSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('sum_sa'))
  wdTopMccEdgeSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('ffsl_merch_cat_code'))
  cols = ['orig_account_nbr','acct_top_mcc_'+str(n)+'d','acct_top_mcc_'+str(n)+'d_amt_per_day']
  topMccodeAttrDf = falconAggDf.where(col('tran_date') >= date_sub(lit(runDay),int(n)))\
                                .withColumn('sum_sa',_sum('tran_amount').over(wdMerchCatSpec))\
                                .withColumn('rk',rank().over(wdTopMerchCatSpec))\
                                .where(col('rk') == 1)\
                                .withColumn('mccSet',collect_list('ffsl_merch_cat_code').over(wdTopMccEdgeSpec))\
                                .groupBy('orig_account_nbr').agg(max(col('mccSet')).alias('maxMCCSet'),max(col('sum_sa')).alias('sum_sa')).withColumn('maxMCCSet',removeDuplicatesListUdf('maxMCCSet'))\
                                .select(col('orig_account_nbr'),col('maxMCCSet'),col('sum_sa')).dropDuplicates()
  topMccodeAttrDf = topMccodeAttrDf.withColumn('sum_sa',col('sum_sa')/(n))
  topMccodeAttrDf = topMccodeAttrDf.toDF(*cols)
  return topMccodeAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing top merchant attributes by category-" + str(e))
 
 ## Function to compute top pos     
def computeTopPOSAttr(falconAggDf,runDay,n):
 try:
  removeDuplicatesListUdf = udf(rFunctions.removeDuplicatesList,ArrayType(StringType()))
  wdTopPosSpec = Window.partitionBy('orig_account_nbr','ffsl_entry_mode_ind').orderBy(lit('X'))
  wdTopPosRankSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('sum_sa'))
  wdTopPosEdgeSpec = Window.partitionBy('orig_account_nbr').orderBy(desc('ffsl_entry_mode_ind'))
  cols = ['orig_account_nbr','acct_top_pos_'+str(n)+'d','acct_top_pos_'+str(n)+'d_amt_per_day']
  topPOSAttrDf = falconAggDf.filter("ffsl_entry_mode_ind in ('05','01','10','91')").where(col('tran_date') >= date_sub(lit(runDay),int(n)))\
                             .withColumn('sum_sa',_sum(col('tran_amount')).over(wdTopPosSpec))\
                             .withColumn('rk',rank().over(wdTopPosRankSpec))\
                             .where(col('rk') == 1).withColumn('setEMI',collect_list('ffsl_entry_mode_ind').over(wdTopPosEdgeSpec))\
                             .groupBy('orig_account_nbr').agg(max(col('setEMI')).alias('maxSetEMI'),max(col('sum_sa')).alias('sum_sa')).withColumn('ddupMaxMCCSet',removeDuplicatesListUdf('maxSetEMI'))\
                             .select(col('orig_account_nbr'),col('ddupMaxMCCSet'),col('sum_sa')).dropDuplicates()
  topPOSAttrDf = topPOSAttrDf.withColumn('sum_sa',col('sum_sa')/(n))
  topPOSAttrDf = topPOSAttrDf.toDF(*cols)
  return topPOSAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing top pos attributes -" + str(e))

## Function to compute sales amount for new merchant last day
def salesAmtNewMrchPreviousDay(falconAggDf,runDay,n):
 getMerchListLastDayUdf = udf(rFunctions.getMerchListLastDay,ArrayType(StringType()))
 wdMerchSpec =  Window.partitionBy('orig_account_nbr').orderBy(lit('X'))
 merchFrom90daysDf = falconAggDf.where((col('tran_date') >= date_sub(lit(runDay),int(n+1))) & (col('tran_date') < date_sub(lit(runDay),int(1))))\
                                 .withColumn('merchList_90days',collect_set('ffsl_frad_mrch_id').over(wdMerchSpec))\
                                 .groupBy('orig_account_nbr')\
                                 .agg(max(col('merchList_90days')).alias('sortedMerchList_90days'))
 merchFrom1dayDf = falconAggDf.where((col('tran_date') >= date_sub(lit(runDay),int(1))))\
                               .withColumn('merchList_1day',collect_set('ffsl_frad_mrch_id').over(wdMerchSpec))\
                               .groupBy('orig_account_nbr')\
                               .agg(max(col('merchList_1day')).alias('sortedMerchList_1day'))
 salesAmtSumLastDayByMerch = falconAggDf.where((col('tran_date') >= date_sub(lit(runDay),int(1))))\
                                         .groupBy('orig_account_nbr','ffsl_frad_mrch_id')\
                                         .agg(_sum(col('tran_amount')).alias('sales_amt_sum')).dropDuplicates()
 combined901Df = merchFrom1dayDf.join(merchFrom90daysDf,['orig_account_nbr'],'full')
 combined901Df = combined901Df.withColumn('newMerchList_1d',getMerchListLastDayUdf('sortedMerchList_90days','sortedMerchList_1day'))\
                              .drop(*['sortedMerchList_90days','sortedMerchList_1day'])\
                              .withColumn('ffsl_frad_mrch_id',explode('newMerchList_1d')).drop(*['newMerchList_1d'])
 salesAmtNewMerchAttrDf = combined901Df.join(salesAmtSumLastDayByMerch,['orig_account_nbr','ffsl_frad_mrch_id'],'inner')
 salesAmtNewMerchAttrDf = salesAmtNewMerchAttrDf.groupBy('orig_account_nbr').agg(_sum('sales_amt_sum').alias('acct_sales_amt_1d_from_newmerchant_90d')).dropDuplicates()
 return salesAmtNewMerchAttrDf

def readGibberishScore(extGibberishPath,spark):
 try:
  mrchGibberishDf = spark.read.parquet(extGibberishPath).where(col('gibberish_flag') == True)
  mrchGibberishDf = mrchGibberishDf[['ffsl_mrch_nm','gibberish_flag']].dropDuplicates()
  
  return mrchGibberishDf
 except Exception as e:
  sys.exit("ERROR: while computing gibberish score for merchants - " + str(e))
 
def createFGibbAgg(mrchGibberishDf,readFalconDf):
 try:
  falconGibbAggDf = readFalconDf.join(broadcast(mrchGibberishDf),['ffsl_mrch_nm'],'left') # joining mrchGibberishDf to falcon df to bring gibberish score back to the dataframe
  falconGibbAggDf.cache()
  return falconGibbAggDf
 except Exception as e:
  sys.exit("ERROR: while creating falcon gibberish agg dataframe - " + str(e)) 
 
## Formatting data frames according to script needs
def readPrecursorTables(pcAcctLexid,edl_amf_falcon_raw,falconRawPath,debug,runDay,spark):
 try:
  wdSpecAccStatus =  Window.partitionBy('orig_account_nbr').orderBy(lit('X'))
  lexidDf = spark.read.parquet(pcAcctLexid).drop(*['edl_load_ts'])
  lexidDf = lexidDf.withColumn('count',count('current_account_nbr').over(wdSpecAccStatus))\
        .withColumn('allClsCnt',_sum(expr("case when (trim(external_status) not in ('A','') and external_status is not null) and orig_account_nbr <> '0000000000000000' then 1 else 0 end")).over(wdSpecAccStatus))\
        .where(col('count') != col('allClsCnt'))\
        .drop(*['external_status','external_status_reason_code','count','allClsCnt'])\
        .dropDuplicates()\
        .withColumnRenamed('customer_acct_nbr','current_account_nbr_pty')\
        .withColumnRenamed('lexid1','cust_lexid')\
        .withColumnRenamed('open_date','acct_open_date')\
        .withColumnRenamed('nbr_of_cards_current','acct_total_cards_issued')\
        .withColumn('last_plastic_date',to_date(col('last_plastic_date')))\
        .withColumn('acct_days_on_book',datediff(lit(runDay),to_date('acct_open_date')))\
        .withColumn('acct_days_since_last_card',datediff(lit(runDay),to_date('last_plastic_date')))\
        .withColumn('acct_card_network',expr("case when sys in ('6362') then 'DISC' \
                                                                        when sys in ('1364','1366','1409','1192','1246','1192','1468','1469','1312','1410','1408','1136','1363','1365','1327','1326','1407','1411','3935') then 'MC' \
                                                                        when sys in ('5156','5166','3179','5160','3941','3394','3394','4543','3659','3665','5168','5931','3895','3894') then 'VISA' \
                                                                        else 'PLCC' end"))
  if debug == 'no':      
       falconDf = spark.sql("""select * from """ + edl_amf_falcon_raw).where(col('as_of_date') < lit(runDay))
  else:
       falconDf = spark.read.orc(falconRawPath).where(col('as_of_date') < lit(runDay))
  
  readFalconDf = falconDf.select(col('ffsl_account_number')
                             ,col('ffsl_tran_amount').alias('tran_amount')
                             ,col('ffsl_fraud_trancode')
                             ,col('ffsl_spcl_cndt_in')
                             ,trim(col('ffsl_ecmm_scrt_ucaf_cd')).alias('ffsl_ecmm_scrt_ucaf_cd')
                             ,trim(col('ffsl_ecom_in')).alias('ffsl_ecom_in')
                             ,trim(col('ffsl_mc_aav_ctrl')).alias('ffsl_mc_aav_ctrl')
                             ,trim(col('ffsl_frad_cdk01_tx')).alias('ffsl_frad_cdk01_tx')
                             ,trim(col('ffsl_mrch_ctry_cd')).alias('ffsl_mrch_ctry_cd')
                             ,trim(col('ffsl_cvv2_cvc2_otcm_cd')).alias('ffsl_cvv2_cvc2_otcm_cd')
                             ,col('ffsl_vaa_risk_scor_nr')
                             ,col('ffsl_ems_frad_scor_nr')
                             ,col('ffsl_mrch_nm')
                             ,trim(col('ffsl_addr_vrfc_otcm_cd')).alias('ffsl_addr_vrfc_otcm_cd')
                             ,col('ffsl_score_2')
                             ,col('ffsl_merch_cat_code')
                             ,col('ffsl_frad_mrch_id')
                             ,col('ffsl_total_pay_amt')
                             ,lpad(trim(col('ffsl_entry_mode_ind')),2,'0').alias('ffsl_entry_mode_ind')
                             ,col('ffsl_date_yy'),col('ffsl_date_mm'),col('ffsl_date_dd'),col('ffsl_time_hh'),col('ffsl_time_mm'),col('ffsl_time_ss'))\
                             .dropDuplicates()\
                             .withColumn('tran_date',to_date(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'))))\
                             .withColumn('ffsl_mrch_ctry_cd',expr("ptyUnprotectStr(ffsl_mrch_ctry_cd,'dtStrNPL')"))
  readFalconDf.cache()
  lexidDf.cache()    
  return lexidDf,readFalconDf
 except Exception as e:
  sys.exit("ERROR: while reading pre-cursor tables - " + str(e))

def combined_dfs(falconAttrDf,runDay):
 try:
  compareListUdf = udf(rFunctions.compareList,StringType())
  falconAttrDf = falconAttrDf.withColumn('acct_sale_amt_change_1d',\
                              when((falconAttrDf.acct_sale_amt_per_tran_1d.isNull()) & (falconAttrDf.acct_sale_amt_per_tran_90d > 0),lit(0))\
                             .when((falconAttrDf.acct_sale_amt_per_tran_1d.isNull())\
                                 | (falconAttrDf.acct_sale_amt_per_tran_90d.isNull())\
                                 | (falconAttrDf.acct_sale_amt_per_tran_90d == 0), lit(None))\
                             .otherwise(falconAttrDf.acct_sale_amt_per_tran_1d/falconAttrDf.acct_sale_amt_per_tran_90d))\
                             .withColumn('acct_sale_amt_change_7d',\
                              when((falconAttrDf.acct_sale_amt_per_tran_7d.isNull()) & (falconAttrDf.acct_sale_amt_per_tran_90d > 0),lit(0))\
                             .when((falconAttrDf.acct_sale_amt_per_tran_7d.isNull())\
                                 | (falconAttrDf.acct_sale_amt_per_tran_90d.isNull())\
                                 | (falconAttrDf.acct_sale_amt_per_tran_90d == 0), lit(None))\
                             .otherwise(falconAttrDf.acct_sale_amt_per_tran_7d/falconAttrDf.acct_sale_amt_per_tran_90d))\
                             .withColumn('acct_sale_amt_change_30d',\
                              when((falconAttrDf.acct_sale_amt_per_tran_30d.isNull()) & (falconAttrDf.acct_sale_amt_per_tran_90d > 0),lit(0))\
                             .when((falconAttrDf.acct_sale_amt_per_tran_30d.isNull())\
                                 | (falconAttrDf.acct_sale_amt_per_tran_90d.isNull())\
                                 | (falconAttrDf.acct_sale_amt_per_tran_90d == 0), lit(None))\
                             .otherwise(falconAttrDf.acct_sale_amt_per_tran_30d/falconAttrDf.acct_sale_amt_per_tran_90d))\
                             .withColumn('acct_pos_shift_7d',compareListUdf(falconAttrDf.acct_top_pos_1d,falconAttrDf.acct_top_pos_7d))\
                             .withColumn('acct_pos_shift_30d',compareListUdf(falconAttrDf.acct_top_pos_7d,falconAttrDf.acct_top_pos_30d))\
                             .withColumn('acct_pos_shift_60d',compareListUdf(falconAttrDf.acct_top_pos_7d,falconAttrDf.acct_top_pos_60d))\
                             .withColumn('acct_pos_shift_90d',compareListUdf(falconAttrDf.acct_top_pos_7d,falconAttrDf.acct_top_pos_90d))\
                             .withColumn('acct_sale_acceleration_7d',\
                              when((falconAttrDf.acct_sale_amt_per_tran_7d.isNull())\
                                 & (falconAttrDf.avg_bw_7_14_days.isNotNull()),falconAttrDf.avg_bw_7_14_days)\
                             .when((falconAttrDf.acct_sale_amt_per_tran_7d.isNotNull())\
                                 & (falconAttrDf.avg_bw_7_14_days.isNull()),falconAttrDf.acct_sale_amt_per_tran_7d)\
                             .when((falconAttrDf.acct_sale_amt_per_tran_7d.isNull())\
                                 & (falconAttrDf.avg_bw_7_14_days.isNull()),lit(None))\
                             .otherwise(falconAttrDf.acct_sale_amt_per_tran_7d.cast(FloatType()) - falconAttrDf.avg_bw_7_14_days.cast(FloatType())))\
                             .withColumn('acct_sale_acceleration_30d',\
                              when((falconAttrDf.acct_sale_amt_per_tran_30d.isNull())\
                                 & (falconAttrDf.avg_bw_30_60_days.isNotNull()),falconAttrDf.avg_bw_30_60_days)\
                             .when((falconAttrDf.acct_sale_amt_per_tran_30d.isNotNull())\
                                 & (falconAttrDf.avg_bw_30_60_days.isNull()),falconAttrDf.acct_sale_amt_per_tran_30d)\
                             .when((falconAttrDf.acct_sale_amt_per_tran_30d.isNull())\
                                 & (falconAttrDf.avg_bw_30_60_days.isNull()),None)\
                             .otherwise(falconAttrDf.acct_sale_amt_per_tran_30d.cast(FloatType()) - falconAttrDf.avg_bw_30_60_days.cast(FloatType())))
 
  falconAttrDf = falconAttrDf.drop(*['avg_bw_7_14_days','avg_bw_30_60_days']).withColumn('edl_load_ts',lit(current_timestamp())).withColumn('as_of_date',lit(runDay))
  return falconAttrDf
 except Exception as e:
  sys.exit("ERROR: while creating combined dataframe:" + str(e))
  
#######################################################################################################################
# Function for loading the C360 hive table                                                                            #
#######################################################################################################################
def loadFalconAttrHive(falconAttrDf,extFalconAttr,falcon_hive_table,writeMode,spark):
 try:
  print("######################## Starting loading Tc360 falcon hive table ######################################")             
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  if falconAttrDf.count() >= 1: falconAttrDf.write.partitionBy('as_of_date').mode(writeMode).option('header','true').format('parquet').save(extFalconAttr)
  else: falconAttrDf.repartition(1).write.partitionBy('as_of_date').mode(writeMode).option('header','true').format('parquet').save(extFalconAttr)
  print("######################## Tc360 falcon hive table load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while loading the data into hive table - " + str(e))

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 mrchGibberishDf = readGibberishScore(inputParams['extGibberishPath'],spark)
 lexidDf,readFalconDf = readPrecursorTables(inputParams['pcAcctLexid'],inputParams['edl_amf_falcon_raw'],inputParams['falconRawPath'],inputParams['debug'],runDay,spark)
 falconGibbDf = createFGibbAgg(mrchGibberishDf,readFalconDf)
 falconAttrDf = computeFalconAttributes(falconGibbDf,lexidDf,runDay)
 falconAttrDf = combined_dfs(falconAttrDf,runDay)
 loadFalconAttrHive(falconAttrDf,inputParams['extFalconAttr'],inputParams['falcon_hive_table'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()