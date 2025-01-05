###################################################################################
# Script Name:amtfServiceTc360RiskModule.py                                       #
# Purpose: Base module containing function definitions                            #
# Created by: Upendra Kumar                                                       #
# Create Date: 12/11/2021                                                         #
# Modify date:                                                                    #
###################################################################################

from pyspark.sql.types import FloatType,IntegerType,ArrayType,StringType,DataType,StructField,StructType,TimestampType,DateType,LongType,DecimalType
from pyspark.sql import SparkSession,SQLContext,HiveContext,Window,Row
from pyspark.sql.functions import sum as _sum,mean as _mean, stddev_pop as _stddev,col,coalesce,lit,split,trim,size,lpad,length,to_date,concat,substring,current_date,expr,datediff,udf,array,desc,date_sub,count,collect_list,max,min,to_timestamp,row_number,rank,collect_set,explode,round,current_timestamp,date_format,broadcast,countDistinct,regexp_replace
from math import sqrt,exp,log
from ConfigParser import ConfigParser
from time import *
import commands
import getopt
import sys
import os
sys.dont_write_bytecode = True
import datetime
from dateutil.relativedelta import relativedelta
sys.dont_write_bytecode = True
from datetime import datetime,date

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
                if ('falconRawPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain falconRawPath")
                else:
                    falconRawPath = config.get('HIVE', 'falconRawPath')
                if ('tc360_risk_mrch_hive' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain tc360_risk_mrch_hive")
                else:
                    tc360_risk_mrch_hive = config.get('HIVE','tc360_risk_mrch_hive')
                if ('ftccPath' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain ftccPath")
                else:
                    ftccPath = config.get('HIVE','ftccPath')
                if ('ftctPath' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain ftctPath")
                else:
                    ftctPath = config.get('HIVE','ftctPath')
                if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_amf_falcon_raw")
                else:
                    edl_amf_falcon_raw = config.get('HIVE','edl_amf_falcon_raw')
                if ('debug' not in config.options('FUNCTIONAL')):
                  sys.exit("ERROR: Section FUNCTIONAL doesn't contain debug")
                else:
                    debug = config.get('FUNCTIONAL','debug')                    
        return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'falconRawPath': falconRawPath
                            ,'edl_amf_falcon_raw': edl_amf_falcon_raw
                            ,'tc360_risk_mrch_hive': tc360_risk_mrch_hive
                            ,'ftccPath': ftccPath
                            ,'ftctPath': ftctPath
                            ,'runDay': runDay
                            ,'debug': debug
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('tc360_risk_merchant_application').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.sql("SET spark.hadoop.hive.mapred.supports.subdirectories=true")
 spark.sql("SET mapreduce.input.fileinputformat.input.dir.recursive=true")
 status,protegrityFile = commands.getstatusoutput('ls /opt/protegrity/pephive/lib/pephive-3.1.0.jar')
 spark.sql("add jar " + protegrityFile)
 spark.sql("add jar /opt/protegrity/pepspark/lib/pepspark-2.3.2.jar")
 spark.sql("add jar /usr/share/java/mysql-connector-java-5.1.17.jar")
 spark.sql("create temporary function ptyUnprotectStr as 'com.protegrity.hive.udf.ptyUnprotectStr'")
 spark.sql("create temporary function ptyProtectStr as 'com.protegrity.hive.udf.ptyProtectStr'")
 spark.sql('add jar /data1/EDL/domains/cdl/synapps/edlservices-crypto-sparkudf-extensions/crypto-sparkudf-extensions-jar-with-dependencies.jar')
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
        sys.exit("The Column Names string is blank. Please provide valid column names")
    else:
        columnStructFields = []
        for column in columnNames.split(","):
           if (column in ('MERGER_DATE','ACQ_SCORE_STRATEGY_FLIP_DATE','ACQ_LASER_STRATEGY_FLIP_DATE','ACM_SCORE_FLIP_DATE')):
              columnStructFields.append(StructField(column, LongType(), True))
           elif (column in ('EDL_LOAD_TS')):
              columnStructFields.append(StructField(column, TimestampType(), True))
           else:
              columnStructFields.append(StructField(column, StringType(), True))
    schema = StructType(columnStructFields)
    return schema
########################################################################################################################

def readFCC(ftccPath,runDay,n,spark):
 ##################################################################################
 # Reading fraud claim case for 90 days based on reported date                    #
 # Reading most recent partition from fraud claim case table                      #
 ##################################################################################
 try:
  fccDf = spark.read.parquet(ftccPath).where(col('reported_date') < lit(runDay))\
                    .where((col('reported_date') >= date_sub(lit(runDay),int(n))) & (col('reported_date') <= date_sub(lit(runDay),int(1))))\
                    .withColumn('bin12',substring(expr("ptyUnprotectStr(current_account_nbr_pty,'dtStrNPL')"),1,12))\
                    .withColumn('bin6',substring(expr("ptyUnprotectStr(current_account_nbr_pty,'dtStrNPL')"),1,6))\
                    .select(trim(col('current_account_nbr_pty')).alias('current_account_nbr_pty'),col('bin12'),col('bin6')
                           ,trim(col('orig_account_nbr')).alias('orig_account_nbr')
                                ,col('case_id'),col('loss_type_cd'),col('possible_fraud_cd')
                                ,col('net_fraud_amt'),col('reported_date')).dropDuplicates()
  fccDf.cache()
  return fccDf
 except Exception as e:
  sys.exit("ERROR: while reading fraud claim case table - " + str(e))

def readFCT(ftctPath,spark):
 try:
  fctDf =  spark.read.parquet(ftctPath).where((trim(col('merchant_account_id')) <> '')).select(col('orig_account_nbr'),col('merchant_account_id').alias('ffsl_frad_mrch_id'),col('merchant_nm').alias('ffsl_mrch_nm'),col('mcc').alias('ffsl_merch_cat_code')).dropDuplicates()
  fctDf.cache()
  return fctDf
 except Exception as e:
  sys.exit("ERROR: while reading fraud claim transaction data - " + str(e))

def readFalconBin6(falconRawPath,edl_amf_falcon_raw,runDay,n,debug,spark):
 try:
  if debug == 'yes':
   falconBin6Df = spark.read.orc(falconRawPath).filter("ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP')")\
                        .where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(n))))\
                        .withColumn('bin6',substring(expr("ptyUnprotectStr(ffsl_account_number,'dtStrNPL')"),1,6))\
                        .withColumn('tran_date',to_date(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'))))\
                        .select(col('bin6'),col("ffsl_tran_amount").alias("tran_amount"),col('tran_date'),col('ffsl_date_yy'),col('ffsl_date_mm'),col('ffsl_date_dd'),col('ffsl_time_hh'),col('ffsl_time_mm'),col('ffsl_time_ss')).dropDuplicates()\
                        .drop(*['ffsl_date_yy','ffsl_date_mm','ffsl_date_dd','ffsl_time_hh','ffsl_time_mm','ffsl_time_ss'])
  else:
   falconBin6Df = spark.sql("""select * from """ + edl_amf_falcon_raw).filter("ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP')")\
                        .where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(n))))\
                        .withColumn('bin6',substring(expr("ptyUnprotectStr(ffsl_account_number,'dtStrNPL')"),1,6))\
                        .withColumn('tran_date',to_date(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'))))\
                        .select(col('bin6'),col("ffsl_tran_amount").alias("tran_amount"),col('tran_date'),col('ffsl_date_yy'),col('ffsl_date_mm'),col('ffsl_date_dd'),col('ffsl_time_hh'),col('ffsl_time_mm'),col('ffsl_time_ss')).dropDuplicates()\
                        .drop(*['ffsl_date_yy','ffsl_date_mm','ffsl_date_dd','ffsl_time_hh','ffsl_time_mm','ffsl_time_ss'])
  falconBin6Df.cache()
  return falconBin6Df
 except Exception as e:
  sys.exit("ERROR: while reading falcon table for Bin6 attributes - " + str(e))
  
def readFalcon(falconRawPath,edl_amf_falcon_raw,runDay,n,debug,spark):
 try:
  if debug == 'yes':
   falconDf = spark.read.orc(falconRawPath).where((trim(col('ffsl_frad_mrch_id')) <> '')).filter("ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP')")\
                        .where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(n))))\
                        .withColumn('bin12',substring(expr("ptyUnprotectStr(ffsl_account_number,'dtStrNPL')"),1,12))\
                        .withColumn('bin6',substring(expr("ptyUnprotectStr(ffsl_account_number,'dtStrNPL')"),1,6))\
                        .withColumn('tran_date',to_date(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'))))\
                        .select(col('bin6'),col('bin12'),trim(col("ffsl_frad_mrch_id")).alias('ffsl_frad_mrch_id'),col('ffsl_mrch_nm'),col('ffsl_merch_cat_code'),col('ffsl_fraud_trancode'),trim(col('ffsl_account_number')).alias('ffsl_account_number'),col("ffsl_tran_amount").alias("tran_amount"),col('tran_date'),col('ffsl_date_yy'),col('ffsl_date_mm'),col('ffsl_date_dd'),col('ffsl_time_hh'),col('ffsl_time_mm'),col('ffsl_time_ss')).dropDuplicates()\
                        .drop(*['ffsl_date_yy','ffsl_date_mm','ffsl_date_dd','ffsl_time_hh','ffsl_time_mm','ffsl_time_ss'])
  else:
   falconDf = spark.sql("""select * from """ + edl_amf_falcon_raw).where((trim(col('ffsl_frad_mrch_id')) <> '')).filter("ffsl_fraud_trancode in ('MA','MD','CA','CD','AP','DP')")\
                        .where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(n))))\
                        .withColumn('bin12',substring(expr("ptyUnprotectStr(ffsl_account_number,'dtStrNPL')"),1,12))\
                        .withColumn('bin6',substring(expr("ptyUnprotectStr(ffsl_account_number,'dtStrNPL')"),1,6))\
                        .withColumn('tran_date',to_date(concat(substring(current_date(),1,2),col('ffsl_date_yy'),lit('-'),col('ffsl_date_mm'),lit('-'),col('ffsl_date_dd'))))\
                        .select(col('bin6'),col('bin12'),trim(col("ffsl_frad_mrch_id")).alias('ffsl_frad_mrch_id'),col('ffsl_mrch_nm'),col('ffsl_merch_cat_code'),col('ffsl_fraud_trancode'),trim(col('ffsl_account_number')).alias('ffsl_account_number'),col("ffsl_tran_amount").alias("tran_amount"),col('tran_date'),col('ffsl_date_yy'),col('ffsl_date_mm'),col('ffsl_date_dd'),col('ffsl_time_hh'),col('ffsl_time_mm'),col('ffsl_time_ss')).dropDuplicates()\
                        .drop(*['ffsl_date_yy','ffsl_date_mm','ffsl_date_dd','ffsl_time_hh','ffsl_time_mm','ffsl_time_ss'])
  falconDf.cache()
  return falconDf
 except Exception as e:
  sys.exit("ERROR: while reading falcon table - " + str(e))
  
def createFccAggDf(fccDf,fctDf):
 try:
  fccAggDf = fccDf.join(fctDf,['orig_account_nbr'],'left')
  fccAggDf.cache()
  return fccAggDf
 except Exception as e:
  sys.exit("ERROR: while creating fraud claim case aggregate dataframe - " + str(e))
  
def computeRiskFalconAttributes(falconDf,aggKey,runDay):
 try:                                         
  riskFalconAttrDf = falconDf.groupBy(aggKey)\
                               .agg(
                                    _sum(expr("case when datediff('" + runDay + "',tran_date) <= 1 then 1 else 0 end")).alias("auth_1d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 7 then 1 else 0 end")).alias("auth_7d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 30 then 1 else 0 end")).alias("auth_30d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 60 then 1 else 0 end")).alias("auth_60d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 90 then 1 else 0 end")).alias("auth_90d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 1 then tran_amount else 0 end")).alias("auth_amt_1d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 7 then tran_amount else 0 end")).alias("auth_amt_7d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 30 then tran_amount else 0 end")).alias("auth_amt_30d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 60 then tran_amount else 0 end")).alias("auth_amt_60d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',tran_date) <= 90 then tran_amount else 0 end")).alias("auth_amt_90d")\
                                   ).dropDuplicates()
  riskFalconAttrDf.cache()
  return riskFalconAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing risk falcon attributes - " + str(e))

def computeRiskFCCAttributes(fccAggDf,aggKey,runDay):
 try:
  riskFccAttrDf = fccAggDf.groupBy(aggKey)\
                               .agg(
                                    _sum(expr("case when datediff('" + runDay + "',reported_date) <= 1 then 1 else 0 end")).alias("fraud_1d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 7 then 1 else 0 end")).alias("fraud_7d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 30 then 1 else 0 end")).alias("fraud_30d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 60 then 1 else 0 end")).alias("fraud_60d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 90 then 1 else 0 end")).alias("fraud_90d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 1 then net_fraud_amt else 0 end")).alias("fraud_amt_1d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 7 then net_fraud_amt else 0 end")).alias("fraud_amt_7d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 30 then net_fraud_amt else 0 end")).alias("fraud_amt_30d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 60 then net_fraud_amt else 0 end")).alias("fraud_amt_60d")\
                                   ,_sum(expr("case when datediff('" + runDay + "',reported_date) <= 90 then net_fraud_amt else 0 end")).alias("fraud_amt_90d")\
                                   ).dropDuplicates()
  riskFccAttrDf.cache()
  return riskFccAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing risk fraud attributes - " + str(e))
  
def combinedDf(riskFalconAttrDf,riskFccAttrDf,aggKey):
 try:
  riskAttrDf = riskFalconAttrDf.join(riskFccAttrDf,aggKey,'full')
  riskAttrDf.cache()
  return riskAttrDf
 except Exception as e:
  sys.exit("ERROR: while forming merchant risk attributes - " + str(e))