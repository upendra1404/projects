###############################################################################
# Script Name:amtf_new_tc360_acct_attributes.py                               #
# Purpose: To calculate new tc360 account attributes and load those into EDL  #
# Created by: Upendra Kumar                                                   #
# Create Date: 02/28/2021                                                     #
# Modify date:                                                                #
###############################################################################

from pyspark.sql.types import FloatType,IntegerType,ArrayType,StringType,DataType,StructField,StructType,TimestampType,DateType,LongType,DecimalType
from pyspark.sql import SparkSession,SQLContext,HiveContext,Window,Row
from pyspark.sql.functions import sum as _sum,mean as _mean, stddev_pop as _stddev,col,coalesce,lit,split,trim,size,lpad,length,to_date,concat,substring,current_date,expr,datediff,udf,array,desc,date_sub,count,collect_list,max,min,to_timestamp,row_number,rank,collect_set,explode,explode_outer,round,current_timestamp,date_format,broadcast,countDistinct,regexp_replace,when,bin
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
                if ('wrksRawUIColumns' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section SCHEMA doesn't contain wrksRawUIColumns")
                else:
                    wrksRawUIColumns = config.get('SCHEMA', 'wrksRawUIColumns')
                if ('colsAcctTable' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section SCHEMA doesn't contain colsAcctTable")
                else:
                    colsAcctTable = config.get('SCHEMA', 'colsAcctTable')
                if ('falconInjectionDate' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain falconInjectionDate")
                else:
                    falconInjectionDate = config.get('HIVE', 'falconInjectionDate')
                if ('pcStmtFact' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcStmtFact")
                else:
                    pcStmtFact = config.get('HIVE', 'pcStmtFact')
                if ('pcAcctLexid' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcAcctLexid")
                else:
                    pcAcctLexid = config.get('HIVE', 'pcAcctLexid')
                if ('tc360fcc' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain tc360fcc")
                else:
                    tc360fcc = config.get('HDFSPATH', 'tc360fcc')
                if ('pcAppFact' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcAppFact")
                else:
                    pcAppFact = config.get('HIVE', 'pcAppFact')
                if ('falconRawPath' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain falconRawPath")
                else:
                    falconRawPath = config.get('HIVE', 'falconRawPath')
                if ('ts797ExtPath' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain ts797ExtPath")
                else:
                    ts797ExtPath = config.get('HDFSPATH', 'ts797ExtPath')
                if ('ts323ExtPath' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain ts323ExtPath")
                else:
                    ts323ExtPath = config.get('HDFSPATH', 'ts323ExtPath')
                if ('ts257ExtPath' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain ts257ExtPath")
                else:
                    ts257ExtPath = config.get('HDFSPATH', 'ts257ExtPath')
                if ('wrksUIRawExtPath' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain wrksUIRawExtPath")
                else:
                    wrksUIRawExtPath = config.get('HDFSPATH', 'wrksUIRawExtPath')
                if ('tc360_account_hive' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain tc360_account_hive")
                else:
                    tc360_account_hive = config.get('HIVE','tc360_account_hive')
                if ('extFalconAttr' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extFalconAttr")
                else:
                    extFalconAttr = config.get('HDFSPATH','extFalconAttr')
                if ('extPathCustomer' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extPathCustomer")
                else:
                    extPathCustomer = config.get('HDFSPATH','extPathCustomer')
                if ('extNMFactPath' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extNMFactPath")
                else:
                    extNMFactPath = config.get('HDFSPATH','extNMFactPath')
                if ('extFalconBin6Path' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extFalconBin6Path")
                else:
                    extFalconBin6Path = config.get('HDFSPATH','extFalconBin6Path')
                if ('extPathAcct' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section HDFSPATH doesn't contain extPathAcct")
                else:
                    extPathAcct = config.get('HDFSPATH','extPathAcct')
                if ('edl_amf_falcon_raw' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_amf_falcon_raw")
                else:
                    edl_amf_falcon_raw = config.get('HIVE','edl_amf_falcon_raw')
                if ('edl_wrks_call_raw' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain edl_wrks_call_raw")
                else:
                    edl_wrks_call_raw = config.get('HIVE','edl_wrks_call_raw')
                if ('edl_amf_nonmon' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_nonmon")
                else:
                    edl_amf_nonmon = config.get('HIVE','edl_amf_nonmon')
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
                            ,'tc360_account_hive': tc360_account_hive
                            ,'pcStmtFact': pcStmtFact
                            ,'pcAppFact': pcAppFact
                            ,'tc360fcc': tc360fcc
                            ,'ts797ExtPath': ts797ExtPath
                            ,'ts323ExtPath': ts323ExtPath
                            ,'ts257ExtPath': ts257ExtPath
                            ,'wrksUIRawExtPath': wrksUIRawExtPath
                            ,'runDay': runDay
                            ,'extFalconAttr': extFalconAttr
                            ,'extPathCustomer': extPathCustomer
                            ,'extFalconBin6Path': extFalconBin6Path
                            ,'extNMFactPath': extNMFactPath
                            ,'wrksRawUIColumns': wrksRawUIColumns
                            ,'colsAcctTable': colsAcctTable
                            ,'falconInjectionDate': falconInjectionDate
                            ,'edl_amf_falcon_raw': edl_amf_falcon_raw
                            ,'edl_wrks_call_raw': edl_wrks_call_raw
                            ,'edl_amf_nonmon': edl_amf_nonmon
                            ,'extPathAcct': extPathAcct
                            ,'debug': debug
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('tc360_account_attribute_feature').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
 spark.conf.set('spark.sql.autoBroadcastJoinThreshold','200000000')
 spark.conf.set('spark.sql.broadcastTimeout','-1')
 spark.sparkContext.addFile("/data1/EDL/domains/amf_qa/edl_amf_qa/python/amtf_reusable_functions.py")
 spark.sql("SET spark.kryo.referenceTrackingEnabled=false")
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
def readFalconAttributes(extFalconAttr,runDay,spark):
 try:
  extFalconAttr = extFalconAttr+"/as_of_date=" + runDay
  falconAttrDf = spark.read.parquet(extFalconAttr).drop(*['as_of_date']).dropDuplicates()
  return falconAttrDf
 except Exception as e:
  sys.exit("ERROR:while reading falcon attributes from falcon attribute table - " + str(e))

def readFCC(tc360fcc,runDay,spark):
 ## Reading fraud claim case data
 try:
  fccDf = spark.read.parquet(tc360fcc).where(col('reported_date') < lit(runDay))\
                                      .select(col('case_id'),trim(col('orig_account_nbr')).alias('orig_account_nbr')
                                             ,col('current_account_nbr_pty')
                                             ,col('loss_type_cd'),col('reported_date')
                                             ,col('possible_fraud_cd')
                                             ,col('gross_fraud_amt')
                                             ,col('net_fraud_amt')).dropDuplicates()
  return fccDf
 except Exception as e:
  sys.exit("ERROR: while reading fraud claim case table - " + str(e))

def readTransmitSegments(ts797ExtPath,ts323ExtPath,ts257ExtPath,runDay,spark):
 ## Reading segment 979 source data
 try:
  ts979Df = spark.read.parquet(ts797ExtPath)\
                      .select(trim(col('v27318_passive_leveling_result')).alias('v27318_passive_leveling_result'),trim(col('v26133_passive_payfone_result')).alias('v26133_passive_payfone_result'),col('v04621_time'),trim(col('v02254_user_id')).alias('v02254_user_id')).dropDuplicates()\
                      .withColumn('v04621_time',date_format((col('v04621_time')/(1e3)).cast(dataType = TimestampType()),'yyyy-MM-dd')).where("v02254_user_id is not null or v02254_user_id !=''").where(col('v04621_time') < lit(runDay))
  ts323Df = spark.read.parquet(ts323ExtPath)\
                      .select(trim(col('v02254_user_id')).alias('v02254_user_id'),trim(col('v02027_token_lvl')).alias('v02027_token_lvl'),col('v04621_time')).dropDuplicates()\
                      .withColumn('v04621_time',to_date((col('v04621_time')/(1e3)).cast(dataType = TimestampType())))\
                      .where(col('v04621_time') < lit(runDay)).where("v02254_user_id is not null or v02254_user_id !=''")
  ts257Df = spark.read.parquet(ts257ExtPath)\
                      .select(trim(col('v02254_user_id')).alias('v02254_user_id'),trim(col('v02027_token_lvl')).alias('v02027_token_lvl'),col('v04621_time')).dropDuplicates()\
                      .withColumn('v04621_time',date_format((col('v04621_time')/(1e3)).cast(dataType = TimestampType()),'yyyy-MM-dd')).where("v02254_user_id is not null or v02254_user_id !=''").where(col('v04621_time') < lit(runDay))
  return ts979Df,ts323Df,ts257Df
 except Exception as e:
  sys.exit("ERROR: while reading transmit segment tables - " + str(e))
  
def readWrksUIRaw(wrksUIRawExtPath,wrksUIRawSchema,edl_wrks_call_raw,debug,spark):
 ## Reading schema based wrks UI raw source data
 try:
  if debug == 'yes':
     wrksUIRawDf = spark.read.schema(wrksUIRawSchema).orc(wrksUIRawExtPath)\
                          .where(col('as_of_date') >= to_date(lit('2020-02-25')))\
                          .withColumn('interaction_id',trim(col('interaction_id')))\
                          .select(trim(col('account_nbr')).alias('account_nbr'),col('interaction_id'),col('d_date'),col('t_time')).where("interaction_id is not null").dropDuplicates()
  else:
     wrksUIRawDf = spark.sql("""select * from """ + edl_wrks_call_raw)\
                          .where(col('as_of_date') >= to_date(lit('2020-02-25')))\
                          .withColumn('interaction_id',trim(col('interaction_id')))\
                          .select(trim(col('account_nbr')).alias('account_nbr'),col('interaction_id'),col('d_date'),col('t_time')).where("interaction_id is not null").dropDuplicates()
  return wrksUIRawDf
 except Exception as e:
  sys.exit("ERROR: while reading transmit segment table - " + str(e))
 
def createAggAuthEDf(accXref,ts979Df,ts323Df,ts257Df,wrksUIRawDf):
 ## Computed agragted dataframe for attribute calculations
 try:
  ## Joining transmit segments with UI raw tables
  eAuth979Df = ts979Df.join(wrksUIRawDf,on = wrksUIRawDf['interaction_id'] == ts979Df['v02254_user_id'], how = 'left')
  eAuth323Df = ts323Df.join(wrksUIRawDf,on = wrksUIRawDf['interaction_id'] == ts323Df['v02254_user_id'], how = 'left')
  eAuth257Df = ts257Df.join(wrksUIRawDf,on = wrksUIRawDf['interaction_id'] == ts257Df['v02254_user_id'], how = 'left')
  ## Joining eAuth df with lexid df to fetch orig_account_nbr
  eAuth979AggDf = accXref.join(eAuth979Df,on = accXref['customer_acct_nbr'] == eAuth979Df['account_nbr'], how = 'left')
  eAuth323AggDf = accXref.join(eAuth323Df,on = accXref['customer_acct_nbr'] == eAuth323Df['account_nbr'], how = 'left')
  eAuth257AggDf = accXref.join(eAuth257Df,on = accXref['customer_acct_nbr'] == eAuth257Df['account_nbr'], how = 'left')
  
  eAuth979AggDf.cache()
  eAuth323AggDf.cache()
  eAuth257AggDf.cache()
  return eAuth979AggDf,eAuth323AggDf,eAuth257AggDf
 except Exception as e:
  sys.exit("ERROR: while forming auth E aggregate dataframe - " + str(e))

def computeAuthEAttr(eAuth979AggDf,eAuth323AggDf,eAuth257AggDf,runDay):
 ###############################################################################################################
 # Aggegated dataframe is constructed by joining transmit segment and wrksUIRawDf data frames                  #
 # Aggegated dataframe is constructed by joining transmit Df's and wrksUIRawDf data frames                     # 
 # Computing time boxed authe E attributes (12) aggregated over orig_account_nbr                               #
 ###############################################################################################################
 try:
  eAuth979AttrDf = eAuth979AggDf.groupBy('orig_account_nbr')\
                         .agg(_sum(expr("case when v27318_passive_leveling_result = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_passive_pass_1d')\
                             ,_sum(expr("case when v27318_passive_leveling_result <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_passive_fail_1d')\
                             ,_sum(expr("case when v27318_passive_leveling_result = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_passive_pass_7d')\
                             ,_sum(expr("case when v27318_passive_leveling_result <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_passive_fail_7d')\
                             ,_sum(expr("case when v27318_passive_leveling_result = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_passive_pass_30d')\
                             ,_sum(expr("case when v27318_passive_leveling_result <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_passive_fail_30d')\
                             ,_sum(expr("case when v26133_passive_payfone_result = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_passive_payfone_pass_1d')\
                             ,_sum(expr("case when v26133_passive_payfone_result <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_passive_payfone_fail_1d')\
                             ,_sum(expr("case when v26133_passive_payfone_result = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_passive_payfone_pass_7d')\
                             ,_sum(expr("case when v26133_passive_payfone_result <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_passive_payfone_fail_7d')\
                             ,_sum(expr("case when v26133_passive_payfone_result = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_passive_payfone_pass_30d')\
                             ,_sum(expr("case when v26133_passive_payfone_result <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_passive_payfone_fail_30d')\
                             ).dropDuplicates()
  eAuth323AttrDf = eAuth323AggDf.groupBy('orig_account_nbr')\
                         .agg(_sum(expr("case when v02027_token_lvl = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_instant_link_pass_1d')\
                             ,_sum(expr("case when v02027_token_lvl <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_instant_link_fail_1d')\
                             ,_sum(expr("case when v02027_token_lvl = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_instant_link_pass_7d')\
                             ,_sum(expr("case when v02027_token_lvl <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_instant_link_fail_7d')\
                             ,_sum(expr("case when v02027_token_lvl = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_instant_link_pass_30d')\
                             ,_sum(expr("case when v02027_token_lvl <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_instant_link_fail_30d')\
                             ).dropDuplicates()
  eAuth257AttrDf = eAuth257AggDf.groupBy('orig_account_nbr')\
                         .agg(_sum(expr("case when v02027_token_lvl = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_trust_stamp_pass_1d')\
                             ,_sum(expr("case when v02027_token_lvl <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=1 then 1 else 0 end")).alias('acct_authe_trust_stamp_fail_1d')\
                             ,_sum(expr("case when v02027_token_lvl = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_trust_stamp_pass_7d')\
                             ,_sum(expr("case when v02027_token_lvl <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=7 then 1 else 0 end")).alias('acct_authe_trust_stamp_fail_7d')\
                             ,_sum(expr("case when v02027_token_lvl = '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_trust_stamp_pass_30d')\
                             ,_sum(expr("case when v02027_token_lvl <> '2.0' and datediff(to_date('" + runDay + "'),v04621_time) <=30 then 1 else 0 end")).alias('acct_authe_trust_stamp_fail_30d')\
                             ).dropDuplicates()
  
  eAuthAttrdf = eAuth979AttrDf.join(eAuth323AttrDf,['orig_account_nbr'],'full')
  eAuthAttrdf = eAuthAttrdf.join(eAuth257AttrDf,['orig_account_nbr'],'full')
  return eAuthAttrdf
 except Exception as e:
  sys.exit("ERROR: while computing auth E attributes - " + str(e))

def computeNonMonAttr(nonMonFactDf,accXref,runDay):
 try:
  nmAggDf = accXref.join(nonMonFactDf,nonMonFactDf.chd_account_number == accXref.customer_acct_nbr,'left')
  nmAttrDf = nmAggDf.groupBy('orig_account_nbr')\
                   .agg(_sum(expr("case when transaction_code = '7' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_addr_chg_1d')\
                       ,_sum(expr("case when transaction_code = '7' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_addr_chg_7d')\
                       ,_sum(expr("case when transaction_code = '7' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_addr_chg_30d')\
                       ,_sum(expr("case when transaction_code = '7' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_addr_chg_60d')\
                       ,_sum(expr("case when transaction_code = '7' and  datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_addr_chg_90d')\
                       ,_sum(expr("case when transaction_code = '793' and  datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_email_chg_1d')\
                       ,_sum(expr("case when transaction_code = '793' and  datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_email_chg_7d')\
                       ,_sum(expr("case when transaction_code = '793' and  datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_email_chg_30d')\
                       ,_sum(expr("case when transaction_code = '793' and  datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_email_chg_60d')\
                       ,_sum(expr("case when transaction_code = '793' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_email_chg_90d')\
                       ,_sum(expr("case when transaction_code = '37' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_home_phn_chg_1d')\
                       ,_sum(expr("case when transaction_code = '37' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_home_phn_chg_7d')\
                       ,_sum(expr("case when transaction_code = '37' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_home_phn_chg_30d')\
                       ,_sum(expr("case when transaction_code = '37' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_home_phn_chg_60d')\
                       ,_sum(expr("case when transaction_code = '37' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_home_phn_chg_90d')\
                       ,_sum(expr("case when transaction_code = '56' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_business_phn_chg_1d')\
                       ,_sum(expr("case when transaction_code = '56' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_business_phn_chg_7d')\
                       ,_sum(expr("case when transaction_code = '56' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_business_phn_chg_30d')\
                       ,_sum(expr("case when transaction_code = '56' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_business_phn_chg_60d')\
                       ,_sum(expr("case when transaction_code = '56' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_business_phn_chg_90d')\
                       ,_sum(expr("case when transaction_code = '200' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_rush_plastic_1d')\
                       ,_sum(expr("case when transaction_code = '200' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_rush_plastic_7d')\
                       ,_sum(expr("case when transaction_code = '200' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_rush_plastic_30d')\
                       ,_sum(expr("case when transaction_code = '200' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_rush_plastic_60d')\
                       ,_sum(expr("case when transaction_code = '200' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_rush_plastic_90d')\
                       ,_sum(expr("case when transaction_code = '110' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_new_plastic_auth_1d')\
                       ,_sum(expr("case when transaction_code = '110' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_new_plastic_auth_7d')\
                       ,_sum(expr("case when transaction_code = '110' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_new_plastic_auth_30d')\
                       ,_sum(expr("case when transaction_code = '110' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_new_plastic_auth_60d')\
                       ,_sum(expr("case when transaction_code = '110' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_new_plastic_auth_90d') \
                       ,_sum(expr("case when transaction_code = '42' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_pin_chg_1d') \
                       ,_sum(expr("case when transaction_code = '42' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_pin_chg_7d') \
                       ,_sum(expr("case when transaction_code = '42' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_pin_chg_30d') \
                       ,_sum(expr("case when transaction_code = '42' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_pin_chg_60d') \
                       ,_sum(expr("case when transaction_code = '42' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_pin_chg_90d') \
                       ,_sum(expr("case when transaction_code = '136' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_mother_maiden_name_chg_1d') \
                       ,_sum(expr("case when transaction_code = '136' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_mother_maiden_name_chg_7d') \
                       ,_sum(expr("case when transaction_code = '136' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_mother_maiden_name_chg_30d') \
                       ,_sum(expr("case when transaction_code = '136' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_mother_maiden_name_chg_60d') \
                       ,_sum(expr("case when transaction_code = '136' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_mother_maiden_name_chg_90d') \
                       ,_sum(expr("case when transaction_code = '9' and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_credit_line_chg_1d')\
                       ,_sum(expr("case when transaction_code = '9' and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_credit_line_chg_7d')\
                       ,_sum(expr("case when transaction_code = '9' and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_credit_line_chg_30d')\
                       ,_sum(expr("case when transaction_code = '9' and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_credit_line_chg_60d')\
                       ,_sum(expr("case when transaction_code = '9' and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_credit_line_chg_90d') \
                       ,_sum(expr("case when transaction_code in ('36','189') and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_dob_chg_1d') \
                       ,_sum(expr("case when transaction_code in ('36','189') and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_dob_chg_7d') \
                       ,_sum(expr("case when transaction_code in ('36','189') and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_dob_chg_30d') \
                       ,_sum(expr("case when transaction_code in ('36','189') and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_dob_chg_60d') \
                       ,_sum(expr("case when transaction_code in ('36','189') and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_dob_chg_90d') \
                       ,_sum(expr("case when transaction_code in ('31','33') and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_name_chg_1d') \
                       ,_sum(expr("case when transaction_code in ('31','33') and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_name_chg_7d') \
                       ,_sum(expr("case when transaction_code in ('31','33') and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_name_chg_30d') \
                       ,_sum(expr("case when transaction_code in ('31','33') and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_name_chg_60d') \
                       ,_sum(expr("case when transaction_code in ('31','33') and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_name_chg_90d') \
                       ,_sum(expr("case when transaction_code in ('35','220') and datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_ssn_chg_1d') \
                       ,_sum(expr("case when transaction_code in ('35','220') and datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_ssn_chg_7d') \
                       ,_sum(expr("case when transaction_code in ('35','220') and datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_ssn_chg_30d') \
                       ,_sum(expr("case when transaction_code in ('35','220') and datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_ssn_chg_60d') \
                       ,_sum(expr("case when transaction_code in ('35','220') and datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_ssn_chg_90d') \
                       ,_sum(expr("case when datediff(to_date('" + runDay + "'),tran_date) <=1 then 1 else 0 end")).alias('acct_cnm_total_1d') \
                       ,_sum(expr("case when datediff(to_date('" + runDay + "'),tran_date) <=7 then 1 else 0 end")).alias('acct_cnm_total_7d') \
                       ,_sum(expr("case when datediff(to_date('" + runDay + "'),tran_date) <=30 then 1 else 0 end")).alias('acct_cnm_total_30d') \
                       ,_sum(expr("case when datediff(to_date('" + runDay + "'),tran_date) <=60 then 1 else 0 end")).alias('acct_cnm_total_60d') \
                       ,_sum(expr("case when datediff(to_date('" + runDay + "'),tran_date) <=90 then 1 else 0 end")).alias('acct_cnm_total_90d') \
                        ).dropDuplicates()
  return nmAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing nonmom attributes - " + str(e))
  
def computeCAAttributes(extPathCustomer,lexidDf,runDay,spark):
 # Computing customer account attributes
  try:
   extPathCustomer = extPathCustomer+"/as_of_date=" + runDay
   readCustDf = spark.read.parquet(extPathCustomer).drop(*['edl_load_ts','as_of_date'])
   lexidDf = lexidDf['cust_lexid','orig_account_nbr'].withColumn('cust_lexid',explode_outer(split(regexp_replace(col('cust_lexid').cast('string'), "([^0-9a-zA-Z,])",""), ",")))
   custAcctAttrDf = lexidDf.join(readCustDf,['cust_lexid'] ,'left')
   custAcctAttrDf = custAcctAttrDf.drop(*['cust_lexid'])
   return custAcctAttrDf
  except Exception as e:
   sys.exit("ERROR: while computing customer account attributes - " + str(e))
 
def readFalconBin6Attributes(extFalconBin6Path,runDay,spark):
 try:
  extFalconBin6Path = extFalconBin6Path+"/as_of_date="+runDay
  colsBin6Attr = ['bin6']+['acct_sale_bin6_1d','acct_sale_bin6_7d','acct_sale_bin6_30d','acct_sale_bin6_60d','acct_sale_bin6_90d']+['acct_sale_amt_bin6_1d','acct_sale_amt_bin6_7d','acct_sale_amt_bin6_30d','acct_sale_amt_bin6_60d','acct_sale_amt_bin6_90d']
  bin6FalconAttr6Df = spark.read.parquet(extFalconBin6Path).drop(*['edl_load_ts']).toDF(*colsBin6Attr)
  return bin6FalconAttr6Df
 except Exception as e:
  sys.exit("ERROR: while computing bin6 falcon attributes - " + str(e))

# Function to read nonmon raw table
def readNonMonFact(extNMFactPath,edl_amf_nonmon,runDay,spark):
 try:
  nonMonFactDf = spark.sql("select * from " + edl_amf_nonmon)\
                      .where((col('as_of_date') < lit(runDay)) & (col('as_of_date') >= date_sub(lit(runDay),int(90))))\
                      .select(trim(col('dat_chd_account_number')).alias('chd_account_number')
                      ,trim(expr("ptyUnprotectStr(dat_new_field_value,'dtStrNPL')")).alias('new_field_value')
                      ,trim(expr("ptyUnprotectStr(dat_old_field_value,'dtStrNPL')")).alias('old_field_value')
                      ,trim(col('dat_transaction_code')).alias('transaction_code')
                      ,col('dat_tran_dt').alias('tran_date')
                      ,col('dat_tran_tm').alias('tran_time')).dropDuplicates()

  nonMonFactDf.cache()
  return nonMonFactDf
 except Exception as e:
  sys.exit("ERROR: while reading nonmon fact table - " + str(e))
  
## Formatting data frames according to script needs
def readPrecursorData(pcAcctLexid,pcStmtFact,pcAppFact,runDay,spark):
 try:
  wdSpecAccStatus = Window.partitionBy('orig_account_nbr').orderBy(lit("X")) 
  lexidDf = spark.read.parquet(pcAcctLexid).drop(*['edl_load_ts'])
  
  accXref = lexidDf['orig_account_nbr','current_account_nbr','customer_acct_nbr','external_status']\
           .withColumn('count',count('current_account_nbr').over(wdSpecAccStatus))\
           .withColumn('allClsCnt',_sum(expr("case when (trim(external_status) not in ('A','') and external_status is not null) and orig_account_nbr <> '0000000000000000' then 1 else 0 end")).over(wdSpecAccStatus))\
           .where(col('count') != col('allClsCnt'))\
           .drop(*['count','allClsCnt','external_status'])\
           .dropDuplicates()
  
  lexidDf = lexidDf.filter("(trim(external_status) in ('A','') or external_status is null) and orig_account_nbr <> '0000000000000000'")\
                   .drop(*['external_status','external_status_reason_code'])\
                   .dropDuplicates().groupBy('orig_account_nbr')\
                   .agg(coalesce(collect_set('lexid1'),array().cast('array<bigint>')).alias('cust_lexid')
                       ,coalesce(collect_set('current_account_nbr'),array().cast('array<string>')).alias('current_account_nbr')
                       ,coalesce(collect_set('customer_acct_nbr'),array().cast('array<string>')).alias('current_account_nbr_pty')
                       ,coalesce(collect_set('acct_bin'),array().cast('array<string>')).alias('acct_bin')
                       ,max('sys').alias('sys')
                       ,to_date(max('last_plastic_date')).alias('acct_last_card_issue_date')
                       ,max('nbr_of_cards_current').alias('acct_total_cards_issued')
                       ,min('open_date').alias('acct_open_date'))\
                   .withColumn('acct_days_on_book',datediff(lit(runDay),to_date('acct_open_date')))\
                   .withColumn('acct_days_since_last_card',datediff(lit(runDay),to_date('acct_last_card_issue_date')))\
                   .withColumn('acct_card_network',expr("case when sys in ('6362') then 'DISC' \
                                                                        when sys in ('1364','1366','1409','1192','1246','1192','1468','1469','1312','1410','1408','1136','1363','1365','1327','1326','1407','1411','3935') then 'MC' \
                                                                        when sys in ('5156','5166','3179','5160','3941','3394','3394','4543','3659','3665','5168','5931','3895','3894') then 'VISA' \
                                                                        else 'PLCC' end"))
        
  
  
  stmtDf = spark.read.parquet(pcStmtFact)\
                .drop(*['edl_load_ts']).filter("trim(external_status) in ('A','') or external_status is null")\
                .drop(*['external_status','writeoff_amt'])\
                .groupBy('orig_account_nbr')\
                .agg(max('credit_limit_amt').alias('acct_credit_line_curr'),max('curr_mnth_balance_amt').alias('acct_bal'))\
                .withColumn('acct_open_to_buy',col('acct_credit_line_curr')-col('acct_bal'))\
                .withColumn('acct_util_ratio',col('acct_bal')/col('acct_credit_line_curr'))\
                .dropDuplicates()
  
  appFactDf = spark.read.parquet(pcAppFact).drop(*['edl_load_ts']).withColumnRenamed('credit_limit_amt','acct_credit_line_orig')
  
  lexidDf.cache()
  stmtDf.cache()
  accXref.cache()
  appFactDf.cache()
  return lexidDf,stmtDf,appFactDf,accXref
 except Exception as e:
  sys.exit("ERROR: while pre-cursor data from precursor tables - " + str(e))

def deriveACI_0_2(df):
 # Logic to derive aci_0_2 field for account table
 try:
  df = df.withColumn('cf_001',col('ACCT_BAL')).withColumn('cf_002',round(col('ACCT_SALE_AMT_PER_DAY_30_STD'),6).cast('double')).withColumn('cf_003',round(col('ACCT_TOP_MCC_90D_AMT_PER_DAY'),2).cast('double')).withColumn('cf_004',col('ACCT_AS_AUTH_90day')).withColumn('cf_005',col('ACCT_FRAUD_CLAIM_NON_TNF_TOT')).withColumn('cf_006',round(col('ACCT_WORLD_AUTH_PCT_90d'),2).cast('double'))
  df = df.na.fill({'cf_001': -99999999,'cf_002': 0,'cf_003':0,'cf_004':0,'cf_005':0,'cf_006':0})
  df = df.withColumn('IND_ACCT_AS_AUTH_60D',when(col('ACCT_AS_AUTH_60day') > lit(float(0.5)), 1).otherwise(0))\
       .withColumn('IND_ACCT_POS81_AUTH_PCT_30D',when(round(col('ACCT_POS81_AUTH_PCT_30D'),2).cast('double') > lit(float(0.005)), 1).otherwise(0))\
       .withColumn('IND_ACCT_SALE_PER_DAY_7',when(round(col('ACCT_SALE_PER_DAY_7'),2).cast('double') > lit(float(0.5)), 1).otherwise(0))\
       .withColumn('IND_ACCT_DECLINED_AUTH_30D',when(col('ACCT_DECLINED_AUTH_30day') > lit(float(0.5)), 1).otherwise(0))\
       .withColumn('NUM_INDICATORS',col('IND_ACCT_AS_AUTH_60D')+col('IND_ACCT_POS81_AUTH_PCT_30D')+col('IND_ACCT_SALE_PER_DAY_7')+col('IND_ACCT_DECLINED_AUTH_30D'))
  df = df.withColumn('aci_h',((col('cf_006') > lit(int(0)))\
                         & (col('cf_001') < lit(int(41)))\
                         & (col('cf_001') > lit(float(1.49)))\
                         & (col('cf_002') < lit(float(2.59)))\
                         & (col('cf_002') > lit(float(2.33)))\
                         & ((col('cf_003') < lit(float(0.185))) | (col('cf_003').isNull()))\
                         & (col('cf_004') > lit(int(0)))\
                         ))
  df = df.withColumn('aci_m',((col('cf_006') > 0) & ((col('cf_005') >= lit(float(9.5))) | ((col('cf_005') >= lit(float(5.5))) & (col('NUM_INDICATORS') >= lit(int(2)))))))
  df = df.withColumn('aci_l',((col('cf_006') > 0) & (col('cf_005') >= lit(float(3.5))) & (col('NUM_INDICATORS') >= lit(int(3)))))
  df = df.withColumn('aci_world',(col('cf_006') > lit(float(0))))
  df = df.withColumn('aci_plcc',col('ACCT_CARD_NETWORK') != 'PLCC')
  df = df.withColumn('aci_0_2',when(col('aci_h'),5)\
                               .when(col('aci_m'),4)\
                               .when(col('aci_l'),3)\
                               .when(col('aci_world'),2)\
                               .when(col('aci_plcc'),1)\
                               .otherwise(0))
  df = df.drop(*['cf_001','cf_002','cf_003','cf_004','cf_005','cf_006','aci_h','aci_m','aci_l','aci_world','aci_plcc','NUM_INDICATORS','IND_ACCT_AS_AUTH_60D','IND_ACCT_POS81_AUTH_PCT_30D','IND_ACCT_SALE_PER_DAY_7','IND_ACCT_DECLINED_AUTH_30D'])
  df.cache()
  return df
 except Exception as e:
  sys.exit("ERROR: while deriving aci_0_2 attribute - " + str(e))
  
def createCombinedDf(lexidDf,falconAttrDf,stmtDf,appFactDf,fccAttrDf,fccBin6AttrDf,bin6FalconAttr6Df,eAuthAttrdf,custAcctAttrDf,nmAttrDf,falconInjectionDate,runDay):
 ################################################################################################################
 # Create dataframe for tc360 attribute computation                                                             #
 ################################################################################################################
 try:
  lexidDf = lexidDf.withColumn('bin',explode(split(regexp_replace(col('acct_bin').cast('string'), "([^0-9a-zA-Z,])",""), ",")))
  lexidDf = lexidDf.join(falconAttrDf,["orig_account_nbr"],'left_outer')
  lexidDf = lexidDf.join(stmtDf,['orig_account_nbr'],'left_outer')
  lexidDf = lexidDf.join(appFactDf,lexidDf['orig_account_nbr'] == appFactDf["logical_application_key"], how = 'left_outer')
  lexidDf = lexidDf.join(fccAttrDf,["orig_account_nbr"],'left_outer')
  lexidDf = lexidDf.join(eAuthAttrdf,["orig_account_nbr"],'left_outer')
  lexidDf = lexidDf.join(custAcctAttrDf,["orig_account_nbr"],'left_outer')
  lexidDf = lexidDf.join(nmAttrDf,['orig_account_nbr'],'left')
  lexidDf = lexidDf.join(broadcast(fccBin6AttrDf),lexidDf['bin']==fccBin6AttrDf['bin6'], how = 'left')
  lexidDf = lexidDf.join(broadcast(bin6FalconAttr6Df),lexidDf['bin']==bin6FalconAttr6Df['bin6'], how = 'left').drop(*['bin6'])
  
  tc360CombinedDf = lexidDf.drop(*['bin']).withColumn('acct_sale_per_day',col('acct_total_sales_cnt')/datediff(lit(runDay),expr("case when acct_open_date<= " + falconInjectionDate + " then acct_open_date else " + falconInjectionDate + " end")))\
                           .withColumn('acct_sale_amt_per_day',col('acct_total_sales_amt')/datediff(lit(runDay),expr("case when acct_open_date<= " + falconInjectionDate + " then acct_open_date else " + falconInjectionDate + " end")))\
                           .select("*")
                           
  tc360CombinedDf.cache()
  return tc360CombinedDf
 except Exception as e:
  sys.exit("ERROR: computing tc360 attributes at final step - " + str(e))

####################################################################################################################################
## Calculating C360 attributes
def calculateFccAttributes(lexidDf,fccDf,runDay):
 #################################################################################################################################################
 # Performing the aggregations on data frame and calculating attributes from fraud claim case                                                    #
 # Computing the distinct count of case id's                                                                                                     #
 # Joining the dataframes to get the lookup field                                                                                                #
 #################################################################################################################################################
 try:
  fccAggDf = lexidDf[['current_account_nbr_pty','orig_account_nbr']].join(broadcast(fccDf),['orig_account_nbr'],'left')
  fccDf = fccDf.withColumn('bin6',substring(expr("ptyUnprotectStr(current_account_nbr_pty,'dtStrNPL')"),1,6))
  fccAttrDf = fccAggDf.groupBy('orig_account_nbr')\
                     .agg(countDistinct(expr("case when trim(loss_type_cd) in ('00','01') then case_id end")).alias('acct_fraud_claim_ls_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) in ('02','04','05') then case_id end")).alias('acct_fraud_claim_other_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '06' then case_id end")).alias('acct_fraud_claim_cnp_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' then case_id end")).alias('acct_fraud_claim_tnf_tot')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) <> '03' then case_id end")).alias('acct_fraud_claim_non_tnf_tot')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' then gross_fraud_amt else 0 end ")).alias('acct_fraud_claim_non_tnf_gross_amt')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' then net_fraud_amt else 0 end ")).alias('acct_fraud_claim_non_tnf_net_amt')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' then gross_fraud_amt else 0 end ")).alias('acct_fraud_claim_tnf_gross_amt')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' then net_fraud_amt else 0 end ")).alias('acct_fraud_claim_tnf_net_amt')
                                       ).dropDuplicates()
  fccBin6AttrDf = fccDf.groupBy('bin6').agg(countDistinct(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=1 then case_id else 0 end")).alias('acct_fraud_claim_non_tnf_bin6_1d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=7 then case_id else 0 end")).alias('acct_fraud_claim_non_tnf_bin6_7d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=30 then case_id else 0 end")).alias('acct_fraud_claim_non_tnf_bin6_30d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=60 then case_id else 0 end")).alias('acct_fraud_claim_non_tnf_bin6_60d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=90 then case_id else 0 end")).alias('acct_fraud_claim_non_tnf_bin6_90d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=1 then case_id else 0 end")).alias('acct_fraud_claim_tnf_bin6_1d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=7 then case_id else 0 end")).alias('acct_fraud_claim_tnf_bin6_7d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=30 then case_id else 0 end")).alias('acct_fraud_claim_tnf_bin6_30d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=60 then case_id else 0 end")).alias('acct_fraud_claim_tnf_bin6_60d')\
                                       ,countDistinct(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=90 then case_id else 0 end")).alias('acct_fraud_claim_tnf_bin6_90d')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=1 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_gross_amt_bin6_1d')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=7 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_gross_amt_bin6_7d')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=30 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_gross_amt_bin6_30d')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=60 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_gross_amt_bin6_60d')\
                                       ,_sum(expr("case when trim(loss_type_cd) <> '03' and datediff(to_date('" + runDay + "'),reported_date)<=90 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_non_tnf_gross_amt_bin6_90d')
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=1 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_gross_amt_bin6_1d')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=7 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_gross_amt_bin6_7d')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=30 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_gross_amt_bin6_30d')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=60 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_gross_amt_bin6_60d')\
                                       ,_sum(expr("case when trim(loss_type_cd) = '03' and datediff(to_date('" + runDay + "'),reported_date)<=90 then gross_fraud_amt else 0 end")).alias('acct_fraud_claim_tnf_gross_amt_bin6_90d')).dropDuplicates()
  return fccAttrDf,fccBin6AttrDf
 except Exception as e:
  sys.exit("ERROR: while computing fcc attributes - " + str(e))
 
def createLoadReadyDf(tc360CombinedDf,colsAcctTable,runDay,spark):
 try:
  ##Forming final tc360 account level attributes data frame
  tc360CombinedDf = deriveACI_0_2(tc360CombinedDf) # Computing aci_0_2 attribute
  tc360CombinedDf.createOrReplaceTempView("v_Tc360AcctAttrDF")
  tc360AcctAttrDf = spark.sql("""select """ + colsAcctTable + """ from v_Tc360AcctAttrDF""")\
                         .dropDuplicates()\
                         .withColumn('cust_lexid',col('cust_lexid').cast('string'))\
                         .withColumn('orig_account_nbr',col('orig_account_nbr').cast('string'))\
                         .withColumn('current_account_nbr',col('current_account_nbr').cast('string'))\
                         .withColumn('current_account_nbr_pty',col('current_account_nbr_pty').cast('string'))\
                         .withColumn('acct_open_date',col('acct_open_date').cast('date'))\
                         .withColumn('acct_days_on_book',col('acct_days_on_book').cast('bigint'))\
                         .withColumn('acct_days_since_last_card',col('acct_days_since_last_card').cast('bigint'))\
                         .withColumn('acct_total_cards_issued',col('acct_total_cards_issued').cast('int'))\
                         .withColumn('acct_card_network',col('acct_card_network').cast('string'))\
                         .withColumn('acct_total_sales_cnt',col('acct_total_sales_cnt').cast('bigint'))\
                         .withColumn('acct_total_sales_amt',round(col('acct_total_sales_amt'),2).cast("double"))\
                         .withColumn('acct_total_sales_amt_avg',round(col('acct_total_sales_amt_avg'),2).cast("double"))\
                         .withColumn('acct_total_sales_amt_std',round(col('acct_total_sales_amt_std'),2).cast("double"))\
                         .withColumn('acct_sale_per_day',round(col('acct_sale_per_day'),6).cast("double"))\
                         .withColumn('acct_sale_amt_per_day',round(col('acct_sale_amt_per_day'),6).cast("double"))\
                         .withColumn('acct_sale_per_day_1',round(col('acct_sale_per_day_1'),2).cast("double"))\
                         .withColumn('acct_sale_per_day_7',round(col('acct_sale_per_day_7'),2).cast("double"))\
                         .withColumn('acct_sale_per_day_30',round(col('acct_sale_per_day_30'),2).cast("double"))\
                         .withColumn('acct_sale_per_day_60',round(col('acct_sale_per_day_60'),2).cast("double"))\
                         .withColumn('acct_sale_per_day_90',round(col('acct_sale_per_day_90'),2).cast("double"))\
                         .withColumn('acct_sale_amt_per_day_1',round(col('acct_sale_amt_per_day_1'),6).cast("double"))\
                         .withColumn('acct_sale_amt_per_day_7',round(col('acct_sale_amt_per_day_7'),6).cast("double"))\
                         .withColumn('acct_sale_amt_per_day_30',round(col('acct_sale_amt_per_day_30'),6).cast("double"))\
                         .withColumn('acct_sale_amt_per_day_60',round(col('acct_sale_amt_per_day_60'),6).cast("double"))\
                         .withColumn('acct_sale_amt_per_day_90',round(col('acct_sale_amt_per_day_90'),6).cast("double"))\
                         .withColumn('acct_credit_line_orig',round(col('acct_credit_line_orig'),2).cast("double"))\
                         .withColumn('acct_credit_line_curr',round(col('acct_credit_line_curr'),2).cast("double"))\
                         .withColumn('acct_bal',round(col('acct_bal'),2).cast("double"))\
                         .withColumn('acct_open_to_buy',round(col('acct_open_to_buy'),2).cast("double"))\
                         .withColumn('acct_util_ratio',round(col('acct_util_ratio'),2).cast("double"))\
                         .withColumn('acct_total_payment',round(col('acct_total_payment'),2).cast("double"))\
                         .withColumn('acct_as_auth_1day',col('acct_as_auth_1day').cast("int"))\
                         .withColumn('acct_as_auth_2day',col('acct_as_auth_2day').cast("int"))\
                         .withColumn('acct_as_auth_7day',col('acct_as_auth_7day').cast("int"))\
                         .withColumn('acct_as_auth_30day',col('acct_as_auth_30day').cast("int"))\
                         .withColumn('acct_as_auth_60day',col('acct_as_auth_60day').cast("int"))\
                         .withColumn('acct_as_auth_90day',col('acct_as_auth_90day').cast("int"))\
                         .withColumn('acct_declined_auth_1day',col('acct_declined_auth_1day').cast("int"))\
                         .withColumn('acct_declined_auth_2day',col('acct_declined_auth_2day').cast("int"))\
                         .withColumn('acct_declined_auth_7day',col('acct_declined_auth_7day').cast("int"))\
                         .withColumn('acct_declined_auth_30day',col('acct_declined_auth_30day').cast("int"))\
                         .withColumn('acct_declined_auth_60day',col('acct_declined_auth_60day').cast("int"))\
                         .withColumn('acct_declined_auth_90day',col('acct_declined_auth_90day').cast("int"))\
                         .withColumn('acct_fraud_claim_ls_tot',col('acct_fraud_claim_ls_tot').cast("int"))\
                         .withColumn('acct_fraud_claim_other_tot',col('acct_fraud_claim_other_tot').cast("int"))\
                         .withColumn('acct_fraud_claim_cnp_tot',col('acct_fraud_claim_cnp_tot').cast("int"))\
                         .withColumn('acct_fraud_claim_tnf_tot',col('acct_fraud_claim_tnf_tot').cast("int"))\
                         .withColumn('acct_fraud_claim_non_tnf_tot',col('acct_fraud_claim_non_tnf_tot').cast("int"))\
                         .withColumn('acct_total_sales_declined_cnt',col('acct_total_sales_declined_cnt').cast("int"))\
                         .withColumn('acct_total_sales_declined_amt',round(col('acct_total_sales_declined_amt'),2).cast("double"))\
                         .withColumn('acct_total_sales_declined_amt_avg',round(col('acct_total_sales_declined_amt_avg'),2).cast("double"))\
                         .withColumn('acct_total_sales_declined_amt_std',round(col('acct_total_sales_declined_amt_std'),2).cast("double"))\
                         .withColumn('acct_fraud_claim_non_tnf_gross_amt',round(col('acct_fraud_claim_non_tnf_gross_amt'),2).cast("double"))\
                         .withColumn('acct_fraud_claim_non_tnf_net_amt',round(col('acct_fraud_claim_non_tnf_net_amt'),2).cast("double"))\
                         .withColumn('acct_fraud_claim_tnf_gross_amt',round(col('acct_fraud_claim_tnf_gross_amt'),2).cast("double"))\
                         .withColumn('acct_fraud_claim_tnf_net_amt',round(col('acct_fraud_claim_tnf_net_amt'),2).cast("double"))\
                         .withColumn('acct_crypto_purchase_1d',col('acct_crypto_purchase_1d').cast('int'))\
                         .withColumn('acct_crypto_purchase_7d',col('acct_crypto_purchase_7d').cast('int'))\
                         .withColumn('acct_crypto_purchase_amt_1d',round(col('acct_crypto_purchase_amt_1d'),2).cast('double'))\
                         .withColumn('acct_crypto_purchase_amt_7d',round(col('acct_crypto_purchase_amt_7d'),2).cast('double'))\
                         .withColumn('acct_tot_highrisk_purchase_1d',col('acct_tot_highrisk_purchase_1d').cast('int'))\
                         .withColumn('acct_tot_highrisk_purchase_7d',col('acct_tot_highrisk_purchase_7d').cast('int'))\
                         .withColumn('acct_3ds_attempt_1d',col('acct_3ds_attempt_1d').cast('int'))\
                         .withColumn('acct_3ds_cb_1d',col('acct_3ds_cb_1d').cast('int'))\
                         .withColumn('acct_3ds_attempt_amt_1d',round(col('acct_3ds_attempt_amt_1d'),2).cast('double'))\
                         .withColumn('acct_3ds_cb_amt_1d',round(col('acct_3ds_cb_amt_1d'),2).cast('double'))\
                         .withColumn('acct_sales_amt_1',round(col('acct_sales_amt_1'),2).cast('double'))\
                         .withColumn('acct_sales_amt_2',round(col('acct_sales_amt_2'),2).cast('double'))\
                         .withColumn('acct_sales_amt_3',round(col('acct_sales_amt_3'),2).cast('double'))\
                         .withColumn('acct_sales_amt_4',round(col('acct_sales_amt_4'),2).cast('double'))\
                         .withColumn('acct_sales_amt_5',round(col('acct_sales_amt_5'),2).cast('double'))\
                         .withColumn('acct_sales_amt_6',round(col('acct_sales_amt_6'),2).cast('double'))\
                         .withColumn('acct_sales_amt_7',round(col('acct_sales_amt_7'),2).cast('double'))\
                         .withColumn('acct_sales_amt_8',round(col('acct_sales_amt_8'),2).cast('double'))\
                         .withColumn('acct_sales_amt_9',round(col('acct_sales_amt_9'),2).cast('double'))\
                         .withColumn('acct_sales_amt_10',round(col('acct_sales_amt_10'),2).cast('double'))\
                         .withColumn('acct_sales_ts_1',col('acct_sales_ts_1').cast('timestamp'))\
                         .withColumn('acct_sales_ts_2',col('acct_sales_ts_2').cast('timestamp'))\
                         .withColumn('acct_sales_ts_3',col('acct_sales_ts_3').cast('timestamp'))\
                         .withColumn('acct_sales_ts_4',col('acct_sales_ts_4').cast('timestamp'))\
                         .withColumn('acct_sales_ts_5',col('acct_sales_ts_5').cast('timestamp'))\
                         .withColumn('acct_sales_ts_6',col('acct_sales_ts_6').cast('timestamp'))\
                         .withColumn('acct_sales_ts_7',col('acct_sales_ts_7').cast('timestamp'))\
                         .withColumn('acct_sales_ts_8',col('acct_sales_ts_8').cast('timestamp'))\
                         .withColumn('acct_sales_ts_9',col('acct_sales_ts_9').cast('timestamp'))\
                         .withColumn('acct_sales_ts_10',col('acct_sales_ts_10').cast('timestamp'))\
                         .withColumn('acct_sales_vaa_scr_1',col('acct_sales_vaa_scr_1').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_2',col('acct_sales_vaa_scr_2').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_3',col('acct_sales_vaa_scr_3').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_4',col('acct_sales_vaa_scr_4').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_5',col('acct_sales_vaa_scr_5').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_6',col('acct_sales_vaa_scr_6').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_7',col('acct_sales_vaa_scr_7').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_8',col('acct_sales_vaa_scr_8').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_9',col('acct_sales_vaa_scr_9').cast('int'))\
                         .withColumn('acct_sales_vaa_scr_10',col('acct_sales_vaa_scr_10').cast('int'))\
                         .withColumn('acct_sales_r3_scr_1',col('acct_sales_r3_scr_1').cast('int'))\
                         .withColumn('acct_sales_r3_scr_2',col('acct_sales_r3_scr_2').cast('int'))\
                         .withColumn('acct_sales_r3_scr_3',col('acct_sales_r3_scr_3').cast('int'))\
                         .withColumn('acct_sales_r3_scr_4',col('acct_sales_r3_scr_4').cast('int'))\
                         .withColumn('acct_sales_r3_scr_5',col('acct_sales_r3_scr_5').cast('int'))\
                         .withColumn('acct_sales_r3_scr_6',col('acct_sales_r3_scr_6').cast('int'))\
                         .withColumn('acct_sales_r3_scr_7',col('acct_sales_r3_scr_7').cast('int'))\
                         .withColumn('acct_sales_r3_scr_8',col('acct_sales_r3_scr_8').cast('int'))\
                         .withColumn('acct_sales_r3_scr_9',col('acct_sales_r3_scr_9').cast('int'))\
                         .withColumn('acct_sales_r3_scr_10',col('acct_sales_r3_scr_10').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_1',col('acct_sales_mcdi_scr_1').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_2',col('acct_sales_mcdi_scr_2').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_3',col('acct_sales_mcdi_scr_3').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_4',col('acct_sales_mcdi_scr_4').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_5',col('acct_sales_mcdi_scr_5').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_6',col('acct_sales_mcdi_scr_6').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_7',col('acct_sales_mcdi_scr_7').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_8',col('acct_sales_mcdi_scr_8').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_9',col('acct_sales_mcdi_scr_9').cast('int'))\
                         .withColumn('acct_sales_mcdi_scr_10',col('acct_sales_mcdi_scr_10').cast('int'))\
                         .withColumn('acct_sale_per_day_30_std',round(col('acct_sale_per_day_30_std'),6).cast('double'))\
                         .withColumn('acct_sale_per_day_60_std',round(col('acct_sale_per_day_60_std'),6).cast('double'))\
                         .withColumn('acct_sale_per_day_90_std',round(col('acct_sale_per_day_90_std'),6).cast('double'))\
                         .withColumn('acct_sale_amt_per_day_30_std',round(col('acct_sale_amt_per_day_30_std'),6).cast('double'))\
                         .withColumn('acct_sale_amt_per_day_60_std',round(col('acct_sale_amt_per_day_60_std'),6).cast('double'))\
                         .withColumn('acct_sale_amt_per_day_90_std',round(col('acct_sale_amt_per_day_90_std'),6).cast('double'))\
                         .withColumn('acct_top_mrch_country_1d_amt',round(col('acct_top_mrch_country_1d_amt'),2).cast('double'))\
                         .withColumn('acct_top_mrch_country_1d',col('acct_top_mrch_country_1d').cast('string'))\
                         .withColumn('acct_top_mcc_1d',col('acct_top_mcc_1d').cast('string'))\
                         .withColumn('acct_top_mcc_7d',col('acct_top_mcc_7d').cast('string'))\
                         .withColumn('acct_top_mcc_30d',col('acct_top_mcc_30d').cast('string'))\
                         .withColumn('acct_top_mcc_60d',col('acct_top_mcc_60d').cast('string'))\
                         .withColumn('acct_top_mcc_90d',col('acct_top_mcc_90d').cast('string'))\
                         .withColumn('acct_top_mcc_1d_amt_per_day',round(col('acct_top_mcc_1d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_mcc_7d_amt_per_day',round(col('acct_top_mcc_7d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_mcc_30d_amt_per_day',round(col('acct_top_mcc_30d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_mcc_60d_amt_per_day',round(col('acct_top_mcc_60d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_mcc_90d_amt_per_day',round(col('acct_top_mcc_90d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_pos_1d',col('acct_top_pos_1d').cast('string'))\
                         .withColumn('acct_top_pos_7d',col('acct_top_pos_7d').cast('string'))\
                         .withColumn('acct_top_pos_30d',col('acct_top_pos_30d').cast('string'))\
                         .withColumn('acct_top_pos_60d',col('acct_top_pos_60d').cast('string'))\
                         .withColumn('acct_top_pos_90d',col('acct_top_pos_90d').cast('string'))\
                         .withColumn('acct_top_pos_1d_amt_per_day',round(col('acct_top_pos_1d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_pos_7d_amt_per_day',round(col('acct_top_pos_7d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_pos_30d_amt_per_day',round(col('acct_top_pos_30d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_pos_60d_amt_per_day',round(col('acct_top_pos_60d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_top_pos_90d_amt_per_day',round(col('acct_top_pos_90d_amt_per_day'),2).cast('double'))\
                         .withColumn('acct_sales_amt_1d_from_newmerchant_90d',round(col('acct_sales_amt_1d_from_newmerchant_90d'),2).cast('double'))\
                         .withColumn('acct_authe_passive_pass_1d',col('acct_authe_passive_pass_1d').cast('int'))\
                         .withColumn('acct_authe_passive_pass_7d',col('acct_authe_passive_pass_7d').cast('int'))\
                         .withColumn('acct_authe_passive_pass_30d',col('acct_authe_passive_pass_30d').cast('int'))\
                         .withColumn('acct_authe_passive_fail_1d',col('acct_authe_passive_fail_1d').cast('int'))\
                         .withColumn('acct_authe_passive_fail_7d',col('acct_authe_passive_fail_7d').cast('int'))\
                         .withColumn('acct_authe_passive_fail_30d',col('acct_authe_passive_fail_30d').cast('int'))\
                         .withColumn('acct_authe_passive_payfone_pass_1d',col('acct_authe_passive_payfone_pass_1d').cast('int'))\
                         .withColumn('acct_authe_passive_payfone_pass_7d',col('acct_authe_passive_payfone_pass_7d').cast('int'))\
                         .withColumn('acct_authe_passive_payfone_pass_30d',col('acct_authe_passive_payfone_pass_30d').cast('int'))\
                         .withColumn('acct_authe_passive_payfone_fail_1d',col('acct_authe_passive_payfone_fail_1d').cast('int'))\
                         .withColumn('acct_authe_passive_payfone_fail_7d',col('acct_authe_passive_payfone_fail_7d').cast('int'))\
                         .withColumn('acct_authe_passive_payfone_fail_30d',col('acct_authe_passive_payfone_fail_30d').cast('int'))\
                         .withColumn('acct_world_auth_pct_1d',round(col('acct_world_auth_pct_1d'),2).cast('double'))\
                         .withColumn('acct_world_auth_pct_7d',round(col('acct_world_auth_pct_7d'),2).cast('double'))\
                         .withColumn('acct_world_auth_pct_30d',round(col('acct_world_auth_pct_30d'),2).cast('double'))\
                         .withColumn('acct_world_auth_pct_60d',round(col('acct_world_auth_pct_60d'),2).cast('double'))\
                         .withColumn('acct_world_auth_pct_90d',round(col('acct_world_auth_pct_90d'),2).cast('double'))\
                         .withColumn('acct_world_auth_amt_pct_1d',round(col('acct_world_auth_amt_pct_1d'),2).cast('double'))\
                         .withColumn('acct_world_auth_amt_pct_7d',round(col('acct_world_auth_amt_pct_7d'),2).cast('double'))\
                         .withColumn('acct_world_auth_amt_pct_30d',round(col('acct_world_auth_amt_pct_30d'),2).cast('double'))\
                         .withColumn('acct_world_auth_amt_pct_60d',round(col('acct_world_auth_amt_pct_60d'),2).cast('double'))\
                         .withColumn('acct_world_auth_amt_pct_90d',round(col('acct_world_auth_amt_pct_90d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_pct_1d',round(col('acct_pos81_auth_pct_1d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_pct_7d',round(col('acct_pos81_auth_pct_7d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_pct_30d',round(col('acct_pos81_auth_pct_30d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_pct_60d',round(col('acct_pos81_auth_pct_60d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_pct_90d',round(col('acct_pos81_auth_pct_90d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_amt_pct_1d',round(col('acct_pos81_auth_amt_pct_1d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_amt_pct_7d',round(col('acct_pos81_auth_amt_pct_7d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_amt_pct_30d',round(col('acct_pos81_auth_amt_pct_30d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_amt_pct_60d',round(col('acct_pos81_auth_amt_pct_60d'),2).cast('double'))\
                         .withColumn('acct_pos81_auth_amt_pct_90d',round(col('acct_pos81_auth_amt_pct_90d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_pct_1d',round(col('acct_cb_auth_pct_1d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_pct_7d',round(col('acct_cb_auth_pct_7d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_pct_30d',round(col('acct_cb_auth_pct_30d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_pct_60d',round(col('acct_cb_auth_pct_60d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_pct_90d',round(col('acct_cb_auth_pct_90d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_amt_pct_1d',round(col('acct_cb_auth_amt_pct_1d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_amt_pct_7d',round(col('acct_cb_auth_amt_pct_7d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_amt_pct_30d',round(col('acct_cb_auth_amt_pct_30d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_amt_pct_60d',round(col('acct_cb_auth_amt_pct_60d'),2).cast('double'))\
                         .withColumn('acct_cb_auth_amt_pct_90d',round(col('acct_cb_auth_amt_pct_90d'),2).cast('double'))\
                         .withColumn('acct_authe_instant_link_pass_1d',col('acct_authe_instant_link_pass_1d').cast('int'))\
                         .withColumn('acct_authe_instant_link_pass_7d',col('acct_authe_instant_link_pass_7d').cast('int'))\
                         .withColumn('acct_authe_instant_link_pass_30d',col('acct_authe_instant_link_pass_30d').cast('int'))\
                         .withColumn('acct_authe_instant_link_fail_1d',col('acct_authe_instant_link_fail_1d').cast('int'))\
                         .withColumn('acct_authe_instant_link_fail_7d',col('acct_authe_instant_link_fail_7d').cast('int'))\
                         .withColumn('acct_authe_instant_link_fail_30d',col('acct_authe_instant_link_fail_30d').cast('int'))\
                         .withColumn('acct_authe_trust_stamp_pass_1d',col('acct_authe_trust_stamp_pass_1d').cast('int'))\
                         .withColumn('acct_authe_trust_stamp_pass_7d',col('acct_authe_trust_stamp_pass_7d').cast('int'))\
                         .withColumn('acct_authe_trust_stamp_pass_30d',col('acct_authe_trust_stamp_pass_30d').cast('int'))\
                         .withColumn('acct_authe_trust_stamp_fail_1d',col('acct_authe_trust_stamp_fail_1d').cast('int'))\
                         .withColumn('acct_authe_trust_stamp_fail_7d',col('acct_authe_trust_stamp_fail_7d').cast('int'))\
                         .withColumn('acct_authe_trust_stamp_fail_30d',col('acct_authe_trust_stamp_fail_30d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_1d',col('acct_cvv2_invalid_auth_1d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_2d',col('acct_cvv2_invalid_auth_2d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_7d',col('acct_cvv2_invalid_auth_7d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_30d',col('acct_cvv2_invalid_auth_30d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_60d',col('acct_cvv2_invalid_auth_60d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_90d',col('acct_cvv2_invalid_auth_90d').cast('int'))\
                         .withColumn('acct_cvv2_invalid_auth_amt_1d',round(col('acct_cvv2_invalid_auth_amt_1d'),2).cast('double'))\
                         .withColumn('acct_cvv2_invalid_auth_amt_2d',round(col('acct_cvv2_invalid_auth_amt_2d'),2).cast('double'))\
                         .withColumn('acct_cvv2_invalid_auth_amt_7d',round(col('acct_cvv2_invalid_auth_amt_7d'),2).cast('double'))\
                         .withColumn('acct_cvv2_invalid_auth_amt_30d',round(col('acct_cvv2_invalid_auth_amt_30d'),2).cast('double'))\
                         .withColumn('acct_cvv2_invalid_auth_amt_60d',round(col('acct_cvv2_invalid_auth_amt_60d'),2).cast('double'))\
                         .withColumn('acct_cvv2_invalid_auth_amt_90d',round(col('acct_cvv2_invalid_auth_amt_90d'),2).cast('double'))\
                         .withColumn('acct_avs_fail_auth_1d',col('acct_avs_fail_auth_1d').cast('int'))\
                         .withColumn('acct_avs_fail_auth_2d',col('acct_avs_fail_auth_2d').cast('int'))\
                         .withColumn('acct_avs_fail_auth_7d',col('acct_avs_fail_auth_7d').cast('int'))\
                         .withColumn('acct_avs_fail_auth_30d',col('acct_avs_fail_auth_30d').cast('int'))\
                         .withColumn('acct_avs_fail_auth_60d',col('acct_avs_fail_auth_60d').cast('int'))\
                         .withColumn('acct_avs_fail_auth_90d',col('acct_avs_fail_auth_90d').cast('int'))\
                         .withColumn('acct_avs_fail_auth_amt_1d',round(col('acct_avs_fail_auth_amt_1d'),2).cast('double'))\
                         .withColumn('acct_avs_fail_auth_amt_2d',round(col('acct_avs_fail_auth_amt_2d'),2).cast('double'))\
                         .withColumn('acct_avs_fail_auth_amt_7d',round(col('acct_avs_fail_auth_amt_7d'),2).cast('double'))\
                         .withColumn('acct_avs_fail_auth_amt_30d',round(col('acct_avs_fail_auth_amt_30d'),2).cast('double'))\
                         .withColumn('acct_avs_fail_auth_amt_60d',round(col('acct_avs_fail_auth_amt_60d'),2).cast('double'))\
                         .withColumn('acct_avs_fail_auth_amt_90d',round(col('acct_avs_fail_auth_amt_90d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_non_tnf_bin6_1d',col('acct_fraud_claim_non_tnf_bin6_1d').cast('int'))\
                         .withColumn('acct_fraud_claim_non_tnf_bin6_7d',col('acct_fraud_claim_non_tnf_bin6_7d').cast('int'))\
                         .withColumn('acct_fraud_claim_non_tnf_bin6_30d',col('acct_fraud_claim_non_tnf_bin6_30d').cast('int'))\
                         .withColumn('acct_fraud_claim_non_tnf_bin6_60d',col('acct_fraud_claim_non_tnf_bin6_60d').cast('int'))\
                         .withColumn('acct_fraud_claim_non_tnf_bin6_90d',col('acct_fraud_claim_non_tnf_bin6_90d').cast('int'))\
                         .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_1d',round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_1d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_7d',round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_7d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_30d',round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_30d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_60d',round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_60d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_non_tnf_gross_amt_bin6_90d',round(col('acct_fraud_claim_non_tnf_gross_amt_bin6_90d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_tnf_bin6_1d',col('acct_fraud_claim_tnf_bin6_1d').cast('int'))\
                         .withColumn('acct_fraud_claim_tnf_bin6_7d',col('acct_fraud_claim_tnf_bin6_7d').cast('int'))\
                         .withColumn('acct_fraud_claim_tnf_bin6_30d',col('acct_fraud_claim_tnf_bin6_30d').cast('int'))\
                         .withColumn('acct_fraud_claim_tnf_bin6_60d',col('acct_fraud_claim_tnf_bin6_60d').cast('int'))\
                         .withColumn('acct_fraud_claim_tnf_bin6_90d',col('acct_fraud_claim_tnf_bin6_90d').cast('int'))\
                         .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_1d',round(col('acct_fraud_claim_tnf_gross_amt_bin6_1d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_7d',round(col('acct_fraud_claim_tnf_gross_amt_bin6_7d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_30d',round(col('acct_fraud_claim_tnf_gross_amt_bin6_30d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_60d',round(col('acct_fraud_claim_tnf_gross_amt_bin6_60d'),2).cast('double'))\
                         .withColumn('acct_fraud_claim_tnf_gross_amt_bin6_90d',round(col('acct_fraud_claim_tnf_gross_amt_bin6_90d'),2).cast('double'))\
                         .withColumn('cust_bk_loss_cnt_tot',col('cust_bk_loss_cnt_tot').cast('int'))\
                         .withColumn('cust_aged_loss_cnt_tot',col('cust_aged_loss_cnt_tot').cast('int'))\
                         .withColumn('cust_fraud_loss_cnt_tot',col('cust_fraud_loss_cnt_tot').cast('int'))\
                         .withColumn('cust_other_loss_cnt_tot',col('cust_other_loss_cnt_tot').cast('int'))\
                         .withColumn('cust_total_loss_cnt_tot',col('cust_total_loss_cnt_tot').cast('int'))\
                         .withColumn('cust_fraud_claim_ls_tot',col('cust_fraud_claim_ls_tot').cast('int'))\
                         .withColumn('cust_fraud_claim_cnp_tot',col('cust_fraud_claim_cnp_tot').cast('int'))\
                         .withColumn('cust_fraud_claim_tnf_tot',col('cust_fraud_claim_tnf_tot').cast('int'))\
                         .withColumn('cust_fraud_claim_other_tot',col('cust_fraud_claim_other_tot').cast('int'))\
                         .withColumn('cust_fraud_claim_non_tnf_tot',col('cust_fraud_claim_non_tnf_tot').cast('int'))\
                         .withColumn('cust_aged_loss_amt_tot',round(col('cust_aged_loss_amt_tot'),2).cast('double'))\
                         .withColumn('cust_bk_loss_amt_tot',round(col('cust_bk_loss_amt_tot'),2).cast('double'))\
                         .withColumn('cust_fraud_loss_amt_tot',round(col('cust_fraud_loss_amt_tot'),2).cast('double'))\
                         .withColumn('cust_other_loss_amt_tot',round(col('cust_other_loss_amt_tot'),2).cast('double'))\
                         .withColumn('cust_total_loss_amt_tot',round(col('cust_total_loss_amt_tot'),2).cast('double'))\
                         .withColumn('cust_multi_account_claim_same_day',col('cust_multi_account_claim_same_day').cast('int'))\
                         .withColumn('acct_sale_bin6_1d',col('acct_sale_bin6_1d').cast('int'))\
                         .withColumn('acct_sale_bin6_7d',col('acct_sale_bin6_7d').cast('int'))\
                         .withColumn('acct_sale_bin6_30d',col('acct_sale_bin6_30d').cast('int'))\
                         .withColumn('acct_sale_bin6_60d',col('acct_sale_bin6_60d').cast('int'))\
                         .withColumn('acct_sale_bin6_90d',col('acct_sale_bin6_90d').cast('int'))\
                         .withColumn('acct_sale_amt_bin6_1d',round(col('acct_sale_amt_bin6_1d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_bin6_7d',round(col('acct_sale_amt_bin6_7d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_bin6_30d',round(col('acct_sale_amt_bin6_30d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_bin6_60d',round(col('acct_sale_amt_bin6_60d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_bin6_90d',round(col('acct_sale_amt_bin6_90d'),2).cast('double'))\
                         .withColumn('acct_cnm_addr_chg_1d',col('acct_cnm_addr_chg_1d').cast('int').alias('acct_cnm_addr_chg_1d'))\
                         .withColumn('acct_cnm_addr_chg_7d',col('acct_cnm_addr_chg_7d').cast('int').alias('acct_cnm_addr_chg_7d'))\
                         .withColumn('acct_cnm_addr_chg_30d',col('acct_cnm_addr_chg_30d').cast('int').alias('acct_cnm_addr_chg_30d'))\
                         .withColumn('acct_cnm_addr_chg_60d',col('acct_cnm_addr_chg_60d').cast('int').alias('acct_cnm_addr_chg_60d'))\
                         .withColumn('acct_cnm_addr_chg_90d',col('acct_cnm_addr_chg_90d').cast('int').alias('acct_cnm_addr_chg_90d'))\
                         .withColumn('acct_cnm_email_chg_1d',col('acct_cnm_email_chg_1d').cast('int').alias('acct_cnm_email_chg_1d'))\
                         .withColumn('acct_cnm_email_chg_7d',col('acct_cnm_email_chg_7d').cast('int').alias('acct_cnm_email_chg_7d'))\
                         .withColumn('acct_cnm_email_chg_30d',col('acct_cnm_email_chg_30d').cast('int').alias('acct_cnm_email_chg_30d'))\
                         .withColumn('acct_cnm_email_chg_60d',col('acct_cnm_email_chg_60d').cast('int').alias('acct_cnm_email_chg_60d'))\
                         .withColumn('acct_cnm_email_chg_90d',col('acct_cnm_email_chg_90d').cast('int').alias('acct_cnm_email_chg_90d'))\
                         .withColumn('acct_cnm_home_phn_chg_1d',col('acct_cnm_home_phn_chg_1d').cast('int').alias('acct_cnm_home_phn_chg_1d'))\
                         .withColumn('acct_cnm_home_phn_chg_7d',col('acct_cnm_home_phn_chg_7d').cast('int').alias('acct_cnm_home_phn_chg_7d'))\
                         .withColumn('acct_cnm_home_phn_chg_30d',col('acct_cnm_home_phn_chg_30d').cast('int').alias('acct_cnm_home_phn_chg_30d'))\
                         .withColumn('acct_cnm_home_phn_chg_60d',col('acct_cnm_home_phn_chg_60d').cast('int').alias('acct_cnm_home_phn_chg_60d'))\
                         .withColumn('acct_cnm_home_phn_chg_90d',col('acct_cnm_home_phn_chg_90d').cast('int').alias('acct_cnm_home_phn_chg_90d'))\
                         .withColumn('acct_cnm_business_phn_chg_1d',col('acct_cnm_business_phn_chg_1d').cast('int').alias('acct_cnm_business_phn_chg_1d'))\
                         .withColumn('acct_cnm_business_phn_chg_7d',col('acct_cnm_business_phn_chg_7d').cast('int').alias('acct_cnm_business_phn_chg_7d'))\
                         .withColumn('acct_cnm_business_phn_chg_30d',col('acct_cnm_business_phn_chg_30d').cast('int').alias('acct_cnm_business_phn_chg_30d'))\
                         .withColumn('acct_cnm_business_phn_chg_60d',col('acct_cnm_business_phn_chg_60d').cast('int').alias('acct_cnm_business_phn_chg_60d'))\
                         .withColumn('acct_cnm_business_phn_chg_90d',col('acct_cnm_business_phn_chg_90d').cast('int').alias('acct_cnm_business_phn_chg_90d'))\
                         .withColumn('acct_cnm_credit_line_chg_1d',col('acct_cnm_credit_line_chg_1d').cast('int').alias('acct_cnm_credit_line_chg_1d'))\
                         .withColumn('acct_cnm_credit_line_chg_7d',col('acct_cnm_credit_line_chg_7d').cast('int').alias('acct_cnm_credit_line_chg_7d'))\
                         .withColumn('acct_cnm_credit_line_chg_30d',col('acct_cnm_credit_line_chg_30d').cast('int').alias('acct_cnm_credit_line_chg_30d'))\
                         .withColumn('acct_cnm_credit_line_chg_60d',col('acct_cnm_credit_line_chg_60d').cast('int').alias('acct_cnm_credit_line_chg_60d'))\
                         .withColumn('acct_cnm_credit_line_chg_90d',col('acct_cnm_credit_line_chg_90d').cast('int').alias('acct_cnm_credit_line_chg_90d'))\
                         .withColumn('acct_cnm_rush_plastic_1d',col('acct_cnm_rush_plastic_1d').cast('int').alias('acct_cnm_rush_plastic_1d'))\
                         .withColumn('acct_cnm_rush_plastic_7d',col('acct_cnm_rush_plastic_7d').cast('int').alias('acct_cnm_rush_plastic_7d'))\
                         .withColumn('acct_cnm_rush_plastic_30d',col('acct_cnm_rush_plastic_30d').cast('int').alias('acct_cnm_rush_plastic_30d'))\
                         .withColumn('acct_cnm_rush_plastic_60d',col('acct_cnm_rush_plastic_60d').cast('int').alias('acct_cnm_rush_plastic_60d'))\
                         .withColumn('acct_cnm_rush_plastic_90d',col('acct_cnm_rush_plastic_90d').cast('int').alias('acct_cnm_rush_plastic_90d'))\
                         .withColumn('acct_cnm_new_plastic_auth_1d',col('acct_cnm_new_plastic_auth_1d').cast('int').alias('acct_cnm_new_plastic_auth_1d'))\
                         .withColumn('acct_cnm_new_plastic_auth_7d',col('acct_cnm_new_plastic_auth_7d').cast('int').alias('acct_cnm_new_plastic_auth_7d'))\
                         .withColumn('acct_cnm_new_plastic_auth_30d',col('acct_cnm_new_plastic_auth_30d').cast('int').alias('acct_cnm_new_plastic_auth_30d'))\
                         .withColumn('acct_cnm_new_plastic_auth_60d',col('acct_cnm_new_plastic_auth_60d').cast('int').alias('acct_cnm_new_plastic_auth_60d'))\
                         .withColumn('acct_cnm_new_plastic_auth_90d',col('acct_cnm_new_plastic_auth_90d').cast('int').alias('acct_cnm_new_plastic_auth_90d'))\
                         .withColumn('acct_cnm_pin_chg_1d',col('acct_cnm_pin_chg_1d').cast('int').alias('acct_cnm_pin_chg_1d')) \
                         .withColumn('acct_cnm_pin_chg_7d',col('acct_cnm_pin_chg_7d').cast('int').alias('acct_cnm_pin_chg_7d')) \
                         .withColumn('acct_cnm_pin_chg_30d',col('acct_cnm_pin_chg_30d').cast('int').alias('acct_cnm_pin_chg_30d')) \
                         .withColumn('acct_cnm_pin_chg_60d',col('acct_cnm_pin_chg_60d').cast('int').alias('acct_cnm_pin_chg_60d')) \
                         .withColumn('acct_cnm_pin_chg_90d',col('acct_cnm_pin_chg_90d').cast('int').alias('acct_cnm_pin_chg_90d')) \
                         .withColumn('acct_cnm_mother_maiden_name_chg_1d',col('acct_cnm_mother_maiden_name_chg_1d').cast('int').alias('acct_cnm_mother_maiden_name_chg_1d')) \
                         .withColumn('acct_cnm_mother_maiden_name_chg_7d',col('acct_cnm_mother_maiden_name_chg_7d').cast('int').alias('acct_cnm_mother_maiden_name_chg_7d')) \
                         .withColumn('acct_cnm_mother_maiden_name_chg_30d',col('acct_cnm_mother_maiden_name_chg_30d').cast('int').alias('acct_cnm_mother_maiden_name_chg_30d')) \
                         .withColumn('acct_cnm_mother_maiden_name_chg_60d',col('acct_cnm_mother_maiden_name_chg_60d').cast('int').alias('acct_cnm_mother_maiden_name_chg_60d')) \
                         .withColumn('acct_cnm_mother_maiden_name_chg_90d',col('acct_cnm_mother_maiden_name_chg_90d').cast('int').alias('acct_cnm_mother_maiden_name_chg_90d'))\
                         .withColumn('acct_cnm_dob_chg_1d',col('acct_cnm_dob_chg_1d').cast('int').alias('acct_cnm_dob_chg_1d')) \
                         .withColumn('acct_cnm_dob_chg_7d',col('acct_cnm_dob_chg_7d').cast('int').alias('acct_cnm_dob_chg_7d')) \
                         .withColumn('acct_cnm_dob_chg_30d',col('acct_cnm_dob_chg_30d').cast('int').alias('acct_cnm_dob_chg_30d')) \
                         .withColumn('acct_cnm_dob_chg_60d',col('acct_cnm_dob_chg_60d').cast('int').alias('acct_cnm_dob_chg_60d')) \
                         .withColumn('acct_cnm_dob_chg_90d',col('acct_cnm_dob_chg_90d').cast('int').alias('acct_cnm_dob_chg_90d'))\
                         .withColumn('acct_cnm_name_chg_1d',col('acct_cnm_name_chg_1d').cast('int').alias('acct_cnm_name_chg_1d'))\
                         .withColumn('acct_cnm_name_chg_7d',col('acct_cnm_name_chg_7d').cast('int').alias('acct_cnm_name_chg_7d'))\
                         .withColumn('acct_cnm_name_chg_30d',col('acct_cnm_name_chg_30d').cast('int').alias('acct_cnm_name_chg_30d'))\
                         .withColumn('acct_cnm_name_chg_60d',col('acct_cnm_name_chg_60d').cast('int').alias('acct_cnm_name_chg_60d'))\
                         .withColumn('acct_cnm_name_chg_90d',col('acct_cnm_name_chg_90d').cast('int').alias('acct_cnm_name_chg_90d')) \
                         .withColumn('acct_cnm_ssn_chg_1d',col('acct_cnm_ssn_chg_1d').cast('int').alias('acct_cnm_ssn_chg_1d')) \
                         .withColumn('acct_cnm_ssn_chg_7d',col('acct_cnm_ssn_chg_7d').cast('int').alias('acct_cnm_ssn_chg_7d')) \
                         .withColumn('acct_cnm_ssn_chg_30d',col('acct_cnm_ssn_chg_30d').cast('int').alias('acct_cnm_ssn_chg_30d'))\
                         .withColumn('acct_cnm_ssn_chg_60d',col('acct_cnm_ssn_chg_60d').cast('int').alias('acct_cnm_ssn_chg_60d'))\
                         .withColumn('acct_cnm_ssn_chg_90d',col('acct_cnm_ssn_chg_90d').cast('int').alias('acct_cnm_ssn_chg_90d'))\
                         .withColumn('acct_cnm_total_1d',col('acct_cnm_total_1d').cast('int').alias('acct_cnm_total_1d'))\
                         .withColumn('acct_cnm_total_7d',col('acct_cnm_total_7d').cast('int').alias('acct_cnm_total_7d'))\
                         .withColumn('acct_cnm_total_30d',col('acct_cnm_total_30d').cast('int').alias('acct_cnm_total_30d'))\
                         .withColumn('acct_cnm_total_60d',col('acct_cnm_total_60d').cast('int').alias('acct_cnm_total_60d'))\
                         .withColumn('acct_cnm_total_90d',col('acct_cnm_total_90d').cast('int').alias('acct_cnm_total_90d'))\
                         .withColumn('acct_auth_amazon_prime_1d',col('acct_auth_amazon_prime_1d').cast('int'))\
                         .withColumn('acct_auth_amazon_prime_7d',col('acct_auth_amazon_prime_7d').cast('int'))\
                         .withColumn('acct_auth_amazon_prime_30d',col('acct_auth_amazon_prime_30d').cast('int'))\
                         .withColumn('acct_auth_amazon_prime_60d',col('acct_auth_amazon_prime_60d').cast('int'))\
                         .withColumn('acct_auth_amazon_prime_90d',col('acct_auth_amazon_prime_90d').cast('int'))\
                         .withColumn('acct_auth_amt_amazon_prime_1d',round(col('acct_auth_amt_amazon_prime_1d'),6).cast('double'))\
                         .withColumn('acct_auth_amt_amazon_prime_7d',round(col('acct_auth_amt_amazon_prime_7d'),6).cast('double'))\
                         .withColumn('acct_auth_amt_amazon_prime_30d',round(col('acct_auth_amt_amazon_prime_30d'),6).cast('double'))\
                         .withColumn('acct_auth_amt_amazon_prime_60d',round(col('acct_auth_amt_amazon_prime_60d'),6).cast('double'))\
                         .withColumn('acct_auth_amt_amazon_prime_90d',round(col('acct_auth_amt_amazon_prime_90d'),6).cast('double'))\
                         .withColumn('acct_unique_pos_1d',col('acct_unique_pos_1d').cast('int'))\
                         .withColumn('acct_unique_pos_7d',col('acct_unique_pos_7d').cast('int'))\
                         .withColumn('acct_unique_pos_30d',col('acct_unique_pos_30d').cast('int'))\
                         .withColumn('acct_unique_pos_60d',col('acct_unique_pos_60d').cast('int'))\
                         .withColumn('acct_unique_pos_90d',col('acct_unique_pos_90d').cast('int'))\
                         .withColumn('acct_unique_mcc_1d',col('acct_unique_mcc_1d').cast('int'))\
                         .withColumn('acct_unique_mcc_7d',col('acct_unique_mcc_7d').cast('int'))\
                         .withColumn('acct_unique_mcc_30d',col('acct_unique_mcc_30d').cast('int'))\
                         .withColumn('acct_unique_mcc_60d',col('acct_unique_mcc_60d').cast('int'))\
                         .withColumn('acct_unique_mcc_90d',col('acct_unique_mcc_90d').cast('int'))\
                         .withColumn('acct_sale_amt_per_tran_1d',round(col('acct_sale_amt_per_tran_1d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_per_tran_7d',round(col('acct_sale_amt_per_tran_7d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_per_tran_30d',round(col('acct_sale_amt_per_tran_30d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_per_tran_60d',round(col('acct_sale_amt_per_tran_60d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_per_tran_90d',round(col('acct_sale_amt_per_tran_90d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_std_1d',round(col('acct_sale_amt_std_1d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_std_7d',round(col('acct_sale_amt_std_7d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_std_30d',round(col('acct_sale_amt_std_30d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_std_60d',round(col('acct_sale_amt_std_60d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_std_90d',round(col('acct_sale_amt_std_90d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_change_1d',round(col('acct_sale_amt_change_1d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_change_7d',round(col('acct_sale_amt_change_7d'),2).cast('double'))\
                         .withColumn('acct_sale_amt_change_30d',round(col('acct_sale_amt_change_30d'),2).cast('double'))\
                         .withColumn('acct_sale_acceleration_7d',round(col('acct_sale_acceleration_7d'),2).cast('double'))\
                         .withColumn('acct_sale_acceleration_30d',round(col('acct_sale_acceleration_30d'),2).cast('double'))\
                         .withColumn('acct_pos_shift_7d',col('acct_pos_shift_7d').cast('string'))\
                         .withColumn('acct_pos_shift_30d',col('acct_pos_shift_30d').cast('string'))\
                         .withColumn('acct_pos_shift_60d',col('acct_pos_shift_60d').cast('string'))\
                         .withColumn('acct_pos_shift_90d',col('acct_pos_shift_90d').cast('string'))\
                         .withColumn('acct_bin',col('acct_bin').cast('string'))\
                         .withColumn('acct_last_card_issue_date',col('acct_last_card_issue_date').cast('date'))\
                         .withColumn('aci_0_2',col('aci_0_2').cast('int'))\
                         .withColumn('edl_load_ts',lit(current_timestamp()))\
                         .withColumn('as_of_date',lit(runDay))
                         
  return tc360AcctAttrDf
 except Exception as e:
  sys.exit("ERROR: while forming the final tc360 attribute data frame - " + str(e))

#######################################################################################################################
# Function for loading the C360 attributes hive table                                                                 #
#######################################################################################################################
def loadC360AttributeHive(tc360AcctAttrDf,tc360_account_hive,extPathAcct,writeMode,spark):
 try:
  print("######################## Starting loading c360 attribute hive table ######################################")              
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  if tc360AcctAttrDf.count() >= 1: tc360AcctAttrDf.write.mode(writeMode).insertInto(tc360_account_hive, overwrite = True)
  else: tc360AcctAttrDf.repartition(1).write.mode(writeMode).insertInto(tc360_account_hive, overwrite = True)
  spark.sql("msck repair table " + tc360_account_hive)
  print("######################## Tc360 attribute hive table load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while loading the data into hive table - " + str(e))

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 falconAttrDf = readFalconAttributes(inputParams['extFalconAttr'],runDay,spark)
 wrksUIRawSchema = createSchema(inputParams['wrksRawUIColumns'])
 nonMonFactDf = readNonMonFact(inputParams['extNMFactPath'],inputParams['edl_amf_nonmon'],runDay,spark)
 bin6FalconAttr6Df = readFalconBin6Attributes(inputParams['extFalconBin6Path'],runDay,spark)
 ts979Df,ts323Df,ts257Df = readTransmitSegments(inputParams['ts797ExtPath'],inputParams['ts323ExtPath'],inputParams['ts257ExtPath'],runDay,spark)
 wrksUIRawDf = readWrksUIRaw(inputParams['wrksUIRawExtPath'],wrksUIRawSchema,inputParams['edl_wrks_call_raw'],inputParams['debug'],spark)
 fccDf = readFCC(inputParams['tc360fcc'],runDay,spark)
 lexidDf,stmtDf,appFactDf,accXref = readPrecursorData(inputParams['pcAcctLexid'],inputParams['pcStmtFact'],inputParams['pcAppFact'],runDay,spark)
 nmAttrDf = computeNonMonAttr(nonMonFactDf,accXref,runDay)
 custAcctAttrDf = computeCAAttributes(inputParams['extPathCustomer'],lexidDf,runDay,spark)
 fccAttrDf,fccBin6AttrDf = calculateFccAttributes(lexidDf,fccDf,runDay)
 eAuth979AggDf,eAuth323AggDf,eAuth257AggDf = createAggAuthEDf(accXref,ts979Df,ts323Df,ts257Df,wrksUIRawDf)
 eAuthAttrdf = computeAuthEAttr(eAuth979AggDf,eAuth323AggDf,eAuth257AggDf,runDay)
 tc360CombinedDf = createCombinedDf(lexidDf,falconAttrDf,stmtDf,appFactDf,fccAttrDf,fccBin6AttrDf,bin6FalconAttr6Df,eAuthAttrdf,custAcctAttrDf,nmAttrDf,inputParams['falconInjectionDate'],runDay)
 tc360AcctAttrDf = createLoadReadyDf(tc360CombinedDf,inputParams['colsAcctTable'],runDay,spark)
 loadC360AttributeHive(tc360AcctAttrDf,inputParams['tc360_account_hive'],inputParams['extPathAcct'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()