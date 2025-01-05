###################################################################################
# Script Name:amtfServiceTc360RiskMerchant.py                                     #
# Purpose: To compute risk merchant attributes and load those into EDL hive table #
# Created by: Upendra Kumar                                                       #
# Create Date: 08/7/2021                                                          #
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
import amtfServiceTc360RiskModule as riskModule

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
                enableHiveSupport().appName('tc360_risk_merchant_id').getOrCreate()
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

#######################################################################################################################
## Create Data Masking UDFs from Masking Jar
def registerJavaFunction(sqlContext):
 sqlContext.registerJavaFunction("encrypt", "com.syf.edl.services.creditservice.crypto.udf.EncrypUDF")
 sqlContext.registerJavaFunction("decrypt", "com.syf.edl.services.creditservice.crypto.udf.DecryptUDF")

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

def createLoadReadyDf(riskAttrDf,runDay):
 try:
  ##Forming final tc360 account level attributes data frame
  riskMerchAttrDf = riskAttrDf\
                   .select(col('ffsl_frad_mrch_id').cast("string").alias('merchant_id')
                          ,col('auth_1d').cast("int").alias('auth_1d')
                          ,col('auth_7d').cast("int").alias('auth_7d')
                          ,col('auth_30d').cast("int").alias('auth_30d')
                          ,col('auth_60d').cast("int").alias('auth_60d')
                          ,col('auth_90d').cast("int").alias('auth_90d')
                          ,round(col('auth_amt_1d'),2).cast("double").alias('auth_amt_1d')
                          ,round(col('auth_amt_7d'),2).cast("double").alias('auth_amt_7d')
                          ,round(col('auth_amt_30d'),2).cast("double").alias('auth_amt_30d')
                          ,round(col('auth_amt_60d'),2).cast("double").alias('auth_amt_60d')
                          ,round(col('auth_amt_90d'),2).cast("double").alias('auth_amt_90d')
                          ,col('fraud_1d').cast('int').alias('fraud_1d')
                          ,col('fraud_7d').cast('int').alias('fraud_7d')
                          ,col('fraud_30d').cast('int').alias('fraud_30d')
                          ,col('fraud_60d').cast('int').alias('fraud_60d')
                          ,col('fraud_90d').cast('int').alias('fraud_90d')
                          ,round(col('fraud_amt_1d'),2).cast('double').alias('fraud_amt_1d')
                          ,round(col('fraud_amt_7d'),2).cast('double').alias('fraud_amt_7d')
                          ,round(col('fraud_amt_30d'),2).cast('double').alias('fraud_amt_30d')
                          ,round(col('fraud_amt_60d'),2).cast('double').alias('fraud_amt_60d')
                          ,round(col('fraud_amt_90d'),2).cast('double').alias('fraud_amt_90d')
                          ).dropDuplicates().withColumn('edl_load_ts',lit(current_timestamp())).withColumn('as_of_date',lit(runDay))
                            
  riskMerchAttrDf.cache()
  return riskMerchAttrDf
 except Exception as e:
  sys.exit("ERROR: while forming the final risk merchant dataframe - " + str(e))

#######################################################################################################################
# Function for loading the C360 attributes hive table                                                                 #
#######################################################################################################################
def loadC360AttributeHive(riskMerchAttrDf,tc360_risk_mrch_hive,writeMode,spark):
 try:
  print("######################## Starting loading tc360 risk merchant table ######################################")              
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  riskMerchAttrDf.write.mode(writeMode).insertInto(tc360_risk_mrch_hive,overwrite=True)
  print("######################## tc360 risk merchant hive table load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while loading the data into risk hive table - " + str(e))

########################################################################################################################
## defining main function for the c360 attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 registerJavaFunction(sqlContext)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 falconDf = riskModule.readFalcon(inputParams['falconRawPath'],inputParams['edl_amf_falcon_raw'],runDay,90,inputParams['debug'],spark)
 fccDf = riskModule.readFCC(inputParams['ftccPath'],runDay,90,spark)
 fctDf = riskModule.readFCT(inputParams['ftctPath'],spark)
 fccAggDf = riskModule.createFccAggDf(fccDf,fctDf)
 riskFalconAttrDf = riskModule.computeRiskFalconAttributes(falconDf,'ffsl_frad_mrch_id',runDay)
 riskFccAttrDf = riskModule.computeRiskFCCAttributes(fccAggDf,'ffsl_frad_mrch_id',runDay)
 riskAttrDf = riskModule.combinedDf(riskFalconAttrDf,riskFccAttrDf,'ffsl_frad_mrch_id')
 riskMerchAttrDf = createLoadReadyDf(riskAttrDf,runDay)
 loadC360AttributeHive(riskMerchAttrDf,inputParams['tc360_risk_mrch_hive'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()