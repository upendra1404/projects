############################################################################################
# Script Name:amf_tc360_fcc_attributes.py                                                  #
# Purpose: To calculate new tc360 fraud claim case attributes and load those into EDL      #
# Created by: Upendra Kumar                                                                #
# Create Date: 10/28/2020                                                                  #
# Modify date: 12/28/2020                                                                  #
############################################################################################

from pyspark.sql import SQLContext
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import HiveContext
from ConfigParser import ConfigParser
from time import strftime
from dateutil.relativedelta import relativedelta
from pyspark.sql.functions import mean as _mean, stddev_pop as _stddev, col
from pyspark.sql.window import Window
import datetime
import pyspark
import os
import sys
sys.dont_write_bytecode = True
import getopt
import datetime
import commands

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
                if ('edl_amf_acc_xref' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain edl_amf_acc_xref")
                else:
                    edl_amf_acc_xref = config.get('HIVE', 'edl_amf_acc_xref')
                if ('tc360_fcc_hive' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain tc360_fcc_hive")
                else:
                    tc360_fcc_hive = config.get('HIVE','tc360_fcc_hive')
                if ('pcAcctLexid' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcAcctLexid")
                else:
                    pcAcctLexid = config.get('HIVE', 'pcAcctLexid')
                if ('ftAcct' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain ftAcct")
                else:
                    ftAcct = config.get('HDFSPATH','ftAcct')
                if ('ftQueue' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain ftQueue")
                else:
                    ftQueue = config.get('HDFSPATH','ftQueue')
                if ('ftSummary' not in config.options('HDFSPATH')):
                  sys.exit("ERROR: Section SCHEMA doesn't contain ftSummary")
                else:
                    ftSummary = config.get('HDFSPATH','ftSummary')
                if ('inputFileDemiliter' not in config.options('SCHEMA')):
                    sys.exit("ERROR: Section HIVE doesn't contain inputFileDemiliter")
                else:
                    inputFileDemiliter = config.get('SCHEMA', 'inputFileDemiliter')
        return {

                            'readMode': readMode
                           ,'writeMode': writeMode
                           ,'inputFileDemiliter': inputFileDemiliter
                           ,'tc360_fcc_hive': tc360_fcc_hive
                           ,'ftAcct': ftAcct
                           ,'ftQueue': ftQueue
                           ,'ftSummary': ftSummary
                           ,'pcAcctLexid': pcAcctLexid
                }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('tc360_fraud_claim_case_app').getOrCreate()
 spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
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
########################################################################################################################

def readPrecursorTables(pcAcctLexid,spark):
 try:
  lexidDf = spark.read.parquet(pcAcctLexid).drop(*['edl_load_ts'])\
                      .withColumnRenamed('lexid1','cust_lexid')\
                      .withColumnRenamed('customer_acct_nbr','current_account_nbr_pty')\
                      .drop(*['sys','open_date','last_plastic_date','nbr_of_cards_current','external_status','external_status_reason_code','logical_application_key'])
  lexidDf.cache()
  return lexidDf
 except Exception as e:
  sys.exit("ERROR: while reading pre-cursor tables - " + str(e))

## Reading ft account table
def readFTAccount(ftAcct,spark):
 windowSpec = Window.partitionBy('pi_id','case_id').orderBy(desc('record_modification_ts'))
 ftAccountDf = spark.read.parquet(ftAcct)\
                    .filter("trim(possible_fraud_cd) <> 'N'")\
                    .select("*")\
                    .withColumn('pi_id',lit(trim(col('pi_id'))))\
                    .withColumn('case_id',lit(trim(col('case_id'))))\
                    .withColumn('rk',rank().over(windowSpec))\
                    .filter("rk=1")\
                    .drop(col('rk'))
 ftAccountDf.cache()
 return ftAccountDf

#########################################################################################################################
## Reading ft case queue table table 
def readFTCaseQueue(ftQueue,spark):
 ftCaseQueueDf = spark.read.parquet(ftQueue).filter("trim(group_id) not in ('*4','15')").drop(*['pi_id']).select("*")
 ftCaseQueueDf.cache()
 return ftCaseQueueDf
## Reading case summary table
def readFTCaseSummary(ftSummary,spark):
 ftCaseSummaryDf = spark.read.parquet(ftSummary).select("*")
 ftCaseSummaryDf.cache()
 return ftCaseSummaryDf

##########################################################################################################################
## Join lexid sysfact and account xref to fetch customer_acct_nbr
## fetch not null account number from xref table
def calculateFccAttributes(ftAccountDf,ftCaseQueueDf,ftCaseSummaryDf,lexidDf,runDay,spark):
 ## Joining all the tables on respective columns
 finalDf = ftAccountDf.join(ftCaseQueueDf,['case_id'],'inner')
 finalDf = finalDf.join(ftCaseSummaryDf,['case_id'],'inner')
 finalDf = finalDf.join(lexidDf,on = lexidDf['current_account_nbr_pty'] == finalDf['pi_id'], how = 'inner')
 ## Selecting the appropriate columns for load ready data frame
 fccAttrDf = finalDf.select(col('cust_lexid').cast('bigint').alias('cust_lexid')
                            ,col('orig_account_nbr').cast('string').alias('orig_account_nbr')
                            ,col('current_account_nbr').cast('string').alias('current_account_nbr')
                            ,col('current_account_nbr_pty').cast('string').alias('current_account_nbr_pty')
                            ,col('case_id').cast('string').alias('case_id')
                            ,col('loss_type_cd').cast('char(2)').alias('loss_type_cd')
                            ,col('possible_fraud_cd').cast('char(1)').alias('possible_fraud_cd')
                            ,col('group_id').cast('string').alias('group_id')
                            ,to_date(col('reported_dt')).cast('date').alias('reported_date')
                            ,to_date(col('open_ts')).cast('date').alias('open_date')
                            ,to_date(col('close_ts')).cast('date').alias('close_date')
                            ,to_date(col('reopen_ts')).cast('date').alias('reopen_date')
                            ,round(col('gross_fraud_am'),2).cast('double').alias('gross_fraud_am')
                            ,round(col('net_fraud_am'),2).cast('double').alias('net_fraud_am')).withColumn('edl_load_ts',current_date()).dropDuplicates()
 fccAttrDf.cache()
 return fccAttrDf
 
#######################################################################################################################
# Function for loading the new tc360 fraud claim case attributes hive table                                           #
#######################################################################################################################
def loadNewTc360CustAttributes(fccAttrDf,tc360_fcc_hive,writeMode,spark):
 print("######################## Starting loading tc360 fraud claim case attributes hive table ######################################")              
 spark.sql("set hive.exec.dynamic.partition=true")
 spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
 fccAttrDf.write.mode(writeMode).insertInto(tc360_fcc_hive,overwrite=True)
 print("Total record loaded into hive table is: "+ str(fccAttrDf.count()))
 print("######################## Finished loading tc360 fraud claim case attributes hive table ###########################################")

########################################################################################################################
## defining main function for the tc360 fcc attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 registerJavaFunction(sqlContext)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 lexidDf = readPrecursorTables(inputParams['pcAcctLexid'],spark)
 ftAccountDf = readFTAccount(inputParams['ftAcct'],spark)
 ftCaseQueueDf = readFTCaseQueue(inputParams['ftQueue'],spark)
 ftCaseSummaryDf = readFTCaseSummary(inputParams['ftSummary'],spark)
 fccAttrDf = calculateFccAttributes(ftAccountDf,ftCaseQueueDf,ftCaseSummaryDf,lexidDf,runDay,spark)
 loadNewTc360CustAttributes(fccAttrDf,inputParams['tc360_fcc_hive'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()