########################################################################################
# Script Name:amf_new_tc360_cust_attributes.py                                         #
# Purpose: To calculate new tc360 customer attributes and load those into EDL          #
# Created by: Upendra Kumar                                                            #
# Create Date: 10/28/2020                                                              #
# Modify date: 12/28/2020                                                              #
########################################################################################

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
                if ('tc360fcc' not in config.options('HDFSPATH')):
                    sys.exit("ERROR: Section HDFSPATH doesn't contain tc360fcc")
                else:
                    tc360fcc = config.get('HDFSPATH', 'tc360fcc')
                if ('tc360_customer_hive' not in config.options('HIVE')):
                  sys.exit("ERROR: Section HIVE doesn't contain tc360_customer_hive")
                else:
                    tc360_customer_hive = config.get('HIVE','tc360_customer_hive')
                if ('pcStmtFact' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcStmtFact")
                else:
                    pcStmtFact = config.get('HIVE', 'pcStmtFact')
                if ('pcAcctLexid' not in config.options('HIVE')):
                    sys.exit("ERROR: Section HIVE doesn't contain pcAcctLexid")
                else:
                    pcAcctLexid = config.get('HIVE', 'pcAcctLexid')
                return {

                            'readMode': readMode
                            ,'inputFileDemiliter': inputFileDemiliter
                            ,'writeMode': writeMode
                            ,'tc360_customer_hive': tc360_customer_hive
                            ,'tc360fcc': tc360fcc
                            ,'pcStmtFact': pcStmtFact
                            ,'pcAcctLexid': pcAcctLexid
                            ,'runDay': runDay
                        }
####################################################################################################################
## Create spark session
def createSparkSession():
 spark = SparkSession.builder.\
                enableHiveSupport().appName('tc360_customer_app').getOrCreate()
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

def readPrecursorTables(pcAcctLexid,pcStmtFact,spark):
 try:
  lexidDf = spark.read.parquet(pcAcctLexid).drop(*['edl_load_ts']).where(trim(col('external_status')) =='Z').select(trim(col('external_status')).alias('external_status'),col('lexid1').alias('cust_lexid'),trim(col('customer_acct_nbr')).alias('customer_acct_nbr'),trim(col('orig_account_nbr')).alias('orig_account_nbr'),trim(col('external_status_reason_code')).alias('esrCode')).drop(*['sys','open_date','last_plastic_date','nbr_of_cards_current','logical_application_key','acct_bin','card_activation_flag']).dropDuplicates()
  stmtDf = spark.read.parquet(pcStmtFact).where((trim(col('external_status')) =='Z') & (col('writeoff_amt') > 0)).drop(*['edl_load_ts']).drop(*['external_status','credit_limit_amt','curr_mnth_balance_amt']).dropDuplicates()
  
  lexidDf.cache()
  stmtDf.cache()
  return lexidDf,stmtDf
 except Exception as e:
  sys.exit("ERROR: while reading pre-cursor tables - " + str(e))

def readFCC(tc360fcc,runDay,spark):
 try:
  fccDf = spark.read.parquet(tc360fcc).where(col('reported_date') < lit(runDay)).select(trim(col('current_account_nbr_pty')).alias('current_account_nbr_pty'),col('case_id'),col('reported_date'),trim(col('loss_type_cd')).alias('loss_type_cd')).dropDuplicates()
  
  fccDf.cache()
  return fccDf
 except Exception as e:
  sys.exit("ERROR: while reading fcc attributes " + str(e))
  
def createAggDF(stmtDf,lexidDf,fccDf,spark):
 ################################################################################################################
 # Create aggregate dataframe for tc360 customer attribute aggregation                                          #
 ################################################################################################################
 try:
  lexStmtDf = lexidDf.join(stmtDf,['orig_account_nbr'], 'left')
  lexStmtFccDf = lexStmtDf.join(fccDf,lexStmtDf['customer_acct_nbr'] == fccDf['current_account_nbr_pty'], how = 'left')
  aggregateDF = lexStmtFccDf.withColumn('woff_type',expr("case when esrCode = '88' then 'fraud'\
                                                 when esrCode in ('67','69','89','33','34') then 'bko'\
                                                 when esrCode in ('54','55','57','58','59','60','61','63','64','65','66','16','86','19','70','90','97','98','17','35','40','80','30','31','32') then 'age'\
                                                 else 'other' end")).drop(*['external_status','esrCode','customer_acct_nbr'])                     
  aggregateDF.cache()
  return aggregateDF
 except Exception as e:
  sys.exit("ERROR: while creating customer aggregate data frame - " + str(e))
 
####################################################################################################################################
## Calculating TC360 customer attributes
def computeCustomerAttr(aggregateDF,runDay,spark):
 #################################################################################################################################################
 # Performing the aggregations on data frames and calculating the tc360 customer level attributes                                                #
 # Applying the explicit type casting before to create load ready dataframe                                                                      #
 #################################################################################################################################################
 try:
  custAttrDf = aggregateDF.groupBy('cust_lexid')\
                                 .agg(_sum(expr("case when woff_type='age' then writeoff_amt else 0 end")).alias('cust_aged_loss_amt_tot')\
                                      ,_sum(expr("case when woff_type='bko' then writeoff_amt else 0 end")).alias('cust_bk_loss_amt_tot')\
                                      ,_sum(expr("case when woff_type='fraud' then writeoff_amt else 0 end")).alias('cust_fraud_loss_amt_tot')\
                                      ,_sum(expr("case when woff_type='other' then writeoff_amt else 0 end")).alias('cust_other_loss_amt_tot')\
                                      ,_sum(col('writeoff_amt')).alias('cust_total_loss_amt_tot')\
                                      ,_sum(expr("case when woff_type='age' then 1 else 0 end")).alias('cust_aged_loss_cnt_tot')\
                                      ,_sum(expr("case when woff_type='bko' then 1 else 0 end")).alias('cust_bk_loss_cnt_tot')\
                                      ,_sum(expr("case when woff_type='fraud' then 1 else 0 end")).alias('cust_fraud_loss_cnt_tot')\
                                      ,_sum(expr("case when woff_type='other' then 1 else 0 end")).alias('cust_other_loss_cnt_tot')\
                                      ,count(col('woff_type')).alias('cust_total_loss_cnt_tot')\
                                      ,countDistinct(expr("case when loss_type_cd in ('00','01') then case_id end")).alias('cust_fraud_claim_ls_tot')\
                                      ,countDistinct(expr("case when loss_type_cd in ('02','04','05') then case_id end")).alias('cust_fraud_claim_other_tot')\
                                      ,countDistinct(expr("case when loss_type_cd = '06' then case_id end")).alias('cust_fraud_claim_cnp_tot')\
                                      ,countDistinct(expr("case when loss_type_cd = '03' then case_id end")).alias('cust_fraud_claim_tnf_tot')\
                                      ,countDistinct(expr("case when loss_type_cd <> '03' then case_id end")).alias('cust_fraud_claim_non_tnf_tot')).dropDuplicates()
  ## Joining intermediate dataframe to fetch attribute - 'cust_multi_account_claim_same_day'
  intermediateDf = aggregateDF.groupBy('cust_lexid','reported_date')\
                             .agg(countDistinct('current_account_nbr_pty').alias('cnt'))\
                             .groupBy('cust_lexid')\
                             .agg(expr("case when max(cnt) > 1 then 1 else 0 end").alias('cust_multi_account_claim_same_day'))
                             
  custAttrDf = custAttrDf.join(intermediateDf,['cust_lexid'],'left_outer')
  custAttrDf.cache()
  return custAttrDf
 except Exception as e:
  sys.exit("ERROR: while computing customer attributes - " + str(e))
 
def createLRDf(custAttrDf,runDay):
 try:
  custAttrDf = custAttrDf.select(col('cust_lexid').cast('bigint').alias('cust_lexid')
                               ,round(col('cust_aged_loss_amt_tot'),2).cast('double').alias('cust_aged_loss_amt_tot')
                               ,round(col('cust_bk_loss_amt_tot'),2).cast('double').alias('cust_bk_loss_amt_tot')
                               ,round(col('cust_fraud_loss_amt_tot'),2).cast('double').alias('cust_fraud_loss_amt_tot')
                               ,round(col('cust_other_loss_amt_tot'),2).cast('double').alias('cust_other_loss_amt_tot')
                               ,round(col('cust_total_loss_amt_tot'),2).cast('double').alias('cust_total_loss_amt_tot')
                               ,col('cust_aged_loss_cnt_tot').cast('int').alias('cust_aged_loss_cnt_tot')
                               ,col('cust_bk_loss_cnt_tot').cast('int').alias('cust_bk_loss_cnt_tot')
                               ,col('cust_fraud_loss_cnt_tot').cast('int').alias('cust_fraud_loss_cnt_tot')
                               ,col('cust_other_loss_cnt_tot').cast('int').alias('cust_other_loss_cnt_tot')
                               ,col('cust_total_loss_cnt_tot').cast('int').alias('cust_total_loss_cnt_tot')
                               ,col('cust_fraud_claim_ls_tot').cast('int').alias('cust_fraud_claim_ls_tot')
                               ,col('cust_fraud_claim_other_tot').cast('int').alias('cust_fraud_claim_other_tot')
                               ,col('cust_fraud_claim_cnp_tot').cast('int').alias('cust_fraud_claim_cnp_tot')
                               ,col('cust_fraud_claim_tnf_tot').cast('int').alias('cust_fraud_claim_tnf_tot')
                               ,col('cust_fraud_claim_non_tnf_tot').cast('int').alias('cust_fraud_claim_non_tnf_tot')
                               ,col('cust_multi_account_claim_same_day').cast('int').alias('cust_multi_account_claim_same_day')).dropDuplicates().withColumn('edl_load_ts',lit(current_timestamp())).withColumn('as_of_date',lit(runDay))
  custAttrDf.cache()
  return custAttrDf
 except Exception as e:
  sys.exit("ERROR: while forming final customer attributes data frame - " + str(e))
  
#######################################################################################################################
# Function for loading the new tc360 customer attributes hive table                                                   #
#######################################################################################################################
def loadCustmerAttr(custAttrDf,tc360_customer_hive,writeMode,spark):
 try:
  print("######################## Starting loading tc360 customer attribute hive table ######################################")              
  spark.sql("set hive.exec.dynamic.partition=true")
  spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
  if custAttrDf.count() >= 1: custAttrDf.write.mode(writeMode).insertInto(tc360_customer_hive,overwrite=True)
  else: custAttrDf.repartition(1).write.mode(writeMode).insertInto(tc360_customer_hive,overwrite=True)
  print("Total record loaded into hive table is: "+ str(custAttrDf.count()))
  print("######################## tc360 customer attribute hive table load complete ###########################################")
 except Exception as e:
  sys.exit("ERROR: while loading customer attributes - "+ str(e))
 
########################################################################################################################
## defining main function for the tc360 customer attribute script
def main():
 spark = createSparkSession()
 spark.sparkContext.setLogLevel("ERROR")
 sqlContext = createSQLContext(spark)
 hive = readHiveContext(spark)
 registerJavaFunction(sqlContext)
 configFileName,runDay = validateArg()
 inputParams = validateConfigFile(configFileName)
 fccDf = readFCC(inputParams['tc360fcc'],runDay,spark)
 lexidDf,stmtDf = readPrecursorTables(inputParams['pcAcctLexid'],inputParams['pcStmtFact'],spark)
 aggregateDF = createAggDF(stmtDf,lexidDf,fccDf,spark)
 custAttrDf = computeCustomerAttr(aggregateDF,runDay,spark)
 custAttrDf = createLRDf(custAttrDf,runDay)
 loadCustmerAttr(custAttrDf,inputParams['tc360_customer_hive'],inputParams['writeMode'],spark)
 
if __name__ == "__main__":
    main()